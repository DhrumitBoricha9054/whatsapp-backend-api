import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import { loadZip, extractZipMeta } from '../utils/zip.js';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, saveBuffer, publicPath, safeName } from '../utils/file.js';

const router = Router();
const upload = multer({ dest: 'tmp/' });

// Normalize filenames for robust matching: lowercase, remove non-alnum except dot and hyphen
function normalizeName(s) {
  if (!s) return '';
  return s.toString().toLowerCase().replace(/[^a-z0-9.\-\.]+/g, '');
}

/** Helper: find or create chat for user by name first, then by participants set */
async function findOrCreateChatForUser(userId, nameGuess, participantsSet, connection) {
  // Load all chats for user with their participants
  const [chats] = await connection.execute('SELECT id, name FROM chats WHERE user_id = ?', [userId]);

  let foundId = null;

  // 1) Try matching by chat name first (most reliable for groups)
  if (nameGuess) {
    const nameNorm = nameGuess.trim().toLowerCase();
    for (const c of chats) {
      if (c.name && c.name.trim().toLowerCase() === nameNorm) {
        foundId = c.id;
        console.log(`Matched chat by name: "${nameGuess}" -> chatId ${foundId}`);
        break;
      }
    }
  }

  // 2) Fall back to participant set matching
  if (!foundId) {
    const chatParts = new Map();
    for (const c of chats) {
      const [rows] = await connection.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
      const set = new Set(rows.map(r => r.name));
      chatParts.set(c.id, set);
    }

    const target = [...participantsSet].sort().join('|');
    for (const [id, set] of chatParts) {
      if ([...set].sort().join('|') === target) {
        foundId = id;
        console.log(`Matched chat by participants -> chatId ${foundId}`);
        break;
      }
    }
  }

  if (foundId) {
    // Update chat name if previously generic and now we have a real name
    if (nameGuess) {
      const existing = chats.find(c => c.id === foundId);
      if (!existing.name || existing.name === 'Chat') {
        await connection.execute('UPDATE chats SET name = ? WHERE id = ?', [nameGuess, foundId]);
        console.log(`Updated chat ${foundId} name to "${nameGuess}"`);
      }
    }

    // Sync participants: add any new ones from this import
    const [existingParts] = await connection.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [foundId]);
    const existingSet = new Set(existingParts.map(r => r.name));
    for (const p of participantsSet) {
      if (!existingSet.has(p)) {
        await connection.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [foundId, p]);
      }
    }

    return { chatId: foundId, created: false };
  }

  // Create new chat
  const [res] = await connection.execute('INSERT INTO chats (user_id, name) VALUES (?, ?)', [userId, nameGuess || 'Chat']);
  const chatId = res.insertId;

  // Insert participants
  for (const p of participantsSet) {
    await connection.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
  }

  return { chatId, created: true };
}

/** POST /api/upload  (auth)  form-data: zip=<file> */
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;

  try {
    console.log(`Processing ZIP upload for user ${userId}, file: ${req.file.originalname}`);

    const zip = await loadZip(req.file.path);
    const { txtFiles, filesMap } = await extractZipMeta(zip);

    console.log(`Found ${txtFiles.length} txt files, ${filesMap.size} media files`);

    if (!txtFiles.length) return res.status(400).json({ error: 'no .txt chat file found in zip' });

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: 0 };

    // Use a single connection/transaction for performance
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        console.log(`\n=== Processing chat file: ${txtPath} ===`);
        console.log(`File size: ${text.length} characters`);
        console.log(`First 500 characters:\n${text.substring(0, 500)}`);
        console.log(`Last 200 characters:\n${text.substring(Math.max(0, text.length - 200))}`);

        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;

        const { chatId, created } = await findOrCreateChatForUser(userId, parsed.nameGuess, participantsSet, conn);
        if (created) stats.addedChats++; else stats.updatedChats++;

        // Prepare uploads dir
        const root = process.env.UPLOAD_ROOT || 'uploads';
        const chatDir = path.join(root, String(userId), String(chatId));
        await ensureDir(chatDir);

        // Get the last message timestamp for this chat to avoid re-importing old messages
        let lastMessageTime = null;
        if (!created) {
          const [lastMsg] = await conn.execute(
            'SELECT MAX(timestamp) as last_time FROM messages WHERE chat_id = ?',
            [chatId]
          );
          lastMessageTime = lastMsg[0]?.last_time;
        }

        // Filter messages to only include new ones (after last message time)
        const newMessages = parsed.messages.filter(m => {
          if (!m.timestamp) return true; // Include messages without timestamp
          if (!lastMessageTime) return true; // First import, include all
          return new Date(m.timestamp) > new Date(lastMessageTime);
        });

        // Insert only new messages
        for (const m of newMessages) {
          let mediaPath = null;

          if (m.filename) {
            const rawName = m.filename;
            const targetNorm = normalizeName(rawName);
            let hit = null;
            let foundZipKey = null;

            // Try exact basename match (lowercased)
            const exactKey = rawName.toLowerCase();
            if (filesMap.has(exactKey)) {
              hit = filesMap.get(exactKey);
              foundZipKey = exactKey;
            }

            // Otherwise scan and match using normalized forms and suffix checks
            if (!hit) {
              for (const [zipFilename, fileData] of filesMap.entries()) {
                const zipNorm = normalizeName(zipFilename);
                // strong checks: exact normalized equality or normalized suffix/contains
                if (
                  zipFilename.endsWith(exactKey) ||
                  zipNorm === targetNorm ||
                  zipNorm.includes(targetNorm) ||
                  targetNorm.includes(zipNorm)
                ) {
                  hit = fileData;
                  foundZipKey = zipFilename;
                  console.log(`Found media file with match: ${rawName} -> ${zipFilename}`);
                  break;
                }
              }
            }

            if (hit) {
              try {
                const savedName = safeName(foundZipKey || exactKey); // Use the matched ZIP filename when possible
                const outPath = path.join(chatDir, savedName);
                await saveBuffer(outPath, hit.data);
                mediaPath = publicPath(String(userId), String(chatId), savedName);
                stats.savedMedia++;
                console.log(`Saved media: ${m.filename} -> ${mediaPath}`);
              } catch (mediaError) {
                console.error(`Failed to save media file ${m.filename}:`, mediaError);
                // Continue without media path
              }
            } else {
              console.log(`Media file not found in ZIP: ${m.filename}`);
              // referenced but missing; store content as-is (mediaPath stays null)
            }
          }

          try {
            await conn.execute(
              `INSERT INTO messages (chat_id, author, content, timestamp, type, media_path)
             VALUES (?, ?, ?, ?, ?, ?)`,
              [chatId, m.author || null, m.content || '', m.timestamp || null, m.type, mediaPath]
            );
            stats.addedMessages++;
          } catch (e) {
            if (e.code === 'ER_DUP_ENTRY') {
              stats.skippedMessages++;
            } else {
              throw e;
            }
          }
        }

        // Count skipped messages (old messages that were filtered out)
        stats.skippedMessages += (parsed.messages.length - newMessages.length);
      }

      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }

    console.log(`Upload completed for user ${userId}:`, stats);
    res.json(stats);

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message || 'Upload failed' });
  }
});

export default router;