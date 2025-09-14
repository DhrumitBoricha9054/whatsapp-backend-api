import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs/promises';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import { loadZip, extractZipMeta } from '../utils/zip.js';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, saveBuffer, publicPath, safeName } from '../utils/file.js';

const router = Router();

// --- Multer disk storage with file size limit ---
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'tmp/'), // temp folder
  filename: (req, file, cb) => cb(null, Date.now() + '-' + file.originalname)
});

const upload = multer({
  storage,
  limits: { fileSize: 500 * 1024 * 1024 } // 500MB max
});

// --- Utility: normalize filenames ---
function normalizeName(s) {
  if (!s) return '';
  return s.toString().toLowerCase().replace(/[^a-z0-9.\-\.]+/g, '');
}

// --- Helper: find or create chat ---
async function findOrCreateChatForUser(userId, nameGuess, participantsSet, connection) {
  const [chats] = await connection.execute('SELECT id, name FROM chats WHERE user_id = ?', [userId]);
  const chatParts = new Map();

  for (const c of chats) {
    const [rows] = await connection.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
    const set = new Set(rows.map(r => r.name));
    chatParts.set(c.id, set);
  }

  const target = [...participantsSet].sort().join('|');
  let foundId = null;
  for (const [id, set] of chatParts) {
    if ([...set].sort().join('|') === target) {
      foundId = id;
      break;
    }
  }

  if (foundId) return { chatId: foundId, created: false };

  const [res] = await connection.execute('INSERT INTO chats (user_id, name) VALUES (?, ?)', [userId, nameGuess || 'Chat']);
  const chatId = res.insertId;

  for (const p of participantsSet) {
    await connection.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
  }

  return { chatId, created: true };
}

// --- POST /upload ---
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;

  try {
    console.log(`Processing ZIP upload for user ${userId}, file: ${req.file.originalname}`);

    // Load ZIP from temp file (disk, not memory)
    const zip = await loadZip(req.file.path);
    const { txtFiles, filesMap } = await extractZipMeta(zip);
    console.log(`Found ${txtFiles.length} txt files, ${filesMap.size} media files`);

    if (!txtFiles.length) return res.status(400).json({ error: 'no .txt chat file found in zip' });

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: 0 };

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        console.log(`\n=== Processing chat file: ${txtPath} ===`);
        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;

        const { chatId, created } = await findOrCreateChatForUser(userId, parsed.nameGuess, participantsSet, conn);
        if (created) stats.addedChats++; else stats.updatedChats++;

        // Prepare uploads dir
        const root = process.env.UPLOAD_ROOT || 'uploads';
        const chatDir = path.join(root, String(userId), String(chatId));
        await ensureDir(chatDir);

        // Last message timestamp to avoid duplicates
        let lastMessageTime = null;
        if (!created) {
          const [lastMsg] = await conn.execute('SELECT MAX(timestamp) as last_time FROM messages WHERE chat_id = ?', [chatId]);
          lastMessageTime = lastMsg[0]?.last_time;
        }

        const newMessages = parsed.messages.filter(m => {
          if (!m.timestamp) return true;
          if (!lastMessageTime) return true;
          return new Date(m.timestamp) > new Date(lastMessageTime);
        });

        for (const m of newMessages) {
          let mediaPath = null;
          if (m.filename) {
            const rawName = m.filename;
            const targetNorm = normalizeName(rawName);
            let hit = null;
            let foundZipKey = null;

            const exactKey = rawName.toLowerCase();
            if (filesMap.has(exactKey)) {
              hit = filesMap.get(exactKey);
              foundZipKey = exactKey;
            }

            if (!hit) {
              for (const [zipFilename, fileData] of filesMap.entries()) {
                const zipNorm = normalizeName(zipFilename);
                if (
                  zipFilename.endsWith(exactKey) ||
                  zipNorm === targetNorm ||
                  zipNorm.includes(targetNorm) ||
                  targetNorm.includes(zipNorm)
                ) {
                  hit = fileData;
                  foundZipKey = zipFilename;
                  break;
                }
              }
            }

            if (hit) {
              try {
                const savedName = safeName(foundZipKey || exactKey);
                const outPath = path.join(chatDir, savedName);
                await saveBuffer(outPath, hit.data);
                mediaPath = publicPath(String(userId), String(chatId), savedName);
                stats.savedMedia++;
              } catch (mediaError) {
                console.error(`Failed to save media file ${m.filename}:`, mediaError);
              }
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
            if (e.code === 'ER_DUP_ENTRY') stats.skippedMessages++;
            else throw e;
          }
        }

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
  } finally {
    // Delete temp uploaded file
    try { await fs.unlink(req.file.path); } catch {}
  }
});

export default router;
