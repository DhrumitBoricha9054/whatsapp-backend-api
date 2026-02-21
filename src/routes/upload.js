import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs/promises';
import crypto from 'crypto';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import { loadZip, extractZipMeta } from '../utils/zip.js';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, saveBuffer, publicPath, safeName } from '../utils/file.js';

const router = Router();
const upload = multer({ dest: 'tmp/' });

// ─── In-memory preview store (previewId -> preview data) ───
// Each preview expires after 15 minutes
const previews = new Map();
const PREVIEW_TTL_MS = 15 * 60 * 1000; // 15 minutes

/** Cleanup expired previews periodically */
setInterval(() => {
  const now = Date.now();
  for (const [id, preview] of previews) {
    if (now - preview.createdAt > PREVIEW_TTL_MS) {
      // Delete temp file
      fs.unlink(preview.tempPath).catch(() => { });
      previews.delete(id);
      console.log(`Cleaned up expired preview: ${id}`);
    }
  }
}, 60 * 1000); // check every minute

// ─── Helpers ───

// Normalize filenames for robust matching
function normalizeName(s) {
  if (!s) return '';
  return s.toString().toLowerCase().replace(/[^a-z0-9.\-\.]+/g, '');
}

// Generic/default chat names that indicate the name was not properly parsed
const GENERIC_NAMES = new Set(['chat', 'whatsapp chat', 'group', '_chat']);

function isGenericName(name) {
  if (!name) return true;
  return GENERIC_NAMES.has(name.trim().toLowerCase());
}

/** Calculate overlap ratio between two participant sets */
function participantOverlap(setA, setB) {
  if (setA.size === 0 && setB.size === 0) return 1;
  if (setA.size === 0 || setB.size === 0) return 0;
  let overlap = 0;
  for (const p of setA) {
    if (setB.has(p)) overlap++;
  }
  const smaller = Math.min(setA.size, setB.size);
  return overlap / smaller;
}

/**
 * Find best matching chats for the imported data.
 * Returns { suggestedChatId, existingChats[] with match scores }
 */
async function findMatchingSuggestions(userId, nameGuess, participantsSet) {
  const [chats] = await pool.execute('SELECT id, name, created_at FROM chats WHERE user_id = ?', [userId]);

  // Pre-load participants and message counts for all chats
  const results = [];
  for (const c of chats) {
    const [parts] = await pool.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
    const [countRow] = await pool.execute('SELECT COUNT(*) as cnt FROM messages WHERE chat_id = ?', [c.id]);
    const partSet = new Set(parts.map(r => r.name));

    // Calculate match score
    const overlap = participantOverlap(participantsSet, partSet);
    const nameMatch = (nameGuess && c.name && nameGuess.trim().toLowerCase() === c.name.trim().toLowerCase());

    let matchScore = 0;
    let matchReason = null;

    if (nameMatch) {
      matchScore = 100;
      matchReason = 'exact_name_match';
    } else if (overlap >= 0.5) {
      matchScore = Math.round(overlap * 80); // max 80 for participant overlap
      matchReason = `participant_overlap_${Math.round(overlap * 100)}%`;
    } else if (isGenericName(c.name) && overlap > 0) {
      matchScore = Math.round(overlap * 50); // max 50 for generic + overlap
      matchReason = `generic_name_with_overlap_${Math.round(overlap * 100)}%`;
    }

    results.push({
      id: c.id,
      name: c.name,
      participants: parts.map(r => r.name),
      messageCount: Number(countRow[0].cnt),
      created_at: c.created_at,
      matchScore,
      matchReason
    });
  }

  // Sort: matched chats first (by score desc), then the rest
  results.sort((a, b) => b.matchScore - a.matchScore);

  // Best suggestion (if score > 0)
  const suggestedChatId = results.length > 0 && results[0].matchScore > 0 ? results[0].id : null;

  return { suggestedChatId, existingChats: results };
}

// ─────────────────────────────────────────────────────────────
// STEP 1:  POST /api/upload/preview
//   Upload ZIP, parse it, return preview + suggestions
// ─────────────────────────────────────────────────────────────
router.post('/upload/preview', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;

  try {
    console.log(`[Preview] Processing ZIP for user ${userId}, file: ${req.file.originalname}`);

    const zip = await loadZip(req.file.path);
    const { txtFiles, filesMap } = await extractZipMeta(zip);

    if (!txtFiles.length) {
      await fs.unlink(req.file.path).catch(() => { });
      return res.status(400).json({ error: 'no .txt chat file found in zip' });
    }

    // Parse all chat files in the ZIP
    const chatPreviews = [];
    for (const { path: txtPath, text } of txtFiles) {
      const parsed = parseWhatsAppText(text, txtPath);

      // Get date range from messages
      const timestamps = parsed.messages
        .map(m => m.timestamp)
        .filter(Boolean)
        .sort();

      chatPreviews.push({
        fileName: txtPath,
        nameGuess: parsed.nameGuess,
        participants: Array.from(parsed.participants),
        messageCount: parsed.messages.length,
        mediaCount: parsed.messages.filter(m => m.type !== 'text').length,
        dateRange: {
          from: timestamps[0] || null,
          to: timestamps[timestamps.length - 1] || null
        }
      });
    }

    // Get matching suggestions for the first (main) chat
    const mainChat = chatPreviews[0];
    const participantsSet = new Set(mainChat.participants);
    const { suggestedChatId, existingChats } = await findMatchingSuggestions(
      userId, mainChat.nameGuess, participantsSet
    );

    // Generate preview ID and store temp data
    const previewId = crypto.randomUUID();
    previews.set(previewId, {
      userId,
      tempPath: req.file.path,
      originalName: req.file.originalname,
      createdAt: Date.now(),
      mediaFileCount: filesMap.size
    });

    console.log(`[Preview] Created preview ${previewId} for user ${userId}: ${chatPreviews.length} chats found`);

    res.json({
      previewId,
      expiresIn: '15 minutes',
      chatPreviews,
      suggestedChatId,
      existingChats
    });

  } catch (error) {
    // Cleanup temp file on error
    await fs.unlink(req.file.path).catch(() => { });
    console.error('Preview error:', error);
    res.status(500).json({ error: error.message || 'Preview failed' });
  }
});

// ─────────────────────────────────────────────────────────────
// STEP 2:  POST /api/upload/confirm
//   body: { previewId, targetChatId? }
//   targetChatId = null/undefined → create new chat
//   targetChatId = <id> → merge into that chat
// ─────────────────────────────────────────────────────────────
router.post('/upload/confirm', auth, async (req, res) => {
  const userId = req.user.id;
  const { previewId, targetChatId } = req.body || {};

  if (!previewId) return res.status(400).json({ error: 'previewId is required' });

  // Retrieve preview data
  const preview = previews.get(previewId);
  if (!preview) return res.status(404).json({ error: 'Preview expired or not found. Please upload again.' });
  if (preview.userId !== userId) return res.status(403).json({ error: 'Unauthorized' });

  // Check expiry
  if (Date.now() - preview.createdAt > PREVIEW_TTL_MS) {
    previews.delete(previewId);
    await fs.unlink(preview.tempPath).catch(() => { });
    return res.status(410).json({ error: 'Preview expired. Please upload again.' });
  }

  try {
    console.log(`[Confirm] Importing preview ${previewId} for user ${userId}, target: ${targetChatId || 'new chat'}`);

    // Re-read and parse the ZIP
    const zip = await loadZip(preview.tempPath);
    const { txtFiles, filesMap } = await extractZipMeta(zip);

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: 0 };

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;
        const nameGuess = parsed.nameGuess;

        let chatId;
        let created = false;

        if (targetChatId) {
          // ── User chose to merge into existing chat ──
          // Verify the chat exists and belongs to this user
          const [rows] = await conn.execute('SELECT id, name FROM chats WHERE id = ? AND user_id = ?', [targetChatId, userId]);
          if (!rows.length) {
            await conn.rollback();
            conn.release();
            return res.status(404).json({ error: `Chat ${targetChatId} not found` });
          }

          chatId = targetChatId;

          // Update chat name if the new import has a better name
          const existingName = rows[0].name;
          if (nameGuess && !isGenericName(nameGuess) && (isGenericName(existingName) || existingName !== nameGuess)) {
            await conn.execute('UPDATE chats SET name = ? WHERE id = ?', [nameGuess, chatId]);
            console.log(`Updated chat ${chatId} name: "${existingName}" -> "${nameGuess}"`);
          }

          // Sync participants (union of old + new)
          const [existingParts] = await conn.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [chatId]);
          const existingSet = new Set(existingParts.map(r => r.name));
          for (const p of participantsSet) {
            if (!existingSet.has(p)) {
              await conn.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
            }
          }

          stats.updatedChats++;
        } else {
          // ── Create new chat ──
          const [result] = await conn.execute('INSERT INTO chats (user_id, name) VALUES (?, ?)', [userId, nameGuess || 'Chat']);
          chatId = result.insertId;
          created = true;

          // Insert participants
          for (const p of participantsSet) {
            await conn.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
          }

          stats.addedChats++;
        }

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

        // Filter messages: only new ones (after last message time)
        const newMessages = parsed.messages.filter(m => {
          if (!m.timestamp) return true;
          if (!lastMessageTime) return true;
          return new Date(m.timestamp) > new Date(lastMessageTime);
        });

        // Insert messages + save media
        for (const m of newMessages) {
          let mediaPath = null;

          if (m.filename) {
            const rawName = m.filename;
            const targetNorm = normalizeName(rawName);
            let hit = null;
            let foundZipKey = null;

            // Try exact basename match
            const exactKey = rawName.toLowerCase();
            if (filesMap.has(exactKey)) {
              hit = filesMap.get(exactKey);
              foundZipKey = exactKey;
            }

            // Scan and match using normalized forms
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
            if (e.code === 'ER_DUP_ENTRY') {
              stats.skippedMessages++;
            } else {
              throw e;
            }
          }
        }

        // Count skipped (old messages filtered out)
        stats.skippedMessages += (parsed.messages.length - newMessages.length);
      }

      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }

    // Cleanup: delete temp file and preview entry
    await fs.unlink(preview.tempPath).catch(() => { });
    previews.delete(previewId);

    console.log(`[Confirm] Import completed for user ${userId}:`, stats);
    res.json(stats);

  } catch (error) {
    console.error('Confirm/import error:', error);
    res.status(500).json({ error: error.message || 'Import failed' });
  }
});

// ─────────────────────────────────────────────────────────────
// LEGACY:  POST /api/upload  (backward compatible)
//   Single-step upload with auto-matching (for old clients)
// ─────────────────────────────────────────────────────────────
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;

  try {
    console.log(`[Legacy Upload] Processing ZIP for user ${userId}, file: ${req.file.originalname}`);

    const zip = await loadZip(req.file.path);
    const { txtFiles, filesMap } = await extractZipMeta(zip);

    if (!txtFiles.length) return res.status(400).json({ error: 'no .txt chat file found in zip' });

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: 0 };

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;
        const nameGuess = parsed.nameGuess;

        // Auto-match: try name, then participants overlap, then generic+overlap
        const { chatId, created } = await autoFindOrCreateChat(userId, nameGuess, participantsSet, conn);
        if (created) stats.addedChats++; else stats.updatedChats++;

        const root = process.env.UPLOAD_ROOT || 'uploads';
        const chatDir = path.join(root, String(userId), String(chatId));
        await ensureDir(chatDir);

        let lastMessageTime = null;
        if (!created) {
          const [lastMsg] = await conn.execute(
            'SELECT MAX(timestamp) as last_time FROM messages WHERE chat_id = ?',
            [chatId]
          );
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
            if (e.code === 'ER_DUP_ENTRY') {
              stats.skippedMessages++;
            } else {
              throw e;
            }
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

    // Cleanup temp file
    await fs.unlink(req.file.path).catch(() => { });

    console.log(`[Legacy Upload] Completed for user ${userId}:`, stats);
    res.json(stats);

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message || 'Upload failed' });
  }
});

/**
 * Auto find-or-create chat (used by legacy single-step upload)
 * 4-step matching: name → participant overlap → generic+overlap → create new
 */
async function autoFindOrCreateChat(userId, nameGuess, participantsSet, connection) {
  const [chats] = await connection.execute('SELECT id, name FROM chats WHERE user_id = ?', [userId]);

  const chatParts = new Map();
  for (const c of chats) {
    const [rows] = await connection.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
    chatParts.set(c.id, new Set(rows.map(r => r.name)));
  }

  let foundId = null;

  // Step 1: Exact name match
  if (nameGuess && !isGenericName(nameGuess)) {
    const nameNorm = nameGuess.trim().toLowerCase();
    for (const c of chats) {
      if (c.name && c.name.trim().toLowerCase() === nameNorm) {
        foundId = c.id;
        break;
      }
    }
  }

  // Step 2: 50%+ participant overlap
  if (!foundId) {
    let bestOverlap = 0;
    for (const [id, existingSet] of chatParts) {
      const overlap = participantOverlap(participantsSet, existingSet);
      if (overlap >= 0.5 && overlap > bestOverlap) {
        bestOverlap = overlap;
        foundId = id;
      }
    }
  }

  // Step 3: Generic-named chat + any participant overlap
  if (!foundId) {
    for (const c of chats) {
      if (isGenericName(c.name)) {
        const existingSet = chatParts.get(c.id) || new Set();
        if (participantOverlap(participantsSet, existingSet) > 0) {
          foundId = c.id;
          break;
        }
      }
    }
  }

  if (foundId) {
    // Update name if better
    if (nameGuess && !isGenericName(nameGuess)) {
      const existing = chats.find(c => c.id === foundId);
      if (existing.name !== nameGuess) {
        await connection.execute('UPDATE chats SET name = ? WHERE id = ?', [nameGuess, foundId]);
      }
    }
    // Sync participants
    const existingSet = chatParts.get(foundId) || new Set();
    for (const p of participantsSet) {
      if (!existingSet.has(p)) {
        await connection.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [foundId, p]);
      }
    }
    return { chatId: foundId, created: false };
  }

  // Step 4: Create new
  const [result] = await connection.execute('INSERT INTO chats (user_id, name) VALUES (?, ?)', [userId, nameGuess || 'Chat']);
  const chatId = result.insertId;
  for (const p of participantsSet) {
    await connection.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
  }
  return { chatId, created: true };
}

export default router;