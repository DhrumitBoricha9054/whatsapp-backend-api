import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import crypto from 'crypto';
import fsPromises from 'fs/promises';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import { loadZip, extractZipMeta } from '../utils/zip.js';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, saveBuffer, publicPath, safeName } from '../utils/file.js';

const router = Router();
const upload = multer({ dest: 'tmp/' });

// ─── Helper functions ───────────────────────────────────────────────

function normalizeName(s) {
  if (!s) return '';
  return s.toString().toLowerCase().replace(/[^a-z0-9.\-\.]+/g, '');
}

const GENERIC_NAMES = new Set(['chat', 'whatsapp chat', 'group', '_chat']);

function isGenericName(name) {
  if (!name) return true;
  return GENERIC_NAMES.has(name.trim().toLowerCase());
}

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

// ─── Preview storage (in-memory with TTL) ────────────────────────────

const previewStore = new Map();
const PREVIEW_TTL_MS = 30 * 60 * 1000; // 30 minutes

// Cleanup expired previews every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [id, preview] of previewStore) {
    if (now > preview.expiresAt) {
      fsPromises.unlink(preview.filePath).catch(() => { });
      previewStore.delete(id);
      console.log(`Cleaned up expired preview: ${id}`);
    }
  }
}, 5 * 60 * 1000);

// ─── Suggestion engine ──────────────────────────────────────────────

/** Find the best matching existing chat for an imported chat (read-only, no mutations) */
async function findSuggestedMatch(userId, nameGuess, participantsSet, connection) {
  const [chats] = await connection.execute('SELECT id, name FROM chats WHERE user_id = ?', [userId]);

  // Pre-load participants for all chats
  const chatParts = new Map();
  for (const c of chats) {
    const [rows] = await connection.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
    chatParts.set(c.id, new Set(rows.map(r => r.name)));
  }

  // Step 1: Exact name match
  if (nameGuess && !isGenericName(nameGuess)) {
    const nameNorm = nameGuess.trim().toLowerCase();
    for (const c of chats) {
      if (c.name && c.name.trim().toLowerCase() === nameNorm) {
        return {
          chatId: c.id,
          chatName: c.name,
          participants: [...(chatParts.get(c.id) || [])],
          matchStep: 'exact_name',
          confidence: 95
        };
      }
    }
  }

  // Step 2: 50%+ participant overlap
  let bestOverlap = 0;
  let bestChat = null;
  for (const [id, existingSet] of chatParts) {
    const overlap = participantOverlap(participantsSet, existingSet);
    if (overlap >= 0.5 && overlap > bestOverlap) {
      bestOverlap = overlap;
      bestChat = chats.find(c => c.id === id);
    }
  }
  if (bestChat) {
    return {
      chatId: bestChat.id,
      chatName: bestChat.name,
      participants: [...(chatParts.get(bestChat.id) || [])],
      matchStep: 'participant_overlap',
      confidence: Math.round(bestOverlap * 100)
    };
  }

  // Step 3: Generic-named chat + any overlap
  for (const c of chats) {
    if (isGenericName(c.name)) {
      const existingSet = chatParts.get(c.id) || new Set();
      const overlap = participantOverlap(participantsSet, existingSet);
      if (overlap > 0) {
        return {
          chatId: c.id,
          chatName: c.name,
          participants: [...existingSet],
          matchStep: 'generic_name_with_overlap',
          confidence: Math.round(overlap * 70)
        };
      }
    }
  }

  return null;
}

// ─── 4-step auto-matching (used by /upload backward compat) ──────────

async function findOrCreateChatForUser(userId, nameGuess, participantsSet, connection) {
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
        console.log(`[Step 1] Matched chat by exact name: "${nameGuess}" -> chatId ${foundId}`);
        break;
      }
    }
  }

  // Step 2: 50%+ participant overlap
  if (!foundId) {
    let bestOverlap = 0;
    let bestId = null;
    for (const [id, existingSet] of chatParts) {
      const overlap = participantOverlap(participantsSet, existingSet);
      if (overlap >= 0.5 && overlap > bestOverlap) {
        bestOverlap = overlap;
        bestId = id;
      }
    }
    if (bestId) {
      foundId = bestId;
      console.log(`[Step 2] Matched chat by participant overlap (${(bestOverlap * 100).toFixed(0)}%) -> chatId ${foundId}`);
    }
  }

  // Step 3: Generic name + any overlap
  if (!foundId) {
    for (const c of chats) {
      if (isGenericName(c.name)) {
        const existingSet = chatParts.get(c.id) || new Set();
        const overlap = participantOverlap(participantsSet, existingSet);
        if (overlap > 0) {
          foundId = c.id;
          console.log(`[Step 3] Matched generic-named chat ("${c.name}") with participant overlap (${(overlap * 100).toFixed(0)}%) -> chatId ${foundId}`);
          break;
        }
      }
    }
  }

  if (foundId) {
    if (nameGuess && !isGenericName(nameGuess)) {
      const existing = chats.find(c => c.id === foundId);
      if (existing.name !== nameGuess) {
        await connection.execute('UPDATE chats SET name = ? WHERE id = ?', [nameGuess, foundId]);
        console.log(`Updated chat ${foundId} name: "${existing.name}" -> "${nameGuess}"`);
      }
    }
    const existingSet = chatParts.get(foundId) || new Set();
    for (const p of participantsSet) {
      if (!existingSet.has(p)) {
        await connection.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [foundId, p]);
      }
    }
    return { chatId: foundId, created: false };
  }

  // Step 4: Create new
  const [res] = await connection.execute('INSERT INTO chats (user_id, name) VALUES (?, ?)', [userId, nameGuess || 'Chat']);
  const chatId = res.insertId;
  console.log(`[Step 4] Created new chat: "${nameGuess || 'Chat'}" -> chatId ${chatId}`);
  for (const p of participantsSet) {
    await connection.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
  }
  return { chatId, created: true };
}

// ─── Shared message import logic ──────────────────────────────────────

/**
 * Import parsed messages into a chat. Used by both /upload and /upload/confirm.
 * @param {number} chatId - Target chat ID
 * @param {object} parsed - Parsed WhatsApp data { nameGuess, participants, messages }
 * @param {Map} filesMap - Media files from ZIP
 * @param {number} userId - User ID
 * @param {boolean} isNewChat - Whether this is a newly created chat
 * @param {object} conn - DB connection (within transaction)
 * @param {object} stats - Stats object to mutate
 */
async function importMessagesIntoChat(chatId, parsed, filesMap, userId, isNewChat, conn, stats) {
  const root = process.env.UPLOAD_ROOT || 'uploads';
  const chatDir = path.join(root, String(userId), String(chatId));
  await ensureDir(chatDir);

  // Get last message timestamp for dedup
  let lastMessageTime = null;
  if (!isNewChat) {
    const [lastMsg] = await conn.execute(
      'SELECT MAX(timestamp) as last_time FROM messages WHERE chat_id = ?',
      [chatId]
    );
    lastMessageTime = lastMsg[0]?.last_time;
  }

  // Filter to new messages only
  const newMessages = parsed.messages.filter(m => {
    if (!m.timestamp) return true;
    if (!lastMessageTime) return true;
    return new Date(m.timestamp) > new Date(lastMessageTime);
  });

  // Insert messages
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
            console.log(`Found media file with match: ${rawName} -> ${zipFilename}`);
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
          console.log(`Saved media: ${m.filename} -> ${mediaPath}`);
        } catch (mediaError) {
          console.error(`Failed to save media file ${m.filename}:`, mediaError);
        }
      } else {
        console.log(`Media file not found in ZIP: ${m.filename}`);
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

// ═══════════════════════════════════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════════════════════════════════

/**
 * POST /api/upload/preview  (auth)  form-data: zip=<file>
 *
 * Step 1 of two-step upload. Parses the ZIP and returns a preview
 * with parsed chat info, suggested match, and all existing chats.
 * The ZIP is stored temporarily for the confirm step.
 *
 * Response: {
 *   previewId: "abc123",
 *   parsedChats: [{ name, participants, messageCount, dateRange }],
 *   suggestedMatch: { chatId, chatName, participants, matchStep, confidence } | null,
 *   existingChats: [{ id, name, participants, messageCount }]
 * }
 */
router.post('/upload/preview', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;

  try {
    console.log(`Preview ZIP for user ${userId}, file: ${req.file.originalname}`);

    const zip = await loadZip(req.file.path);
    const { txtFiles } = await extractZipMeta(zip);

    if (!txtFiles.length) {
      // Clean up temp file
      fsPromises.unlink(req.file.path).catch(() => { });
      return res.status(400).json({ error: 'no .txt chat file found in zip' });
    }

    // Parse all chats in the ZIP
    const parsedChats = [];
    for (const { path: txtPath, text } of txtFiles) {
      const parsed = parseWhatsAppText(text, txtPath);
      const timestamps = parsed.messages
        .map(m => m.timestamp)
        .filter(Boolean)
        .sort();

      parsedChats.push({
        name: parsed.nameGuess || null,
        participants: [...parsed.participants],
        messageCount: parsed.messages.length,
        dateRange: {
          from: timestamps[0] || null,
          to: timestamps[timestamps.length - 1] || null
        }
      });
    }

    // Find suggested match for the first (primary) chat
    const primaryChat = parsedChats[0];
    let suggestedMatch = null;
    try {
      suggestedMatch = await findSuggestedMatch(
        userId,
        primaryChat.name,
        new Set(primaryChat.participants),
        pool
      );
    } catch (e) {
      console.error('Error finding suggested match:', e);
    }

    // Get all existing chats for the user (for manual selection)
    const [chatRows] = await pool.execute(
      `SELECT c.id, c.name, c.created_at, COUNT(m.id) AS messageCount
       FROM chats c
       LEFT JOIN messages m ON m.chat_id = c.id
       WHERE c.user_id = ?
       GROUP BY c.id
       ORDER BY c.created_at DESC`,
      [userId]
    );

    const existingChats = [];
    for (const c of chatRows) {
      const [parts] = await pool.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
      existingChats.push({
        id: c.id,
        name: c.name,
        participants: parts.map(p => p.name),
        messageCount: Number(c.messageCount)
      });
    }

    // Store preview in memory
    const previewId = crypto.randomUUID();
    previewStore.set(previewId, {
      filePath: req.file.path,
      userId,
      expiresAt: Date.now() + PREVIEW_TTL_MS
    });

    console.log(`Preview created: ${previewId}, ${parsedChats.length} chats found`);

    res.json({
      previewId,
      parsedChats,
      suggestedMatch,
      existingChats
    });

  } catch (error) {
    console.error('Preview error:', error);
    fsPromises.unlink(req.file.path).catch(() => { });
    res.status(500).json({ error: error.message || 'Preview failed' });
  }
});

/**
 * POST /api/upload/confirm  (auth)  JSON body: { previewId, targetChatId }
 *
 * Step 2 of two-step upload. Imports the previously previewed ZIP
 * into the selected chat (or creates a new one).
 *
 * Body: {
 *   previewId: "abc123",
 *   targetChatId: 5       // existing chat ID to merge into, or null to create new
 * }
 *
 * Response: { addedChats, updatedChats, addedMessages, skippedMessages, savedMedia }
 */
router.post('/upload/confirm', auth, async (req, res) => {
  const userId = req.user.id;
  const { previewId, targetChatId } = req.body || {};

  if (!previewId) return res.status(400).json({ error: 'previewId required' });

  // Retrieve preview
  const preview = previewStore.get(previewId);
  if (!preview) return res.status(404).json({ error: 'Preview not found or expired. Please upload again.' });
  if (preview.userId !== userId) return res.status(403).json({ error: 'Unauthorized' });
  if (Date.now() > preview.expiresAt) {
    previewStore.delete(previewId);
    fsPromises.unlink(preview.filePath).catch(() => { });
    return res.status(410).json({ error: 'Preview expired. Please upload again.' });
  }

  try {
    console.log(`Confirm import for user ${userId}, previewId: ${previewId}, targetChatId: ${targetChatId}`);

    // Re-load and parse the ZIP
    const zip = await loadZip(preview.filePath);
    const { txtFiles, filesMap } = await extractZipMeta(zip);

    if (!txtFiles.length) return res.status(400).json({ error: 'no .txt chat file found in zip' });

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: 0 };

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        console.log(`\n=== Processing chat file: ${txtPath} ===`);

        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;
        let chatId;
        let isNewChat;

        if (targetChatId !== null && targetChatId !== undefined) {
          // ── User selected an existing chat to merge into ──
          const [rows] = await conn.execute('SELECT id, name FROM chats WHERE id = ? AND user_id = ?', [targetChatId, userId]);
          if (!rows.length) {
            await conn.rollback();
            conn.release();
            return res.status(404).json({ error: `Chat ${targetChatId} not found` });
          }

          chatId = targetChatId;
          isNewChat = false;
          stats.updatedChats++;

          // Update chat name to the latest parsed name (newer = more accurate)
          if (parsed.nameGuess && !isGenericName(parsed.nameGuess)) {
            const existingName = rows[0].name;
            if (existingName !== parsed.nameGuess) {
              await conn.execute('UPDATE chats SET name = ? WHERE id = ?', [parsed.nameGuess, chatId]);
              console.log(`Updated chat ${chatId} name: "${existingName}" -> "${parsed.nameGuess}"`);
            }
          }

          // Sync participants (union of old + new)
          const [existingParts] = await conn.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [chatId]);
          const existingSet = new Set(existingParts.map(r => r.name));
          for (const p of participantsSet) {
            if (!existingSet.has(p)) {
              await conn.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
              console.log(`Added new participant "${p}" to chat ${chatId}`);
            }
          }

        } else {
          // ── User chose to create a new chat ──
          const [result] = await conn.execute(
            'INSERT INTO chats (user_id, name) VALUES (?, ?)',
            [userId, parsed.nameGuess || 'Chat']
          );
          chatId = result.insertId;
          isNewChat = true;
          stats.addedChats++;
          console.log(`Created new chat: "${parsed.nameGuess || 'Chat'}" -> chatId ${chatId}`);

          for (const p of participantsSet) {
            await conn.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
          }
        }

        // Import messages
        await importMessagesIntoChat(chatId, parsed, filesMap, userId, isNewChat, conn, stats);
      }

      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }

    // Clean up preview
    previewStore.delete(previewId);
    fsPromises.unlink(preview.filePath).catch(() => { });

    console.log(`Import completed for user ${userId}:`, stats);
    res.json(stats);

  } catch (error) {
    console.error('Confirm error:', error);
    res.status(500).json({ error: error.message || 'Import failed' });
  }
});

// ─── Backward-compatible single-step upload ─────────────────────────

/** POST /api/upload  (auth)  form-data: zip=<file>
 *  Original single-step upload with auto-matching.
 *  Kept for backward compatibility. */
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

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        console.log(`\n=== Processing chat file: ${txtPath} ===`);

        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;

        const { chatId, created } = await findOrCreateChatForUser(userId, parsed.nameGuess, participantsSet, conn);
        if (created) stats.addedChats++; else stats.updatedChats++;

        // Import messages
        await importMessagesIntoChat(chatId, parsed, filesMap, userId, created, conn, stats);
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