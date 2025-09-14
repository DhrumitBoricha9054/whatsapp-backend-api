import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs/promises';
import fsSync from 'fs';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import JSZip from 'jszip';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, saveBuffer, publicPath, safeName } from '../utils/file.js';

const router = Router();

// --- Multer disk storage ---
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'tmp/'),
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
    chatParts.set(c.id, new Set(rows.map(r => r.name)));
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

  const [res] = await connection.execute(
    'INSERT INTO chats (user_id, name) VALUES (?, ?)',
    [userId, nameGuess || 'Chat']
  );
  const chatId = res.insertId;

  for (const p of participantsSet) {
    await connection.execute('INSERT INTO chat_participants (chat_id, name) VALUES (?, ?)', [chatId, p]);
  }

  return { chatId, created: true };
}

// --- Load ZIP via stream (memory-efficient) ---
async function loadZipStream(filePath) {
  const zip = new JSZip();
  const stream = fsSync.createReadStream(filePath);
  return zip.loadAsync(stream);
}

// --- Extract files, streaming media to disk ---
async function extractZipMetaStream(zip, uploadDir) {
  const txtFiles = [];
  const filesMap = new Map();

  const entries = Object.values(zip.files);

  for (const entry of entries) {
    if (entry.dir) continue;

    const lower = entry.name.toLowerCase();

    if (lower.endsWith('.txt')) {
      const text = await entry.async('string');
      txtFiles.push({ path: entry.name, text });
    } else {
      // Save media file directly to disk
      const safeOutName = safeName(path.basename(entry.name));
      const outPath = path.join(uploadDir, safeOutName);
      await ensureDir(path.dirname(outPath));

      const buf = await entry.async('nodebuffer'); // Node.js Buffer
      await fs.writeFile(outPath, buf);

      filesMap.set(lower, { path: outPath });
    }
  }

  return { txtFiles, filesMap };
}

// --- POST /upload ---
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });

  const userId = req.user.id;
  const tempUploadDir = path.join('uploads', String(userId), 'tmp');

  try {
    console.log(`Processing ZIP upload for user ${userId}, file: ${req.file.originalname}`);

    // Load ZIP stream
    const zip = await loadZipStream(req.file.path);

    // Extract files: text + media (media saved directly)
    const { txtFiles, filesMap } = await extractZipMetaStream(zip, tempUploadDir);

    if (!txtFiles.length) return res.status(400).json({ error: 'no .txt chat file found in zip' });

    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: filesMap.size };

    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const { path: txtPath, text } of txtFiles) {
        const parsed = parseWhatsAppText(text, txtPath);
        const participantsSet = parsed.participants;

        const { chatId, created } = await findOrCreateChatForUser(userId, parsed.nameGuess, participantsSet, conn);
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
            const key = m.filename.toLowerCase();
            const fileEntry = filesMap.get(key);
            if (fileEntry) {
              const destName = safeName(path.basename(fileEntry.path));
              const destPath = path.join(chatDir, destName);
              await ensureDir(path.dirname(destPath));
              await fs.copyFile(fileEntry.path, destPath);
              mediaPath = publicPath(String(userId), String(chatId), destName);
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
    // Delete temp uploaded ZIP
    try { await fs.unlink(req.file.path); } catch {}
    // Clean temporary uploaded media
    try { await fs.rm(tempUploadDir, { recursive: true, force: true }); } catch {}
  }
});

export default router;
