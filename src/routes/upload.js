import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs/promises';
import fsSync from 'fs';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import JSZip from 'jszip';
import { parseWhatsAppText } from '../utils/parseWhatsApp.js';
import { ensureDir, publicPath, safeName } from '../utils/file.js';

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

// --- Efficient ZIP processing ---
async function processZipEfficiently(filePath, uploadDir) {
  const zip = new JSZip();
  const fileBuffer = await fs.readFile(filePath);
  const zipData = await zip.loadAsync(fileBuffer);
  
  const txtFiles = [];
  const filesMap = new Map();
  const entries = Object.values(zipData.files);
  
  for (const entry of entries) {
    if (entry.dir) continue;
    
    const lower = entry.name.toLowerCase();
    
    if (lower.endsWith('.txt')) {
      const text = await entry.async('string');
      txtFiles.push({ path: entry.name, text });
    } else {
      const safeOutName = safeName(path.basename(entry.name));
      const outPath = path.join(uploadDir, safeOutName);
      await ensureDir(path.dirname(outPath));
      
      const readStream = entry.nodeStream();
      const writeStream = fsSync.createWriteStream(outPath);
      
      await new Promise((resolve, reject) => {
        readStream.pipe(writeStream)
          .on('finish', resolve)
          .on('error', reject);
      });
      
      filesMap.set(lower, { path: outPath });
    }
  }
  return { txtFiles, filesMap };
}

// --- Batch insert messages for better performance ---
async function batchInsertMessages(messages, chatId, connection) {
  if (messages.length === 0) return { inserted: 0, skipped: 0 };
  
  const BATCH_SIZE = 1000;
  let inserted = 0;
  let skipped = 0;
  
  for (let i = 0; i < messages.length; i += BATCH_SIZE) {
    const batch = messages.slice(i, i + BATCH_SIZE);
    
    const values = batch.map(m => [
      chatId,
      m.author || null,
      m.content || '',
      m.timestamp || null,
      m.type,
      m.mediaPath
    ]);
    
    const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?)').join(', ');
    const flatValues = values.flat();
    
    try {
      const [result] = await connection.execute(
        `INSERT IGNORE INTO messages (chat_id, author, content, timestamp, type, media_path) VALUES ${placeholders}`,
        flatValues
      );
      inserted += result.affectedRows;
    } catch (e) {
      console.error('Batch insert error:', e);
      for (const m of batch) {
        try {
          await connection.execute(
            `INSERT INTO messages (chat_id, author, content, timestamp, type, media_path) VALUES (?, ?, ?, ?, ?, ?)`,
            [chatId, m.author || null, m.content || '', m.timestamp || null, m.type, m.mediaPath]
          );
          inserted++;
        } catch (err) {
          if (err.code === 'ER_DUP_ENTRY') skipped++;
          else console.error('Individual insert error:', err);
        }
      }
    }
  }
  return { inserted, skipped };
}

// --- The core processing logic ---
async function processUpload(userId, filePath, originalname) {
  const tempUploadDir = path.join('uploads', String(userId), 'tmp');
  
  try {
    console.log(`Starting processing for user ${userId}, file: ${originalname}`);
    const { txtFiles, filesMap } = await processZipEfficiently(filePath, tempUploadDir);
    
    if (!txtFiles.length) {
      throw new Error('No .txt chat file found in zip');
    }
    
    const stats = { addedChats: 0, updatedChats: 0, addedMessages: 0, skippedMessages: 0, savedMedia: filesMap.size };

    for (const { path: txtPath, text } of txtFiles) {
      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        
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

        const messagesWithMedia = await Promise.all(newMessages.map(async (m) => {
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
          return { ...m, mediaPath };
        }));

        const { inserted, skipped } = await batchInsertMessages(messagesWithMedia, chatId, conn);
        stats.addedMessages += inserted;
        stats.skippedMessages += skipped + (parsed.messages.length - newMessages.length);
        await conn.commit();
      } catch (e) {
        await conn.rollback();
        throw e;
      } finally {
        conn.release();
      }
    }
    console.log(`Processing completed for user ${userId}:`, stats);
    return stats;
  } catch (error) {
    console.error('Processing error:', error);
    throw error;
  } finally {
    try { await fs.unlink(filePath); } catch {}
    try { await fs.rm(tempUploadDir, { recursive: true, force: true }); } catch {}
  }
}

// --- POST /upload (now waits for processing to complete) ---
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });
  const userId = req.user.id;
  try {
    // This is the key change: we now await the result.
    const stats = await processUpload(userId, req.file.path, req.file.originalname);
    res.json(stats);
  } catch (error) {
    console.error('Upload failed:', error);
    res.status(500).json({ error: error.message || 'Upload failed' });
  }
});

export default router;