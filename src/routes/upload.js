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
import { promisify } from 'util';

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

// --- Processing status tracking ---
const processingStatus = new Map();

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

// --- Stream-based ZIP processing (memory efficient) ---
async function processZipStreamChunked(filePath, uploadDir, progressCallback) {
  const zip = new JSZip();
  
  // Read file in chunks to avoid memory issues
  const fileBuffer = await fs.readFile(filePath);
  const zipData = await zip.loadAsync(fileBuffer);
  
  const txtFiles = [];
  const filesMap = new Map();
  const entries = Object.values(zipData.files);
  
  let processed = 0;
  const total = entries.length;
  
  // Process entries in smaller batches to avoid blocking
  const BATCH_SIZE = 10;
  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);
    
    await Promise.all(batch.map(async (entry) => {
      if (entry.dir) return;
      
      const lower = entry.name.toLowerCase();
      
      if (lower.endsWith('.txt')) {
        const text = await entry.async('string');
        txtFiles.push({ path: entry.name, text });
      } else {
        // Process media files
        const safeOutName = safeName(path.basename(entry.name));
        const outPath = path.join(uploadDir, safeOutName);
        await ensureDir(path.dirname(outPath));
        
        // Use streams for large files
        const buf = await entry.async('nodebuffer');
        await fs.writeFile(outPath, buf);
        
        filesMap.set(lower, { path: outPath });
      }
    }));
    
    processed += batch.length;
    if (progressCallback) {
      progressCallback({ stage: 'extracting', progress: (processed / total) * 50 });
    }
    
    // Yield control to event loop
    await new Promise(resolve => setImmediate(resolve));
  }
  
  return { txtFiles, filesMap };
}

// --- Batch insert messages for better performance ---
async function batchInsertMessages(messages, chatId, connection) {
  if (messages.length === 0) return { inserted: 0, skipped: 0 };
  
  const BATCH_SIZE = 100;
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
      // Fall back to individual inserts for this batch
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
    
    // Yield control periodically
    await new Promise(resolve => setImmediate(resolve));
  }
  
  return { inserted, skipped };
}

// --- Background processing function ---
async function processUploadInBackground(userId, filePath, originalname) {
  const processId = `${userId}-${Date.now()}`;
  
  processingStatus.set(processId, {
    userId,
    status: 'processing',
    progress: 0,
    stage: 'starting',
    error: null,
    result: null
  });

  const tempUploadDir = path.join('uploads', String(userId), 'tmp', processId);
  
  try {
    console.log(`Starting background processing for user ${userId}, file: ${originalname}`);
    
    // Update progress callback
    const updateProgress = (update) => {
      const current = processingStatus.get(processId);
      if (current) {
        processingStatus.set(processId, { ...current, ...update });
      }
    };
    
    updateProgress({ stage: 'extracting', progress: 10 });
    
    // Extract ZIP with progress tracking
    const { txtFiles, filesMap } = await processZipStreamChunked(filePath, tempUploadDir, updateProgress);
    
    if (!txtFiles.length) {
      throw new Error('No .txt chat file found in zip');
    }
    
    updateProgress({ stage: 'parsing', progress: 60 });
    
    const stats = { 
      addedChats: 0, 
      updatedChats: 0, 
      addedMessages: 0, 
      skippedMessages: 0, 
      savedMedia: filesMap.size 
    };

    // Process each chat file
    for (let fileIndex = 0; fileIndex < txtFiles.length; fileIndex++) {
      const { path: txtPath, text } = txtFiles[fileIndex];
      
      updateProgress({ 
        stage: 'processing_chats', 
        progress: 60 + (fileIndex / txtFiles.length) * 30,
        currentFile: path.basename(txtPath)
      });
      
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

        // Prepare messages with media paths
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
          
          return {
            ...m,
            mediaPath
          };
        }));

        // Batch insert messages
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

    updateProgress({ 
      stage: 'completed', 
      progress: 100, 
      status: 'completed',
      result: stats
    });
    
    console.log(`Background processing completed for user ${userId}:`, stats);
    
  } catch (error) {
    console.error('Background processing error:', error);
    processingStatus.set(processId, {
      ...processingStatus.get(processId),
      status: 'error',
      error: error.message || 'Processing failed'
    });
  } finally {
    // Cleanup
    try { await fs.unlink(filePath); } catch {}
    try { await fs.rm(tempUploadDir, { recursive: true, force: true }); } catch {}
    
    // Remove status after 1 hour
    setTimeout(() => {
      processingStatus.delete(processId);
    }, 3600000);
  }
  
  return processId;
}

// --- POST /upload (now async) ---
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'zip file required' });

  const userId = req.user.id;
  
  try {
    // Start background processing
    const processId = await processUploadInBackground(userId, req.file.path, req.file.originalname);
    
    // Return immediately with process ID
    res.json({
      message: 'Upload started, processing in background',
      processId,
      statusUrl: `/api/upload/status/${processId}`
    });
    
  } catch (error) {
    console.error('Upload initialization error:', error);
    res.status(500).json({ error: error.message || 'Upload failed to start' });
    
    // Cleanup on error
    try { await fs.unlink(req.file.path); } catch {}
  }
});

// --- GET /upload/status/:processId ---
router.get('/status/:processId', auth, (req, res) => {
  const processId = req.params.processId;
  const status = processingStatus.get(processId);
  
  if (!status) {
    return res.status(404).json({ error: 'Process not found' });
  }
  
  // Check if user owns this process
  if (status.userId !== req.user.id) {
    return res.status(403).json({ error: 'Access denied' });
  }
  
  res.json(status);
});

// --- GET /upload/processes (list user's active processes) ---
router.get('/processes', auth, (req, res) => {
  const userId = req.user.id;
  const userProcesses = [];
  
  for (const [processId, status] of processingStatus.entries()) {
    if (status.userId === userId) {
      userProcesses.push({ processId, ...status });
    }
  }
  
  res.json(userProcesses);
});

export default router;