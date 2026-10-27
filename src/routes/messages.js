import { Router } from 'express';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';

const router = Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const uploadsRoot = path.resolve(__dirname, '..', '..', process.env.UPLOAD_ROOT || 'uploads');

function pad(n) {
  return String(n).padStart(2, '0');
}

function formatLocalDateTime(date) {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

/**
 * Accepts YYYY-MM-DD, YYYY-MM-DD HH:mm[:ss], or ISO datetime.
 * Date-only values expand to start or end of that day (inclusive range).
 */
function parseRangeBound(value, { endOfDay = false } = {}) {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return endOfDay ? `${trimmed} 23:59:59` : `${trimmed} 00:00:00`;
  }

  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}$/.test(trimmed)) {
    return `${trimmed.replace('T', ' ')}:00`;
  }

  if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$/.test(trimmed)) {
    return trimmed.replace('T', ' ');
  }

  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return null;
  return formatLocalDateTime(parsed);
}

function readDateRange(source = {}) {
  const startRaw = source.startDate ?? source.start_date ?? source.from;
  const endRaw = source.endDate ?? source.end_date ?? source.to;
  const start = parseRangeBound(startRaw, { endOfDay: false });
  const end = parseRangeBound(endRaw, { endOfDay: true });
  return { startRaw, endRaw, start, end };
}

async function assertOwnedChat(chatId, userId) {
  const [own] = await pool.execute(
    'SELECT id FROM chats WHERE id = ? AND user_id = ?',
    [chatId, userId]
  );
  return own.length ? own[0] : null;
}

function mediaDiskPath(mediaPath) {
  if (!mediaPath) return null;
  const normalized = String(mediaPath).replace(/\\/g, '/');
  const prefix = '/uploads/';
  if (!normalized.startsWith(prefix)) return null;
  const relative = normalized.slice(prefix.length);
  if (!relative || relative.includes('..')) return null;
  const abs = path.resolve(uploadsRoot, relative);
  if (!abs.startsWith(uploadsRoot + path.sep) && abs !== uploadsRoot) return null;
  return abs;
}

async function deleteMediaFiles(rows) {
  let deletedMedia = 0;
  for (const row of rows) {
    const diskPath = mediaDiskPath(row.media_path);
    if (!diskPath) continue;
    try {
      await fs.unlink(diskPath);
      deletedMedia++;
    } catch (err) {
      if (err.code !== 'ENOENT') {
        console.error('Failed to delete media file:', diskPath, err.message);
      }
    }
  }
  return deletedMedia;
}

/** GET /api/chats/:id/messages/date-range */
router.get('/chats/:id/messages/date-range', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  if (!chatId) return res.status(400).json({ error: 'Invalid chat id' });

  const chat = await assertOwnedChat(chatId, userId);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  const [rows] = await pool.execute(
    `SELECT
        MIN(timestamp) AS minTimestamp,
        MAX(timestamp) AS maxTimestamp,
        COUNT(*) AS total,
        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS nullTimestampCount
     FROM messages
     WHERE chat_id = ?`,
    [chatId]
  );

  const row = rows[0] || {};
  res.json({
    chatId,
    minTimestamp: row.minTimestamp || null,
    maxTimestamp: row.maxTimestamp || null,
    total: Number(row.total || 0),
    nullTimestampCount: Number(row.nullTimestampCount || 0)
  });
});

/** GET /api/chats/:id/messages/by-date?startDate=&endDate=  (preview count, does not delete) */
router.get('/chats/:id/messages/by-date', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  if (!chatId) return res.status(400).json({ error: 'Invalid chat id' });

  const chat = await assertOwnedChat(chatId, userId);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  const { start, end } = readDateRange(req.query);
  if (!start || !end) {
    return res.status(400).json({ error: 'startDate and endDate are required (YYYY-MM-DD or datetime)' });
  }
  if (start > end) {
    return res.status(400).json({ error: 'startDate must be before or equal to endDate' });
  }

  const [cntRows] = await pool.execute(
    `SELECT COUNT(*) AS cnt
     FROM messages
     WHERE chat_id = ? AND timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?`,
    [chatId, start, end]
  );

  res.json({
    chatId,
    startDate: start,
    endDate: end,
    matchingCount: Number(cntRows[0].cnt)
  });
});

/** GET /api/chats/:id/messages?page=1&limit=50&search=... */
router.get('/chats/:id/messages', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  const search = (req.query.search || '').trim();

  const chat = await assertOwnedChat(chatId, userId);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  let where = 'WHERE chat_id = ?';
  const params = [chatId];
  if (search) {
    where += ' AND (author LIKE ? OR content LIKE ?)';
    params.push(`%${search}%`, `%${search}%`);
  }

  const [rows] = await pool.execute(
    `SELECT id, author, content, timestamp, type, media_path
     FROM messages
     ${where}
     ORDER BY timestamp ASC, id ASC`,
    params
  );

  const messagesWithMedia = rows.filter(m => m.media_path);
  if (messagesWithMedia.length > 0) {
    console.log(`Found ${messagesWithMedia.length} messages with media in chat ${chatId}:`);
    messagesWithMedia.slice(0, 3).forEach(m => {
      console.log(`  - ${m.type}: ${m.media_path}`);
    });
  }

  const [cntRows] = await pool.execute(
    `SELECT COUNT(*) as cnt FROM messages ${where}`,
    params
  );

  res.json({ items: rows, total: Number(cntRows[0].cnt) });
});

/**
 * DELETE /api/chats/:id/messages/by-date
 * Deletes messages in THIS chat only, between startDate and endDate (inclusive).
 * Other chats are never touched.
 */
router.delete('/chats/:id/messages/by-date', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  if (!chatId) return res.status(400).json({ error: 'Invalid chat id' });

  const chat = await assertOwnedChat(chatId, userId);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  const source = { ...(req.query || {}), ...(req.body || {}) };
  const { start, end } = readDateRange(source);
  if (!start || !end) {
    return res.status(400).json({ error: 'startDate and endDate are required (YYYY-MM-DD or datetime)' });
  }
  if (start > end) {
    return res.status(400).json({ error: 'startDate must be before or equal to endDate' });
  }

  const [mediaRows] = await pool.execute(
    `SELECT id, media_path
     FROM messages
     WHERE chat_id = ? AND timestamp IS NOT NULL AND timestamp >= ? AND timestamp <= ?
       AND media_path IS NOT NULL AND media_path <> ''`,
    [chatId, start, end]
  );

  const [result] = await pool.execute(
    `DELETE FROM messages
     WHERE chat_id = ?
       AND timestamp IS NOT NULL
       AND timestamp >= ?
       AND timestamp <= ?`,
    [chatId, start, end]
  );

  const deletedMedia = await deleteMediaFiles(mediaRows);

  const [remainRows] = await pool.execute(
    'SELECT COUNT(*) AS cnt FROM messages WHERE chat_id = ?',
    [chatId]
  );

  res.json({
    ok: true,
    chatId,
    startDate: start,
    endDate: end,
    deleted: result.affectedRows || 0,
    deletedMedia,
    remaining: Number(remainRows[0].cnt)
  });
});

/** DELETE /api/chats/:id/messages/:messageId  (one message in this chat only) */
router.delete('/chats/:id/messages/:messageId', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  const messageId = Number(req.params.messageId);
  if (!chatId || !messageId) return res.status(400).json({ error: 'Invalid chat or message id' });

  const chat = await assertOwnedChat(chatId, userId);
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  const [rows] = await pool.execute(
    'SELECT id, media_path FROM messages WHERE id = ? AND chat_id = ?',
    [messageId, chatId]
  );
  const message = rows[0];
  if (!message) return res.status(404).json({ error: 'Message not found' });

  await pool.execute('DELETE FROM messages WHERE id = ? AND chat_id = ?', [messageId, chatId]);
  const deletedMedia = await deleteMediaFiles(message.media_path ? [message] : []);

  res.json({
    ok: true,
    chatId,
    messageId,
    deleted: 1,
    deletedMedia
  });
});

export default router;
