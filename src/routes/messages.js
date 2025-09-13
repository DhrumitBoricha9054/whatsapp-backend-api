const { Router } = require('express');
const { pool } = require('../db');
const { auth } = require('../middleware/auth');

const router = Router();

/** GET /api/chats/:id/messages?page=1&limit=50&search=... */
router.get('/chats/:id/messages', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  const search = (req.query.search || '').trim();

  // Verify ownership
  const [own] = await pool.execute('SELECT id FROM chats WHERE id = ? AND user_id = ?', [chatId, userId]);
  if (!own.length) return res.status(404).json({ error: 'Chat not found' });

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

  // Debug: Log media info
  const messagesWithMedia = rows.filter(m => m.media_path);
  if (messagesWithMedia.length > 0) {
    console.log(`Found ${messagesWithMedia.length} messages with media in chat ${chatId}:`);
    messagesWithMedia.slice(0, 3).forEach(m => {
      console.log(`  - ${m.type}: ${m.media_path}`);
    });
  }

  // total count
  const [cntRows] = await pool.execute(
    `SELECT COUNT(*) as cnt FROM messages ${where}`,
    params
  );

  res.json({ items: rows, total: Number(cntRows[0].cnt) });
});

module.exports = router;
