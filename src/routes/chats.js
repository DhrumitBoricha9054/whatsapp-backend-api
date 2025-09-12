import { Router } from 'express';
import { pool, deleteChatsByIdsForUser } from '../db.js';
import { auth } from '../middleware/auth.js';

const router = Router();

/** GET /api/chats */
router.get('/chats', auth, async (req, res) => {
  const userId = req.user.id;
  const [rows] = await pool.execute(
    `SELECT c.id, c.name, c.created_at,
            COUNT(m.id) AS messageCount
     FROM chats c
     LEFT JOIN messages m ON m.chat_id = c.id
     WHERE c.user_id = ?
     GROUP BY c.id
     ORDER BY c.created_at DESC`,
    [userId]
  );

  // Attach participants
  const results = [];
  for (const c of rows) {
    const [parts] = await pool.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [c.id]);
    results.push({
      id: c.id,
      name: c.name,
      participants: parts.map(p => p.name),
      messageCount: Number(c.messageCount),
      created_at: c.created_at
    });
  }
  res.json(results);
});

/** GET /api/chats/:id */
router.get('/chats/:id', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  const [rows] = await pool.execute('SELECT * FROM chats WHERE id = ? AND user_id = ?', [chatId, userId]);
  const chat = rows[0];
  if (!chat) return res.status(404).json({ error: 'Chat not found' });

  const [parts] = await pool.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [chatId]);
  res.json({ ...chat, participants: parts.map(p => p.name) });
});

/** DELETE /api/chats/:id */

/** DELETE /api/chats/selected (delete specific chats) */
router.delete('/chats/selected', auth, async (req, res) => {
  const userId = req.user.id;
  // Debugging: log incoming body and content-type to diagnose routing/body parsing issues
  console.log('DELETE /api/chats/selected called - userId=', userId);
  console.log('  content-type=', req.headers['content-type']);
  console.log('  raw body=', req.body);

  const ids = req.body && Array.isArray(req.body.ids) ? req.body.ids.map(Number).filter(Boolean) : [];
  if (ids.length === 0) return res.status(400).json({ error: 'No chat IDs provided' });
  const [result] = await deleteChatsByIdsForUser(userId, ids);
  res.json({ ok: true, deleted: result.affectedRows || 0 });
});

/** DELETE /api/chats/:id */
router.delete('/chats/:id', auth, async (req, res) => {
  const userId = req.user.id;
  const chatId = Number(req.params.id);
  const [rows] = await pool.execute('DELETE FROM chats WHERE id = ? AND user_id = ?', [chatId, userId]);
  if (rows.affectedRows === 0) return res.status(404).json({ error: 'Chat not found' });
  res.json({ ok: true });
});

export default router;
