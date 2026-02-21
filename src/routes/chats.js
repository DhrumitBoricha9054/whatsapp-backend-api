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

/** POST /api/chats/merge - Merge multiple chats into one */
router.post('/chats/merge', auth, async (req, res) => {
  const userId = req.user.id;
  const chatIds = req.body && Array.isArray(req.body.chatIds)
    ? [...new Set(req.body.chatIds.map(Number).filter(Boolean))]
    : [];

  if (chatIds.length < 2) {
    return res.status(400).json({ error: 'At least 2 chat IDs required to merge' });
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // Verify all chats belong to this user
    const placeholders = chatIds.map(() => '?').join(',');
    const [ownedChats] = await conn.execute(
      `SELECT id, name FROM chats WHERE user_id = ? AND id IN (${placeholders})`,
      [userId, ...chatIds]
    );

    if (ownedChats.length !== chatIds.length) {
      await conn.rollback();
      return res.status(404).json({ error: 'One or more chats not found or not owned by you' });
    }

    // Pick the target chat: the one with the most messages
    const chatMsgCounts = [];
    for (const cid of chatIds) {
      const [cnt] = await conn.execute('SELECT COUNT(*) as c FROM messages WHERE chat_id = ?', [cid]);
      chatMsgCounts.push({ id: cid, count: Number(cnt[0].c) });
    }
    chatMsgCounts.sort((a, b) => b.count - a.count);
    const targetId = chatMsgCounts[0].id;
    const sourceIds = chatIds.filter(id => id !== targetId);

    console.log(`[Merge] Target chat: ${targetId}, source chats: ${sourceIds.join(', ')}`);

    // Pick the best name (prefer non-generic names)
    const genericNames = new Set(['chat', 'whatsapp chat', 'group', '_chat', '']);
    let bestName = null;
    for (const c of ownedChats) {
      if (c.name && !genericNames.has(c.name.trim().toLowerCase())) {
        bestName = c.name;
        break; // take the first non-generic name found
      }
    }
    if (bestName) {
      await conn.execute('UPDATE chats SET name = ? WHERE id = ?', [bestName, targetId]);
      console.log(`[Merge] Updated target chat name to "${bestName}"`);
    }

    // Merge participants: collect all unique participants into target chat
    const [targetParts] = await conn.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [targetId]);
    const existingParticipants = new Set(targetParts.map(r => r.name));

    for (const srcId of sourceIds) {
      const [srcParts] = await conn.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [srcId]);
      for (const p of srcParts) {
        if (!existingParticipants.has(p.name)) {
          await conn.execute('INSERT IGNORE INTO chat_participants (chat_id, name) VALUES (?, ?)', [targetId, p.name]);
          existingParticipants.add(p.name);
        }
      }
    }

    // Move messages from source chats to target chat
    // Use a temp approach to handle the UNIQUE KEY constraint:
    // The unique key is (chat_id, author, timestamp, content(255))
    // Some messages might already exist in the target — skip those
    let movedMessages = 0;
    let skippedDuplicates = 0;

    for (const srcId of sourceIds) {
      const [srcMessages] = await conn.execute(
        'SELECT author, content, timestamp, type, media_path FROM messages WHERE chat_id = ?',
        [srcId]
      );

      for (const m of srcMessages) {
        try {
          await conn.execute(
            `INSERT INTO messages (chat_id, author, content, timestamp, type, media_path)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [targetId, m.author, m.content, m.timestamp, m.type, m.media_path]
          );
          movedMessages++;
        } catch (e) {
          if (e.code === 'ER_DUP_ENTRY') {
            skippedDuplicates++;
          } else {
            throw e;
          }
        }
      }

      // Delete source chat (CASCADE will delete its messages and participants)
      await conn.execute('DELETE FROM chats WHERE id = ?', [srcId]);
    }

    await conn.commit();

    console.log(`[Merge] Done: moved ${movedMessages} messages, skipped ${skippedDuplicates} duplicates, deleted ${sourceIds.length} source chats`);

    // Return merged chat info
    const [mergedChat] = await pool.execute('SELECT * FROM chats WHERE id = ?', [targetId]);
    const [mergedParts] = await pool.execute('SELECT name FROM chat_participants WHERE chat_id = ?', [targetId]);
    const [mergedCount] = await pool.execute('SELECT COUNT(*) as cnt FROM messages WHERE chat_id = ?', [targetId]);

    res.json({
      ok: true,
      mergedChatId: targetId,
      chat: {
        id: targetId,
        name: mergedChat[0].name,
        participants: mergedParts.map(p => p.name),
        messageCount: Number(mergedCount[0].cnt),
        created_at: mergedChat[0].created_at
      },
      movedMessages,
      skippedDuplicates,
      deletedChats: sourceIds
    });

  } catch (e) {
    await conn.rollback();
    console.error('Merge error:', e);
    res.status(500).json({ error: e.message || 'Merge failed' });
  } finally {
    conn.release();
  }
});

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
