import { Router } from 'express';
import { pool } from '../db.js';
import { auth } from '../middleware/auth.js';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const router = Router();

/** POST /api/register {username, password} */
router.post('/register', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username & password required' });

  const hash = await bcrypt.hash(password, 10);
  try {
    const [result] = await pool.execute(
      'INSERT INTO users (username, password_hash) VALUES (?, ?)',
      [username, hash]
    );
    res.json({ id: result.insertId, username });
  } catch (e) {
    if (e.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'username already exists' });
    throw e;
  }
});

/** POST /api/login {username, password} */
router.post('/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username & password required' });

  const [rows] = await pool.execute('SELECT * FROM users WHERE username = ?', [username]);
  const user = rows[0];
  if (!user) return res.status(401).json({ error: 'invalid credentials' });
  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: 'invalid credentials' });

  const token = jwt.sign({ id: user.id, username: user.username }, process.env.JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, user: { id: user.id, username: user.username } });
});

/** POST /api/logout (auth) */
router.post('/logout', auth, async (_req, res) => {
  // With stateless JWT, logout is handled client-side by deleting the token.
  // This endpoint exists so the client has a unified flow to call and can be extended later.
  res.json({ ok: true });
});

  /** POST /api/change-password {oldPassword, newPassword} (auth required) */
  router.post('/change-password', auth, async (req, res) => {
    const userId = req.user.id;
    const { oldPassword, newPassword } = req.body || {};
    if (!oldPassword || !newPassword) return res.status(400).json({ error: 'oldPassword & newPassword required' });

    // Get user from DB
    const [rows] = await pool.execute('SELECT * FROM users WHERE id = ?', [userId]);
    const user = rows[0];
    if (!user) return res.status(404).json({ error: 'user not found' });

    // Check old password
    const ok = await bcrypt.compare(oldPassword, user.password_hash);
    if (!ok) return res.status(401).json({ error: 'old password incorrect' });

    // Hash new password and update
    const hash = await bcrypt.hash(newPassword, 10);
    await pool.execute('UPDATE users SET password_hash = ? WHERE id = ?', [hash, userId]);
    res.json({ ok: true });
  });

  /** POST /api/reset-password {username, newPassword} (no auth required) */
  router.post('/reset-password', async (req, res) => {
    const { username, newPassword } = req.body || {};
    if (!username || !newPassword) return res.status(400).json({ error: 'username & newPassword required' });

    // Get user from DB
    const [rows] = await pool.execute('SELECT * FROM users WHERE username = ?', [username]);
    const user = rows[0];
    if (!user) return res.status(404).json({ error: 'user not found' });

    // Hash new password and update
    const hash = await bcrypt.hash(newPassword, 10);
    await pool.execute('UPDATE users SET password_hash = ? WHERE username = ?', [hash, username]);
    res.json({ ok: true });
  });

export default router;
