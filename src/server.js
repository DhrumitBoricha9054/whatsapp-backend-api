require('dotenv').config();
require('express-async-errors');
const express = require('express');
const path = require('path');
const { fileURLToPath } = require('url');
const authRoutes = require('./routes/auth');
const chatRoutes = require('./routes/chats');
const messageRoutes = require('./routes/messages');
const uploadRoutes = require('./routes/upload');

const app = express();
app.use(express.json());

// static media (secured by path scoping; data ownership enforced at query time)
// If running under CommonJS, __filename/__dirname are available. When bundled from ESM to CJS,
// fileURLToPath(import.meta.url) isn't available, so we fallback to the native globals.
let __filename = typeof __filename !== 'undefined' ? __filename : undefined;
let __dirname = typeof __dirname !== 'undefined' ? __dirname : undefined;
if (!__filename || !__dirname) {
  try {
    const _filename = fileURLToPath(import.meta.url);
    __filename = _filename;
    __dirname = path.dirname(_filename);
  } catch (e) {
    // fallback to process.cwd()
    __dirname = process.cwd();
  }
}
const uploadsDir = process.env.UPLOAD_ROOT || 'uploads';
app.use('/uploads', express.static(path.join(__dirname, '..', uploadsDir)));

// routes
app.use('/api', authRoutes);
app.use('/api', uploadRoutes);
app.use('/api', chatRoutes);
app.use('/api', messageRoutes);

// centralized error handler
app.use((err, req, res, _next) => {
  console.error(err);
  res.status(err.status || 500).json({ error: err.message || 'Server error' });
});

const port = process.env.PORT || 5000;
app.listen(port, () => console.log(`API running on http://localhost:${port}`));
