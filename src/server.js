import 'dotenv/config';
import 'express-async-errors';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import authRoutes from './routes/auth.js';
import chatRoutes from './routes/chats.js';
import messageRoutes from './routes/messages.js';
import uploadRoutes from './routes/upload.js';

const app = express();
app.use(express.json());

// static media (secured by path scoping; data ownership enforced at query time)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
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
