import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs/promises';
import { auth } from '../middleware/auth.js';
// import { uploadQueue } from '../queue.js'; // You would import your queue here

const router = Router();

// Multer disk storage for temporary file
const storage = multer.diskStorage({
  destination: async (req, file, cb) => {
    try {
      await fs.mkdir('tmp', { recursive: true });
      cb(null, 'tmp/');
    } catch (err) {
      cb(err);
    }
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, uniqueSuffix + '-' + file.originalname);
  }
});

const upload = multer({
  storage,
  limits: { 
    fileSize: 500 * 1024 * 1024, // 500MB max
    files: 1
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype !== 'application/zip' && file.mimetype !== 'application/x-zip-compressed') {
      cb(new Error('Only ZIP files are allowed'), false);
    }
    cb(null, true);
  }
});// --- POST /upload ---
router.post('/upload', auth, upload.single('zip'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'zip file required' });
  }

  const userId = req.user.id;
  const filePath = req.file.path;

  try {
    console.log(`Received ZIP upload from user ${userId}. File saved to: ${filePath}`);

    // This is the key change: we add a job to a queue instead of processing it here
    // await uploadQueue.add('process-zip', { userId, filePath });

    // Respond immediately to the user
    res.status(202).json({ 
      message: 'Upload received and is being processed in the background.',
      jobId: 'some-generated-job-id' 
    });

  } catch (error) {
    console.error('Upload error:', error);
    // Clean up the file if queuing fails
    await fs.unlink(filePath).catch(() => {});
    res.status(500).json({ error: error.message || 'Failed to queue upload job' });
  }
});

export default router;