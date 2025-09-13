const fs = require('fs/promises');
const path = require('path');
const { lookup: mimeLookup } = require('mime-types');

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function saveBuffer(filePath, buf) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, buf);
}

function publicPath(...segments) {
  return path.posix.join('/uploads', ...segments);
}

function safeName(name) {
  return name.replace(/[^\w.\- ]+/g, '_').slice(0, 180);
}

function detectMime(filename) {
  return mimeLookup(filename) || 'application/octet-stream';
}

exports.ensureDir = ensureDir;
exports.saveBuffer = saveBuffer;
exports.publicPath = publicPath;
exports.safeName = safeName;
exports.detectMime = detectMime;
