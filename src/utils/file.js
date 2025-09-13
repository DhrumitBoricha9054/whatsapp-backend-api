import fs from 'fs/promises';
import path from 'path';
import { lookup as mimeLookup } from 'mime-types';

export async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

export async function saveBuffer(filePath, buf) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, buf);
}

export function publicPath(...segments) {
  return path.posix.join('/uploads', ...segments);
}

export function safeName(name) {
  return name.replace(/[^\w.\- ]+/g, '_').slice(0, 180);
}

export function detectMime(filename) {
  return mimeLookup(filename) || 'application/octet-stream';
}
