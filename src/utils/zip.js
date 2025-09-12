import JSZip from 'jszip';
import fs from 'fs/promises';

/** Load ZIP buffer from uploaded temp file path */
export async function loadZip(tempPath) {
  const buf = await fs.readFile(tempPath);
  return JSZip.loadAsync(buf);
}

/** Return: { txtFiles: Array<{path, text}>, filesMap: Map<lowerBasename, {path, arrayBuffer}> } */
export async function extractZipMeta(zip) {
  const txtFiles = [];
  const filesMap = new Map();

  const entries = Object.values(zip.files);
  console.log(`Processing ${entries.length} entries from ZIP`);
  
  for (const entry of entries) {
    if (entry.dir) continue;
    const lower = entry.name.toLowerCase();

    if (lower.endsWith('.txt')) {
      const text = await entry.async('string');
      txtFiles.push({ path: entry.name, text });
      console.log(`Found txt file: ${entry.name}`);
    } else {
      const basename = lower.split('/').pop();
      try {
        const buf = await entry.async('uint8array'); // ArrayBuffer-like
        filesMap.set(basename, { path: entry.name, data: Buffer.from(buf) });
        console.log(`Found media file: ${entry.name} (${buf.length} bytes)`);
      } catch (error) {
        console.error(`Failed to extract media file ${entry.name}:`, error);
      }
    }
  }

  console.log(`Extracted ${txtFiles.length} txt files and ${filesMap.size} media files`);
  return { txtFiles, filesMap };
}
