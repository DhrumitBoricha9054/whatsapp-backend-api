// Robust WhatsApp .txt parser: handles common 12/24h, bracketed, multiline, attachments.
const CLEAN_RE = /[\u200E\u200F\u202A-\u202E\u00A0\u202F]/g; // remove LTR/RTL, NBSPs
// match either: <attached: ...> (captures everything until >) OR a bare filename (allow spaces, parentheses)
const FILENAME_RE = /(?:<attached:\s*([^>]+)>|(?:\s|^)([A-Za-z0-9._\-\s()]+\.(?:jpg|jpeg|png|gif|mp4|mov|m4a|mp3|opus|pdf|docx?|xlsx?|pptx?)))/i;

// Try a few formats, e.g.:
// "24/07/25, 10:15 pm - Name: message"
// "[24/07/25, 22:15] Name: message"
// "[27/06/25, 8:32:18PM] Name: message" (with seconds)
const LINE_RES = [
  /^(\d{1,2}\/\d{1,2}\/\d{2,4}),\s+(\d{1,2}:\d{2}\s?[APap][Mm])\s+-\s+([^:]+):\s+([\s\S]*)$/,
  /^\[(\d{1,2}\/\d{1,2}\/\d{2,4}),\s+(\d{1,2}:\d{2}(?:\:\d{2})?(?:\s?[APap][Mm])?)\]\s+([^:]+):\s+([\s\S]*)$/,
  /^(\d{1,2}\/\d{1,2}\/\d{2,4}),\s+(\d{1,2}:\d{2})\s+-\s+([^:]+):\s+([\s\S]*)$/
];

function parseDate(d, t) {
  // d = DD/MM/YY(YY) ; t = "HH:mm" or "hh:mm:ss AM" or "hh:mm AM"
  const [dd, mm, yy] = d.split('/').map(Number);
  let year = yy < 100 ? 2000 + yy : yy;
  let hours = 0, minutes = 0, seconds = 0;

  // Try to match with seconds first, then without
  let m = t.trim().match(/^(\d{1,2}):(\d{2}):(\d{2})\s*([AaPp][Mm])?$/);
  if (m) {
    hours = Number(m[1]); minutes = Number(m[2]); seconds = Number(m[3]);
  } else {
    m = t.trim().match(/^(\d{1,2}):(\d{2})\s*([AaPp][Mm])?$/);
    if (!m) return null;
    hours = Number(m[1]); minutes = Number(m[2]);
  }
  
  const ampm = m[4]?.toLowerCase() || m[3]?.toLowerCase();

  if (ampm) {
    if (ampm === 'pm' && hours < 12) hours += 12;
    if (ampm === 'am' && hours === 12) hours = 0;
  }
  // local time: store as "YYYY-MM-DD HH:mm:ss"
  const mmPadded = String(mm).padStart(2,'0');
  const ddPadded = String(dd).padStart(2,'0');
  const hhPadded = String(hours).padStart(2,'0');
  const miPadded = String(minutes).padStart(2,'0');
  const sePadded = String(seconds).padStart(2,'0');

  return `${year}-${mmPadded}-${ddPadded} ${hhPadded}:${miPadded}:${sePadded}`;
}

/** Returns { nameGuess, participants:Set, messages:[{author, content, timestamp, type, filename}] } */
function parseWhatsAppText(txt, filePath = 'chat.txt') {
  console.log(`Parsing WhatsApp text from ${filePath}, ${txt.length} characters`);
  const lines = txt.replace(CLEAN_RE, '').split(/\r?\n/);
  console.log(`Split into ${lines.length} lines`);
  const messages = [];
  const participants = new Set();

  let cur = null;

  function pushCur() {
    if (!cur) return;
    const rec = { ...cur };

    // detect media
    let type = 'text';
    let filename = null;

    // Use match() (non-global regex) to avoid lastIndex state issues across messages
    const match = rec.content.match(FILENAME_RE);
    if (match) {
      filename = (match[1] || match[2] || '').trim();
      if (filename) {
        console.log(`Extracted filename from message: "${filename}" from content: "${rec.content.substring(0, 100)}..."`);
      }
    }
    if (filename) {
      const ext = filename.split('.').pop().toLowerCase();
      if (['jpg','jpeg','png','gif'].includes(ext)) type = 'image';
      else if (['mp4','mov'].includes(ext)) type = 'video';
      else if (['mp3','m4a','opus'].includes(ext)) type = 'audio';
      else if (ext === 'pdf') type = 'pdf';
      else type = 'file';
    }

    messages.push({ ...rec, type, filename });
  }

  let parsedLines = 0;
  let skippedLines = 0;

  for (const raw of lines) {
    const line = raw.trimEnd();
    if (!line) continue;

    let parsed = null;
    for (const R of LINE_RES) {
      const m = line.match(R);
      if (m) {
        const d = m[1], t = m[2], author = m[3].trim();
        const content = m[4] ?? '';
        const ts = parseDate(d, t);
        parsed = { ts, author, content };
        parsedLines++;
        if (parsedLines <= 3) {
          console.log(`Parsed message ${parsedLines}: ${author} at ${ts} - "${content.substring(0, 50)}..."`);
        }
        break;
      }
    }

    if (parsed) {
      // start of a new message
      pushCur();
      cur = { author: parsed.author, timestamp: parsed.ts, content: parsed.content };
      if (parsed.author && !/added|removed|joined|left|created/i.test(parsed.author)) {
        participants.add(parsed.author);
      }
    } else if (cur) {
      // multiline continuation
      cur.content += `\n${line}`;
    } else {
      skippedLines++;
      if (skippedLines <= 3) {
        console.log(`Skipped line (no match): "${line.substring(0, 100)}..."`);
      }
    }
  }
  pushCur();

  console.log(`Parsing complete: ${parsedLines} messages parsed, ${skippedLines} lines skipped`);
  console.log(`Found ${messages.length} total messages, ${participants.size} participants:`, Array.from(participants));

  // name guess from filePath
  const base = filePath.split('/').pop().replace(/\.[Tt][Xx][Tt]$/,'');
  const nameGuess = base === '_chat' || base === 'chat' ? null : base;

  return { nameGuess, participants, messages };
}

exports.parseWhatsAppText = parseWhatsAppText;
