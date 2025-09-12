// import path from 'path';

// function normalizeName(s) {
//   if (!s) return '';
//   return s.toString().toLowerCase().replace(/[^a-z0-9.\-\.]+/g, '');
// }

// function safeName(name) {
//   return name.replace(/[^\w.\- ]+/g, '_').slice(0, 180);
// }

// const filesMap = new Map([
//   ['00000234-photo-2025-07-30-20-40-18.jpg', { path: 'photos/00000234-PHOTO-2025-07-30-20-40-18.jpg', data: Buffer.from('x') }],
//   ['00000464-recreating_business_insider_s_cv_of_marissa_mayer (1).pdf', { path: 'docs/00000464-recreating_business_insider_s_cv_of_marissa_mayer (1).pdf', data: Buffer.from('x') }],
//   ['00000128-harmis task list.docx', { path: 'docs/00000128-harmis task list.docx', data: Buffer.from('x') }]
// ]);

// const examples = [
//   '00000234-PHOTO-2025-07-30-20-40-18.jpg',
//   '00000464-Recreating_Business_Insider_s_CV_of_Marissa_Mayer _1_.pdf',
//   '00000128-harmis task list.docx',
//   '00000464-recreating_business_insider_s_cv_of_marissa_mayer (1).pdf'
// ];

// for (const rawName of examples) {
//   const targetNorm = normalizeName(rawName);
//   let hit = null; let foundZipKey = null;
//   const exactKey = rawName.toLowerCase();
//   if (filesMap.has(exactKey)) { hit = filesMap.get(exactKey); foundZipKey = exactKey; }
//   if (!hit) {
//     for (const [zipFilename, fileData] of filesMap.entries()) {
//       const zipNorm = normalizeName(zipFilename);
//       if (zipFilename.endsWith(exactKey) || zipNorm === targetNorm || zipNorm.includes(targetNorm) || targetNorm.includes(zipNorm)) {
//         hit = fileData; foundZipKey = zipFilename; break;
//       }
//     }
//   }
//   console.log(rawName, '->', hit ? `matched ${foundZipKey}` : 'NO MATCH');
// }
