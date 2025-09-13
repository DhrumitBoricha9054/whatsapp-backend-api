const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  multipleStatements: false,
  connectionLimit: 10,
  charset: 'utf8mb4'
});
/**
 * Delete specific chats by id list for a user. ids should be an array of numbers.
 * Returns [result] same as pool.execute.
 */
async function deleteChatsByIdsForUser(userId, ids) {
  if (!Array.isArray(ids) || ids.length === 0) {
    // nothing to delete
    return [{ affectedRows: 0 }];
  }

  // Build placeholders safely
  const placeholders = ids.map(() => '?').join(',');
  const sql = `DELETE FROM chats WHERE user_id = ? AND id IN (${placeholders})`;
  const params = [userId, ...ids];
  return pool.execute(sql, params);
}

exports.pool = pool;
exports.deleteChatsByIdsForUser = deleteChatsByIdsForUser;
