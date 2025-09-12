-- Create database (run once, or create manually)
CREATE DATABASE IF NOT EXISTS whatsapp_viewer
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE whatsapp_viewer;

-- Users
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(100) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Chats (owned by a single app user)
CREATE TABLE IF NOT EXISTS chats (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  name VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Participants (WhatsApp display names)
CREATE TABLE IF NOT EXISTS chat_participants (
  id INT AUTO_INCREMENT PRIMARY KEY,
  chat_id INT NOT NULL,
  name VARCHAR(191) NOT NULL, -- 191 keeps index-friendly
  FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE,
  UNIQUE KEY chat_participant_unique (chat_id, name)
) ENGINE=InnoDB;

-- Messages
CREATE TABLE IF NOT EXISTS messages (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  chat_id INT NOT NULL,
  author VARCHAR(191),
  content TEXT,
  timestamp DATETIME,
  type ENUM('text','image','video','audio','pdf','file') DEFAULT 'text',
  media_path VARCHAR(512),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE,
  -- Prevent duplicates when importing again
  UNIQUE KEY unique_message (chat_id, author, timestamp, content(255))
) ENGINE=InnoDB;

-- Helpful indexes
CREATE INDEX idx_messages_chat_time ON messages (chat_id, timestamp);
