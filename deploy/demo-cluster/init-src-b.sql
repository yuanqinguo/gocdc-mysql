-- 源 B（用户域）：msb.users / msb.addresses / msb.user_profiles
CREATE DATABASE IF NOT EXISTS msb CHARACTER SET utf8mb4;

CREATE TABLE msb.users (
  id BIGINT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  phone VARCHAR(20) NULL,
  email VARCHAR(128) NULL,
  level INT NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE msb.addresses (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  city VARCHAR(32) NOT NULL,
  detail VARCHAR(128) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_addr_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE msb.user_profiles (
  id BIGINT NOT NULL AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  nickname VARCHAR(64) NULL,
  bio VARCHAR(255) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_profiles_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO msb.users (name, phone, email, level) VALUES
  ('seed-user-1', '13900000001', 'u1@example.com', 1),
  ('seed-user-2', '13900000002', 'u2@example.com', 2),
  ('seed-user-3', NULL,          'u3@example.com', 0),
  ('seed-user-4', '13900000004', NULL,             3),
  ('seed-user-5', '13900000005', 'u5@example.com', 1);

INSERT INTO msb.addresses (user_id, city, detail) VALUES
  (1, '北京', '朝阳区 seed 1 号'),
  (1, '上海', '浦东新区 seed 2 号'),
  (2, '广州', '天河区 seed 3 号'),
  (3, '深圳', '南山区 seed 4 号'),
  (4, '杭州', '西湖区 seed 5 号');

INSERT INTO msb.user_profiles (user_id, nickname, bio) VALUES
  (1, 'nick-1', 'bio-seed-1'),
  (2, 'nick-2', 'bio-seed-2'),
  (3, 'nick-3', 'bio-seed-3'),
  (4, 'nick-4', 'bio-seed-4'),
  (5, 'nick-5', 'bio-seed-5');
