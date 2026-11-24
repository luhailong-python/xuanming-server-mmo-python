-- chat 独占库 mmorpg_chat 的建表 DDL（与 Go 侧 go/chat/internal/authority/schema.go 逐字对齐；
-- TiDB TTL 扩展注释在 MySQL 下视作普通注释忽略，每条语句幂等）。

CREATE TABLE IF NOT EXISTS chat_send_authority (
  player_id BIGINT UNSIGNED NOT NULL,
  request_id VARBINARY(64) NOT NULL,
  owner_account VARBINARY(191) NOT NULL,
  owner_revision BIGINT UNSIGNED NOT NULL,
  request_digest BINARY(32) NOT NULL,
  request_payload MEDIUMBLOB NOT NULL,
  message_payload MEDIUMBLOB NOT NULL,
  stream_key VARBINARY(191) NOT NULL,
  state TINYINT UNSIGNED NOT NULL,
  accepted_ms BIGINT UNSIGNED NOT NULL,
  result_tip INT UNSIGNED NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  PRIMARY KEY(player_id,request_id),
  KEY idx_chat_stream(stream_key,state,accepted_ms,player_id,request_id),
  KEY idx_chat_rate(player_id,state,accepted_ms)
) ENGINE=InnoDB /*T![ttl] TTL=`accepted_at` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */;

CREATE TABLE IF NOT EXISTS chat_policy (
  id TINYINT UNSIGNED NOT NULL,
  policy_hash BINARY(32) NOT NULL,
  PRIMARY KEY(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chat_world_retraction (
  sender_player_id BIGINT UNSIGNED NOT NULL,
  accepted_ms BIGINT UNSIGNED NOT NULL,
  retracted_ms BIGINT UNSIGNED NOT NULL,
  reason VARBINARY(128) NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  PRIMARY KEY(sender_player_id,accepted_ms),
  KEY idx_chat_retract_ms(accepted_ms,sender_player_id)
) ENGINE=InnoDB /*T![ttl] TTL=`accepted_at` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */;

CREATE TABLE IF NOT EXISTS chat_report (
  reporter_player_id BIGINT UNSIGNED NOT NULL,
  channel TINYINT UNSIGNED NOT NULL,
  target_sender_player_id BIGINT UNSIGNED NOT NULL,
  send_time_ms BIGINT UNSIGNED NOT NULL,
  reason INT UNSIGNED NOT NULL,
  detail VARBINARY(255) NOT NULL,
  reported_ms BIGINT UNSIGNED NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  PRIMARY KEY(reporter_player_id,channel,target_sender_player_id,send_time_ms),
  KEY idx_chat_report_target(target_sender_player_id,send_time_ms)
) ENGINE=InnoDB /*T![ttl] TTL=`accepted_at` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */;

CREATE TABLE IF NOT EXISTS chat_repeat_event (
  content_hash BINARY(32) NOT NULL,
  accepted_ms BIGINT UNSIGNED NOT NULL,
  sender_player_id BIGINT UNSIGNED NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  PRIMARY KEY(content_hash,accepted_ms,sender_player_id)
) ENGINE=InnoDB /*T![ttl] TTL=`accepted_at` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */;

CREATE TABLE IF NOT EXISTS chat_private_daily_target (
  sender_player_id BIGINT UNSIGNED NOT NULL,
  day_ymd INT UNSIGNED NOT NULL,
  target_player_id BIGINT UNSIGNED NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  PRIMARY KEY(sender_player_id,day_ymd,target_player_id)
) ENGINE=InnoDB /*T![ttl] TTL=`accepted_at` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */;
