-- 地域玩家存档库（player_store_{storage_id}_db）权威表结构。
-- 与 xuanming-server-mmo-cpp/proto/common/database/mysql_database_table.proto
-- 及 go/db/internal/playerstore/{store.go,audit_outbox.go} 保持逐列一致。
-- 所有语句均幂等，按“分号 + 换行”逐条执行。

CREATE TABLE IF NOT EXISTS `player_store` (
  `player_id`       BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',
  `kind`            INT UNSIGNED NOT NULL COMMENT 'pb:2',
  `owner_epoch`     BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',
  `save_seq`        BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',
  `schema_gen`      INT UNSIGNED NOT NULL COMMENT 'pb:5',
  `holder_instance` VARBINARY(64) NOT NULL COMMENT 'pb:6',
  `released`        INT UNSIGNED NOT NULL COMMENT 'pb:7',
  `payload`         MEDIUMBLOB NOT NULL COMMENT 'pb:8',
  `claim_seq`       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:9',
  PRIMARY KEY (`player_id`, `kind`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='player_store';

CREATE TABLE IF NOT EXISTS `player_audit_outbox` (
  `shard`      INT UNSIGNED NOT NULL COMMENT 'pb:1',
  `kind`       INT UNSIGNED NOT NULL COMMENT 'pb:2',
  `created_ms` BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',
  `record_id`  BIGINT UNSIGNED NOT NULL COMMENT 'pb:5',
  `save_seq`   BIGINT UNSIGNED NOT NULL COMMENT 'pb:6',
  `payload`    MEDIUMBLOB NOT NULL COMMENT 'pb:7',
  PRIMARY KEY (`shard`, `kind`, `created_ms`, `player_id`, `record_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='player_audit_outbox';
