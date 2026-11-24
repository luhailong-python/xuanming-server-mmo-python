-- rank 独占库 mmorpg_rank 的建表 DDL（与 Go 侧 proto/rank/rank_table.proto 及 proto2mysql v0.2.0
-- 逐字对齐；全部 9 张 rank_* 表均幂等，按“分号 + 换行”逐条执行）。

CREATE TABLE IF NOT EXISTS `rank_board` (
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `board_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `scope` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `scope_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `period` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:5',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `producer` MEDIUMTEXT COMMENT 'pb:7',
  `source_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `ascending` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `settlement_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `open_request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `open_request_hash` MEDIUMTEXT COMMENT 'pb:14',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `sealed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `deleted_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  PRIMARY KEY (`board_id`),
  UNIQUE KEY `uk_rank_board` (`board_type`,`scope`,`scope_id`,`period`,`generation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_board';

CREATE TABLE IF NOT EXISTS `rank_board_shard` (
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `source_shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `committed_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `entries` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`board_id`,`generation`,`source_shard`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_board_shard';

CREATE TABLE IF NOT EXISTS `rank_score` (
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `entity_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `score` bigint NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `achieved_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `source_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `score_version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `order_key` bigint NOT NULL DEFAULT 0 COMMENT 'pb:8',
  PRIMARY KEY (`board_id`,`generation`,`entity_id`),
  INDEX `idx_rank_score_0` (`board_id`,`generation`,`order_key`,`achieved_ms`,`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_score';

CREATE TABLE IF NOT EXISTS `rank_input_receipt` (
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `source_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `request_hash` MEDIUMTEXT COMMENT 'pb:5',
  `committed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `source_shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  PRIMARY KEY (`board_id`,`generation`,`source_shard`,`source_cursor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_input_receipt';

CREATE TABLE IF NOT EXISTS `rank_settlement` (
  `settlement_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `source_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `final_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `request_hash` MEDIUMTEXT COMMENT 'pb:7',
  `winner_count` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `sealed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  PRIMARY KEY (`settlement_id`),
  UNIQUE KEY `uk_rank_settlement` (`board_id`,`generation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_settlement';

CREATE TABLE IF NOT EXISTS `rank_snapshot` (
  `settlement_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `rank` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `entity_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `score` bigint NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `achieved_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`settlement_id`,`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_snapshot';

CREATE TABLE IF NOT EXISTS `rank_mail_reward` (
  `reward_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `settlement_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `rank` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `attachments` MEDIUMBLOB COMMENT 'pb:5',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `mail_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `lease_token` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `expire_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  PRIMARY KEY (`reward_id`),
  INDEX `idx_rank_mail_reward_0` (`status`,`next_attempt_ms`),
  UNIQUE KEY `uk_rank_mail_reward` (`settlement_id`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_mail_reward';

CREATE TABLE IF NOT EXISTS `rank_mutation` (
  `request_hash` MEDIUMTEXT COMMENT 'pb:1',
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`board_id`,`generation`,`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_mutation';

CREATE TABLE IF NOT EXISTS `rank_removed_entry` (
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `entity_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `removed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  PRIMARY KEY (`board_id`,`generation`,`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='rank_removed_entry';
