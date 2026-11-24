-- friend 独占库 mmorpg_friend 的建表 DDL（与 Go 侧 proto/friend/friend_table.proto
-- 及 shared/rolefence/rolefence.go 逐字对齐；TiDB 扩展注释在 MySQL 下视作普通注释，每条语句幂等）。

CREATE TABLE IF NOT EXISTS `friend` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `friend_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `since_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`,`friend_player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_friend_0` (`friend_player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='friend';

CREATE TABLE IF NOT EXISTS `friend_request` (
  `from_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `to_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `request_time_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`from_player_id`,`to_player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_friend_request_0` (`to_player_id`,`status`),
  INDEX `idx_friend_request_1` (`status`,`updated_ms`),
  INDEX `idx_friend_request_2` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='friend_request';

CREATE TABLE IF NOT EXISTS `friend_capacity` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `friend_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_friend_capacity_0` (`friend_count`,`created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='friend_capacity';

CREATE TABLE IF NOT EXISTS `friend_block` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `blocked_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `since_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`,`blocked_player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_friend_block_0` (`blocked_player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='friend_block';

CREATE TABLE IF NOT EXISTS role_fence (
  player_id BIGINT UNSIGNED NOT NULL,
  account VARBINARY(191) NOT NULL,
  owner_revision BIGINT UNSIGNED NOT NULL,
  freeze_token BIGINT UNSIGNED NOT NULL,
  freeze_final TINYINT(1) NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  home_zone INT UNSIGNED NOT NULL,
  updated_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY(player_id) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;
