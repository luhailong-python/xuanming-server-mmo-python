-- player_inbox 独占库 mmorpg_player_inbox 的建表 DDL（与 Go 侧 proto/player_inbox/player_inbox_table.proto
-- 及 shared/rolefence/rolefence.go 逐字对齐；TiDB 扩展注释在 MySQL 下视作普通注释，每条语句幂等）。

CREATE TABLE IF NOT EXISTS `player_inbox_stream` (
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `domain` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `next_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `fence_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `open_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `notify_due_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  PRIMARY KEY (`shard`,`player_id`,`domain`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_player_inbox_stream_0` (`shard`,`notify_due_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_inbox_stream';

CREATE TABLE IF NOT EXISTS `player_inbox_item` (
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `domain` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `msg_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `producer_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `content_sha256` MEDIUMBLOB COMMENT 'pb:8',
  `payload` MEDIUMBLOB COMMENT 'pb:9',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `directive` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `void_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `delivery_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `last_delivered_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `last_delivered_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `outcome_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `finalized_by` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `decided_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `issued_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `operator_name` MEDIUMTEXT COMMENT 'pb:22',
  `note` MEDIUMTEXT COMMENT 'pb:23',
  `owner_account` MEDIUMTEXT COMMENT 'pb:24',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  PRIMARY KEY (`shard`,`player_id`,`domain`,`seq`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_player_inbox_item_0` (`shard`,`state`,`updated_ms`),
  INDEX `idx_player_inbox_item_1` (`shard`,`player_id`,`state`),
  UNIQUE KEY `uk_player_inbox_item` (`shard`,`player_id`,`producer_kind`,`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_inbox_item';

CREATE TABLE IF NOT EXISTS `player_inbox_audit` (
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `domain` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `action` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `operator` MEDIUMTEXT COMMENT 'pb:6',
  `peer_addr` MEDIUMTEXT COMMENT 'pb:7',
  `reason` MEDIUMTEXT COMMENT 'pb:8',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `msg_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `issued_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `content_sha256` MEDIUMBLOB COMMENT 'pb:13',
  `payload` MEDIUMBLOB COMMENT 'pb:14',
  `void_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  PRIMARY KEY (`shard`,`player_id`,`domain`,`seq`,`action`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 */ COMMENT='player_inbox_audit';

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
