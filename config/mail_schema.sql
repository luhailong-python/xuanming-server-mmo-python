-- mail 独占库 mmorpg_mail 的建表 DDL（与 Go 侧 proto/mail/mail_table.proto
-- 及 shared/rolefence/rolefence.go 逐字对齐；TiDB 扩展注释在 MySQL 下视作普通注释，每条语句幂等）。

CREATE TABLE IF NOT EXISTS `mail_message` (
  `mail_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `owner_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `sender_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `request_hash` MEDIUMTEXT COMMENT 'pb:7',
  `title` MEDIUMTEXT COMMENT 'pb:8',
  `body` MEDIUMTEXT COMMENT 'pb:9',
  `attachments` MEDIUMBLOB COMMENT 'pb:10',
  `sent_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `expire_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `campaign_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `manifest_generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  PRIMARY KEY (`mail_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_mail_message` (`sender_kind`,`request_id`),
  INDEX `idx_mail_message_0` (`owner_player_id`,`mail_id`),
  INDEX `idx_mail_message_1` (`kind`,`expire_ms`),
  INDEX `idx_mail_message_2` (`guild_id`,`mail_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='mail_message';

CREATE TABLE IF NOT EXISTS `mail_campaign` (
  `sender_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `campaign_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `manifest_generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `mail_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `producer` MEDIUMTEXT COMMENT 'pb:6',
  `request_hash` MEDIUMTEXT COMMENT 'pb:7',
  `title` MEDIUMTEXT COMMENT 'pb:8',
  `body` MEDIUMTEXT COMMENT 'pb:9',
  `attachments` MEDIUMBLOB COMMENT 'pb:10',
  `expire_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `expected_recipient_count` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `expected_shard_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `manifest_hash` MEDIUMTEXT COMMENT 'pb:14',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `sealed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  PRIMARY KEY (`sender_kind`,`campaign_id`,`manifest_generation`),
  UNIQUE KEY `uk_mail_campaign` (`sender_kind`,`request_id`),
  INDEX `idx_mail_campaign_0` (`mail_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_campaign';

CREATE TABLE IF NOT EXISTS `mail_campaign_shard` (
  `sender_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `campaign_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `manifest_generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `shard_index` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `shard_hash` MEDIUMTEXT COMMENT 'pb:5',
  `recipient_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`sender_kind`,`campaign_id`,`manifest_generation`,`shard_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_campaign_shard';

CREATE TABLE IF NOT EXISTS `mail_campaign_recipient` (
  `sender_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `campaign_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `manifest_generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `shard_index` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`sender_kind`,`campaign_id`,`manifest_generation`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_campaign_recipient';

CREATE TABLE IF NOT EXISTS `mail_box` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_box';

CREATE TABLE IF NOT EXISTS `mail_state` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `mail_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `read_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `deleted_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `claim_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `current_op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `claim_version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `reason_tip_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  PRIMARY KEY (`player_id`,`mail_id`),
  INDEX `idx_mail_state_0` (`mail_id`,`claim_state`),
  INDEX `idx_mail_state_1` (`claim_state`,`updated_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_state';

CREATE TABLE IF NOT EXISTS `mail_player_op_seq` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `next_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`,`stream`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='mail_player_op_seq';

CREATE TABLE IF NOT EXISTS `mail_asset_op` (
  `op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `stream_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `mail_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `claim_version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `payload` MEDIUMBLOB COMMENT 'pb:9',
  `tx_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `lease_token` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `last_outcome` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `last_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `durable` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `reason_tip_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `last_send_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `resolved_by` MEDIUMTEXT COMMENT 'pb:22',
  `resolve_reason` MEDIUMTEXT COMMENT 'pb:23',
  PRIMARY KEY (`op_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_mail_asset_op` (`player_id`,`stream`,`stream_epoch`,`seq`),
  INDEX `idx_mail_asset_op_0` (`status`,`next_attempt_ms`,`op_id`,`player_id`),
  INDEX `idx_mail_asset_op_1` (`player_id`,`stream`,`stream_epoch`,`status`,`seq`),
  INDEX `idx_mail_asset_op_2` (`player_id`,`mail_id`,`claim_version`),
  INDEX `idx_mail_asset_op_3` (`mail_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='mail_asset_op';

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
