-- guild 独占库 mmorpg_guild 的建表 DDL（与 Go 侧 proto/guild/guild_db.proto
-- 及 shared/rolefence/rolefence.go 逐字对齐；TiDB 扩展注释在 MySQL 下视作普通注释，每条语句幂等）。

CREATE TABLE IF NOT EXISTS `guild` (
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `name` MEDIUMTEXT COMMENT 'pb:2',
  `leader_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `level` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `announcement` MEDIUMTEXT COMMENT 'pb:5',
  `create_time_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `max_members` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `zone_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `score` bigint NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `funds` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `name_norm` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:11',
  `score_version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `renamed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  PRIMARY KEY (`guild_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_guild` (`name_norm`),
  INDEX `idx_guild_0` (`zone_id`),
  INDEX `idx_guild_1` (`leader_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild';

CREATE TABLE IF NOT EXISTS `guild_trial_lobby` (
  `lobby_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `activity_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `initiator_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `view_proto` MEDIUMBLOB COMMENT 'pb:6',
  `original_proto` MEDIUMBLOB COMMENT 'pb:7',
  `consent_proto` MEDIUMBLOB COMMENT 'pb:8',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `launch_deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  PRIMARY KEY (`lobby_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_lobby_0` (`guild_id`,`state`,`lobby_id`),
  INDEX `idx_guild_trial_lobby_1` (`state`,`updated_ms`,`lobby_id`),
  INDEX `idx_guild_trial_lobby_2` (`state`,`lobby_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_lobby';

CREATE TABLE IF NOT EXISTS `guild_trial_lobby_slot` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `lobby_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `cooldown_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_lobby_slot_0` (`lobby_id`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_lobby_slot';

CREATE TABLE IF NOT EXISTS `guild_trial_launch` (
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `original_proto` MEDIUMBLOB COMMENT 'pb:4',
  `request_digest` MEDIUMBLOB COMMENT 'pb:5',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `cancel_receipt` MEDIUMBLOB COMMENT 'pb:8',
  `next_check_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `check_attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `realtime_end_tip` MEDIUMBLOB COMMENT 'pb:11',
  PRIMARY KEY (`request_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_launch_0` (`state`,`updated_ms`),
  INDEX `idx_guild_trial_launch_1` (`battle_id`),
  INDEX `idx_guild_trial_launch_2` (`state`,`next_check_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_launch';

CREATE TABLE IF NOT EXISTS `guild_trial_launch_role` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `account` MEDIUMTEXT COMMENT 'pb:3',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`,`request_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_launch_role_0` (`request_id`,`player_id`),
  INDEX `idx_guild_trial_launch_role_1` (`player_id`,`state`,`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_launch_role';

CREATE TABLE IF NOT EXISTS `guild_player_state` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_player_state';

CREATE TABLE IF NOT EXISTS `guild_member` (
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `role` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `join_time_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `last_active_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `contribution_total` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `contribution_balance` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  PRIMARY KEY (`guild_id`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_guild_member` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_member';

CREATE TABLE IF NOT EXISTS `guild_application` (
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `apply_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `expire_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  PRIMARY KEY (`guild_id`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_application_0` (`player_id`),
  INDEX `idx_guild_application_1` (`expire_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_application';

CREATE TABLE IF NOT EXISTS `guild_player_op_seq` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `next_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`,`stream`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_player_op_seq';

CREATE TABLE IF NOT EXISTS `guild_asset_op` (
  `op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `durable` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `payload` MEDIUMBLOB COMMENT 'pb:12',
  `ref_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `ref_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `contribution_delta` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `funds_delta` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `reason_tip_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `lease_token` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  `tx_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:23',
  `last_outcome` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:24',
  `last_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  `stream_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:26',
  `resolved_by` MEDIUMTEXT COMMENT 'pb:27',
  `resolve_reason` MEDIUMTEXT COMMENT 'pb:28',
  `last_send_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:29',
  PRIMARY KEY (`op_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_guild_asset_op` (`player_id`,`stream`,`stream_epoch`,`seq`),
  INDEX `idx_guild_asset_op_0` (`status`,`next_attempt_ms`,`op_id`,`player_id`),
  INDEX `idx_guild_asset_op_1` (`guild_id`,`op_id`),
  INDEX `idx_guild_asset_op_2` (`player_id`,`stream`,`stream_epoch`,`status`,`seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_asset_op';

CREATE TABLE IF NOT EXISTS `guild_daily_counter` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `counter_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `ref_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `used_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`player_id`,`counter_kind`,`ref_id`,`period_key`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_daily_counter_0` (`period_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_daily_counter';

CREATE TABLE IF NOT EXISTS `guild_activity_progress` (
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `activity_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `progress_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `threshold_reached_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `funds_granted` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  PRIMARY KEY (`guild_id`,`activity_id`,`period_key`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_activity_progress';

CREATE TABLE IF NOT EXISTS `guild_trial_battle` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `activity_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `guild_period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `initiator_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `settle_result` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `rewarded_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `settled_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `launch_request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_battle_0` (`guild_id`,`state`),
  INDEX `idx_guild_trial_battle_1` (`state`,`created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_battle';

CREATE TABLE IF NOT EXISTS `guild_trial_reward_owed` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `activity_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `period_key` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `payload` MEDIUMBLOB COMMENT 'pb:6',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  PRIMARY KEY (`player_id`,`battle_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_trial_reward_owed_0` (`created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_trial_reward_owed';

CREATE TABLE IF NOT EXISTS `guild_client_request` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `client_request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  PRIMARY KEY (`player_id`,`client_request_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_guild_client_request_0` (`created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_client_request';

CREATE TABLE IF NOT EXISTS `guild_name_hold` (
  `hold_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `name_norm` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2',
  `guild_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `held_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  PRIMARY KEY (`hold_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_guild_name_hold` (`name_norm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='guild_name_hold';

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
