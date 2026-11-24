-- match 与 team 独占库 mmorpg_match 的建表 DDL
-- 与 Go 侧 proto/team/team_authority.proto、proto/match/match_authority.proto 及 shared/rolefence/rolefence.go 对齐；
-- TiDB 扩展注释在标准 MySQL 下视作普通注释，每条语句幂等可执行。

CREATE TABLE IF NOT EXISTS `team_authority` (
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `payload` MEDIUMBLOB COMMENT 'pb:3',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `idle_expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`team_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_team_authority_0` (`state`,`idle_expires_ms`,`team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_authority';

CREATE TABLE IF NOT EXISTS `team_membership` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_membership';

CREATE TABLE IF NOT EXISTS `team_invite_index` (
  `invitee_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`invitee_id`,`team_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_team_invite_index_0` (`invitee_id`,`expires_ms`,`team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_invite_index';

CREATE TABLE IF NOT EXISTS `team_application_index` (
  `applicant_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`applicant_id`,`team_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_team_application_index_0` (`applicant_id`,`expires_ms`,`team_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_application_index';

CREATE TABLE IF NOT EXISTS `team_action_receipt` (
  `actor_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_high` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `request_low` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `method` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `payload` MEDIUMBLOB COMMENT 'pb:5',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `result` MEDIUMBLOB COMMENT 'pb:8',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  PRIMARY KEY (`actor_id`,`request_high`,`request_low`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_action_receipt';

CREATE TABLE IF NOT EXISTS `team_launch` (
  `actor_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_high` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `request_low` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `team_version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `lock_token` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `target_size` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `member_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `enqueued_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `not_before_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `payload` MEDIUMBLOB COMMENT 'pb:14',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:15',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `pool_region` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  PRIMARY KEY (`actor_id`,`request_high`,`request_low`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_team_launch_0` (`state`,`enqueued_ms`,`actor_id`,`request_high`,`request_low`),
  INDEX `idx_team_launch_1` (`team_id`,`state`,`enqueued_ms`,`actor_id`,`request_high`,`request_low`),
  INDEX `idx_team_launch_2` (`state`,`mode`,`config_id`,`target_size`,`enqueued_ms`,`actor_id`,`request_high`,`request_low`),
  INDEX `idx_team_launch_3` (`state`,`mode`,`config_id`,`target_size`,`pool_region`,`enqueued_ms`,`actor_id`,`request_high`,`request_low`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_launch';

CREATE TABLE IF NOT EXISTS `team_launch_role` (
  `actor_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_high` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `request_low` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`actor_id`,`request_high`,`request_low`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_team_launch_role_0` (`player_id`,`state`,`actor_id`,`request_high`,`request_low`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='team_launch_role';

CREATE TABLE IF NOT EXISTS `match_network_profile` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `payload` MEDIUMBLOB COMMENT 'pb:2',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_network_profile';

CREATE TABLE IF NOT EXISTS `match_authority_ticket` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `ticket` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `request_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:3',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `team_size` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `enqueued_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `not_before_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `zone_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `rating_centi` bigint NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `formation_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `team_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `network_profile` MEDIUMBLOB COMMENT 'pb:14',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `selected_region` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:18',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `pool_region` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `ranked_context` MEDIUMBLOB COMMENT 'pb:21',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_authority_ticket_0` (`state`,`mode`,`config_id`,`team_size`,`team_id`,`enqueued_ms`,`player_id`),
  INDEX `idx_match_authority_ticket_1` (`state`,`enqueued_ms`,`player_id`),
  INDEX `idx_match_authority_ticket_2` (`state`,`mode`,`config_id`,`team_size`,`team_id`,`pool_region`,`enqueued_ms`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_authority_ticket';

CREATE TABLE IF NOT EXISTS `match_join_intent` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_high` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `request_low` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `request_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `ticket_record` MEDIUMBLOB COMMENT 'pb:3',
  `cancelled` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:7',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `spectator_exit` MEDIUMBLOB COMMENT 'pb:9',
  `exit_prepared` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `request_payload` MEDIUMBLOB COMMENT 'pb:11',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  PRIMARY KEY (`player_id`,`request_high`,`request_low`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_join_intent_0` (`state`,`player_id`,`request_high`,`request_low`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_join_intent';

CREATE TABLE IF NOT EXISTS `match_role_occupancy` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `owner_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `owner_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:4',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_role_occupancy_0` (`owner_kind`,`owner_id`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_role_occupancy';

CREATE TABLE IF NOT EXISTS `match_challenge_invitation` (
  `challenge_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `challenger_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `target_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `challenger_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `challenger_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `target_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:7',
  `target_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  PRIMARY KEY (`challenge_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_challenge_invitation_0` (`target_player_id`,`state`,`expires_ms`,`challenge_id`),
  INDEX `idx_match_challenge_invitation_1` (`challenger_player_id`,`state`,`expires_ms`,`challenge_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_challenge_invitation';

CREATE TABLE IF NOT EXISTS `match_formation` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `operation_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `physical_region` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `target_node_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `target_instance_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `target_endpoint` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'pb:7',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `members` MEDIUMBLOB COMMENT 'pb:10',
  `create_request` MEDIUMBLOB COMMENT 'pb:11',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `retry_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `activity_context` MEDIUMBLOB COMMENT 'pb:15',
  `requeue_on_failure` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `prepared` MEDIUMBLOB COMMENT 'pb:17',
  `outcome_protocol_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `preflight_supported_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `preflight_instance_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:20',
  `adopted_outcome_protocol_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `adopted_outcome_observed` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:22',
  `release_track` VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'pb:23',
  `ranked_context` MEDIUMBLOB COMMENT 'pb:24',
  `ranked_season_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_formation_0` (`state`,`retry_at_ms`),
  INDEX `idx_match_formation_1` (`ranked_season_id`,`state`,`battle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_formation';

CREATE TABLE IF NOT EXISTS `match_battle_completion` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `raw_payload` MEDIUMBLOB COMMENT 'pb:2',
  `payload_digest` VARBINARY(64) NOT NULL DEFAULT '' COMMENT 'pb:3',
  `format_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `archived_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `source_topic` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `source_partition` int NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `source_offset` bigint NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `decision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `source_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `placement_operation_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:11',
  `target_instance_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:12',
  `outcome_protocol_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `result_published` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `next_retry_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_battle_completion_0` (`decision`,`result_published`,`next_retry_ms`,`battle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_completion';

CREATE TABLE IF NOT EXISTS `match_battle_settlement_source` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `settlement_proto` MEDIUMBLOB COMMENT 'pb:3',
  `payload_digest` VARBINARY(64) NOT NULL DEFAULT '' COMMENT 'pb:4',
  `archived_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `issued_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `inbox_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `inbox_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `dispatch_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `next_retry_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  PRIMARY KEY (`battle_id`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_battle_settlement_source_0` (`dispatch_state`,`next_retry_ms`,`battle_id`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_settlement_source';

CREATE TABLE IF NOT EXISTS `match_spectator` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `operation_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `target_endpoint` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'pb:4',
  `target_instance_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `request_payload` MEDIUMBLOB COMMENT 'pb:7',
  `owner_account` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:8',
  `owner_revision` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `add_request` MEDIUMBLOB COMMENT 'pb:10',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `request_high` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `request_low` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_spectator_0` (`updated_ms`,`player_id`),
  INDEX `idx_match_spectator_1` (`state`,`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_spectator';

CREATE TABLE IF NOT EXISTS `match_battle_report` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `match_mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `battle_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `envelope` MEDIUMBLOB COMMENT 'pb:5',
  `envelope_digest` VARBINARY(64) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `ref_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_report';

CREATE TABLE IF NOT EXISTS `match_player_battle_report` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `summary` MEDIUMBLOB COMMENT 'pb:4',
  PRIMARY KEY (`player_id`,`created_ms`,`battle_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_player_battle_report';

CREATE TABLE IF NOT EXISTS `match_manual_resolution` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `action` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `formation_class` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `failed_check` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:4',
  `operator_name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `reason` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `resolved_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `formation_digest_before` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:8',
  `formation_before` MEDIUMBLOB COMMENT 'pb:9',
  `tickets_before` MEDIUMBLOB COMMENT 'pb:10',
  `effective_deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `replica_instance` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:12',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_manual_resolution';

-- ---- 战斗剧本表（proto/match/match_authority.proto）----

CREATE TABLE IF NOT EXISTS `match_battle_script` (
  `script_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `owner_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `revision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `content` MEDIUMBLOB COMMENT 'pb:4',
  `review_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `approved_revision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `approved_content` MEDIUMBLOB COMMENT 'pb:7',
  `reject_reason` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:8',
  `submitted_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `reviewed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  PRIMARY KEY (`script_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_battle_script_0` (`owner_player_id`,`script_id`),
  INDEX `idx_match_battle_script_1` (`review_state`,`submitted_ms`,`script_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_script';

CREATE TABLE IF NOT EXISTS `match_battle_script_review` (
  `script_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `revision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `owner_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `approved` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `reviewer` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `reason` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `reviewed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `content` MEDIUMBLOB COMMENT 'pb:8',
  PRIMARY KEY (`script_id`,`revision`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_battle_script_review_0` (`reviewed_ms`,`script_id`,`revision`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_script_review';

CREATE TABLE IF NOT EXISTS `match_battle_script_loadout` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `match_mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `battle_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `origin` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `official_script_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `player_script_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  PRIMARY KEY (`player_id`,`match_mode`,`battle_config_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_battle_script_loadout';

-- ---- Elo 评分与投榜发件箱表（proto/match/match_authority.proto）----

CREATE TABLE IF NOT EXISTS `match_rating` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `rating_centi` bigint NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `games` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `score_changed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`,`mode`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_rating';

CREATE TABLE IF NOT EXISTS `match_rating_applied` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `payload_digest` VARBINARY(64) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `applied_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`battle_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_rating_applied';

CREATE TABLE IF NOT EXISTS `match_rank_source` (
  `source_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `produced_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `committed_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `stopped` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`source_epoch`,`mode`,`shard`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_rank_source';

CREATE TABLE IF NOT EXISTS `match_rank_outbox` (
  `source_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `source_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `entries` MEDIUMBLOB COMMENT 'pb:4',
  PRIMARY KEY (`source_epoch`,`mode`,`shard`,`source_cursor`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_rank_outbox';

CREATE TABLE IF NOT EXISTS `match_rank_removal` (
  `mode` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`mode`,`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_rank_removal';

-- ---- 论剑排位首发 V1 权威表（proto/match/match_authority.proto）----

CREATE TABLE IF NOT EXISTS `match_ranked_season` (
  `season_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `season_key` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `season_name` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:3',
  `rank_generation` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `source_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `rule_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `rollover_stage` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `start_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `end_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `rank_board_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `expected_rank_cursor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `champion_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `champion_name` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:14',
  `champion_rating_centi` bigint NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  PRIMARY KEY (`season_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_ranked_season_0` (`state`,`season_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_ranked_season';

CREATE TABLE IF NOT EXISTS `match_ranked_player_prefs` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `character_preset_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `pet_preset_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `rule_version` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `cosmetic_loadout` MEDIUMBLOB COMMENT 'pb:5',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_ranked_player_prefs';

CREATE TABLE IF NOT EXISTS `match_ranked_player_season` (
  `season_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `qualified` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `qualified_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `rating_centi` bigint NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `peak_rating_centi` bigint NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `peak_tier` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `valid_matches` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `wins` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `losses` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `draws` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `milestone_bits` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `honor_bits` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `final_rank` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  PRIMARY KEY (`season_id`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_ranked_player_season_0` (`player_id`,`season_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_ranked_player_season';

CREATE TABLE IF NOT EXISTS `match_ranked_reward` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `season_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `reward_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `tier_value` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `item_code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `granted_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`player_id`,`season_id`,`reward_kind`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_ranked_reward';

CREATE TABLE IF NOT EXISTS `match_ranked_battle` (
  `battle_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `season_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `settled_at_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `summary` MEDIUMBLOB COMMENT 'pb:5',
  PRIMARY KEY (`battle_id`,`player_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_match_ranked_battle_0` (`player_id`,`settled_at_ms`,`battle_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='match_ranked_battle';

CREATE TABLE IF NOT EXISTS `role_fence` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `account` VARBINARY(191) NOT NULL,
  `owner_revision` BIGINT UNSIGNED NOT NULL,
  `freeze_token` BIGINT UNSIGNED NOT NULL,
  `freeze_final` TINYINT(1) NOT NULL,
  `version` BIGINT UNSIGNED NOT NULL,
  `home_zone` INT UNSIGNED NOT NULL,
  `updated_ms` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;
