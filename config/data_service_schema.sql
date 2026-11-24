-- data_service global database (single logical shard, Directory.LogicalShards = 1).
-- DDL copied from the Go bootstrap constants (xuanming-server-mmo-cpp/go/data_service/internal/store/*.go,
-- go/shared/authoritysql/catalog.go); every statement is idempotent. TiDB-only /*T! ... */ comments are
-- plain comments on MySQL. Applied statement by statement (split on a line that ends with ';') at startup.
--
-- Not created here (owned by go/db / the player-store service): `player_store` in the player store databases.
-- Python-only addition: `data_service_id_counter` (see the last statement); Go keeps the id counter in etcd.

-- account_directory_store.go:27
CREATE TABLE IF NOT EXISTS account_directory (account VARBINARY(191) NOT NULL COMMENT 'pb:1',created_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:2',deletion_state INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:3',deletion_requested_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:4',deletion_execute_after_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:5',deletion_lease_until_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:6',deletion_source INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:7',real_name_pi VARBINARY(38) NOT NULL DEFAULT '' COMMENT 'pb:8',PRIMARY KEY(account),INDEX idx_account_directory_deletion(deletion_state,deletion_execute_after_ms)) ENGINE=InnoDB;

-- account_directory_store.go:34
CREATE TABLE IF NOT EXISTS account_role (player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',account VARBINARY(191) NOT NULL COMMENT 'pb:2',role_proto MEDIUMBLOB NOT NULL COMMENT 'pb:3',owner_revision BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',updated_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:5',trade_listing_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:6',initial_player_commit BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:7',fence_version BIGINT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'pb:8',prepare_token BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:9',PRIMARY KEY(player_id) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;

-- account_role_ref.go:46
CREATE TABLE IF NOT EXISTS account_role_ref (account VARBINARY(191) NOT NULL COMMENT 'pb:1',player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:2',state INT UNSIGNED NOT NULL COMMENT 'pb:3',tx_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:4',listing_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:5',home_zone INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:6',nonce BIGINT UNSIGNED NOT NULL COMMENT 'pb:7',created_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:8',held_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:9',held_reason VARBINARY(32) NOT NULL DEFAULT '' COMMENT 'pb:10',PRIMARY KEY(account,player_id) /*T![clustered_index] NONCLUSTERED */,INDEX idx_account_role_ref_state(state,held_ms,created_ms),INDEX idx_account_role_ref_player(player_id)) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;

-- account_role_admission_store.go:22
CREATE TABLE IF NOT EXISTS account_role_admission (player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',scene_instance_id VARBINARY(36) NOT NULL COMMENT 'pb:2',highest_generation BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',closed_through BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',identity_proto MEDIUMBLOB NOT NULL COMMENT 'pb:5',request_digest BINARY(32) NOT NULL COMMENT 'pb:6',required_owner_epoch BIGINT UNSIGNED NOT NULL COMMENT 'pb:7',required_save_seq BIGINT UNSIGNED NOT NULL COMMENT 'pb:8',close_digest VARBINARY(32) NOT NULL COMMENT 'pb:9',updated_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:10',PRIMARY KEY(player_id,scene_instance_id)) ENGINE=InnoDB;

-- schema.go:123
CREATE TABLE IF NOT EXISTS `player_name` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:2',
  `name_norm` VARCHAR(191) NOT NULL COMMENT 'pb:3',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `bound_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `held_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  PRIMARY KEY (`name_norm`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_name';

-- schema.go:136
CREATE TABLE IF NOT EXISTS `player_name_owner` (
  `player_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `name_norm` VARCHAR(191) NOT NULL COMMENT 'pb:2',
  `name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:3',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_name_owner';

-- schema.go:188
CREATE TABLE IF NOT EXISTS `player_route` (
  `route_shard` tinyint unsigned NOT NULL COMMENT 'pb:10',
  `player_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `home_zone` int unsigned NULL COMMENT 'pb:2',
  `storage_id` int unsigned NULL COMMENT 'pb:3',
  `placement_version` bigint unsigned NULL COMMENT 'pb:4',
  `rev` bigint unsigned NOT NULL COMMENT 'pb:6',
  `updated_by` varchar(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT 'pb:7',
  `created_at_ms` bigint unsigned NOT NULL COMMENT 'pb:8',
  `updated_at_ms` bigint unsigned NOT NULL COMMENT 'pb:9',
  PRIMARY KEY (`route_shard`, `player_id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_route_home` (`home_zone`, `route_shard`, `player_id`),
  KEY `idx_route_storage` (`storage_id`, `route_shard`, `player_id`),
  CONSTRAINT `chk_route_shard` CHECK (`route_shard` = `player_id` % 64),
  CONSTRAINT `chk_route_home` CHECK (`home_zone` IS NULL OR `home_zone` BETWEEN 1 AND 999999),
  CONSTRAINT `chk_route_place` CHECK ((`storage_id` IS NULL) = (`placement_version` IS NULL)),
  CONSTRAINT `chk_route_ver` CHECK (`placement_version` IS NULL OR `placement_version` >= 1),
  CONSTRAINT `chk_route_rev` CHECK (`rev` >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='player_route';

-- schema.go:210
CREATE TABLE IF NOT EXISTS `zone_merged_into` (
  `src_zone` int unsigned NOT NULL COMMENT 'pb:1',
  `dst_zone` int unsigned NOT NULL COMMENT 'pb:2',
  `run_id` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'pb:3',
  `merged_at_ms` bigint unsigned NOT NULL COMMENT 'pb:4',
  PRIMARY KEY (`src_zone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='zone_merged_into';

-- authoritysql/catalog.go:1216 (the row id=1 is seeded by the service, without ON DUPLICATE KEY, like Go)
CREATE TABLE IF NOT EXISTS `route_meta` (
  `id` tinyint unsigned NOT NULL COMMENT 'pb:1',
  `generation` varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'pb:2',
  `writable` tinyint unsigned NOT NULL DEFAULT 1 COMMENT 'pb:3',
  `writer_epoch` bigint unsigned NOT NULL DEFAULT 1 COMMENT 'pb:4',
  `catalog_revision` bigint unsigned NOT NULL DEFAULT 1 COMMENT 'pb:5',
  `freeze_run_id` varbinary(16) NOT NULL DEFAULT '' COMMENT 'pb:6',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='route_meta';

-- Python-only: single-process id counter for AllocateIdSegment (one shared counter, like the Go issuer's
-- "all tags share one counter"). Region bits are 0 (dev/test); next is the first unissued id.
CREATE TABLE IF NOT EXISTS data_service_id_counter (id TINYINT UNSIGNED NOT NULL,next_id BIGINT UNSIGNED NOT NULL,PRIMARY KEY(id)) ENGINE=InnoDB;

-- account_channel_identity.go:16
CREATE TABLE IF NOT EXISTS account_channel_identity (account VARBINARY(191) NOT NULL COMMENT 'pb:1',channel VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'pb:2',open_id VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:3',updated_ms BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:4',PRIMARY KEY(account)) ENGINE=InnoDB;

-- account_deletion.go:31
CREATE TABLE IF NOT EXISTS account_deletion_receipt (account_digest VARBINARY(32) NOT NULL COMMENT 'pb:1',requested_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:2',deleted_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',source INT UNSIGNED NOT NULL COMMENT 'pb:4',PRIMARY KEY(account_digest)) ENGINE=InnoDB;

-- account_role_removal.go:18
CREATE TABLE IF NOT EXISTS account_role_removal (player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',account VARBINARY(191) NOT NULL COMMENT 'pb:2',owner_revision BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',started_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',PRIMARY KEY(player_id) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;

-- account_role_rename.go:46
CREATE TABLE IF NOT EXISTS `account_role_rename` (
  `player_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `seq` bigint unsigned NOT NULL COMMENT 'pb:2',
  `state` int unsigned NOT NULL COMMENT 'pb:3',
  `result` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `new_name` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:5',
  `new_norm` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `old_norm` VARCHAR(191) NOT NULL DEFAULT '' COMMENT 'pb:7',
  `renamed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `held_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_account_role_rename_open` (`state`, `updated_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='account_role_rename';

-- account_role_trade_store.go:30
CREATE TABLE IF NOT EXISTS account_role_trade (listing_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:2',seller_account VARBINARY(191) NOT NULL COMMENT 'pb:3',seller_owner_revision BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',request_digest BINARY(32) NOT NULL COMMENT 'pb:5',state INT UNSIGNED NOT NULL COMMENT 'pb:6',freeze_evidence MEDIUMBLOB NOT NULL COMMENT 'pb:7',created_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:8',updated_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:9',required_owner_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:10',required_save_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'pb:11',PRIMARY KEY(listing_id) /*T![clustered_index] NONCLUSTERED */,INDEX idx_account_role_trade_state(state,created_ms)) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;

-- account_role_trade_store.go:31
CREATE TABLE IF NOT EXISTS account_role_transfer_receipt (tx_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',request_digest BINARY(32) NOT NULL COMMENT 'pb:2',listing_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:3',player_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:4',seller_account VARBINARY(191) NOT NULL COMMENT 'pb:5',buyer_account VARBINARY(191) NOT NULL COMMENT 'pb:6',seller_owner_revision BIGINT UNSIGNED NOT NULL COMMENT 'pb:7',buyer_owner_revision BIGINT UNSIGNED NOT NULL COMMENT 'pb:8',created_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:9',role_proto MEDIUMBLOB NOT NULL COMMENT 'pb:10',PRIMARY KEY(tx_id) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */;

-- account_role_admission_store.go:23
CREATE TABLE IF NOT EXISTS account_role_trade_holder (listing_id BIGINT UNSIGNED NOT NULL COMMENT 'pb:1',scene_instance_id VARBINARY(36) NOT NULL COMMENT 'pb:2',seal_proto MEDIUMBLOB NOT NULL COMMENT 'pb:3',drained INT UNSIGNED NOT NULL COMMENT 'pb:4',ack_digest VARBINARY(32) NOT NULL COMMENT 'pb:5',updated_ms BIGINT UNSIGNED NOT NULL COMMENT 'pb:6',PRIMARY KEY(listing_id,scene_instance_id)) ENGINE=InnoDB;

-- rollback_database_table.proto: player_snapshot
CREATE TABLE IF NOT EXISTS `player_snapshot` (
  `id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `zone_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `snapshot_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `created_at` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `reason` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'pb:6',
  `operator` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:7',
  `data` MEDIUMBLOB NOT NULL COMMENT 'pb:8',
  `snapshot_guid` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `source` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_player_snapshot_player` (`player_id`, `created_at`),
  KEY `idx_player_snapshot_guid` (`snapshot_guid`),
  KEY `idx_player_snapshot_zone` (`zone_id`, `created_at`),
  KEY `idx_player_snapshot_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_snapshot';

-- rollback_database_table.proto: rollback_audit_log
CREATE TABLE IF NOT EXISTS `rollback_audit_log` (
  `id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `zone_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `rollback_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `snapshot_id_used` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `pre_rollback_snapshot_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `target_time` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `players_affected` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `players_failed` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `reason` TEXT NOT NULL COMMENT 'pb:10',
  `operator` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:11',
  `created_at` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `orphans_cleaned` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='rollback_audit_log';

-- rollback_database_table.proto: transaction_log
CREATE TABLE IF NOT EXISTS `transaction_log` (
  `tx_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `timestamp_sec` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `tx_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `from_player` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `to_player` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `item_uuid` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `item_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `item_quantity` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `currency_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `currency_delta` bigint NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `balance_before` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `balance_after` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `correlation_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `extra` MEDIUMTEXT NOT NULL COMMENT 'pb:14',
  `zone_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `shard` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  PRIMARY KEY (`tx_id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_txlog_from` (`from_player`, `timestamp_sec`),
  KEY `idx_txlog_to` (`to_player`, `timestamp_sec`),
  KEY `idx_txlog_to_corr` (`to_player`, `correlation_id`),
  KEY `idx_txlog_item` (`item_config_id`, `timestamp_sec`),
  KEY `idx_txlog_type` (`tx_type`, `timestamp_sec`),
  KEY `idx_txlog_ts` (`timestamp_sec`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='transaction_log';

-- rollback_database_table.proto: gm_recall_batch
CREATE TABLE IF NOT EXISTS `gm_recall_batch` (
  `request_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `request_digest` VARBINARY(32) NOT NULL COMMENT 'pb:2',
  `plan_digest` VARBINARY(32) NOT NULL COMMENT 'pb:3',
  `recall_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `item_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `currency_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `time_start` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `time_end` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `operator` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:9',
  `reason` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'pb:10',
  `peer_addr` VARCHAR(128) NOT NULL DEFAULT '' COMMENT 'pb:11',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `player_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `op_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `open_ops` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `source_rows` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `ops_complete` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `ops_shortfall` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `ops_failed` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `amount_requested` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `amount_taken` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `amount_debt` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  `amount_shortfall` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:23',
  `cancelled_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:24',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:26',
  `finished_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:27',
  PRIMARY KEY (`request_id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_gm_recall_batch_state` (`state`, `created_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='gm_recall_batch';

-- rollback_database_table.proto: gm_recall_op
CREATE TABLE IF NOT EXISTS `gm_recall_op` (
  `op_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `stream_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `payload` MEDIUMBLOB NOT NULL COMMENT 'pb:8',
  `tx_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `lease_token` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `last_outcome` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `last_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `durable` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `last_send_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `recall_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `currency_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  `item_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:23',
  `requested` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:24',
  `taken` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  `debt` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:26',
  `shortfall` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:27',
  `resolved_by` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'pb:28',
  `resolve_reason` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'pb:29',
  PRIMARY KEY (`op_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_gm_recall_op_stream` (`player_id`, `stream`, `stream_epoch`, `seq`),
  KEY `idx_gm_recall_op_due` (`status`, `next_attempt_ms`),
  KEY `idx_gm_recall_op_player` (`player_id`, `status`),
  KEY `idx_gm_recall_op_req` (`request_id`, `op_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='gm_recall_op';

-- rollback_database_table.proto: gm_recall_seq
CREATE TABLE IF NOT EXISTS `gm_recall_seq` (
  `player_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `stream` int unsigned NOT NULL COMMENT 'pb:2',
  `next_seq` bigint unsigned NOT NULL DEFAULT 1 COMMENT 'pb:3',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`, `stream`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='gm_recall_seq';

-- rollback_database_table.proto: gm_recall_source
CREATE TABLE IF NOT EXISTS `gm_recall_source` (
  `tx_id` bigint unsigned NOT NULL COMMENT 'pb:1',
  `request_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`tx_id`) /*T![clustered_index] NONCLUSTERED */,
  KEY `idx_gm_recall_source_req` (`request_id`),
  KEY `idx_gm_recall_source_op` (`op_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='gm_recall_source';
