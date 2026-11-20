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
