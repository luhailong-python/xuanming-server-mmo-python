-- trade 独占库 mmorpg_trade 的建表 DDL（与 Go 侧 proto/trade/trade_table.proto
-- 及 shared/rolefence/rolefence.go 逐字对齐；TiDB 扩展注释在 MySQL 下视作普通注释，每条语句幂等）。

CREATE TABLE IF NOT EXISTS `trade_listing` (
  `listing_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `seller_account` MEDIUMTEXT COMMENT 'pb:3',
  `market_zone` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `seller_zone_at_listing` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `category` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `subcategory` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `title` MEDIUMTEXT COMMENT 'pb:8',
  `level` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `price_fen` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `summary` MEDIUMTEXT COMMENT 'pb:12',
  `description` MEDIUMTEXT COMMENT 'pb:13',
  `icon_key` MEDIUMTEXT COMMENT 'pb:14',
  `notice_end_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `sale_end_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  PRIMARY KEY (`listing_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_trade_listing_0` (`market_zone`,`status`,`category`,`subcategory`,`price_fen`),
  INDEX `idx_trade_listing_1` (`status`,`category`,`subcategory`,`price_fen`),
  INDEX `idx_trade_listing_2` (`seller_player_id`,`listing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_listing';

CREATE TABLE IF NOT EXISTS `trade_favorite` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `listing_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`player_id`,`listing_id`) /*T![clustered_index] NONCLUSTERED */,
  INDEX `idx_trade_favorite_0` (`listing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_favorite';

CREATE TABLE IF NOT EXISTS `trade_player_op_seq` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `next_seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`player_id`,`stream`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_player_op_seq';

CREATE TABLE IF NOT EXISTS `trade_asset_op` (
  `op_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `stream` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `stream_epoch` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `seq` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `durable` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `deadline_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `lease_token` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `tx_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `ref_kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `ref_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `payload` MEDIUMBLOB COMMENT 'pb:17',
  `last_outcome` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `last_reason` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `resolved_by` MEDIUMTEXT COMMENT 'pb:22',
  `resolve_reason` MEDIUMTEXT COMMENT 'pb:23',
  `last_send_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:24',
  `escrow_snapshot` MEDIUMBLOB COMMENT 'pb:25',
  `receipt_ack_pending` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:26',
  `business_consumed` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:27',
  PRIMARY KEY (`op_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_trade_asset_op` (`player_id`,`stream`,`stream_epoch`,`seq`),
  INDEX `idx_trade_asset_op_0` (`status`,`next_attempt_ms`,`op_id`,`player_id`),
  INDEX `idx_trade_asset_op_1` (`player_id`,`stream`,`stream_epoch`,`status`,`seq`),
  INDEX `idx_trade_asset_op_2` (`ref_kind`,`ref_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_asset_op';

CREATE TABLE IF NOT EXISTS `trade_commerce` (
  `commerce_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `request_key` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2',
  `request_digest` MEDIUMBLOB COMMENT 'pb:3',
  `kind` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `status` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `seller_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `buyer_player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:7',
  `market_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `item_config_id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `side` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `unit_price` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `quantity` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `filled_quantity` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `currency_type` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `expires_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `version` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:18',
  `workflow_proto` MEDIUMBLOB COMMENT 'pb:19',
  `parent_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `counterparty_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `on_book` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  PRIMARY KEY (`commerce_id`) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY `uk_trade_commerce` (`request_key`),
  INDEX `idx_trade_commerce_0` (`status`,`updated_ms`),
  INDEX `idx_trade_commerce_1` (`seller_player_id`,`commerce_id`),
  INDEX `idx_trade_commerce_2` (`buyer_player_id`,`commerce_id`),
  INDEX `idx_trade_commerce_3` (`kind`,`market_id`,`item_config_id`,`side`,`status`,`on_book`,`unit_price`,`commerce_id`),
  INDEX `idx_trade_commerce_4` (`kind`,`status`,`expires_ms`),
  INDEX `idx_trade_commerce_5` (`parent_id`,`status`),
  INDEX `idx_trade_commerce_6` (`counterparty_id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='trade_commerce';

CREATE TABLE IF NOT EXISTS `trade_commerce_guard` (
  `guard_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  PRIMARY KEY (`guard_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='trade_commerce_guard';

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
