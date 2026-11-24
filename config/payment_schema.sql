-- payment 独占库 mmorpg_payment 的建表 DDL（与 Go 侧 proto/payment/payment_table.proto
-- 及 proto2mysql / schemamigrate 逐字对齐；全部 4 张表均幂等，按“分号 + 换行”逐条执行）。

CREATE TABLE IF NOT EXISTS `payment_intent` (
  `order_hi` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `order_lo` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `order_id` MEDIUMTEXT COMMENT 'pb:3',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `channel` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `amount_minor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `currency` MEDIUMTEXT COMMENT 'pb:7',
  `state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `expire_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `next_check_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:10',
  `check_count` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `checked_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:13',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `channel_ref` MEDIUMTEXT COMMENT 'pb:15',
  `channel_ref_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:16',
  `sku_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `payer_id` MEDIUMTEXT COMMENT 'pb:18',
  `spender_hi` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:19',
  `spender_lo` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `paid_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `minor_age` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  PRIMARY KEY (`order_hi`,`order_lo`),
  INDEX `idx_payment_intent_0` (`state`,`next_check_ms`),
  INDEX `idx_payment_intent_1` (`spender_hi`,`spender_lo`,`state`,`paid_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='payment_intent';

CREATE TABLE IF NOT EXISTS `payment_spend` (
  `spender_hi` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `spender_lo` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  PRIMARY KEY (`spender_hi`,`spender_lo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='payment_spend';

CREATE TABLE IF NOT EXISTS `payment_txn` (
  `channel` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `txn_hi` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `txn_lo` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `transaction_id` MEDIUMTEXT COMMENT 'pb:4',
  `order_hi` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  `order_lo` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:6',
  `out_trade_no` MEDIUMTEXT COMMENT 'pb:7',
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:8',
  `paid_amount_minor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:9',
  `paid_currency` MEDIUMTEXT COMMENT 'pb:10',
  `paid_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:11',
  `mall_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:12',
  `mall_refund_reason` MEDIUMTEXT COMMENT 'pb:13',
  `mall_attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:14',
  `next_attempt_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:15',
  `mall_last_error` MEDIUMTEXT COMMENT 'pb:16',
  `refund_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:17',
  `out_refund_no` MEDIUMTEXT COMMENT 'pb:18',
  `refund_id` MEDIUMTEXT COMMENT 'pb:19',
  `refund_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:20',
  `refund_attempts` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:21',
  `refund_next_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:22',
  `refund_last_error` MEDIUMTEXT COMMENT 'pb:23',
  `created_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:24',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:25',
  `fact_revision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:26',
  `fact_observed_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:27',
  `mall_revision` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:28',
  `mall_due` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:29',
  `channel_token` MEDIUMTEXT COMMENT 'pb:30',
  `channel_order_id` MEDIUMTEXT COMMENT 'pb:31',
  `store_amount_minor` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:32',
  `store_currency` MEDIUMTEXT COMMENT 'pb:33',
  `finalize_state` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:34',
  `finalize_next_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:35',
  `product_id` MEDIUMTEXT COMMENT 'pb:36',
  PRIMARY KEY (`channel`,`txn_hi`,`txn_lo`),
  INDEX `idx_payment_txn_0` (`mall_due`,`next_attempt_ms`),
  INDEX `idx_payment_txn_1` (`finalize_state`,`finalize_next_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='payment_txn';

CREATE TABLE IF NOT EXISTS `payment_channel_cursor` (
  `channel` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `cursor_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:2',
  `lease_until_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:3',
  `lease_owner` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:4',
  `updated_ms` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:5',
  PRIMARY KEY (`channel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='payment_channel_cursor';
