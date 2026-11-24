-- 商城服务（mall_game）独立库表结构（对应 Java mall_game schema.sql 与 V2-V5 迁移）。
-- 金额列全部采用最小货币单位 BIGINT；player_id / listing_id 使用规范 uint64 十进制字符串；
-- 外部/不透明标识列（account / request_id / channel / transaction_id / nonce / actor）采用 VARBINARY。

CREATE TABLE IF NOT EXISTS commerce_schema_version (
  version INT PRIMARY KEY,
  storage_contract VARCHAR(80) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_buyer_guard (
  buyer_id VARCHAR(20) PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS pms_sku_stock (
  id BIGINT PRIMARY KEY,
  product_id BIGINT NOT NULL,
  sku_code VARCHAR(64) NOT NULL,
  price_minor BIGINT NOT NULL,
  currency VARCHAR(16) NOT NULL,
  stock INT NOT NULL,
  lock_stock INT NOT NULL DEFAULT 0,
  published INT NOT NULL DEFAULT 0,
  catalog_revision BIGINT NOT NULL DEFAULT 0,
  CHECK (price_minor > 0),
  CHECK (stock >= 0),
  CHECK (lock_stock >= 0 AND lock_stock <= stock)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS oms_order (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_sn VARCHAR(36) NOT NULL,
  buyer_id VARCHAR(20) NOT NULL,
  request_id VARBINARY(128) NOT NULL,
  sku_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  pay_amount BIGINT NOT NULL,
  currency VARCHAR(16) NOT NULL,
  status INT NOT NULL,
  create_time_ms BIGINT NOT NULL,
  payment_deadline_ms BIGINT NOT NULL,
  payment_time_ms BIGINT NULL,
  UNIQUE KEY uk_oms_order_sn (order_sn),
  UNIQUE KEY uk_oms_buyer_request (buyer_id, request_id),
  INDEX idx_commerce_buyer_status (buyer_id, status),
  INDEX idx_commerce_status_deadline (status, payment_deadline_ms),
  CHECK (quantity > 0),
  CHECK (pay_amount > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS oms_order_item (
  order_sn VARCHAR(36) PRIMARY KEY,
  product_id BIGINT NOT NULL,
  product_sku_id BIGINT NOT NULL,
  product_sku_code VARCHAR(64) NOT NULL,
  product_quantity INT NOT NULL,
  product_price_minor BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_official_owner (
  order_sn VARCHAR(36) PRIMARY KEY,
  buyer_account VARBINARY(191) NOT NULL,
  buyer_owner_revision VARBINARY(20) NOT NULL,
  INDEX idx_official_owner (buyer_account, buyer_owner_revision, order_sn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_payment_receipt (
  channel VARBINARY(32) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  order_sn VARCHAR(36) NULL,
  amount_minor BIGINT NOT NULL,
  currency VARCHAR(16) NOT NULL,
  revision INT NOT NULL DEFAULT 0,
  fact VARCHAR(8) NOT NULL DEFAULT 'PAID',
  PRIMARY KEY (channel, transaction_id),
  UNIQUE KEY uk_payment_receipt_order (order_sn),
  CHECK (revision >= 0),
  CHECK (fact IN ('PAID', 'REVOKED')),
  CHECK ((revision % 2 = 1) = (fact = 'REVOKED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_abnormal_payment (
  channel VARBINARY(32) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  order_sn VARCHAR(36) NOT NULL,
  reason VARCHAR(24) NOT NULL,
  created_ms BIGINT NOT NULL,
  PRIMARY KEY (channel, transaction_id),
  INDEX idx_commerce_abnormal_order (order_sn),
  CHECK (reason IN ('DUPLICATE_PAYMENT', 'AMOUNT_MISMATCH', 'CURRENCY_MISMATCH')),
  CHECK (created_ms >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_delivery_intent (
  order_sn VARCHAR(36) PRIMARY KEY,
  buyer_id VARCHAR(20) NOT NULL,
  sku_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  status VARCHAR(24) NOT NULL,
  created_ms BIGINT NOT NULL,
  INDEX idx_commerce_delivery_pending (status, created_ms, order_sn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_request_nonce (
  caller_role VARCHAR(16) NOT NULL,
  nonce VARBINARY(128) NOT NULL,
  expires_ms BIGINT NOT NULL,
  PRIMARY KEY (caller_role, nonce),
  INDEX idx_commerce_nonce_expiry (expires_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_sku_grant (
  sku_id BIGINT PRIMARY KEY,
  item_config_id INT NOT NULL,
  count_per_unit INT NOT NULL,
  currency_type INT NOT NULL DEFAULT 0,
  currency_amount BIGINT NOT NULL DEFAULT 0,
  title VARCHAR(128) NOT NULL,
  CHECK ((item_config_id > 0 AND count_per_unit > 0 AND count_per_unit <= 4096 AND currency_type = 0 AND currency_amount = 0)
    OR (item_config_id = 0 AND count_per_unit = 0 AND currency_type IN (0, 1, 2) AND currency_amount > 0 AND currency_amount <= 9000000000000000))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_order_grant (
  order_sn VARCHAR(36) PRIMARY KEY,
  item_config_id INT NOT NULL,
  count_per_unit INT NOT NULL,
  currency_type INT NOT NULL DEFAULT 0,
  currency_amount BIGINT NOT NULL DEFAULT 0,
  title VARCHAR(128) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_delivery_receipt (
  order_sn VARCHAR(36) PRIMARY KEY,
  trade_order_id VARCHAR(20) NOT NULL,
  state VARCHAR(24) NOT NULL,
  proof VARCHAR(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_refund_intent (
  channel VARBINARY(32) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  order_sn VARCHAR(36) NOT NULL,
  source VARCHAR(24) NOT NULL,
  status VARCHAR(24) NOT NULL,
  close_reason VARCHAR(64) NULL,
  PRIMARY KEY (channel, transaction_id),
  INDEX idx_commerce_refund_pending (status, order_sn, channel, transaction_id),
  CHECK (source IN ('CANCELLED_LATE', 'DELIVERY_REJECTED', 'DUPLICATE_PAYMENT', 'AMOUNT_MISMATCH', 'CURRENCY_MISMATCH')),
  CHECK (status IN ('PENDING', 'REFUNDED', 'MANUAL', 'REVOKED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_refund_receipt (
  channel VARBINARY(32) NOT NULL,
  payment_transaction_id VARBINARY(128) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  order_sn VARCHAR(36) NOT NULL,
  amount_minor BIGINT NOT NULL,
  currency VARCHAR(16) NOT NULL,
  PRIMARY KEY (channel, payment_transaction_id),
  UNIQUE KEY uk_refund_receipt_txn (channel, transaction_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_admin_command (
  actor VARBINARY(64) NOT NULL,
  request_id VARBINARY(128) NOT NULL,
  command_type VARCHAR(16) NOT NULL,
  request_digest VARCHAR(64) NOT NULL,
  result_json MEDIUMTEXT NOT NULL,
  created_ms BIGINT NOT NULL,
  PRIMARY KEY (actor, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_mock_transaction (
  kind VARCHAR(8) NOT NULL,
  order_sn VARCHAR(36) NOT NULL,
  request_id VARBINARY(128) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  payment_transaction_id VARBINARY(128) NOT NULL,
  amount_minor BIGINT NOT NULL,
  currency VARCHAR(16) NOT NULL,
  created_ms BIGINT NOT NULL,
  applied INT NOT NULL DEFAULT 0,
  PRIMARY KEY (kind, order_sn),
  UNIQUE KEY uk_mock_tx_request (kind, request_id),
  UNIQUE KEY uk_mock_tx_id (transaction_id),
  INDEX idx_commerce_mock_pending (kind, applied, order_sn),
  CHECK (kind IN ('PAYMENT', 'REFUND')),
  CHECK (amount_minor > 0),
  CHECK (created_ms >= 0),
  CHECK (applied IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_consignment (
  listing_id VARCHAR(20) PRIMARY KEY,
  seller_player_id VARCHAR(20) NOT NULL,
  seller_account VARBINARY(191) NOT NULL,
  market_zone BIGINT NOT NULL,
  asset_kind VARCHAR(16) NOT NULL,
  asset_currency_type INT NULL,
  escrow_reference VARCHAR(20) NOT NULL,
  escrow_proof VARCHAR(64) NOT NULL,
  request_digest VARCHAR(64) NOT NULL,
  title VARCHAR(128) NOT NULL,
  price_fen BIGINT NOT NULL,
  fee_bps INT NOT NULL,
  cooling_ms BIGINT NOT NULL,
  notice_end_ms BIGINT NOT NULL,
  sale_end_ms BIGINT NOT NULL,
  state VARCHAR(24) NOT NULL,
  current_order_sn VARCHAR(36) NULL,
  created_ms BIGINT NOT NULL,
  UNIQUE KEY uk_consignment_escrow (escrow_reference),
  INDEX idx_consignment_state (state, listing_id),
  CHECK (price_fen > 0),
  CHECK (fee_bps >= 0 AND fee_bps <= 10000),
  CHECK (cooling_ms >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_consignment_order (
  order_sn VARCHAR(36) PRIMARY KEY,
  listing_id VARCHAR(20) NOT NULL,
  buyer_account VARBINARY(191) NOT NULL,
  buyer_owner_revision VARBINARY(20) NOT NULL,
  request_id VARBINARY(128) NOT NULL,
  seller_account VARBINARY(191) NOT NULL,
  escrow_reference VARCHAR(20) NOT NULL,
  escrow_proof VARCHAR(64) NOT NULL,
  fee_bps INT NOT NULL,
  cooling_ms BIGINT NOT NULL,
  INDEX idx_consignment_order_listing (listing_id),
  UNIQUE KEY uk_consignment_order_req (buyer_account, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_seller_guard (
  account VARBINARY(191) PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_seller_income (
  order_sn VARCHAR(36) PRIMARY KEY,
  account VARBINARY(191) NOT NULL,
  gross_fen BIGINT NOT NULL,
  fee_fen BIGINT NOT NULL,
  net_fen BIGINT NOT NULL,
  reserved_fen BIGINT NOT NULL DEFAULT 0,
  paid_fen BIGINT NOT NULL DEFAULT 0,
  forfeited_fen BIGINT NOT NULL DEFAULT 0,
  available_ms BIGINT NOT NULL,
  INDEX idx_seller_income (account, order_sn),
  CHECK (gross_fen > 0),
  CHECK (fee_fen >= 0 AND net_fen >= 0),
  CHECK (fee_fen + net_fen = gross_fen),
  CHECK (reserved_fen >= 0 AND paid_fen >= 0 AND forfeited_fen >= 0 AND reserved_fen + paid_fen + forfeited_fen <= net_fen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_withdrawal (
  withdrawal_id VARCHAR(36) PRIMARY KEY,
  account VARBINARY(191) NOT NULL,
  request_id VARBINARY(128) NOT NULL,
  amount_fen BIGINT NOT NULL,
  state VARCHAR(24) NOT NULL,
  channel VARBINARY(32) NULL,
  transaction_id VARBINARY(128) NULL,
  created_ms BIGINT NOT NULL,
  UNIQUE KEY uk_withdrawal_req (account, request_id),
  INDEX idx_withdrawal_pending (state, withdrawal_id),
  CHECK (amount_fen > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_withdrawal_allocation (
  withdrawal_id VARCHAR(36) NOT NULL,
  order_sn VARCHAR(36) NOT NULL,
  amount_fen BIGINT NOT NULL,
  PRIMARY KEY (withdrawal_id, order_sn),
  CHECK (amount_fen > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_withdrawal_receipt (
  withdrawal_id VARCHAR(36) PRIMARY KEY,
  channel VARBINARY(32) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  amount_fen BIGINT NOT NULL,
  successful INT NOT NULL,
  UNIQUE KEY uk_withdrawal_receipt_txn (channel, transaction_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_consignment_return (
  listing_id VARCHAR(20) PRIMARY KEY,
  return_proof VARCHAR(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_mock_payout (
  withdrawal_id VARCHAR(36) PRIMARY KEY,
  request_id VARBINARY(128) NOT NULL,
  transaction_id VARBINARY(128) NOT NULL,
  amount_fen BIGINT NOT NULL,
  successful INT NOT NULL,
  created_ms BIGINT NOT NULL,
  applied INT NOT NULL DEFAULT 0,
  UNIQUE KEY uk_mock_payout_req (request_id),
  UNIQUE KEY uk_mock_payout_txn (transaction_id),
  INDEX idx_mock_payout_pending (applied, withdrawal_id),
  CHECK (amount_fen > 0),
  CHECK (successful IN (0, 1)),
  CHECK (applied IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_reversal_intent (
  order_sn VARCHAR(36) PRIMARY KEY,
  revision INT NOT NULL,
  status VARCHAR(24) NOT NULL,
  updated_ms BIGINT NOT NULL,
  INDEX idx_commerce_reversal_pending (status, updated_ms, order_sn),
  CHECK (revision > 0),
  CHECK (status IN ('PENDING', 'REVERSED', 'DELIVERED', 'MANUAL')),
  CHECK (updated_ms >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS commerce_reversal_receipt (
  order_sn VARCHAR(36) NOT NULL,
  revision INT NOT NULL,
  trade_order_id VARCHAR(20) NOT NULL,
  state VARCHAR(24) NOT NULL,
  proof VARCHAR(64) NOT NULL,
  PRIMARY KEY (order_sn, revision)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
