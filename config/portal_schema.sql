-- Initialize the portal tables in the same database used by Go Login.
-- CREATE IF NOT EXISTS does not migrate an existing table.
CREATE TABLE IF NOT EXISTS zone_config (
    zone_id BIGINT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    manual_status INT NOT NULL DEFAULT 0,
    capacity INT DEFAULT 45000,
    maintenance_msg VARCHAR(256) DEFAULT '',
    open_time DATETIME DEFAULT NULL,
    recommended BIT(1) DEFAULT 0,
    sort_order INT DEFAULT 0,
    create_role_closed BIT(1) NOT NULL DEFAULT 0,
    role_capacity INT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS zone_whitelist (
    id BIGINT PRIMARY KEY,
    zone_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    note VARCHAR(128) DEFAULT '',
    UNIQUE KEY uk_zone_account (zone_id, account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS announcement (
    id BIGINT PRIMARY KEY,
    title VARCHAR(128) NOT NULL,
    content TEXT,
    type VARCHAR(32) NOT NULL DEFAULT 'notice',
    start_time DATETIME DEFAULT NULL,
    end_time DATETIME DEFAULT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
