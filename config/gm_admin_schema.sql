-- gm_admin 独占库 gm_admin 的建表 DDL（与 Java 侧 gm-schema.sql 及 GmInstallation v5 索引对齐，
-- 兼容 ops/schemamigrate.py 静态校验与 MySQL/TiDB 幂等建表）。

CREATE TABLE IF NOT EXISTS `gm_command` (
    `id` VARCHAR(20) NOT NULL,
    `command_type` VARCHAR(16) NOT NULL DEFAULT 'MAIL',
    `operator_name` VARBINARY(512) NOT NULL,
    `client_request_id` VARBINARY(128) NOT NULL,
    `player_id` VARCHAR(20) NOT NULL,
    `reason` TEXT NOT NULL,
    `input_json` LONGTEXT NOT NULL,
    `request_json` LONGTEXT NOT NULL,
    `result_json` LONGTEXT,
    `status` VARCHAR(16) NOT NULL,
    `attempt` BIGINT NOT NULL DEFAULT 0,
    `created_ms` BIGINT NOT NULL,
    `updated_ms` BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_gm_command_request` (`operator_name`, `client_request_id`),
    KEY `idx_gm_command_type_created` (`command_type`, `created_ms`, `id`),
    KEY `idx_gm_command_type_player_created` (`command_type`, `player_id`, `created_ms`, `id`),
    KEY `idx_gm_command_type_status_created` (`command_type`, `status`, `created_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='gm_command';

CREATE TABLE IF NOT EXISTS `gm_command_audit` (
    `id` BIGINT AUTO_INCREMENT NOT NULL,
    `command_id` VARCHAR(20) NOT NULL,
    `operator_name` VARBINARY(512) NOT NULL,
    `event` VARCHAR(16) NOT NULL,
    `attempt` BIGINT NOT NULL,
    `result_json` LONGTEXT,
    `created_ms` BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_gm_command_audit_command` (`command_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='gm_command_audit';
