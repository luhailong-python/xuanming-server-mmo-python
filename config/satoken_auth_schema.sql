-- satoken_auth 认证服务独占库建表 DDL（对应 Java springboot_satoken_auth_starter auth-schema.sql，
-- 兼容 ops/schemamigrate.py 静态校验与 MySQL/TiDB 幂等建表）。

CREATE TABLE IF NOT EXISTS `sys_user` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `username` VARCHAR(128) NOT NULL,
    `password_hash` VARCHAR(255) NOT NULL DEFAULT '',
    `nickname` VARCHAR(255) NOT NULL DEFAULT '',
    `avatar` VARCHAR(512) NOT NULL DEFAULT '',
    `is_guest` TINYINT(1) NOT NULL DEFAULT 0,
    `status` TINYINT NOT NULL DEFAULT 1,
    `created_ms` BIGINT NOT NULL,
    `last_login_ms` BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_sys_user_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='sys_user';

CREATE TABLE IF NOT EXISTS `sys_user_oauth` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `user_id` BIGINT NOT NULL,
    `provider` VARCHAR(64) NOT NULL,
    `open_id` VARCHAR(255) NOT NULL,
    `union_id` VARCHAR(255) NOT NULL DEFAULT '',
    `created_ms` BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_sys_user_oauth_provider_open_id` (`provider`, `open_id`),
    KEY `idx_sys_user_oauth_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='sys_user_oauth';
