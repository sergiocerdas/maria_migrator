CREATE DATABASE IF NOT EXISTS mariaDBaaS_migcontrol;
USE mariaDBaaS_migcontrol;

-- Table to store cluster nodes information
CREATE TABLE cluster_nodes (
    node_id INT AUTO_INCREMENT PRIMARY KEY,
    node_name VARCHAR(100) NOT NULL UNIQUE,
    server_hostname VARCHAR(255) NOT NULL,
    server_ip VARCHAR(45) NULL, -- IPv4/IPv6 support
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_node_name (node_name),
    INDEX idx_server_ip (server_ip)
);

-- Main migration configuration
CREATE TABLE migration_config (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    migration_name VARCHAR(100) NOT NULL UNIQUE,
    
    -- Source Instance Configuration
    source_instance_name VARCHAR(100) NOT NULL,
    source_local_port INT NOT NULL,
    source_vip_port INT NOT NULL,
    source_cluster_id INT NOT NULL, -- References which cluster this source belongs to
    
    -- Target Instance Configuration  
    target_instance_name VARCHAR(100) NOT NULL,
    target_port INT NOT NULL,
    target_host VARCHAR(255) NOT NULL,
    
    -- Migration Settings
    binlog_local_path VARCHAR(500) NOT NULL DEFAULT '/var/lib/mysql',
    migration_username VARCHAR(100) NOT NULL,
    migration_password_encrypted TEXT NOT NULL, -- AES encrypted
    
    -- Status and Control
    is_active BOOLEAN DEFAULT TRUE,
    is_paused BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT USER(),
    
    INDEX idx_migration_name (migration_name),
    INDEX idx_source_instance (source_instance_name),
    INDEX idx_target_instance (target_instance_name)
);

-- Maps source instances to their cluster nodes
CREATE TABLE source_cluster_mapping (
    mapping_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,
    node_id INT NOT NULL,
    server_id INT NOT NULL, -- MariaDB server_id (1, 2, etc.)
    is_primary BOOLEAN DEFAULT FALSE,
    priority_order INT DEFAULT 1, -- For failover ordering
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES cluster_nodes(node_id) ON DELETE CASCADE,
    
    UNIQUE KEY unique_config_server_id (config_id, server_id),
    INDEX idx_config_id (config_id),
    INDEX idx_server_id (server_id)
);


-- Current migration processing status
CREATE TABLE migration_status (
    status_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,
    
    -- Current Processing Information
    current_processing_node_id INT NOT NULL,
    current_processing_server_id INT NOT NULL,
    current_binlog_file VARCHAR(255) NOT NULL,
    current_binlog_position BIGINT UNSIGNED DEFAULT 0,
    current_gtid_position VARCHAR(500),
    
    -- Processing State
    processing_status ENUM('RUNNING','PAUSED','STOPPED','ERROR','FAILOVER_DETECTED','FAILOVER_HANDOFF','ACTIVE_BINLOG_REACHED') DEFAULT 'STOPPED',


    last_processed_timestamp TIMESTAMP NULL,
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Process Information
    process_pid INT,
    process_hostname VARCHAR(255),
    process_start_time TIMESTAMP NULL,
    
    -- Statistics
    total_binlogs_processed INT DEFAULT 0,
    total_transactions_processed BIGINT DEFAULT 0,
    total_maintenance_operations_ignored INT DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    FOREIGN KEY (current_processing_node_id) REFERENCES cluster_nodes(node_id),
    
    UNIQUE KEY unique_config_status (config_id), -- Only one active status per migration
    INDEX idx_config_id (config_id),
    INDEX idx_processing_node (current_processing_node_id),
    INDEX idx_processing_status (processing_status)
);

-- Cutover control and state machine (separate from migration_status)
CREATE TABLE cutover_control (
    cutover_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,

    -- Manually managed schedule (required)
    scheduled_cutover_at DATETIME NOT NULL,

    -- Manual override: request cutover now.
    -- Valid only when requested_at date equals DATE(scheduled_cutover_at).
    trigger_cutover_now BOOLEAN DEFAULT FALSE,
    trigger_cutover_now_at DATETIME NULL,

    -- Cutover state machine
    cutover_status ENUM('CUTOVER_PENDING','CUTOVER_READY','CUTOVER_CONFIRMED','CUTOVER_COMPLETE') DEFAULT 'CUTOVER_PENDING',

    -- Audit timestamps
    cutover_ready_at DATETIME NULL,
    cutover_confirmed_at DATETIME NULL,
    cutover_completed_at DATETIME NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    UNIQUE KEY unique_config_cutover (config_id),
    INDEX idx_cutover_status (cutover_status),
    INDEX idx_scheduled_cutover_at (scheduled_cutover_at)
);
-- ============================================================================
-- OPERATOR COMMAND: Initiate cutover by inserting a record into cutover_control for the migration config.
-- ============================================================================
-- INSERT INTO cutover_control (
--    config_id,
--    scheduled_cutover_at,
--    trigger_cutover_now,
--    cutover_status
-- ) VALUES (
--    1,                           -- Replace with your actual config_id
--    '2026-05-15 22:00:00',       -- Replace with your scheduled cutover date/time
--    FALSE,
--    'CUTOVER_PENDING'
-- );

-- ============================================================================
-- OPERATOR COMMAND: Trigger immediate cutover
-- ============================================================================
-- Use this command to manually trigger cutover before the scheduled time.
-- The trigger is only valid if executed on the SAME calendar date as the
-- scheduled_cutover_at. If dates don't match, the script ignores the trigger
-- and logs a warning.
--
-- UPDATE cutover_control
-- SET trigger_cutover_now = 1,
--     trigger_cutover_now_at = NOW()
-- WHERE config_id = <CONFIG_ID>;
-- ============================================================================

-- Failover detection and handoff information
CREATE TABLE failover_events (
    failover_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,
    
    -- Failover Detection Details
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    detected_by_node_id INT NOT NULL,
    detected_by_server_id INT NOT NULL,
    detected_in_binlog VARCHAR(255) NOT NULL,
    detected_at_position BIGINT UNSIGNED NOT NULL,
    detected_gtid VARCHAR(500),
    
    -- Foreign Server Information
    foreign_server_id INT NOT NULL,
    foreign_sql_statement TEXT,
    maintenance_operations_count INT DEFAULT 0,
    
    -- New Primary Information
    new_primary_node_id INT NOT NULL,
    new_primary_server_id INT NOT NULL,
    
    -- Handoff Status
    handoff_status ENUM('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
    handoff_attempted_at TIMESTAMP NULL,
    handoff_completed_at TIMESTAMP NULL,
    handoff_error_message TEXT,
    
    -- Processing Instructions for New Primary
    resume_from_binlog VARCHAR(255),
    resume_from_position BIGINT UNSIGNED,
    resume_from_gtid VARCHAR(500),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    FOREIGN KEY (detected_by_node_id) REFERENCES cluster_nodes(node_id),
    FOREIGN KEY (new_primary_node_id) REFERENCES cluster_nodes(node_id),
    
    INDEX idx_config_id (config_id),
    INDEX idx_detected_at (detected_at),
    INDEX idx_handoff_status (handoff_status),
    INDEX idx_foreign_server_id (foreign_server_id)
);

-- Detailed processing log for troubleshooting
CREATE TABLE processing_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,
    node_id INT NOT NULL,
    
    -- Log Details
    log_level ENUM('DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL') DEFAULT 'INFO',
    log_message TEXT NOT NULL,
    binlog_file VARCHAR(255),
    binlog_position BIGINT UNSIGNED,
    gtid_position VARCHAR(500),
    
    -- Context
    process_pid INT,
    thread_info VARCHAR(255),
    
    -- Timestamp
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES cluster_nodes(node_id),
    
    INDEX idx_config_logged_at (config_id, logged_at),
    INDEX idx_log_level (log_level),
    INDEX idx_binlog_file (binlog_file)
);

-- Track target database binlog position before each source binlog apply
-- This enables point-in-time recovery (flashback) if apply fails or is interrupted
-- Recovery: Use target_binlog_file_before/target_binlog_position_before to flashback target DB
CREATE TABLE binlog_apply_checkpoints (
    checkpoint_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NOT NULL,
    
    -- Source binlog being applied
    source_binlog_file VARCHAR(255) NOT NULL,
    source_binlog_position BIGINT UNSIGNED NOT NULL,
    
    -- Target database state BEFORE apply (recovery point)
    target_binlog_file_before VARCHAR(255) NOT NULL,
    target_binlog_position_before BIGINT UNSIGNED NOT NULL,
    target_gtid_before VARCHAR(500),
    
    -- Target database state AFTER apply (if completed successfully)
    target_binlog_file_after VARCHAR(255),
    target_binlog_position_after BIGINT UNSIGNED,
    target_gtid_after VARCHAR(500),
    
    -- Apply status tracking
    apply_status ENUM('STARTED', 'COMPLETED', 'FAILED', 'INTERRUPTED') DEFAULT 'STARTED',
    apply_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    apply_completed_at TIMESTAMP NULL,
    
    -- Error information (if failed)
    error_message TEXT,
    error_code INT,
    
    -- Metadata
    process_pid INT,
    process_hostname VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    
    INDEX idx_config_source_binlog (config_id, source_binlog_file),
    INDEX idx_apply_status (apply_status),
    INDEX idx_apply_started (apply_started_at),
    INDEX idx_target_binlog (target_binlog_file_before, target_binlog_position_before)
) ENGINE=InnoDB;

-- ============================================================================
-- RECOVERY PROCEDURE: Point-in-Time Recovery after failed/interrupted apply
-- ============================================================================
-- If a binlog apply fails or is interrupted, use the checkpoint data to recover:
--
-- 1. Find the failed/interrupted checkpoint:
--    SELECT * FROM binlog_apply_checkpoints 
--    WHERE apply_status IN ('FAILED', 'INTERRUPTED') 
--    ORDER BY checkpoint_id DESC LIMIT 1;
--
-- 2. Use the target_binlog_file_before and target_binlog_position_before 
--    to perform point-in-time recovery on the TARGET database:
--
--    Option A: Using mariabackup flashback (if available):
--      mariabackup --prepare --target-dir=/backup/path \
--        --apply-log-only \
--        --binlog-info=<target_binlog_file_before>,<target_binlog_position_before>
--
--    Option B: Using mysqlbinlog to revert (if binlog still available):
--      mysqlbinlog --start-position=<target_binlog_position_before> \
--        /var/lib/mysql/<target_binlog_file_before> | mysql
--
--    Option C: Restore from backup + replay binlogs up to recovery point
--
-- 3. After recovery, update migration_status to resume from the failed binlog:
--    UPDATE migration_status SET 
--      current_binlog_file = '<source_binlog_file>',
--      current_binlog_position = <source_binlog_position>,
--      processing_status = 'RUNNING'
--    WHERE config_id = <config_id>;
-- ============================================================================

-- Function to encrypt passwords (simple AES encryption)
DELIMITER //
CREATE FUNCTION encrypt_password(plain_password TEXT, encryption_key VARCHAR(255))
RETURNS TEXT
READS SQL DATA
DETERMINISTIC
BEGIN
    RETURN AES_ENCRYPT(plain_password, encryption_key);
END//

CREATE FUNCTION decrypt_password(encrypted_password TEXT, encryption_key VARCHAR(255))
RETURNS TEXT
READS SQL DATA
DETERMINISTIC
BEGIN
    RETURN AES_DECRYPT(encrypted_password, encryption_key);
END//

-- Truncates all tables in dependency-safe order (child tables first)
CREATE PROCEDURE reset_all_data()
BEGIN
    SET FOREIGN_KEY_CHECKS = 0;

    TRUNCATE TABLE binlog_apply_checkpoints;
    TRUNCATE TABLE processing_log;
    TRUNCATE TABLE failover_events;
    TRUNCATE TABLE cutover_control;
    TRUNCATE TABLE migration_status;
    TRUNCATE TABLE source_cluster_mapping;
    TRUNCATE TABLE migration_config;
    TRUNCATE TABLE cluster_nodes;

    SET FOREIGN_KEY_CHECKS = 1;
END//

DELIMITER ;


-- Insert cluster nodes
/*

INSERT INTO cluster_nodes (node_name, server_hostname, server_ip) VALUES
('mariadb-node1', 'mariadb-cluster-01.example.com', '10.0.1.10'),
('mariadb-node2', 'mariadb-cluster-02.example.com', '10.0.1.11');

-- Insert migration configuration
INSERT INTO migration_config (
    migration_name, 
    source_instance_name, 
    source_local_port, 
    source_vip_port,
    source_cluster_id,
    target_instance_name, 
    target_port, 
    target_host,
    binlog_local_path,
    migration_username, 
    migration_password_encrypted
) VALUES (
    'prod_to_analytics',
    'mariadb-prod-cluster',
    3306,
    3307,
    1,
    'analytics-db',
    3306,
    'analytics-server.example.com',
    '/var/lib/mysql',
    'migration_user',
    encrypt_password('secure_password_123', 'your_encryption_key_here')
);

-- Map cluster nodes to the migration
INSERT INTO source_cluster_mapping (config_id, node_id, server_id, is_primary, priority_order) VALUES
(1, 1, 1, TRUE, 1),   -- Node 1 is primary with server_id 1
(1, 2, 2, FALSE, 2);  -- Node 2 is secondary with server_id 2

-- Initialize migration status
INSERT INTO migration_status (
    config_id,
    current_processing_node_id,
    current_processing_server_id,
    current_binlog_file,
    current_binlog_position,
    processing_status,
    process_hostname
) VALUES (
    1,
    1,  -- Starting with node 1
    1,  -- Server ID 1
    'mysql-bin.000001',
    0,
    'STOPPED',
    'migration-server-01'
);


*/

-- ============================================================================
-- UPGRADE SCRIPT: Add binlog_apply_checkpoints table to existing deployments
-- ============================================================================
-- Run this if you have an existing control database that was created before
-- the checkpoint feature was added:
--
-- USE mariaDBaaS_migcontrol;
-- 
-- CREATE TABLE IF NOT EXISTS binlog_apply_checkpoints (
--     checkpoint_id BIGINT AUTO_INCREMENT PRIMARY KEY,
--     config_id INT NOT NULL,
--     source_binlog_file VARCHAR(255) NOT NULL,
--     source_binlog_position BIGINT UNSIGNED NOT NULL,
--     target_binlog_file_before VARCHAR(255) NOT NULL,
--     target_binlog_position_before BIGINT UNSIGNED NOT NULL,
--     target_gtid_before VARCHAR(500),
--     target_binlog_file_after VARCHAR(255),
--     target_binlog_position_after BIGINT UNSIGNED,
--     target_gtid_after VARCHAR(500),
--     apply_status ENUM('STARTED', 'COMPLETED', 'FAILED', 'INTERRUPTED') DEFAULT 'STARTED',
--     apply_started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     apply_completed_at TIMESTAMP NULL,
--     error_message TEXT,
--     error_code INT,
--     process_pid INT,
--     process_hostname VARCHAR(255),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--     FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
--     INDEX idx_config_source_binlog (config_id, source_binlog_file),
--     INDEX idx_apply_status (apply_status),
--     INDEX idx_apply_started (apply_started_at),
--     INDEX idx_target_binlog (target_binlog_file_before, target_binlog_position_before)
-- ) ENGINE=InnoDB;
-- ============================================================================