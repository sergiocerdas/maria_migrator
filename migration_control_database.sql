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
    is_active BOOLEAN DEFAULT FALSE,
    is_paused BOOLEAN DEFAULT TRUE, -- Start in paused state until operator explicitly starts migration
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT USER(),
    
    INDEX idx_migration_name (migration_name),
    INDEX idx_source_instance (source_instance_name),
    INDEX idx_target_instance (target_instance_name)
);

-- ============================================================================
-- OPERATOR COMMAND: Pause/Unpause migration processing
-- ============================================================================
-- When is_paused = TRUE, the migration script will skip processing on each
-- scheduled iteration. The script exits cleanly without applying any binlogs.
-- Use this for temporary maintenance windows or to safely halt replication.
--
-- To PAUSE a migration:
--   UPDATE migration_config SET is_paused = TRUE WHERE config_id = <CONFIG_ID>;
--
-- To UNPAUSE and resume processing:
--   UPDATE migration_config SET is_paused = FALSE WHERE config_id = <CONFIG_ID>;
-- ============================================================================

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
    
    -- Error acknowledgment tracking
    -- When NULL, any ERROR logs or non-COMPLETED checkpoints will block processing
    -- Update this timestamp to acknowledge errors and allow processing to continue
    error_acknowledged_at TIMESTAMP NULL,
    
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

-- ============================================================================
-- OPERATOR COMMAND: Acknowledge errors and unblock processing
-- ============================================================================
-- The migration script will BLOCK if it detects:
--   1. ERROR entries in processing_log (newer than last acknowledgment)
--   2. Non-COMPLETED entries in binlog_apply_checkpoints (STARTED, FAILED, INTERRUPTED)
--
-- Before acknowledging, you MUST:
--   1. Review the error conditions in processing_log and binlog_apply_checkpoints
--   2. If a binlog apply was interrupted, either:
--      a) Verify target data integrity and mark checkpoint COMPLETED, OR
--      b) Perform flashback recovery using checkpoint's target_binlog_file_before
--   3. Fix the root cause of the error
--
-- To acknowledge errors and allow processing to continue:
--
--   UPDATE migration_status 
--   SET error_acknowledged_at = NOW() 
--   WHERE config_id = <CONFIG_ID>;
--
-- To also clear incomplete checkpoints (ONLY after verifying/recovering target data):
--
--   UPDATE binlog_apply_checkpoints 
--   SET apply_status = 'COMPLETED', 
--       apply_completed_at = NOW(),
--       error_message = 'Manually verified and cleared by operator'
--   WHERE config_id = <CONFIG_ID> 
--     AND apply_status IN ('STARTED', 'FAILED', 'INTERRUPTED');
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
-- (Cluster-Aware for Galera/MariaDB Cluster target instances)
-- ============================================================================
-- If a binlog apply fails or is interrupted, use the checkpoint data to recover.
-- IMPORTANT: The target is a CLUSTERED instance - recovery must be coordinated
-- across all cluster nodes to avoid split-brain and data inconsistency.
--
-- STEP 0: PAUSE MIGRATION IMMEDIATELY
-- ------------------------------------
-- Prevent further processing while recovery is in progress:
--
--   UPDATE migration_config SET is_paused = TRUE WHERE config_id = <CONFIG_ID>;
--
-- STEP 1: IDENTIFY THE FAILED CHECKPOINT
-- --------------------------------------
-- Find the failed/interrupted checkpoint:
--
--   SELECT checkpoint_id, source_binlog_file, source_binlog_position,
--          target_binlog_file_before, target_binlog_position_before,
--          target_gtid_before, apply_status, error_message
--   FROM binlog_apply_checkpoints 
--   WHERE config_id = <CONFIG_ID>
--     AND apply_status IN ('FAILED', 'INTERRUPTED') 
--   ORDER BY checkpoint_id DESC LIMIT 1;
--
-- STEP 2: STOP APPLICATION TRAFFIC TO TARGET CLUSTER
-- --------------------------------------------------
-- Before recovery, ensure no writes are occurring on the target cluster:
--
--   a) Update VIP/load balancer to stop routing traffic to target
--   b) On target PRIMARY node, verify no active connections:
--      SHOW PROCESSLIST;
--   c) Optionally set read_only on all nodes:
--      SET GLOBAL read_only = ON;
--
-- STEP 3: ISOLATE PRIMARY NODE FOR RECOVERY (Galera Cluster)
-- ----------------------------------------------------------
-- For Galera clusters, you must desync the primary before recovery:
--
--   -- On the PRIMARY node only:
--   SET GLOBAL wsrep_desync = ON;
--   SET GLOBAL wsrep_on = OFF;
--
-- This prevents the recovery operations from replicating to other nodes
-- (which would corrupt their state since they already have the bad data).
--
-- STEP 4: PERFORM POINT-IN-TIME RECOVERY ON PRIMARY
-- -------------------------------------------------
-- Use the target_binlog_file_before and target_binlog_position_before 
-- to roll back to the consistent state BEFORE the failed apply:
--
--   Option A: Using mariadb-binlog with rollback (if available):
--     # Generate reverse SQL from target's own binlogs
--     mariadb-binlog --start-position=<target_binlog_position_before> \
--       --to-last-log \
--       /var/lib/mysql/<target_binlog_file_before> \
--       | mariadb -u root
--
--   Option B: Restore from backup + replay binlogs to recovery point:
--     # 1. Restore latest full backup
--     mariabackup --prepare --target-dir=/backup/full
--     mariabackup --copy-back --target-dir=/backup/full
--     
--     # 2. Replay binlogs up to (but not including) the failed position
--     mariadb-binlog /var/lib/mysql/binlog.* \
--       --stop-position=<target_binlog_position_before> \
--       | mariadb -u root
--
--   Option C: Using GTID-based flashback (if target_gtid_before is available):
--     # Identify transactions to roll back
--     mariadb-binlog --start-position=<target_binlog_position_before> \
--       --base64-output=decode-rows -v \
--       /var/lib/mysql/<target_binlog_file_before>
--     
--     # Manually construct reverse DML or use third-party flashback tools
--
-- STEP 5: VERIFY RECOVERY ON PRIMARY
-- ----------------------------------
-- Confirm the primary is at the expected recovery point:
--
--   SHOW MASTER STATUS;
--   -- Should show position <= target_binlog_position_before
--
--   SELECT @@gtid_current_pos;
--   -- Should match or precede target_gtid_before
--
-- STEP 6: RE-SYNC SECONDARY NODES (Galera Cluster)
-- ------------------------------------------------
-- Secondary nodes have inconsistent data. You must rebuild them from primary:
--
--   Option A: Full SST (State Snapshot Transfer) - RECOMMENDED
--   -----------------------------------------------------------
--   On EACH SECONDARY node:
--
--     # 1. Stop MariaDB
--     systemctl stop mariadb
--
--     # 2. Remove data directory (backup first if needed)
--     rm -rf /var/lib/mysql/*
--
--     # 3. Restart - will trigger full SST from donor (primary)
--     systemctl start mariadb
--
--     # 4. Monitor SST progress
--     tail -f /var/log/mysql/error.log
--
--   Option B: Incremental State Transfer (IST) - if gcache has required data
--   -------------------------------------------------------------------------
--   Only works if the gcache on primary contains all transactions since
--   the secondary's last known position. Usually NOT viable after recovery.
--
-- STEP 7: RE-ENABLE CLUSTER REPLICATION ON PRIMARY
-- ------------------------------------------------
-- After all secondaries have rejoined and are synced:
--
--   -- On PRIMARY node:
--   SET GLOBAL wsrep_on = ON;
--   SET GLOBAL wsrep_desync = OFF;
--
--   -- Verify cluster is healthy:
--   SHOW STATUS LIKE 'wsrep_cluster_size';
--   SHOW STATUS LIKE 'wsrep_cluster_status';   -- Should be 'Primary'
--   SHOW STATUS LIKE 'wsrep_local_state_comment';  -- Should be 'Synced'
--
-- STEP 8: UPDATE MIGRATION STATE
-- ------------------------------
-- Reset migration to resume from the failed binlog:
--
--   UPDATE migration_status SET 
--     current_binlog_file = '<source_binlog_file>',
--     current_binlog_position = <source_binlog_position>,
--     processing_status = 'STOPPED'
--   WHERE config_id = <CONFIG_ID>;
--
--   -- Mark the failed checkpoint as manually resolved
--   UPDATE binlog_apply_checkpoints 
--   SET apply_status = 'COMPLETED',
--       apply_completed_at = NOW(),
--       error_message = 'Manually recovered via cluster PITR procedure'
--   WHERE checkpoint_id = <CHECKPOINT_ID>;
--
-- STEP 9: RE-ENABLE APPLICATION TRAFFIC
-- -------------------------------------
--   a) Disable read_only on target nodes:
--      SET GLOBAL read_only = OFF;
--   b) Update VIP/load balancer to resume traffic routing
--
-- STEP 10: UNPAUSE MIGRATION
-- --------------------------
-- Resume migration processing:
--
--   UPDATE migration_config SET is_paused = FALSE WHERE config_id = <CONFIG_ID>;
--
-- ============================================================================
-- NOTES FOR DIFFERENT CLUSTER TYPES:
-- ============================================================================
--
-- MariaDB Galera Cluster:
--   - Uses wsrep_desync/wsrep_on as described above
--   - SST methods: rsync, mariabackup, xtrabackup
--   - gcache contains recent transactions for IST
--
-- MySQL InnoDB Cluster (Group Replication):
--   - Use SET GLOBAL group_replication_exit_state_action = 'OFFLINE_MODE';
--   - Recovery node: STOP GROUP_REPLICATION; <recover> START GROUP_REPLICATION;
--   - Clone plugin may auto-rebuild secondaries
--
-- Standard Async Replication:
--   - Stop slave on all replicas: STOP SLAVE;
--   - Recover master, then CHANGE MASTER TO on each replica
--   - May need to use MASTER_AUTO_POSITION=1 with GTID
--
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
    TRUNCATE TABLE migration_status;
    TRUNCATE TABLE source_cluster_mapping;
    TRUNCATE TABLE migration_config;
    TRUNCATE TABLE cluster_nodes;

    SET FOREIGN_KEY_CHECKS = 1;
END//

DELIMITER ;
