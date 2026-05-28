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
--   UPDATE migration_config SET is_paused = TRUE, is_active = FALSE WHERE config_id = <CONFIG_ID>;
--
-- To UNPAUSE and resume processing:
--   UPDATE migration_config SET is_paused = FALSE, is_active = TRUE WHERE config_id = <CONFIG_ID>;
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

-- Maintenance windows for scheduled downtime (e.g., patching)
-- Migration processing will skip iterations during these windows
-- Supports: weekly recurring, monthly recurring (by day or weekday), and one-time specific dates
CREATE TABLE maintenance_windows (
    window_id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT NULL, -- NULL = applies to ALL migrations (global window)
    
    -- Schedule type determines which fields are used
    schedule_type ENUM('WEEKLY', 'MONTHLY_DAY', 'MONTHLY_WEEKDAY', 'SPECIFIC_DATE') DEFAULT 'WEEKLY',
    
    -- For WEEKLY: day_of_week (0-6)
    -- For MONTHLY_WEEKDAY: day_of_week (0-6) + week_of_month (1-5)
    day_of_week TINYINT NULL, -- 0=Sunday, 1=Monday, ..., 6=Saturday
    
    -- For MONTHLY_DAY: day_of_month (1-31)
    day_of_month TINYINT NULL, -- 1-31; if day doesn't exist in month, window is skipped
    
    -- For MONTHLY_WEEKDAY: week_of_month (1=first, 2=second, 3=third, 4=fourth, 5=last)
    week_of_month TINYINT NULL, -- 1-5 (5 = last occurrence of that weekday)
    
    -- For SPECIFIC_DATE: one-time window on exact date
    specific_date DATE NULL,
    
    -- Time range (applies to all schedule types)
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    
    -- Window metadata
    window_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT USER(),
    
    FOREIGN KEY (config_id) REFERENCES migration_config(config_id) ON DELETE CASCADE,
    
    INDEX idx_config_id (config_id),
    INDEX idx_schedule_type (schedule_type),
    INDEX idx_day_time (day_of_week, start_time, end_time),
    INDEX idx_specific_date (specific_date),
    INDEX idx_is_active (is_active)
);

-- ============================================================================
-- OPERATOR COMMAND: Manage maintenance windows
-- ============================================================================
-- Maintenance windows define time periods where migration processing will be
-- skipped. Supports weekly, monthly, and one-time schedules.
--
-- SCHEDULE TYPES:
--   WEEKLY         - Repeats every week on specified day_of_week
--   MONTHLY_DAY    - Repeats monthly on specified day_of_month (1-31)
--   MONTHLY_WEEKDAY- Repeats monthly on Nth weekday (e.g., 2nd Tuesday)
--   SPECIFIC_DATE  - One-time window on exact date
--
-- DAY MAPPINGS:
--   day_of_week:  0=Sunday, 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday, 5=Friday, 6=Saturday
--   week_of_month: 1=first, 2=second, 3=third, 4=fourth, 5=last
--   day_of_month: 1-31 (if day doesn't exist, window is skipped that month)
--
-- ============================================================================
-- EXAMPLES:
-- ============================================================================
--
-- WEEKLY: Every Tuesday 2-6 AM (config-specific):
--   INSERT INTO maintenance_windows 
--     (config_id, schedule_type, day_of_week, start_time, end_time, window_name)
--   VALUES (1, 'WEEKLY', 2, '02:00:00', '06:00:00', 'Weekly Tuesday Patching');
--
-- WEEKLY: Every Sunday 3-5 AM (GLOBAL - applies to ALL migrations):
--   INSERT INTO maintenance_windows 
--     (config_id, schedule_type, day_of_week, start_time, end_time, window_name)
--   VALUES (NULL, 'WEEKLY', 0, '03:00:00', '05:00:00', 'Sunday Global Maintenance');
--
-- WEEKLY window spanning midnight (Tuesday 5 PM to Wednesday 8:30 AM):
--   INSERT INTO maintenance_windows (config_id, schedule_type, day_of_week, start_time, end_time, window_name) VALUES
--   (NULL, 'WEEKLY', 2, '17:00:00', '23:59:59', 'Weekly Patching - Tuesday Evening'),
--   (NULL, 'WEEKLY', 3, '00:00:00', '08:30:00', 'Weekly Patching - Wednesday Morning');
--
-- MONTHLY_DAY: 15th of every month, 1-5 AM (GLOBAL):
--   INSERT INTO maintenance_windows 
--     (config_id, schedule_type, day_of_month, start_time, end_time, window_name)
--   VALUES (NULL, 'MONTHLY_DAY', 15, '01:00:00', '05:00:00', 'Monthly Mid-Month Patching');
--
-- MONTHLY_WEEKDAY: 2nd Tuesday of every month, 2-6 AM (GLOBAL):
--   INSERT INTO maintenance_windows 
--     (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name)
--   VALUES (NULL, 'MONTHLY_WEEKDAY', 2, 2, '02:00:00', '06:00:00', 'Monthly 2nd Tuesday Patching');
--
-- MONTHLY_WEEKDAY: Last Friday of every month, 10 PM - 2 AM:
--   INSERT INTO maintenance_windows (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name) VALUES
--   (NULL, 'MONTHLY_WEEKDAY', 5, 5, '22:00:00', '23:59:59', 'Monthly Last Friday - Evening'),
--   (NULL, 'MONTHLY_WEEKDAY', 6, 1, '00:00:00', '02:00:00', 'Monthly Last Friday - Next Morning');
--
-- SPECIFIC_DATE: One-time window on June 15, 2026, 1-5 AM:
--   INSERT INTO maintenance_windows 
--     (config_id, schedule_type, specific_date, start_time, end_time, window_name)
--   VALUES (NULL, 'SPECIFIC_DATE', '2026-06-15', '01:00:00', '05:00:00', 'Q2 Infrastructure Upgrade');
--
-- ============================================================================
-- MANAGEMENT COMMANDS:
-- ============================================================================
--
-- DISABLE a window temporarily:
--   UPDATE maintenance_windows SET is_active = FALSE WHERE window_id = <WINDOW_ID>;
--
-- RE-ENABLE a window:
--   UPDATE maintenance_windows SET is_active = TRUE WHERE window_id = <WINDOW_ID>;
--
-- DELETE a window:
--   DELETE FROM maintenance_windows WHERE window_id = <WINDOW_ID>;
--
-- VIEW all active windows:
--   SELECT * FROM maintenance_windows WHERE is_active = TRUE;
--
-- CHECK if currently in maintenance (for config_id=1):
--   CALL is_in_maintenance_window(1);
--
-- ============================================================================
-- LEGACY COMPATIBILITY: Existing WEEKLY windows with day_of_week set will
-- continue to work. schedule_type defaults to 'WEEKLY'.
-- ============================================================================


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
--   UPDATE migration_config SET is_paused = TRUE, is_active = FALSE WHERE config_id = <CONFIG_ID>;
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
--     # # Correct command for generating reverse SQL
--     mariadb-binlog --flashback \
--     --start-position=<target_binlog_position_before> \
--     --to-last-log \
--     /var/lib/mysql/<target_binlog_file_before> \
--     | mariadb -u root
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
    TRUNCATE TABLE maintenance_windows;
    TRUNCATE TABLE source_cluster_mapping;
    TRUNCATE TABLE migration_config;
    TRUNCATE TABLE cluster_nodes;

    SET FOREIGN_KEY_CHECKS = 1;
END//

-- ============================================================================
-- PROCEDURE: Check if currently in a maintenance window (simple boolean)
-- ============================================================================
-- Returns 1 if currently in a maintenance window, 0 if not
-- Checks both config-specific windows and global windows (config_id IS NULL)
-- Supports all schedule types: WEEKLY, MONTHLY_DAY, MONTHLY_WEEKDAY, SPECIFIC_DATE
--
-- Usage: CALL is_in_maintenance_window(<CONFIG_ID>);
-- ============================================================================
CREATE PROCEDURE is_in_maintenance_window(IN p_config_id INT)
BEGIN
    DECLARE v_current_dow TINYINT;           -- 0-6 (Sunday=0)
    DECLARE v_current_dom TINYINT;           -- 1-31
    DECLARE v_current_week_of_month TINYINT; -- Which occurrence of this weekday (1-5)
    DECLARE v_is_last_of_month TINYINT;      -- Is this the last occurrence of this weekday?
    DECLARE v_current_date DATE;
    DECLARE v_days_in_month TINYINT;
    
    -- Calculate current date components
    SET v_current_date = CURDATE();
    SET v_current_dow = DAYOFWEEK(NOW()) - 1;  -- MySQL DAYOFWEEK is 1-7, we store 0-6
    SET v_current_dom = DAY(v_current_date);
    SET v_current_week_of_month = CEIL(v_current_dom / 7);  -- 1-5
    SET v_days_in_month = DAY(LAST_DAY(v_current_date));
    
    -- Check if this is the LAST occurrence of this weekday in the month
    -- (i.e., there's no same weekday in the next 7 days within this month)
    SET v_is_last_of_month = CASE 
        WHEN v_current_dom + 7 > v_days_in_month THEN 1 
        ELSE 0 
    END;
    
    SELECT EXISTS (
        SELECT 1 FROM maintenance_windows mw
        WHERE mw.is_active = TRUE
          AND (mw.config_id = p_config_id OR mw.config_id IS NULL)
          AND CURTIME() BETWEEN mw.start_time AND mw.end_time
          AND (
              -- WEEKLY: matches day of week
              (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_current_dow)
              -- MONTHLY_DAY: matches day of month
              OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_current_dom)
              -- MONTHLY_WEEKDAY: matches Nth weekday (1-4) or last weekday (5)
              OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                  AND mw.day_of_week = v_current_dow
                  AND (
                      (mw.week_of_month = v_current_week_of_month)
                      OR (mw.week_of_month = 5 AND v_is_last_of_month = 1)
                  ))
              -- SPECIFIC_DATE: matches exact date
              OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_current_date)
          )
    ) AS in_maintenance_window;
END//

-- ============================================================================
-- PROCEDURE: Check maintenance window with details
-- ============================================================================
-- Returns details about current/upcoming maintenance windows
-- Supports all schedule types: WEEKLY, MONTHLY_DAY, MONTHLY_WEEKDAY, SPECIFIC_DATE
--
-- Usage: CALL get_maintenance_window_status(<CONFIG_ID>);
-- ============================================================================
CREATE PROCEDURE get_maintenance_window_status(IN p_config_id INT)
BEGIN
    DECLARE v_current_dow TINYINT;
    DECLARE v_current_dom TINYINT;
    DECLARE v_current_week_of_month TINYINT;
    DECLARE v_is_last_of_month TINYINT;
    DECLARE v_current_date DATE;
    DECLARE v_days_in_month TINYINT;
    
    SET v_current_date = CURDATE();
    SET v_current_dow = DAYOFWEEK(NOW()) - 1;
    SET v_current_dom = DAY(v_current_date);
    SET v_current_week_of_month = CEIL(v_current_dom / 7);
    SET v_days_in_month = DAY(LAST_DAY(v_current_date));
    SET v_is_last_of_month = CASE WHEN v_current_dom + 7 > v_days_in_month THEN 1 ELSE 0 END;
    
    SELECT 
        mw.window_id,
        mw.window_name,
        mw.schedule_type,
        mw.description,
        -- Schedule description
        CASE mw.schedule_type
            WHEN 'WEEKLY' THEN CONCAT('Every ', 
                CASE mw.day_of_week
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END)
            WHEN 'MONTHLY_DAY' THEN CONCAT('Monthly on day ', mw.day_of_month)
            WHEN 'MONTHLY_WEEKDAY' THEN CONCAT(
                CASE mw.week_of_month
                    WHEN 1 THEN '1st '
                    WHEN 2 THEN '2nd '
                    WHEN 3 THEN '3rd '
                    WHEN 4 THEN '4th '
                    WHEN 5 THEN 'Last '
                END,
                CASE mw.day_of_week
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END,
                ' of each month')
            WHEN 'SPECIFIC_DATE' THEN CONCAT('One-time: ', mw.specific_date)
        END AS schedule_description,
        mw.start_time,
        mw.end_time,
        CASE WHEN mw.config_id IS NULL THEN 'GLOBAL' ELSE 'CONFIG-SPECIFIC' END AS scope,
        -- Status: check if active now based on schedule type
        CASE 
            WHEN CURTIME() BETWEEN mw.start_time AND mw.end_time
                 AND (
                     (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_current_dow)
                     OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_current_dom)
                     OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                         AND mw.day_of_week = v_current_dow
                         AND ((mw.week_of_month = v_current_week_of_month) 
                              OR (mw.week_of_month = 5 AND v_is_last_of_month = 1)))
                     OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_current_date)
                 )
            THEN 'ACTIVE NOW'
            WHEN mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date < v_current_date
            THEN 'EXPIRED'
            ELSE 'SCHEDULED'
        END AS status
    FROM maintenance_windows mw
    WHERE mw.is_active = TRUE
      AND (mw.config_id = p_config_id OR mw.config_id IS NULL)
    ORDER BY 
        -- Active windows first
        CASE 
            WHEN CURTIME() BETWEEN mw.start_time AND mw.end_time
                 AND (
                     (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_current_dow)
                     OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_current_dom)
                     OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                         AND mw.day_of_week = v_current_dow
                         AND ((mw.week_of_month = v_current_week_of_month) 
                              OR (mw.week_of_month = 5 AND v_is_last_of_month = 1)))
                     OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_current_date)
                 )
            THEN 0
            ELSE 1
        END,
        mw.schedule_type,
        mw.specific_date,
        mw.day_of_week,
        mw.start_time;
END//

-- ============================================================================
-- PROCEDURE: Check if migration is caught up to active binlog
-- ============================================================================
-- Returns migration sync status based on:
--   1. All binlog_apply_checkpoints entries are COMPLETED
--   2. processing_log shows "Reached active binlog" with binlog 1 ahead of last checkpoint
--
-- Usage: CALL get_migration_sync_status(<CONFIG_ID>);
-- ============================================================================
CREATE PROCEDURE get_migration_sync_status(IN p_config_id INT)
BEGIN
    SELECT 
        CASE 
            WHEN incomplete_checkpoints = 0 
                 AND active_binlog_reached = 1 
                 AND active_binlog_is_next = 1
            THEN 'CAUGHT_UP'
            WHEN incomplete_checkpoints > 0
            THEN 'INCOMPLETE_CHECKPOINTS'
            WHEN active_binlog_reached = 0
            THEN 'ACTIVE_BINLOG_NOT_REACHED'
            WHEN active_binlog_is_next = 0
            THEN 'BINLOG_GAP_DETECTED'
            ELSE 'UNKNOWN'
        END AS migration_status,
        incomplete_checkpoints,
        last_completed_binlog,
        last_completed_binlog_num,
        active_binlog_file,
        active_binlog_num,
        active_binlog_reached
    FROM (
        SELECT 
            -- Count non-COMPLETED checkpoints
            (SELECT COUNT(*) 
             FROM binlog_apply_checkpoints 
             WHERE config_id = p_config_id 
               AND apply_status != 'COMPLETED'
            ) AS incomplete_checkpoints,
            
            -- Get latest completed source binlog file
            (SELECT source_binlog_file 
             FROM binlog_apply_checkpoints 
             WHERE config_id = p_config_id 
               AND apply_status = 'COMPLETED'
             ORDER BY checkpoint_id DESC LIMIT 1
            ) AS last_completed_binlog,
            
            -- Extract numeric suffix from last completed binlog
            (SELECT CAST(SUBSTRING_INDEX(source_binlog_file, '.', -1) AS UNSIGNED)
             FROM binlog_apply_checkpoints 
             WHERE config_id = p_config_id 
               AND apply_status = 'COMPLETED'
             ORDER BY checkpoint_id DESC LIMIT 1
            ) AS last_completed_binlog_num,
            
            -- Get binlog_file from "Reached active binlog" log entry
            (SELECT binlog_file 
             FROM processing_log 
             WHERE config_id = p_config_id 
               AND log_message LIKE 'Reached active binlog:%'
             ORDER BY log_id DESC LIMIT 1
            ) AS active_binlog_file,
            
            -- Extract numeric suffix from active binlog
            (SELECT CAST(SUBSTRING_INDEX(binlog_file, '.', -1) AS UNSIGNED)
             FROM processing_log 
             WHERE config_id = p_config_id 
               AND log_message LIKE 'Reached active binlog:%'
             ORDER BY log_id DESC LIMIT 1
            ) AS active_binlog_num,
            
            -- Check if "Reached active binlog" entry exists
            (SELECT COUNT(*) > 0
             FROM processing_log 
             WHERE config_id = p_config_id 
               AND log_message LIKE 'Reached active binlog:%'
            ) AS active_binlog_reached
    ) AS status_check
    CROSS JOIN (
        SELECT 
            -- Check if active binlog is exactly 1 ahead of last completed
            COALESCE(
                (SELECT CAST(SUBSTRING_INDEX(binlog_file, '.', -1) AS UNSIGNED)
                 FROM processing_log 
                 WHERE config_id = p_config_id 
                   AND log_message LIKE 'Reached active binlog:%'
                 ORDER BY log_id DESC LIMIT 1
                ) = 
                (SELECT CAST(SUBSTRING_INDEX(source_binlog_file, '.', -1) AS UNSIGNED) + 1
                 FROM binlog_apply_checkpoints 
                 WHERE config_id = p_config_id 
                   AND apply_status = 'COMPLETED'
                 ORDER BY checkpoint_id DESC LIMIT 1
                ),
                0
            ) AS active_binlog_is_next
    ) AS gap_check;
END//

-- ============================================================================
-- PROCEDURE: Check if migration is blocked (simple boolean)
-- ============================================================================
-- Returns 1 if migration is blocked, 0 if OK
-- Blocked conditions:
--   1. Last checkpoint is INTERRUPTED, FAILED, or STARTED (incomplete)
--   2. Blocking message exists in processing_log
--
-- Usage: CALL check_migration_blocked(<CONFIG_ID>);
-- ============================================================================
CREATE PROCEDURE check_migration_blocked(IN p_config_id INT)
BEGIN
    SELECT 
        (
            -- Last checkpoint is INTERRUPTED, FAILED, or STARTED (not COMPLETED)
            EXISTS (
                SELECT 1 FROM binlog_apply_checkpoints 
                WHERE config_id = p_config_id
                  AND apply_status IN ('INTERRUPTED', 'FAILED', 'STARTED')
                ORDER BY checkpoint_id DESC LIMIT 1
            )
            OR
            -- Blocking message exists in processing_log
            EXISTS (
                SELECT 1 FROM processing_log 
                WHERE config_id = p_config_id
                  AND log_message LIKE 'Processing blocked: % Manual intervention required'
            )
        ) AS is_blocked;
END//

-- ============================================================================
-- PROCEDURE: Check if migration is blocked (extended details)
-- ============================================================================
-- Returns detailed information about blocked migration state
-- Includes checkpoint details and recovery information
--
-- Usage: CALL check_migration_blocked_extended(<CONFIG_ID>);
-- ============================================================================
CREATE PROCEDURE check_migration_blocked_extended(IN p_config_id INT)
BEGIN
    SELECT 
        COALESCE(bac.checkpoint_id, 0) AS checkpoint_id,
        bac.source_binlog_file,
        bac.source_binlog_position,
        bac.apply_status,
        bac.error_message AS checkpoint_error,
        bac.apply_started_at,
        bac.apply_completed_at,
        bac.target_binlog_file_before AS recovery_binlog,
        bac.target_binlog_position_before AS recovery_position,
        bac.target_gtid_before AS recovery_gtid,
        pl.log_id AS blocking_log_id,
        pl.log_message AS blocking_message,
        pl.logged_at AS blocked_at,
        CASE 
            WHEN bac.apply_status IN ('INTERRUPTED', 'FAILED', 'STARTED')
                 OR pl.log_id IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_blocked,
        CASE 
            WHEN bac.apply_status IN ('INTERRUPTED', 'FAILED', 'STARTED')
                 OR pl.log_id IS NOT NULL
            THEN 'BLOCKED - MANUAL INTERVENTION REQUIRED'
            ELSE 'OK'
        END AS status
    FROM (SELECT 1) AS dummy
    LEFT JOIN (
        SELECT * FROM binlog_apply_checkpoints 
        WHERE config_id = p_config_id
        ORDER BY checkpoint_id DESC LIMIT 1
    ) bac ON 1=1
    LEFT JOIN (
        SELECT * FROM processing_log 
        WHERE config_id = p_config_id
          AND log_message LIKE 'Processing blocked: % Manual intervention required'
        ORDER BY log_id DESC LIMIT 1
    ) pl ON 1=1;
END//

-- ============================================================================
-- PROCEDURE: Create standard patching maintenance windows
-- ============================================================================
-- Inserts maintenance window records based on the organization's patching schedule.
--
-- Parameters:
--   p_config_id   - Migration config ID (NULL = global, applies to all migrations)
--   p_network     - 'HTZ', 'SIZ', 'DMZ' (weekly patching) or 'IGBN' (monthly patching)
--   p_environment - 'PROD' or 'PRE-PROD'
--   p_server_type - 'VM' or 'PHYSICAL'
--
-- Patching Schedule Matrix:
-- ============================================================================
-- HTZ/SIZ/DMZ Networks (WEEKLY patching):
--   +-------------+----------+---------------------------+
--   | Environment | Type     | Schedule                  |
--   +-------------+----------+---------------------------+
--   | PRE-PROD    | VM       | Every Monday 9PM PST (5h) |
--   | PRE-PROD    | PHYSICAL | Every Monday 9PM PST (5h) |
--   | PROD        | VM       | Every Wednesday 9PM PST (5h) |
--   | PROD        | PHYSICAL | Every Saturday 6AM PST (5h) |
--   +-------------+----------+---------------------------+
--
-- IGBN Network (MONTHLY patching):
--   +-------------+----------+----------------------------------+
--   | Environment | Type     | Schedule                         |
--   +-------------+----------+----------------------------------+
--   | PRE-PROD    | VM       | Last Monday of month 9PM PST (5h)|
--   | PRE-PROD    | PHYSICAL | Last Monday of month 9PM PST (5h)|
--   | PROD        | VM       | Last Wednesday of month 9PM PST (5h)|
--   | PROD        | PHYSICAL | (none - no monthly patching)     |
--   +-------------+----------+----------------------------------+
--
-- Note: Windows spanning midnight are split into two records.
--
-- Usage:
--   CALL create_patching_maintenance_windows(NULL, 'HTZ', 'PROD', 'VM');
--   CALL create_patching_maintenance_windows(NULL, 'IGBN', 'PRE-PROD', 'PHYSICAL');
--   CALL create_patching_maintenance_windows(1, 'DMZ', 'PROD', 'PHYSICAL');
-- ============================================================================
CREATE PROCEDURE create_patching_maintenance_windows(
    IN p_config_id INT,
    IN p_network VARCHAR(20),
    IN p_environment VARCHAR(20),
    IN p_server_type VARCHAR(20)
)
proc_body: BEGIN
    DECLARE v_network VARCHAR(20);
    DECLARE v_env VARCHAR(20);
    DECLARE v_type VARCHAR(20);
    DECLARE v_window_prefix VARCHAR(100);
    DECLARE v_is_weekly BOOLEAN;
    
    -- Normalize inputs to uppercase
    SET v_network = UPPER(TRIM(p_network));
    SET v_env = UPPER(TRIM(p_environment));
    SET v_type = UPPER(TRIM(p_server_type));
    
    -- Validate inputs
    IF v_network NOT IN ('HTZ', 'SIZ', 'DMZ', 'IGBN') THEN
        SIGNAL SQLSTATE '45000' 
            SET MESSAGE_TEXT = 'Invalid network. Must be HTZ, SIZ, DMZ, or IGBN';
    END IF;
    
    IF v_env NOT IN ('PROD', 'PRE-PROD') THEN
        SIGNAL SQLSTATE '45000' 
            SET MESSAGE_TEXT = 'Invalid environment. Must be PROD or PRE-PROD';
    END IF;
    
    IF v_type NOT IN ('VM', 'PHYSICAL') THEN
        SIGNAL SQLSTATE '45000' 
            SET MESSAGE_TEXT = 'Invalid server_type. Must be VM or PHYSICAL';
    END IF;
    
    -- Determine if weekly (HTZ/SIZ/DMZ) or monthly (IGBN)
    SET v_is_weekly = (v_network IN ('HTZ', 'SIZ', 'DMZ'));
    
    -- Build window name prefix including network
    SET v_window_prefix = CONCAT(v_network, ' ', v_env, ' ', v_type, ' Patching');
    
    -- ========================================================================
    -- HTZ/SIZ/DMZ Networks - WEEKLY PATCHING
    -- ========================================================================
    IF v_is_weekly THEN
        
        -- PRE-PROD (VMs and Physical): Monday 9PM PST (5h = until 2AM Tuesday)
        IF v_env = 'PRE-PROD' THEN
            -- Monday 9PM-midnight
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'WEEKLY', 1, '21:00:00', '23:59:59', 
                 CONCAT(v_window_prefix, ' - Mon 9PM'),
                 CONCAT('Weekly patching for PRE-PROD ', v_type, 's (', v_network, ') - Monday evening'));
            
            -- Tuesday midnight-2AM (continuation)
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'WEEKLY', 2, '00:00:00', '02:00:00', 
                 CONCAT(v_window_prefix, ' - Tue 2AM'),
                 CONCAT('Weekly patching for PRE-PROD ', v_type, 's (', v_network, ') - Tuesday morning'));
        
        -- PROD VMs: Wednesday 9PM PST (5h = until 2AM Thursday)
        ELSEIF v_env = 'PROD' AND v_type = 'VM' THEN
            -- Wednesday 9PM-midnight
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'WEEKLY', 3, '21:00:00', '23:59:59', 
                 CONCAT(v_window_prefix, ' - Wed 9PM'),
                 CONCAT('Weekly patching for PROD VMs (', v_network, ') - Wednesday evening'));
            
            -- Thursday midnight-2AM (continuation)
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'WEEKLY', 4, '00:00:00', '02:00:00', 
                 CONCAT(v_window_prefix, ' - Thu 2AM'),
                 CONCAT('Weekly patching for PROD VMs (', v_network, ') - Thursday morning'));
        
        -- PROD Physical: Saturday 6AM PST (5h = until 11AM Saturday)
        ELSEIF v_env = 'PROD' AND v_type = 'PHYSICAL' THEN
            -- Saturday 6AM-11AM (no midnight split needed)
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'WEEKLY', 6, '06:00:00', '11:00:00', 
                 CONCAT(v_window_prefix, ' - Sat 6AM'),
                 CONCAT('Weekly patching for PROD Physical Servers (', v_network, ') - Saturday morning'));
        END IF;
    
    -- ========================================================================
    -- IGBN Network - MONTHLY PATCHING
    -- ========================================================================
    ELSE
        -- PRE-PROD (VMs and Physical): Last Monday 9PM PST (5h = until 2AM Tuesday)
        IF v_env = 'PRE-PROD' THEN
            -- Last Monday 9PM-midnight
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'MONTHLY_WEEKDAY', 1, 5, '21:00:00', '23:59:59', 
                 CONCAT(v_window_prefix, ' - Last Mon 9PM'),
                 CONCAT('Monthly patching for PRE-PROD ', v_type, 's (IGBN) - Last Monday evening'));
            
            -- Tuesday after last Monday, midnight-2AM (continuation)
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'MONTHLY_WEEKDAY', 2, 5, '00:00:00', '02:00:00', 
                 CONCAT(v_window_prefix, ' - Last Mon cont.'),
                 CONCAT('Monthly patching for PRE-PROD ', v_type, 's (IGBN) - Tuesday after last Monday'));
        
        -- PROD VMs: Last Wednesday 9PM PST (5h = until 2AM Thursday)
        ELSEIF v_env = 'PROD' AND v_type = 'VM' THEN
            -- Last Wednesday 9PM-midnight
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'MONTHLY_WEEKDAY', 3, 5, '21:00:00', '23:59:59', 
                 CONCAT(v_window_prefix, ' - Last Wed 9PM'),
                 'Monthly patching for PROD VMs (IGBN) - Last Wednesday evening');
            
            -- Thursday after last Wednesday, midnight-2AM (continuation)
            INSERT INTO maintenance_windows 
                (config_id, schedule_type, day_of_week, week_of_month, start_time, end_time, window_name, description)
            VALUES 
                (p_config_id, 'MONTHLY_WEEKDAY', 4, 5, '00:00:00', '02:00:00', 
                 CONCAT(v_window_prefix, ' - Last Wed cont.'),
                 'Monthly patching for PROD VMs (IGBN) - Thursday after last Wednesday');
        
        -- PROD Physical: NO monthly patching for IGBN
        ELSEIF v_env = 'PROD' AND v_type = 'PHYSICAL' THEN
            -- Return message that no windows are needed
            SELECT 'No maintenance windows created: PROD Physical Servers on IGBN have no monthly patching schedule' AS message;
            -- Early return - skip the summary query
            LEAVE proc_body;
        END IF;
    END IF;
    
    -- Return summary of created windows
    SELECT 
        window_id,
        window_name,
        schedule_type,
        CASE day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        week_of_month,
        start_time,
        end_time,
        description
    FROM maintenance_windows
    WHERE (config_id = p_config_id OR (p_config_id IS NULL AND config_id IS NULL))
      AND window_name LIKE CONCAT(v_window_prefix, '%')
    ORDER BY window_id DESC
    LIMIT 10;
END//

-- ============================================================================
-- PROCEDURE: Test maintenance window logic with simulated date/time
-- ============================================================================
-- Validates maintenance window logic by testing against a specific date/time.
-- Useful for verifying windows will trigger correctly before actual patching.
--
-- Parameters:
--   p_config_id  - Migration config ID to test
--   p_test_datetime - DateTime to simulate (e.g., '2026-05-25 21:30:00')
--                     If NULL, uses current datetime
--
-- Returns: Detailed analysis showing which windows match and why
--
-- Usage:
--   -- Test if Monday 9:30 PM would be in maintenance
--   CALL test_maintenance_window(1, '2026-05-25 21:30:00');
--
--   -- Test current time
--   CALL test_maintenance_window(1, NULL);
--
--   -- Test last Monday of month at 9:30 PM
--   CALL test_maintenance_window(1, '2026-05-25 21:30:00');
--
--   -- Test a Wednesday at 10 PM
--   CALL test_maintenance_window(1, '2026-05-27 22:00:00');
-- ============================================================================
CREATE PROCEDURE test_maintenance_window(
    IN p_config_id INT,
    IN p_test_datetime DATETIME
)
BEGIN
    DECLARE v_test_dt DATETIME;
    DECLARE v_test_date DATE;
    DECLARE v_test_time TIME;
    DECLARE v_dow TINYINT;
    DECLARE v_dom TINYINT;
    DECLARE v_wom TINYINT;
    DECLARE v_is_last TINYINT;
    DECLARE v_days_in_month TINYINT;
    DECLARE v_day_name VARCHAR(10);
    
    -- Use provided datetime or current
    SET v_test_dt = COALESCE(p_test_datetime, NOW());
    SET v_test_date = DATE(v_test_dt);
    SET v_test_time = TIME(v_test_dt);
    
    -- Calculate date components
    SET v_dow = DAYOFWEEK(v_test_dt) - 1;  -- 0=Sunday, 6=Saturday
    SET v_dom = DAY(v_test_date);
    SET v_wom = CEIL(v_dom / 7);
    SET v_days_in_month = DAY(LAST_DAY(v_test_date));
    SET v_is_last = CASE WHEN v_dom + 7 > v_days_in_month THEN 1 ELSE 0 END;
    SET v_day_name = CASE v_dow
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END;
    
    -- Output test parameters
    SELECT 
        '=== MAINTENANCE WINDOW TEST ===' AS section,
        v_test_dt AS test_datetime,
        v_test_date AS test_date,
        v_test_time AS test_time,
        v_day_name AS day_name,
        v_dow AS day_of_week_num,
        v_dom AS day_of_month,
        v_wom AS week_of_month,
        v_is_last AS is_last_occurrence,
        v_days_in_month AS days_in_month,
        p_config_id AS config_id;
    
    -- Check each maintenance window and show match status
    SELECT 
        mw.window_id,
        mw.window_name,
        mw.schedule_type,
        mw.day_of_week AS mw_dow,
        mw.day_of_month AS mw_dom,
        mw.week_of_month AS mw_wom,
        mw.specific_date AS mw_specific_date,
        mw.start_time,
        mw.end_time,
        CASE WHEN mw.config_id IS NULL THEN 'GLOBAL' ELSE CONCAT('config_id=', mw.config_id) END AS scope,
        -- Time check
        CASE WHEN v_test_time BETWEEN mw.start_time AND mw.end_time 
             THEN 'YES' ELSE 'NO' END AS time_matches,
        -- Schedule type specific checks
        CASE mw.schedule_type
            WHEN 'WEEKLY' THEN 
                CASE WHEN mw.day_of_week = v_dow THEN 'YES' ELSE 'NO' END
            WHEN 'MONTHLY_DAY' THEN 
                CASE WHEN mw.day_of_month = v_dom THEN 'YES' ELSE 'NO' END
            WHEN 'MONTHLY_WEEKDAY' THEN 
                CASE WHEN mw.day_of_week = v_dow 
                          AND (mw.week_of_month = v_wom OR (mw.week_of_month = 5 AND v_is_last = 1))
                     THEN 'YES' ELSE 'NO' END
            WHEN 'SPECIFIC_DATE' THEN 
                CASE WHEN mw.specific_date = v_test_date THEN 'YES' ELSE 'NO' END
        END AS schedule_matches,
        -- Overall match
        CASE 
            WHEN mw.is_active = TRUE
                 AND v_test_time BETWEEN mw.start_time AND mw.end_time
                 AND (
                     (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_dow)
                     OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_dom)
                     OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                         AND mw.day_of_week = v_dow
                         AND (mw.week_of_month = v_wom OR (mw.week_of_month = 5 AND v_is_last = 1)))
                     OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_test_date)
                 )
            THEN '>>> IN MAINTENANCE <<<'
            WHEN mw.is_active = FALSE
            THEN 'DISABLED'
            ELSE 'not in window'
        END AS result
    FROM maintenance_windows mw
    WHERE mw.config_id = p_config_id OR mw.config_id IS NULL
    ORDER BY 
        CASE 
            WHEN mw.is_active = TRUE
                 AND v_test_time BETWEEN mw.start_time AND mw.end_time
                 AND (
                     (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_dow)
                     OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_dom)
                     OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                         AND mw.day_of_week = v_dow
                         AND (mw.week_of_month = v_wom OR (mw.week_of_month = 5 AND v_is_last = 1)))
                     OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_test_date)
                 )
            THEN 0
            ELSE 1
        END,
        mw.window_id;
    
    -- Summary: would processing be blocked?
    SELECT 
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM maintenance_windows mw
                WHERE mw.is_active = TRUE
                  AND (mw.config_id = p_config_id OR mw.config_id IS NULL)
                  AND v_test_time BETWEEN mw.start_time AND mw.end_time
                  AND (
                      (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = v_dow)
                      OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = v_dom)
                      OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                          AND mw.day_of_week = v_dow
                          AND (mw.week_of_month = v_wom OR (mw.week_of_month = 5 AND v_is_last = 1)))
                      OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = v_test_date)
                  )
            )
            THEN CONCAT('BLOCKED: Migration would skip processing at ', v_test_dt)
            ELSE CONCAT('OK: Migration would process normally at ', v_test_dt)
        END AS final_result;
END//

DELIMITER ;
