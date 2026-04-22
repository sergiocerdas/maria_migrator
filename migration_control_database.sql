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
    processing_status ENUM('RUNNING', 'PAUSED', 'STOPPED', 'ERROR', 'FAILOVER_DETECTED') DEFAULT 'STOPPED',
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

    TRUNCATE TABLE processing_log;
    TRUNCATE TABLE failover_events;
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

