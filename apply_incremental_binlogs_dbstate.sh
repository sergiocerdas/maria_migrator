#!/usr/bin/env bash
set -euo pipefail

############################################
# PARAMETERS
############################################

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <CONFIG_FILE>"
    exit 1
fi

CONFIG_FILE="$1"

shift 1   # Remove config file from argument list

############################################
# CONFIGURATION - loaded from external file
############################################

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Config file not found: $CONFIG_FILE"
    exit 1
fi

# shellcheck disable=SC1090
source "$CONFIG_FILE"

# Validate all required scalar variables are present in the config file
for _var in PORT BINLOG_DIR WORKDIR LOG_FILE \
            TARGET_HOST TARGET_PORT TARGET_USER TARGET_PASS \
            MYSQL_BINLOG MYSQL \
            DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME DB_SSL\
            MIGRATION_NAME SOURCE_INSTANCE_NAME SOURCE_VIP_PORT \
            SOURCE_CLUSTER_ID TARGET_INSTANCE_NAME \
            INITIAL_BINLOG INITIAL_POS; do
    if [[ -z "${!_var:-}" ]]; then
        echo "ERROR: Required config variable '$_var' is not set in $CONFIG_FILE"
        exit 1
    fi
done

# Validate SOURCE_CLUSTER_NODES array
if [[ ${#SOURCE_CLUSTER_NODES[@]} -lt 2 ]]; then
    echo "ERROR: SOURCE_CLUSTER_NODES must contain at least 2 entries in $CONFIG_FILE"
    exit 1
fi

if [[ "${SOURCE_CLUSTER_NODES[0]}" == "${SOURCE_CLUSTER_NODES[1]}" ]]; then
    echo "ERROR: SOURCE_CLUSTER_NODES entries are identical in $CONFIG_FILE"
    exit 1
fi

CLUSTER_NODES=("${SOURCE_CLUSTER_NODES[@]}")

echo "CLUSTER_NODES: (${CLUSTER_NODES[*]})"


############################################
# LOGGING HELPERS
############################################
mkdir -p "$WORKDIR"



log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG_FILE"
}


section() {
    log ""
    log "========== $* =========="
}

############################################
# INSTANCE LOCK - prevent concurrent executions
############################################
LOCK_FILE="/var/run/mariadb-migration.lock"

# Try to acquire an exclusive lock (non-blocking)
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    log "Another instance is already running — skipping this iteration"
    exit 0
fi

# Write PID to lock file for debugging
echo $$ >&200

############################################
# SIGNAL/CRASH HANDLER
############################################

# Track if we're in the middle of processing for crash reporting
PROCESSING_STATE="INITIALIZING"
PROCESSING_GTID=""
PROCESSING_BINLOG=""
PROCESSING_POSITION=""

# Handler for signals and unexpected exits
handle_interrupt() {
    local exit_code=$?
    local signal_name="${1:-UNKNOWN}"
    
    log ""
    log "========== INTERRUPT/CRASH DETECTED =========="
    log "Signal: $signal_name"
    log "Exit code: $exit_code"
    log "Processing state: $PROCESSING_STATE"
    log "Current GTID: ${PROCESSING_GTID:-none}"
    log "Current binlog: ${PROCESSING_BINLOG:-none}"
    log "Current position: ${PROCESSING_POSITION:-none}"
    
    # Log to database if CONFIG_ID is available
    if [[ -n "${CONFIG_ID:-}" && -n "${CURRENT_NODE_ID:-}" ]]; then
        local msg="Script interrupted: signal=$signal_name state=$PROCESSING_STATE"
        if [[ -n "$PROCESSING_GTID" ]]; then
            msg+=" gtid=$PROCESSING_GTID"
        fi
        
        # Use direct mysql call since db_log_critical might not work if db_log isn't fully initialized
        mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
              --port="$DB_PORT" $DB_SSL "$DB_NAME" \
              -e "INSERT INTO processing_log (config_id, node_id, log_level, log_message, binlog_file, binlog_position, gtid_position, process_pid, thread_info)
                  VALUES ($CONFIG_ID, $CURRENT_NODE_ID, 'CRITICAL', 
                          '${msg//\'/\'\'}',
                          $(if [[ -n "$PROCESSING_BINLOG" ]]; then echo "'$PROCESSING_BINLOG'"; else echo "NULL"; fi),
                          $(if [[ -n "$PROCESSING_POSITION" ]]; then echo "$PROCESSING_POSITION"; else echo "NULL"; fi),
                          $(if [[ -n "$PROCESSING_GTID" ]]; then echo "'$PROCESSING_GTID'"; else echo "NULL"; fi),
                          $$, 'crash_handler');" 2>/dev/null || true
        
        # Update migration_status to reflect interrupted state
        mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
              --port="$DB_PORT" $DB_SSL "$DB_NAME" \
              -e "UPDATE migration_status SET 
                  processing_status = 'ERROR',
                  last_processed_timestamp = NOW()
                  WHERE config_id = $CONFIG_ID;" 2>/dev/null || true
        
        # Mark any in-progress checkpoints as INTERRUPTED
        # This provides recovery information for partially applied binlogs
        mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
              --port="$DB_PORT" $DB_SSL "$DB_NAME" \
              -e "UPDATE binlog_apply_checkpoints SET 
                  apply_status = 'INTERRUPTED',
                  apply_completed_at = NOW(),
                  error_message = 'Process interrupted: signal=$signal_name'
                  WHERE config_id = $CONFIG_ID 
                    AND apply_status = 'STARTED'
                    AND process_pid = $$;" 2>/dev/null || true
        
        # Log recovery information if we have a PROCESSING_BINLOG
        if [[ -n "$PROCESSING_BINLOG" && "$PROCESSING_STATE" == "APPLYING" ]]; then
            local recovery_info
            recovery_info=$(mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
                  --port="$DB_PORT" $DB_SSL --skip-column-names --batch "$DB_NAME" \
                  -e "SELECT CONCAT('Recovery point: ', target_binlog_file_before, ':', target_binlog_position_before, ' (checkpoint_id=', checkpoint_id, ')')
                      FROM binlog_apply_checkpoints 
                      WHERE config_id = $CONFIG_ID 
                        AND source_binlog_file = '$PROCESSING_BINLOG'
                        AND apply_status = 'INTERRUPTED'
                      ORDER BY checkpoint_id DESC LIMIT 1;" 2>/dev/null || true)
            
            if [[ -n "$recovery_info" ]]; then
                log "RECOVERY INFO: $recovery_info"
                log "Use flashback/PITR to target binlog position above to restore pre-apply state"
            fi
        fi
    fi
    
    log "Interrupt handling complete - exiting"
    
    # Exit with appropriate code
    if [[ "$signal_name" == "EXIT" ]]; then
        exit $exit_code
    else
        exit 130  # Standard exit code for SIGINT
    fi
}

# Set up traps for various signals
trap 'handle_interrupt SIGINT' SIGINT
trap 'handle_interrupt SIGTERM' SIGTERM
trap 'handle_interrupt SIGHUP' SIGHUP
trap 'handle_interrupt EXIT' EXIT

############################################
# DATABASE FUNCTIONS
############################################

# Low-level helper: execute a query against the control DB and return stdout
db_query() {
    #log "   Query : $1 "
    mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" --port="$DB_PORT" $DB_SSL "$DB_NAME" -e  "$1"
}

# Scalar query helper: returns a single clean value with no column headers
db_scalar() {
    mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
          --port="$DB_PORT" $DB_SSL --skip-column-names --batch \
          "$DB_NAME" -e "$1" 2>/dev/null || true
}

# db connection validation
test_db_connection() {
    if mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" --port="$DB_PORT" $DB_SSL "$DB_NAME" -e "SELECT 1;" >/dev/null 2>&1; then
        echo "Database connection successful"
        return 0
    else
        echo "Database connection failed"
        return 1
    fi
}

# Database logging function
db_log() {
    local log_level="$1"
    local log_message="$2"
    local binlog_file="$3"      # Optional
    local binlog_position="$4"  # Optional
    local gtid_position="$5"    # Optional

    # Validate log level
    case "$log_level" in
        DEBUG|INFO|WARN|ERROR|CRITICAL)
            ;;
        *)
            log_level="INFO"  # Default to INFO if invalid level
            ;;
    esac

    # Get current process info
    local process_pid=$$
    local current_hostname=$(hostname)

    # Escape single quotes in log message for SQL safety
    local escaped_message=$(echo "$log_message" | sed "s/'/''/g")

    # Build the SQL INSERT statement
    local sql_query="INSERT INTO processing_log (
        config_id,
        node_id,
        log_level,
        log_message,
        binlog_file,
        binlog_position,
        gtid_position,
        process_pid,
        thread_info
    ) VALUES (
        $CONFIG_ID,
        $CURRENT_NODE_ID,
        '$log_level',
        '$escaped_message',
        $(if [[ -n "$binlog_file" ]]; then echo "'$binlog_file'"; else echo "NULL"; fi),
        $(if [[ -n "$binlog_position" ]]; then echo "$binlog_position"; else echo "NULL"; fi),
        $(if [[ -n "$gtid_position" ]]; then echo "'$gtid_position'"; else echo "NULL"; fi),
        $process_pid,
        'bash_migration_script'
    );"

    # Execute the SQL query
    #if ! mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" -e "$sql_query" 2>/dev/null; then
    if ! mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" --port="$DB_PORT" $DB_SSL "$DB_NAME" -e "$sql_query" 2>/dev/null; then
        # If database logging fails, fall back to file logging
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [DB_LOG_FAILED] [$log_level] $log_message" >> "$LOG_FILE"
    fi
}

# Convenience wrapper functions
db_log_debug() {
    db_log "DEBUG" "$1" "$2" "$3" "$4"
}

db_log_info() {
    db_log "INFO" "$1" "$2" "$3" "$4"
}

db_log_warn() {
    db_log "WARN" "$1" "$2" "$3" "$4"
}

db_log_error() {
    db_log "ERROR" "$1" "$2" "$3" "$4"
}

db_log_critical() {
    db_log "CRITICAL" "$1" "$2" "$3" "$4"
}

############################################
# BLOCKING CONDITIONS CHECK
############################################
# Checks for unresolved error conditions that require manual intervention
# before processing can continue. This prevents applying binlogs when:
#   1. There are ERROR entries in processing_log (newer than last acknowledgment)
#   2. There are non-COMPLETED checkpoints (STARTED, FAILED, INTERRUPTED)
#
# Returns: 0 if OK to proceed, 1 if blocked
############################################
check_blocking_conditions() {
    local error_ack_timestamp
    local error_count
    local checkpoint_count
    local blocked=0
    
    log "Checking for blocking error conditions..."
    
    # Get the last error acknowledgment timestamp (NULL means never acknowledged)
    error_ack_timestamp=$(db_scalar "SELECT COALESCE(error_acknowledged_at, '1970-01-01 00:00:00') 
        FROM migration_status WHERE config_id = $CONFIG_ID;")
    
    if [[ -z "$error_ack_timestamp" ]]; then
        # No migration_status record yet - this is a fresh start, no blocking conditions
        log "No migration_status record found - fresh start, no blocking conditions"
        return 0
    fi
    
    # Check 1: ERROR entries in processing_log newer than last acknowledgment
    error_count=$(db_scalar "SELECT COUNT(*) FROM processing_log 
        WHERE config_id = $CONFIG_ID 
          AND log_level = 'ERROR' 
          AND logged_at > '$error_ack_timestamp';")
    
    if [[ "$error_count" -gt 0 ]]; then
        blocked=1
        log ""
        log "============================================================"
        log "BLOCKING CONDITION: $error_count unacknowledged ERROR(s) in processing_log"
        log "============================================================"
        log ""
        log "Recent ERROR entries:"
        
        # Show the most recent errors
        mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
              --port="$DB_PORT" $DB_SSL "$DB_NAME" --table \
              -e "SELECT log_id, logged_at, log_message, binlog_file, binlog_position 
                  FROM processing_log 
                  WHERE config_id = $CONFIG_ID 
                    AND log_level = 'ERROR' 
                    AND logged_at > '$error_ack_timestamp'
                  ORDER BY logged_at DESC 
                  LIMIT 5;" 2>/dev/null | tee -a "$LOG_FILE"
        
        log ""
    fi
    
    # Check 2: Non-COMPLETED checkpoints (STARTED, FAILED, INTERRUPTED)
    checkpoint_count=$(db_scalar "SELECT COUNT(*) FROM binlog_apply_checkpoints 
        WHERE config_id = $CONFIG_ID 
          AND apply_status IN ('STARTED', 'FAILED', 'INTERRUPTED');")
    
    if [[ "$checkpoint_count" -gt 0 ]]; then
        blocked=1
        log ""
        log "============================================================"
        log "BLOCKING CONDITION: $checkpoint_count incomplete checkpoint(s) in binlog_apply_checkpoints"
        log "============================================================"
        log ""
        log "Incomplete checkpoints (recovery points for flashback):"
        
        mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
              --port="$DB_PORT" $DB_SSL "$DB_NAME" --table \
              -e "SELECT checkpoint_id, source_binlog_file, source_binlog_position,
                         apply_status, apply_started_at, 
                         target_binlog_file_before, target_binlog_position_before,
                         error_message
                  FROM binlog_apply_checkpoints 
                  WHERE config_id = $CONFIG_ID 
                    AND apply_status IN ('STARTED', 'FAILED', 'INTERRUPTED')
                  ORDER BY checkpoint_id DESC;" 2>/dev/null | tee -a "$LOG_FILE"
        
        log ""
    fi
    
    if [[ $blocked -eq 1 ]]; then
        log "============================================================"
        log "MANUAL INTERVENTION REQUIRED"
        log "============================================================"
        log ""
        log "NOTE: Before determining an actual error condition occurred, verify that the"
        log "process is not actively applying binlogs on the current primary that is"
        log "a different server other than the local one. If so, ignore this manual"
        log "intervention requirement and let the next iteration run as expected."
        log ""
        log "Processing is BLOCKED due to the above condition(s)."
        log "Review the errors and take corrective action before continuing."
        log ""
        log "To acknowledge errors and unblock processing, run:"
        log ""
        log "  UPDATE migration_status"
        log "  SET error_acknowledged_at = NOW()"
        log "  WHERE config_id = $CONFIG_ID;"
        log ""
        log "If there are incomplete checkpoints, you must ALSO either:"
        log "  1. Verify target data integrity and mark as COMPLETED:"
        log "     UPDATE binlog_apply_checkpoints"
        log "     SET apply_status = 'COMPLETED', apply_completed_at = NOW(),"
        log "         error_message = 'Manually verified by operator'"
        log "     WHERE config_id = $CONFIG_ID AND apply_status != 'COMPLETED';"
        log ""
        log "  OR"
        log ""
        log "  2. Perform flashback recovery using target_binlog_file_before"
        log "     and target_binlog_position_before from the checkpoint record."
        log ""
        log "============================================================"
        
        db_log_warn "Processing blocked: $error_count unacknowledged error(s), $checkpoint_count incomplete checkpoint(s). Manual intervention required." "" "" ""
        
        return 1
    fi
    
    log "No blocking conditions found - OK to proceed"
    return 0
}

############################################
# BINLOG APPLY FUNCTION
############################################

# Apply extracted SQL to target database
# Returns 0 on success, 1 on failure
# Logs errors clearly to both file and control database
# Creates checkpoint with target binlog position before apply for recovery
apply_binlog_to_target() {
    local sql_file="$1"
    local binlog_file="$2"
    local start_pos="$3"
    
    local output
    local rc
    local checkpoint_id=""
    
    log "Applying binlog $binlog_file to target..."
    
    ############################################
    # GET TARGET BINLOG POSITION (BEFORE APPLY)
    ############################################
    # Query target database's current binlog position for recovery checkpoint
    local target_status
    target_status=$("$MYSQL" "--host=$TARGET_HOST" "--user=$TARGET_USER" \
        "--password=$TARGET_PASS" "--port=$TARGET_PORT" "--ssl=true" \
        --skip-column-names --batch \
        -e "SHOW MASTER STATUS;" 2>/dev/null)
    
    local target_binlog_before=""
    local target_pos_before=""
    local target_gtid_before=""
    
    if [[ -n "$target_status" ]]; then
        target_binlog_before=$(echo "$target_status" | awk '{print $1}')
        target_pos_before=$(echo "$target_status" | awk '{print $2}')
        # GTID is typically in column 5 for MariaDB
        target_gtid_before=$(echo "$target_status" | awk '{print $5}')
        
        log "Target checkpoint BEFORE apply:"
        log "  Target binlog: $target_binlog_before"
        log "  Target position: $target_pos_before"
        log "  Target GTID: ${target_gtid_before:-none}"
    else
        log "WARNING: Could not get target binlog position - checkpoint will be incomplete"
    fi
    
    ############################################
    # CREATE CHECKPOINT RECORD
    ############################################
    if [[ -n "$target_binlog_before" && -n "$target_pos_before" ]]; then
        db_query "INSERT INTO binlog_apply_checkpoints (
            config_id,
            source_binlog_file,
            source_binlog_position,
            target_binlog_file_before,
            target_binlog_position_before,
            target_gtid_before,
            apply_status,
            process_pid,
            process_hostname
        ) VALUES (
            $CONFIG_ID,
            '${binlog_file//\'/\'\'}',
            $start_pos,
            '${target_binlog_before//\'/\'\'}',
            $target_pos_before,
            $(if [[ -n "$target_gtid_before" ]]; then echo "'${target_gtid_before//\'/\'\'}'"; else echo "NULL"; fi),
            'STARTED',
            $$,
            '$(hostname -s)'
        );" 2>/dev/null
        
        # Get the checkpoint ID for later update
        checkpoint_id=$(db_scalar "SELECT MAX(checkpoint_id) FROM binlog_apply_checkpoints 
            WHERE config_id = $CONFIG_ID AND source_binlog_file = '${binlog_file//\'/\'\'}';")
        
        log "Checkpoint created: ID=$checkpoint_id (recovery point: $target_binlog_before:$target_pos_before)"
    fi
    
    db_log_info "Starting binlog apply: $binlog_file (start_pos=$start_pos) checkpoint=$checkpoint_id target_recovery_point=$target_binlog_before:$target_pos_before" "$binlog_file" "$start_pos" ""
    
    ############################################
    # APPLY SQL TO TARGET
    ############################################
    output=$("$MYSQL" "--host=$TARGET_HOST" "--user=$TARGET_USER" \
        "--password=$TARGET_PASS" "--port=$TARGET_PORT" "--ssl=true" \
        < "$sql_file" 2>&1)
    rc=$?
    
    ############################################
    # HANDLE SUCCESS
    ############################################
    if [[ $rc -eq 0 ]]; then
        log "Binlog applied successfully: $binlog_file"
        
        # Get target position AFTER apply
        local target_status_after
        target_status_after=$("$MYSQL" "--host=$TARGET_HOST" "--user=$TARGET_USER" \
            "--password=$TARGET_PASS" "--port=$TARGET_PORT" "--ssl=true" \
            --skip-column-names --batch \
            -e "SHOW MASTER STATUS;" 2>/dev/null)
        
        local target_binlog_after=""
        local target_pos_after=""
        local target_gtid_after=""
        
        if [[ -n "$target_status_after" ]]; then
            target_binlog_after=$(echo "$target_status_after" | awk '{print $1}')
            target_pos_after=$(echo "$target_status_after" | awk '{print $2}')
            target_gtid_after=$(echo "$target_status_after" | awk '{print $5}')
        fi
        
        # Update checkpoint as completed
        if [[ -n "$checkpoint_id" ]]; then
            db_query "UPDATE binlog_apply_checkpoints SET
                apply_status = 'COMPLETED',
                apply_completed_at = NOW(),
                target_binlog_file_after = $(if [[ -n "$target_binlog_after" ]]; then echo "'${target_binlog_after//\'/\'\'}'"; else echo "NULL"; fi),
                target_binlog_position_after = $(if [[ -n "$target_pos_after" ]]; then echo "$target_pos_after"; else echo "NULL"; fi),
                target_gtid_after = $(if [[ -n "$target_gtid_after" ]]; then echo "'${target_gtid_after//\'/\'\'}'"; else echo "NULL"; fi)
                WHERE checkpoint_id = $checkpoint_id;" 2>/dev/null || true
        fi
        
        db_log_info "Binlog applied successfully: $binlog_file checkpoint=$checkpoint_id" "$binlog_file" "$start_pos" ""
        return 0
    fi
    
    ############################################
    # HANDLE FAILURE
    ############################################
    log "ERROR: Failed to apply binlog $binlog_file"
    log "ERROR: MySQL exit code: $rc"
    log "ERROR: MySQL output: $output"
    log ""
    log "========== RECOVERY INFORMATION =========="
    log "To recover target database to pre-apply state:"
    log "  Target binlog file: $target_binlog_before"
    log "  Target position: $target_pos_before"
    log "  Target GTID: ${target_gtid_before:-none}"
    log "  Checkpoint ID: ${checkpoint_id:-none}"
    log ""
    log "Use point-in-time recovery or flashback to: $target_binlog_before:$target_pos_before"
    log "==========================================="
    
    # Log to database with full error details
    local escaped_output="${output//\'/\'\'}"  # Escape single quotes for SQL
    escaped_output="${escaped_output:0:4000}"   # Truncate to avoid SQL issues
    
    # Update checkpoint as failed
    if [[ -n "$checkpoint_id" ]]; then
        db_query "UPDATE binlog_apply_checkpoints SET
            apply_status = 'FAILED',
            apply_completed_at = NOW(),
            error_message = '${escaped_output:0:2000}',
            error_code = $rc
            WHERE checkpoint_id = $checkpoint_id;" 2>/dev/null || true
    fi
    
    db_log_error "BINLOG APPLY FAILED: $binlog_file - exit_code=$rc - $escaped_output" "$binlog_file" "$start_pos" ""
    db_log_critical "ACTION REQUIRED: Binlog $binlog_file failed. Recovery point: $target_binlog_before:$target_pos_before (checkpoint_id=$checkpoint_id). Use flashback/PITR to recover target." "$binlog_file" "$start_pos" ""
    
    # Update migration status to ERROR state
    db_query "UPDATE migration_status SET 
        processing_status = 'ERROR',
        last_processed_timestamp = NOW()
        WHERE config_id = $CONFIG_ID;" 2>/dev/null || true
    
    return 1
}

############################################
# LOG CLUSTER CONFIG
############################################

section "CLUSTER CONFIGURATION"

log "Cluster nodes defined (${#CLUSTER_NODES[@]} total):"
for _i in "${!CLUSTER_NODES[@]}"; do
    log "  Node $((_i + 1)): ${CLUSTER_NODES[$_i]}"
done
log "Local host: $(hostname)"

############################################
# OS REQUIREMENTS
############################################
section "OS REQUIREMENTS"

log "Checking for required commands: mariadb-binlog, mariadb"

# Create mariadb-binlog symlink (required for podman-based MariaDB installations)
if [[ -f /usr/bin/podman_client && ! -e /usr/bin/mariadb-binlog ]]; then
    log "Creating symbolic link: /usr/bin/mariadb-binlog -> /usr/bin/podman_client"
    ln -s /usr/bin/podman_client /usr/bin/mariadb-binlog
elif [[ -L /usr/bin/mariadb-binlog ]]; then
    log "Symbolic link /usr/bin/mariadb-binlog already exists"
elif [[ -f /usr/bin/mariadb-binlog ]]; then
    log "mariadb-binlog binary already exists at /usr/bin/mariadb-binlog"
else
    log "WARNING: /usr/bin/podman_client not found - mariadb-binlog symlink not created"
fi

# Create mariadb symlink (required for podman-based MariaDB installations)
if [[ -f /usr/bin/podman_client && ! -e /usr/bin/mariadb ]]; then
    log "Creating symbolic link: /usr/bin/mariadb -> /usr/bin/podman_client"
    ln -s /usr/bin/podman_client /usr/bin/mariadb
elif [[ -L /usr/bin/mariadb ]]; then
    log "Symbolic link /usr/bin/mariadb already exists"
elif [[ -f /usr/bin/mariadb ]]; then
    log "mariadb binary already exists at /usr/bin/mariadb"
else
    log "WARNING: /usr/bin/podman_client not found - mariadb symlink not created"
fi

############################################
# SCRIPT START
############################################
section "SCRIPT START"
log "PID=$$"
log "WORKDIR=$WORKDIR"
log "LOG_FILE=$LOG_FILE"

############################################
# CONTROL DATABASE INITIALIZATION
############################################
section "CONTROL DATABASE CHECK"

# Check if control database exists; if not, create it from schema file
_db_exists=$(mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
    --port="$DB_PORT" $DB_SSL --skip-column-names --batch \
    -e "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '$DB_NAME';" 2>/dev/null || true)

if [[ -z "$_db_exists" ]]; then
    log "Control database '$DB_NAME' does not exist — attempting to create schema"

    # Look for migration_control_database.sql in several locations
    SCHEMA_FILE=""
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    CONFIG_DIR="$(dirname "$CONFIG_FILE")"

    for _candidate in \
        "$SCRIPT_DIR/migration_control_database.sql" \
        "$CONFIG_DIR/migration_control_database.sql" \
        "/opt/mariadb-migration/migration_control_database.sql"; do
        if [[ -f "$_candidate" ]]; then
            SCHEMA_FILE="$_candidate"
            break
        fi
    done

    if [[ -z "$SCHEMA_FILE" ]]; then
        log "ERROR: Control database does not exist and migration_control_database.sql not found"
        log "Searched locations:"
        log "  - $SCRIPT_DIR/migration_control_database.sql"
        log "  - $CONFIG_DIR/migration_control_database.sql"
        log "  - /opt/mariadb-migration/migration_control_database.sql"
        log "Please create the control database manually or place migration_control_database.sql in one of the above locations"
        exit 1
    fi

    log "Found schema file: $SCHEMA_FILE"
    log "Creating control database and tables..."

    if mysql --host="$DB_HOST" --user="$DB_USER" --password="$DB_PASSWORD" \
        --port="$DB_PORT" $DB_SSL < "$SCHEMA_FILE" 2>&1; then
        log "Control database '$DB_NAME' created successfully"
    else
        log "ERROR: Failed to create control database from $SCHEMA_FILE"
        exit 1
    fi
else
    log "Control database '$DB_NAME' exists"
fi

############################################
# DETERMINE SERVER ID
############################################
section "DETERMINE SERVER ID"

HOSTNAME_SHORT=$(hostname -s)
log "Local hostname: $HOSTNAME_SHORT"

# Match local hostname against CLUSTER_NODES to find the local index
LOCAL_NODE_INDEX=-1
for _i in "${!CLUSTER_NODES[@]}"; do
    if [[ "${CLUSTER_NODES[$_i],,}" == "${HOSTNAME_SHORT,,}" ]]; then
        LOCAL_NODE_INDEX=$_i
        break
    fi
done

if [[ "$LOCAL_NODE_INDEX" -lt 0 ]]; then
    log "ERROR: Local hostname '$HOSTNAME_SHORT' not found in CLUSTER_NODES"
    exit 1
fi

log "Matched local node: ${CLUSTER_NODES[$LOCAL_NODE_INDEX]} (index=$LOCAL_NODE_INDEX)"

# Query local MySQL for this node's server_id
LOCAL_SERVER_ID=$(mysql --host "$HOSTNAME_SHORT" --user=mysql01 --port="$PORT" \
    --skip-column-names --silent -e "SELECT @@server_id;")

if [[ -z "$LOCAL_SERVER_ID" ]]; then
    log "ERROR: Could not retrieve server_id from local node '$HOSTNAME_SHORT'"
    exit 1
fi
log "Local server_id: $LOCAL_SERVER_ID"

# Query all other cluster nodes for their server_ids
OTHER_SERVER_IDS=()
for _i in "${!CLUSTER_NODES[@]}"; do
    [[ "$_i" -eq "$LOCAL_NODE_INDEX" ]] && continue
    _other_node="${CLUSTER_NODES[$_i]}"
    _sid=$(mysql --host "$_other_node" --user=mysql01 --port="$PORT" \
        --skip-column-names --silent -e "SELECT @@server_id;" 2>/dev/null || true)
    if [[ -n "$_sid" ]]; then
        OTHER_SERVER_IDS+=("$_sid")
        log "Other node: $_other_node → server_id=$_sid"
    else
        log "WARNING: Could not retrieve server_id from node '$_other_node'"
    fi
done

if [[ ${#OTHER_SERVER_IDS[@]} -eq 0 ]]; then
    log "ERROR: Could not retrieve server_id from any other cluster node"
    exit 1
fi

# Build a grep-compatible pattern: single value or alternation group for multiple nodes
if [[ ${#OTHER_SERVER_IDS[@]} -eq 1 ]]; then
    OTHER_SERVER_ID="${OTHER_SERVER_IDS[0]}"
else
    OTHER_SERVER_ID="($(IFS='|'; echo "${OTHER_SERVER_IDS[*]}"))"
fi

log "Other server_id pattern: $OTHER_SERVER_ID"

############################################
# CLUSTER NODES CHECK
############################################
section "CLUSTER NODES CHECK"

log "Checking/registering ${#SOURCE_CLUSTER_NODES[@]} cluster node(s) in cluster_nodes table..."

for _idx in "${!SOURCE_CLUSTER_NODES[@]}"; do
    _node_name="${SOURCE_CLUSTER_NODES[$_idx]}"
    _node_ip="${SOURCE_CLUSTER_IPS[$_idx]:-}"

    log "  Checking node: $_node_name (ip=${_node_ip:-NULL})"

    _existing_node_id=$(db_scalar "SELECT node_id FROM cluster_nodes \
        WHERE node_name = '${_node_name//\'/\'\'}' LIMIT 1;" || true)

    if [[ -z "$_existing_node_id" ]]; then
        log "    Not found → inserting"

        # Build server_ip value: SQL NULL if not provided, quoted string otherwise
        _node_ip_sql=$([[ -n "$_node_ip" ]] && echo "'${_node_ip//\'/\'\'}'") || echo "NULL"

        db_query "INSERT INTO cluster_nodes (node_name, server_hostname, server_ip) \
            VALUES ( \
                '${_node_name//\'/\'\'}', \
                '${_node_name//\'/\'\'}', \
                $_node_ip_sql \
            );" \
            || { log "    ERROR: Failed to insert cluster node '$_node_name'";exit 1; }
        _existing_node_id=$(db_scalar "SELECT node_id FROM cluster_nodes \
            WHERE node_name = '${_node_name//\'/\'\'}' LIMIT 1;" || true)
        log "    Inserted → node_id=$_existing_node_id"
    else
        log "    Already exists → node_id=$_existing_node_id (skipping)"
    fi
done

log "Cluster nodes check complete"



############################################
# MIGRATION CONFIG CHECK
############################################
section "MIGRATION CONFIG CHECK"

log "Checking migration_config for: $MIGRATION_NAME"

CONFIG_ID=$(db_scalar "SELECT config_id FROM migration_config \
    WHERE migration_name = '${MIGRATION_NAME//\'/\'\'}' LIMIT 1;" || true)

if [[ -z "$CONFIG_ID" ]]; then
    log "No existing migration config found → inserting new record"

    db_query "INSERT INTO migration_config (
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
        '${MIGRATION_NAME//\'/\'\'}',
        '${SOURCE_INSTANCE_NAME//\'/\'\'}',
        $PORT,
        $SOURCE_VIP_PORT,
        $SOURCE_CLUSTER_ID,
        '${TARGET_INSTANCE_NAME//\'/\'\'}',
        $TARGET_PORT,
        '${TARGET_HOST//\'/\'\'}',
        '${BINLOG_DIR//\'/\'\'}',
        '${TARGET_USER//\'/\'\'}',
        '${TARGET_PASS//\'/\'\'}'
    );" || { log "ERROR: Failed to insert migration_config record"; exit 1; }
    
    CONFIG_ID=$(db_scalar "SELECT config_id FROM migration_config \
        WHERE migration_name = '${MIGRATION_NAME//\'/\'\'}'  LIMIT 1;" || true)

    if [[ -z "$CONFIG_ID" || "$CONFIG_ID" == "0" ]]; then
        log "ERROR: INSERT succeeded but could not retrieve config_id"
        exit 1
    fi

    log "New migration created → config_id=$CONFIG_ID"

    ############################################
    # POPULATE source_cluster_mapping
    ############################################
    log "Populating source_cluster_mapping for ${#SOURCE_CLUSTER_NODES[@]} node(s)..."

    for _idx in "${!SOURCE_CLUSTER_NODES[@]}"; do
        _node_name="${SOURCE_CLUSTER_NODES[$_idx]}"
        _priority=$(( _idx + 1 ))
        _is_primary=$([[ $_idx -eq 0 ]] && echo "TRUE" || echo "FALSE")

        log "  Node $_priority: $_node_name (primary=$_is_primary)"

        # Query the server_id directly from that node — same pattern as DETERMINE SERVER ID
        _node_server_id=$(mysql --host "$_node_name" --user=mysql01 --port="$PORT" \
            --skip-column-names --silent \
            -e "SELECT @@server_id;" 2>/dev/null || true)

        if [[ -z "$_node_server_id" ]]; then
            log "  ERROR: Could not retrieve server_id from node '$_node_name'"
            exit 1
        fi
        log "    server_id=$_node_server_id"

        # Resolve node_id from cluster_nodes
        _node_id=$(db_scalar "SELECT node_id FROM cluster_nodes \
            WHERE node_name = '${_node_name//\'/\'\'}' LIMIT 1;" || true)

        if [[ -z "$_node_id" ]]; then
            log "  ERROR: Node '$_node_name' not found in cluster_nodes — cannot populate source_cluster_mapping"
            exit 1
        fi
        log "    node_id=$_node_id"

        db_query "INSERT INTO source_cluster_mapping \
            (config_id, node_id, server_id, is_primary, priority_order) \
            VALUES ($CONFIG_ID, $_node_id, $_node_server_id, $_is_primary, $_priority) \
            ON DUPLICATE KEY UPDATE \
                is_primary = VALUES(is_primary), \
                priority_order = VALUES(priority_order);" \
            || { log "  ERROR: Failed to insert source_cluster_mapping for node '$_node_name'"; exit 1; }

        log "    Mapping inserted/updated successfully"
    done

    log "source_cluster_mapping populated for config_id=$CONFIG_ID"
else
    log "Existing migration found → config_id=$CONFIG_ID (RESUMING)"
fi

# Resolve the current node's node_id from cluster_nodes
LOCAL_NODE_NAME="${CLUSTER_NODES[$((LOCAL_SERVER_ID - 1))]}"

CURRENT_NODE_ID=$(db_scalar "SELECT node_id FROM cluster_nodes \
    WHERE node_name = '${LOCAL_NODE_NAME//\'/\'\'}' LIMIT 1;" || true)

if [[ -z "$CURRENT_NODE_ID" ]]; then
    log "WARNING: Node '$LOCAL_NODE_NAME' not found in cluster_nodes — DB logging will be degraded"
    CURRENT_NODE_ID=0
fi

log "Using CONFIG_ID=$CONFIG_ID  CURRENT_NODE_ID=$CURRENT_NODE_ID"

############################################
# PAUSE CHECK
############################################
section "PAUSE CHECK"

IS_PAUSED=$(db_scalar "SELECT is_paused FROM migration_config \
    WHERE config_id = $CONFIG_ID LIMIT 1;")

if [[ "$IS_PAUSED" == "1" ]]; then
    log "Migration is paused (is_paused=1 in migration_config) — skipping this iteration"
    db_log_info "Skipping iteration: migration is paused" "" "" ""
    trap - EXIT
    exit 0
fi

log "Migration is not paused — continuing"

############################################
# MAINTENANCE WINDOW CHECK
############################################
section "MAINTENANCE WINDOW CHECK"

# Check if currently in a maintenance window (config-specific or global)
# Supports schedule types: WEEKLY, MONTHLY_DAY, MONTHLY_WEEKDAY, SPECIFIC_DATE
#
# Variables used in SQL:
#   v_dow: day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
#   v_dom: day of month (1-31)
#   v_wom: week of month (1-5, which occurrence of this weekday)
#   v_is_last: 1 if this is the last occurrence of this weekday in the month
#   v_date: current date
#
IN_MAINTENANCE=$(db_scalar "
    SELECT EXISTS (
        SELECT 1 FROM maintenance_windows mw,
        (SELECT 
            DAYOFWEEK(NOW()) - 1 AS v_dow,
            DAY(CURDATE()) AS v_dom,
            CEIL(DAY(CURDATE()) / 7) AS v_wom,
            CASE WHEN DAY(CURDATE()) + 7 > DAY(LAST_DAY(CURDATE())) THEN 1 ELSE 0 END AS v_is_last,
            CURDATE() AS v_date
        ) vars
        WHERE mw.is_active = TRUE
          AND (mw.config_id = $CONFIG_ID OR mw.config_id IS NULL)
          AND CURTIME() BETWEEN mw.start_time AND mw.end_time
          AND (
              -- WEEKLY: matches day of week
              (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = vars.v_dow)
              -- MONTHLY_DAY: matches day of month (1-31)
              OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = vars.v_dom)
              -- MONTHLY_WEEKDAY: matches Nth weekday (1-4) or last weekday (5)
              OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                  AND mw.day_of_week = vars.v_dow
                  AND (mw.week_of_month = vars.v_wom OR (mw.week_of_month = 5 AND vars.v_is_last = 1)))
              -- SPECIFIC_DATE: matches exact date
              OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = vars.v_date)
          )
    ) AS in_window;")

if [[ "$IN_MAINTENANCE" == "1" ]]; then
    # Get window details for logging
    WINDOW_INFO=$(db_scalar "
        SELECT CONCAT(mw.window_name, ' (', mw.schedule_type, ')') 
        FROM maintenance_windows mw,
        (SELECT 
            DAYOFWEEK(NOW()) - 1 AS v_dow,
            DAY(CURDATE()) AS v_dom,
            CEIL(DAY(CURDATE()) / 7) AS v_wom,
            CASE WHEN DAY(CURDATE()) + 7 > DAY(LAST_DAY(CURDATE())) THEN 1 ELSE 0 END AS v_is_last,
            CURDATE() AS v_date
        ) vars
        WHERE mw.is_active = TRUE
          AND (mw.config_id = $CONFIG_ID OR mw.config_id IS NULL)
          AND CURTIME() BETWEEN mw.start_time AND mw.end_time
          AND (
              (mw.schedule_type = 'WEEKLY' AND mw.day_of_week = vars.v_dow)
              OR (mw.schedule_type = 'MONTHLY_DAY' AND mw.day_of_month = vars.v_dom)
              OR (mw.schedule_type = 'MONTHLY_WEEKDAY' 
                  AND mw.day_of_week = vars.v_dow
                  AND (mw.week_of_month = vars.v_wom OR (mw.week_of_month = 5 AND vars.v_is_last = 1)))
              OR (mw.schedule_type = 'SPECIFIC_DATE' AND mw.specific_date = vars.v_date)
          )
        LIMIT 1;")
    
    log "Currently in maintenance window: '$WINDOW_INFO' — skipping this iteration"
    db_log_info "Skipping iteration: in maintenance window '$WINDOW_INFO'" "" "" ""
    trap - EXIT
    exit 0
fi

log "Not in maintenance window — continuing"

############################################
# FAILOVER RESUME DETECTION (NEW PRIMARY)
############################################
section "FAILOVER RESUME CHECK"

FAILOVER_STATUS=$(db_scalar "SELECT processing_status FROM migration_status \
    WHERE config_id = $CONFIG_ID LIMIT 1;")

if [[ "$FAILOVER_STATUS" == "FAILOVER_HANDOFF" ]]; then
    log "Failover handoff detected in migration_status"

    HANDOFF_NODE_ID=$(db_scalar "SELECT current_processing_node_id FROM migration_status \
        WHERE config_id = $CONFIG_ID LIMIT 1;")
    HANDOFF_SERVER_ID=$(db_scalar "SELECT current_processing_server_id FROM migration_status \
        WHERE config_id = $CONFIG_ID LIMIT 1;")

    if [[ -z "$HANDOFF_NODE_ID" || -z "$HANDOFF_SERVER_ID" ]]; then
        log "ERROR: FAILOVER_HANDOFF is set but current processing node/server are missing in migration_status"
        exit 1
    fi

    if [[ "$HANDOFF_NODE_ID" != "$CURRENT_NODE_ID" ]]; then
        log "This node is not the designated new primary (db_node_id=$HANDOFF_NODE_ID, local_node_id=$CURRENT_NODE_ID)"
        log "Failover handoff belongs to another node — exiting"
        trap - EXIT
        exit 0
    fi

    FAILOVER_EVENT_ID=$(db_scalar "SELECT failover_id FROM failover_events \
        WHERE config_id = $CONFIG_ID \
          AND new_primary_node_id = $CURRENT_NODE_ID \
          AND new_primary_server_id = $HANDOFF_SERVER_ID \
        ORDER BY failover_id DESC LIMIT 1;")
    FAILOVER_GTID=$(db_scalar "SELECT resume_from_gtid FROM failover_events \
        WHERE config_id = $CONFIG_ID \
          AND new_primary_node_id = $CURRENT_NODE_ID \
          AND new_primary_server_id = $HANDOFF_SERVER_ID \
        ORDER BY failover_id DESC LIMIT 1;")

    if [[ -z "$FAILOVER_EVENT_ID" || -z "$FAILOVER_GTID" ]]; then
        log "ERROR: Could not resolve failover resume GTID from failover_events for this new primary"
        exit 1
    fi

    log "Failover event resolved: failover_id=$FAILOVER_EVENT_ID resume_gtid=$FAILOVER_GTID"
    log "Searching local binlogs for GTID..."

    FOUND_LINE=""

    for f in "$BINLOG_DIR"/binlogs*[0-9]; do
        RESULT=$(mariadb-binlog "$f" \
            | grep -n "GTID $FAILOVER_GTID" \
            | sed "s|^|$f:|" || true)

        if [[ -n "$RESULT" ]]; then
            FOUND_LINE="$RESULT"
            break
        fi
    done

    if [[ -z "$FOUND_LINE" ]]; then
        log "ERROR: GTID $FAILOVER_GTID not found in local binlogs"
        exit 1
    fi

    log "GTID located:"
    log "$FOUND_LINE"

    # File is before first colon
    CURRENT_BINLOG=$(echo "$FOUND_LINE" | cut -d':' -f1 | xargs basename)

    # Extract end_log_pos value
    CURRENT_POS=$(echo "$FOUND_LINE" \
        | grep -oE 'end_log_pos [0-9]+' \
        | awk '{print $2}')

    if [[ -z "$CURRENT_BINLOG" || -z "$CURRENT_POS" ]]; then
        log "ERROR: Failed to extract resume binlog name or position from local binlogs"
        exit 1
    fi

    log "Resume binlog determined from control DB handoff:"
    log "  Binlog   : $CURRENT_BINLOG"
    log "  Position : $CURRENT_POS"

    db_query "UPDATE migration_status SET \
        current_binlog_file = '${CURRENT_BINLOG//\'/\'\'}', \
        current_binlog_position = $CURRENT_POS, \
        current_gtid_position = '${FAILOVER_GTID//\'/\'\'}', \
        last_processed_timestamp = NOW() \
        WHERE config_id = $CONFIG_ID;" \
        || { log "ERROR: Failed to persist failover resume location to migration_status"; exit 1; }

    db_log_info \
        "New primary resumed from failover handoff: failover_id=$FAILOVER_EVENT_ID gtid=$FAILOVER_GTID resolved to $CURRENT_BINLOG:$CURRENT_POS" \
        "$CURRENT_BINLOG" "$CURRENT_POS" "$FAILOVER_GTID"
fi



############################################
# INITIAL ARGUMENT / STATE HANDLING
############################################
section "STATE INITIALIZATION"

# Check migration_status for existing state
_status_binlog=$(db_scalar "SELECT current_binlog_file FROM migration_status \
    WHERE config_id = $CONFIG_ID LIMIT 1;")

if [[ -z "$_status_binlog" ]]; then
    log "No migration status found in DB → FIRST RUN"

    CURRENT_BINLOG="$INITIAL_BINLOG"
    CURRENT_POS="$INITIAL_POS"

    # Extract server name from binlog filename: binlogs_<servername>.<sequence>
    _binlog_server=$(echo "$CURRENT_BINLOG" | sed -n 's/^[^_]*_\([^.]*\)\..*/\1/p')

    if [[ -z "$_binlog_server" ]]; then
        log "ERROR: Could not extract server name from binlog '$CURRENT_BINLOG'"
        log "Expected format: binlogs_<servername>.<sequence>  (e.g. binlogs_d1fm1mar043.000001)"
        exit 1
    fi

    log "Server name extracted from initial binlog: $_binlog_server"

    CURRENT_PROCESSING_NODE_ID=$(db_scalar "SELECT node_id FROM cluster_nodes \
        WHERE LOWER(node_name) = LOWER('${_binlog_server//\'/\'\'}') LIMIT 1;")

    if [[ -z "$CURRENT_PROCESSING_NODE_ID" ]]; then
        log "ERROR: No cluster_nodes entry found for server name '$_binlog_server'"
        exit 1
    fi

    log "Config ID: $CONFIG_ID"
    log "Current Processing Node ID: $CURRENT_PROCESSING_NODE_ID"

    _processing_server_id=$(db_scalar "SELECT server_id FROM source_cluster_mapping \
        WHERE config_id = $CONFIG_ID AND node_id = $CURRENT_PROCESSING_NODE_ID LIMIT 1;")
    CURRENT_PROCESSING_SERVER_ID="${_processing_server_id:-0}"

    log "Initial binlog            : $CURRENT_BINLOG"
    log "Initial position          : $CURRENT_POS"
    log "Processing node_id        : $CURRENT_PROCESSING_NODE_ID"
    log "Processing server_id      : ${CURRENT_PROCESSING_SERVER_ID:-unknown}"

    db_query "INSERT INTO migration_status \
        (config_id, current_processing_node_id, current_processing_server_id, \
         current_binlog_file, current_binlog_position, processing_status, \
         process_pid, process_hostname, process_start_time) \
        VALUES ($CONFIG_ID, $CURRENT_PROCESSING_NODE_ID, \
            ${CURRENT_PROCESSING_SERVER_ID:-0}, \
            '${CURRENT_BINLOG//\'/\'\'}', $CURRENT_POS, \
            'RUNNING', $$, '$(hostname -s)', NOW());" \
        || { log "ERROR: Failed to create migration_status record"; exit 1; }

    log "Migration status record created in DB"
else
    log "Migration status found in DB → RESUMING"

    CURRENT_BINLOG="$_status_binlog"
    CURRENT_POS=$(db_scalar "SELECT current_binlog_position FROM migration_status \
        WHERE config_id = $CONFIG_ID LIMIT 1;")
    CURRENT_PROCESSING_NODE_ID=$(db_scalar "SELECT current_processing_node_id FROM migration_status \
        WHERE config_id = $CONFIG_ID LIMIT 1;")
    CURRENT_PROCESSING_SERVER_ID=$(db_scalar "SELECT current_processing_server_id FROM migration_status \
        WHERE config_id = $CONFIG_ID LIMIT 1;")

    log "Resuming from binlog      : $CURRENT_BINLOG"
    log "Resuming from position    : $CURRENT_POS"
    log "Processing node_id        : $CURRENT_PROCESSING_NODE_ID"
    log "Processing server_id      : ${CURRENT_PROCESSING_SERVER_ID:-unknown}"

    db_query "UPDATE migration_status SET \
        processing_status = 'RUNNING', \
        process_pid = $$, \
        process_hostname = '$(hostname -s)', \
        process_start_time = NOW() \
        WHERE config_id = $CONFIG_ID;" \
        || log "WARNING: Failed to update migration_status to RUNNING"
fi

if [[ -z "${CURRENT_PROCESSING_SERVER_ID:-}" ]]; then
    log "ERROR: current_processing_server_id is not set in migration_status"
    exit 1
fi

############################################
# CHECK FOR BLOCKING CONDITIONS
############################################
# Before processing, check if there are unresolved errors from a previous run
# that require manual intervention (ERROR logs or incomplete checkpoints)
# This check runs on ALL nodes (primary and secondary) so that alerts
# are visible regardless of which node the service runs on
############################################

if ! check_blocking_conditions; then
    log "Exiting due to blocking conditions - manual intervention required"
    trap - EXIT
    exit 0
fi

############################################
# NON-PROCESSING SERVER CHECK
############################################
# After confirming no blocking errors, check if this node should process
############################################

if [[ "$LOCAL_SERVER_ID" != "$CURRENT_PROCESSING_SERVER_ID" ]]; then
    log "This node is not the current processing server (local_server_id=$LOCAL_SERVER_ID, current_processing_server_id=$CURRENT_PROCESSING_SERVER_ID)"
    db_log_info "Skipping iteration on non-processing server: local_server_id=$LOCAL_SERVER_ID current_processing_server_id=$CURRENT_PROCESSING_SERVER_ID" "$CURRENT_BINLOG" "$CURRENT_POS" ""
    trap - EXIT
    exit 0
fi

############################################
# BINLOG PROCESSING LOOP
############################################
# Process all available binlogs until a stop condition:
#   - Binlog file doesn't exist (caught up)
#   - Active binlog reached (still being written)
#   - Failover detected (handoff to other node)
#   - Error during apply
############################################

TOTAL_BINLOGS_PROCESSED=0
LOOP_START_TIME=$(date +%s)

while true; do
    # Update paths for current binlog
    BINLOG_PATH="$BINLOG_DIR/$CURRENT_BINLOG"
    SQL_FILE="$WORKDIR/$CURRENT_BINLOG.sql"
    ERR_FILE="$WORKDIR/$CURRENT_BINLOG.err"

    section "PROCESSING BINLOG: $CURRENT_BINLOG (iteration $((TOTAL_BINLOGS_PROCESSED + 1)))"
    log "BINLOG_PATH=$BINLOG_PATH"
    log "SQL_FILE=$SQL_FILE"
    log "ERR_FILE=$ERR_FILE"

    ############################################
    # BINLOG EXISTENCE CHECK
    ############################################
    
    if [[ ! -f "$BINLOG_PATH" ]]; then
        log "Binlog does not exist → caught up with available binlogs"
        if [[ $TOTAL_BINLOGS_PROCESSED -gt 0 ]]; then
            db_log_info "Binlog processing complete: $TOTAL_BINLOGS_PROCESSED binlog(s) processed, caught up at $CURRENT_BINLOG" "$CURRENT_BINLOG" "$CURRENT_POS" ""
        else
            db_log_info "No binlogs to process: $CURRENT_BINLOG not found" "$CURRENT_BINLOG" "$CURRENT_POS" ""
        fi
        break  # Exit loop - caught up
    fi

############################################
# BINLOG EXTRACTION
############################################
section "BINLOG EXTRACTION"

BINLOG_CMD=(
    "$MYSQL_BINLOG"
    "--start-position=$CURRENT_POS"
    "$BINLOG_PATH"
)

log "Executing command:"
log "${BINLOG_CMD[*]} > $SQL_FILE 2> $ERR_FILE"
db_log_info "Extracting binlog contents: $CURRENT_BINLOG (start_pos=$CURRENT_POS)" "$CURRENT_BINLOG" "$CURRENT_POS" ""

"${BINLOG_CMD[@]}" > "$SQL_FILE" 2> "$ERR_FILE"

############################################
# BINLOG-IN-USE DETECTION
############################################
section "BINLOG IN-USE CHECK"

BINLOG_IN_USE=0
if grep -q "not closed properly" "$SQL_FILE"; then
    BINLOG_IN_USE=1
    log "WARNING detected in binlog extraction:"
    log "[Not closed properly] line found in $SQL_FILE"
    log "Binlog still in use — running failover detection only; apply/state advance will be skipped unless failover handoff is triggered"
    db_log_warn "Binlog still in use (not closed properly): running failover detection only for $CURRENT_BINLOG" "$CURRENT_BINLOG" "$CURRENT_POS" ""
fi

############################################
# ACTIVE BINLOG STATUS SYNC
############################################
section "ACTIVE BINLOG STATUS SYNC"

if [[ "$BINLOG_IN_USE" -eq 1 ]]; then
    db_query "UPDATE migration_status SET \
        processing_status = 'ACTIVE_BINLOG_REACHED', \
        last_processed_timestamp = NOW() \
        WHERE config_id = $CONFIG_ID;" \
        || log "WARNING: Failed to update migration_status to ACTIVE_BINLOG_REACHED"
    log "Migration status set to ACTIVE_BINLOG_REACHED (binlog still in use)"
else
    db_query "UPDATE migration_status SET \
        processing_status = 'RUNNING', \
        last_processed_timestamp = NOW() \
        WHERE config_id = $CONFIG_ID \
          AND processing_status = 'ACTIVE_BINLOG_REACHED';" \
        || log "WARNING: Failed to update migration_status back to RUNNING"
fi

############################################
# FAILOVER DETECTION
############################################
section "FAILOVER DETECTION"

# Initialize variables that may be referenced later (avoid unbound variable errors with set -u)
MAINTENANCE_GTIDS_FILE="$WORKDIR/$CURRENT_BINLOG.maintenance_gtids.txt"
MAINTENANCE_COUNT=0
TRANSACTION_COUNT=0
FAILOVER_DETECTED=0

# Save the extraction start position before failover detection
# The loop below may overwrite CURRENT_POS - we restore it afterward
BINLOG_START_POS="$CURRENT_POS"

# Find foreign GTID header lines only (one line per foreign transaction start)
ALL_FOREIGN_LINES=$(grep -n -E "server id $OTHER_SERVER_ID.*GTID [0-9]-[0-9]-[0-9]" "$SQL_FILE" || true)

if [[ -n "$ALL_FOREIGN_LINES" ]]; then
    log "Found $(echo "$ALL_FOREIGN_LINES" | wc -l | tr -d ' ') foreign GTID transaction(s) to analyze"

    : > "$MAINTENANCE_GTIDS_FILE"

    # Process each foreign server_id transaction — stop at the first real failover
    while IFS= read -r FOREIGN_LINE_WITH_NUM; do

        if [[ -z "$FOREIGN_LINE_WITH_NUM" ]]; then
            continue
        fi

        TRANSACTION_COUNT=$((TRANSACTION_COUNT + 1))
        FOREIGN_LINE_NUM=$(echo "$FOREIGN_LINE_WITH_NUM" | cut -d: -f1)
        FOREIGN_LINE=$(echo "$FOREIGN_LINE_WITH_NUM" | cut -d: -f2-)

        # Extract the ACTUAL numeric server_id from this specific line
        # (OTHER_SERVER_ID may be a regex alternation like (101|102); we need the real number)
        ACTUAL_FOREIGN_SERVER_ID=$(echo "$FOREIGN_LINE" | sed -n 's/.*server id \([0-9]\+\).*/\1/p')

        # Extract the binlog position at the end of this GTID header event
        CURRENT_POS=$(echo "$FOREIGN_LINE" | grep -oE 'end_log_pos [0-9]+' | awk '{print $2}')

        log "Analyzing transaction #$TRANSACTION_COUNT at line $FOREIGN_LINE_NUM (server_id=$ACTUAL_FOREIGN_SERVER_ID, end_log_pos=$CURRENT_POS)..."

        # Find the next GTID line to bound this transaction block.
        # Use awk so "no next GTID found" is an empty result, not a shell-fatal pipeline status.
        NEXT_GTID_LINE_NUM=$(awk -v start_line="$((FOREIGN_LINE_NUM + 1))" '
            NR < start_line { next }
            /GTID [0-9]-[0-9]-[0-9]/ {
                print NR - start_line + 1
                exit
            }
        ' "$SQL_FILE")

        if [[ -n "$NEXT_GTID_LINE_NUM" ]]; then
            TXN_END_LINE_NUM=$(( FOREIGN_LINE_NUM + NEXT_GTID_LINE_NUM - 1 ))
            log "  Transaction ends at line $TXN_END_LINE_NUM (next GTID found)"
        else
            TXN_END_LINE_NUM=$(wc -l < "$SQL_FILE")
            log "  Transaction ends at EOF (line $TXN_END_LINE_NUM)"
        fi

        # Look for the first meaningful SQL statement in the block.
        # Read directly from the file slice to avoid large shell variables and pipeline exit-code issues.
        SQL_STATEMENT=$(awk -v start_line="$FOREIGN_LINE_NUM" -v end_line="$TXN_END_LINE_NUM" '
            NR < start_line || NR > end_line { next }
            /^(SET @@session\.|\/\*!|#|$)/ { next }
            /^[[:space:]]*(truncate|insert|update|delete|create|drop|alter|replace|flush|optimize|analyze|repair|show|start|stop|reset|change)/ {
                line = $0
                sub(/^[[:space:]]+/, "", line)
                sub(/[[:space:]]+$/, "", line)
                sub(/\/\*!\*\/;*$/, "", line)
                print line
                exit
            }
        ' "$SQL_FILE")

        # Patterns that are routine cluster maintenance — safe to ignore regardless of which node emits them
        MAINTENANCE_PATTERNS="(truncate[[:space:]]+mysql\.(slow_log|general_log|error_log))|(flush[[:space:]]+(logs|binary[[:space:]]+logs))|(optimize[[:space:]]+table)|(analyze[[:space:]]+table)|(repair[[:space:]]+table)|(show[[:space:]]+(slave|master)[[:space:]]+status)|(start[[:space:]]+slave)|(stop[[:space:]]+slave)|(reset[[:space:]]+slave)"

        if [[ -n "$SQL_STATEMENT" ]]; then
            log "  SQL found: $SQL_STATEMENT"

            if echo "$SQL_STATEMENT" | grep -qE "$MAINTENANCE_PATTERNS"; then
                MAINTENANCE_COUNT=$((MAINTENANCE_COUNT + 1))
                MAINTENANCE_GTID=$(echo "$FOREIGN_LINE" | sed -n 's/.*GTID \([0-9-]\+\).*/\1/p')
                log "  -> MAINTENANCE #$MAINTENANCE_COUNT (server_id=$ACTUAL_FOREIGN_SERVER_ID): $SQL_STATEMENT (IGNORED)"
                if [[ -n "$MAINTENANCE_GTID" ]]; then
                    echo "$MAINTENANCE_GTID" >> "$MAINTENANCE_GTIDS_FILE"
                fi
            else
                log "  -> REAL FAILOVER DETECTED (server_id=$ACTUAL_FOREIGN_SERVER_ID): $SQL_STATEMENT"
                FAILOVER_DETECTED=1
                FAILOVER_POS="$CURRENT_POS"
                FAILOVER_GTID=$(echo "$FOREIGN_LINE" | sed -n 's/.*GTID \([0-9-]\+\).*/\1/p')
                FAILOVER_SERVER_ID="$ACTUAL_FOREIGN_SERVER_ID"
                FAILOVER_TIME=$(date '+%Y-%m-%d %H:%M:%S')
                FAILOVER_SQL="$SQL_STATEMENT"
                FAILOVER_LINE="$FOREIGN_LINE"
                break
            fi
        else
            log "  WARNING: No SQL found in transaction block (server_id=$ACTUAL_FOREIGN_SERVER_ID) — treating as potential failover for safety"
            FAILOVER_DETECTED=1
            FAILOVER_POS="$CURRENT_POS"
            FAILOVER_GTID=$(echo "$FOREIGN_LINE" | sed -n 's/.*GTID \([0-9-]\+\).*/\1/p')
            FAILOVER_SERVER_ID="$ACTUAL_FOREIGN_SERVER_ID"
            FAILOVER_TIME=$(date '+%Y-%m-%d %H:%M:%S')
            FAILOVER_SQL="UNKNOWN_TRANSACTION"
            FAILOVER_LINE="$FOREIGN_LINE"
            break
        fi

    done <<< "$ALL_FOREIGN_LINES"

    if [[ "$FAILOVER_DETECTED" -eq 1 ]]; then

        ############################################
        # RESOLVE NEW PRIMARY VIA DB LOOKUP
        ############################################
        log "Resolving new primary for server_id=$FAILOVER_SERVER_ID via source_cluster_mapping..."

        NEW_PRIMARY_NODE_ID=$(db_scalar "SELECT scm.node_id \
            FROM source_cluster_mapping scm \
            WHERE scm.config_id = $CONFIG_ID AND scm.server_id = $FAILOVER_SERVER_ID \
            LIMIT 1;")
        NEW_PRIMARY_HOST=$(db_scalar "SELECT cn.node_name \
            FROM source_cluster_mapping scm \
            JOIN cluster_nodes cn ON scm.node_id = cn.node_id \
            WHERE scm.config_id = $CONFIG_ID AND scm.server_id = $FAILOVER_SERVER_ID \
            LIMIT 1;")

        if [[ -z "$NEW_PRIMARY_NODE_ID" || -z "$NEW_PRIMARY_HOST" ]]; then
            log "ERROR: Could not resolve new primary for server_id=$FAILOVER_SERVER_ID — check source_cluster_mapping"
            NEW_PRIMARY_NODE_ID=0
            NEW_PRIMARY_HOST=""
        else
            log "New primary resolved: node_id=$NEW_PRIMARY_NODE_ID host=$NEW_PRIMARY_HOST"
        fi

        ############################################
        # APPLY BINLOG UP TO FAILOVER POSITION
        # Apply all local transactions that came before the foreign takeover
        ############################################
        log "Applying binlog transactions up to failover position $FAILOVER_POS..."

        PRE_FAILOVER_SQL="$WORKDIR/$CURRENT_BINLOG.prefailover.sql"
        "$MYSQL_BINLOG" \
            "--start-position=$BINLOG_START_POS" \
            "--stop-position=$FAILOVER_POS" \
            "$BINLOG_PATH" > "$PRE_FAILOVER_SQL" 2>/dev/null || true

        # NOTE: We do NOT filter maintenance transactions from pre-failover SQL.
        # Text-based GTID block filtering can accidentally capture unrelated events.
        # Maintenance ops (truncate slow_log, etc.) are harmless to apply to target.
        if [[ -s "$MAINTENANCE_GTIDS_FILE" ]]; then
            log "DEBUG: Pre-failover range includes $MAINTENANCE_COUNT maintenance transaction(s); will apply (harmless)"
        fi

        if [[ -s "$PRE_FAILOVER_SQL" ]]; then
            if "$MYSQL" "--host=$TARGET_HOST" "--user=$TARGET_USER" \
                    "--password=$TARGET_PASS" "--port=$TARGET_PORT" "--ssl=true" \
                    < "$PRE_FAILOVER_SQL"; then
                log "Pre-failover transactions applied to target (up to position $FAILOVER_POS)"
            else
                log "WARNING: Failed to apply pre-failover SQL — target may be incomplete"
            fi
        else
            log "No pre-failover transactions to apply (empty SQL output for this range)"
        fi
        rm -f "$PRE_FAILOVER_SQL"

        ############################################
        # RECORD FAILOVER IN DATABASE
        ############################################
        log "Recording failover event in database..."

        _escaped_sql="${FAILOVER_SQL//\'/\'\'}"
        _escaped_gtid="${FAILOVER_GTID//\'/\'\'}"

        db_query "INSERT INTO failover_events (
            config_id,
            detected_by_node_id,
            detected_by_server_id,
            detected_in_binlog,
            detected_at_position,
            detected_gtid,
            foreign_server_id,
            foreign_sql_statement,
            maintenance_operations_count,
            new_primary_node_id,
            new_primary_server_id,
            handoff_status,
            resume_from_binlog,
            resume_from_position,
            resume_from_gtid
        ) VALUES (
            $CONFIG_ID,
            $CURRENT_NODE_ID,
            $LOCAL_SERVER_ID,
            '${CURRENT_BINLOG//\'/\'\'}',
            $FAILOVER_POS,
            '$_escaped_gtid',
            $FAILOVER_SERVER_ID,
            '$_escaped_sql',
            $MAINTENANCE_COUNT,
            ${NEW_PRIMARY_NODE_ID:-0},
            $FAILOVER_SERVER_ID,
            'PENDING',
            '${CURRENT_BINLOG//\'/\'\'}',
            $FAILOVER_POS,
            '$_escaped_gtid'
        );" || log "WARNING: Failed to insert failover_events record"

        # Mark this node as no longer the active processor
        db_query "UPDATE migration_status SET
            processing_status = 'FAILOVER_DETECTED',
            current_binlog_position = $FAILOVER_POS,
            last_processed_timestamp = NOW()
            WHERE config_id = $CONFIG_ID;" \
            || log "WARNING: Failed to update migration_status to FAILOVER_DETECTED"

        db_log_critical \
            "FAILOVER DETECTED: server_id=$FAILOVER_SERVER_ID took over at $CURRENT_BINLOG:$FAILOVER_POS. New primary: $NEW_PRIMARY_HOST (node_id=${NEW_PRIMARY_NODE_ID:-unknown}). Maintenance ops ignored: $MAINTENANCE_COUNT" \
            "$CURRENT_BINLOG" "$FAILOVER_POS" "$FAILOVER_GTID"

        ############################################
        # LOG FAILOVER SUMMARY
        ############################################
        
        log "FAILOVER SUMMARY - THIS NODE WON'T BE APPLYING TRANSACTIONS TO TARGET FOR NOW:"
        log "  Foreign server_id : $FAILOVER_SERVER_ID"
        log "  Failover SQL      : $FAILOVER_SQL"
        log "  Binlog            : $CURRENT_BINLOG"
        log "  Stop position     : $FAILOVER_POS"
        log "  GTID              : $FAILOVER_GTID"
        log "  New primary host  : ${NEW_PRIMARY_HOST:-UNKNOWN}"
        log "  Maintenance ops   : $MAINTENANCE_COUNT (ignored)"
        log "  Transactions seen : $TRANSACTION_COUNT"

        ############################################
        # UPDATE DB FOR NEW PRIMARY TAKEOVER
        ############################################
        log "Updating database to mark new primary as active processor..."

        db_query "UPDATE migration_status SET
            current_processing_node_id = ${NEW_PRIMARY_NODE_ID:-0},
            current_processing_server_id = $FAILOVER_SERVER_ID,
            processing_status = 'FAILOVER_HANDOFF',
            last_processed_timestamp = NOW()
            WHERE config_id = $CONFIG_ID;" \
            || log "WARNING: Failed to update migration_status with new primary"

        db_query "UPDATE failover_events SET
            handoff_status = 'COMPLETED',
            handoff_attempted_at = NOW(),
            handoff_completed_at = NOW()
            WHERE config_id = $CONFIG_ID
            ORDER BY failover_id DESC LIMIT 1;" \
            || log "WARNING: Failed to update failover_events handoff_status to COMPLETED"

        db_log_info \
            "Failover handoff complete: New primary node_id=${NEW_PRIMARY_NODE_ID:-unknown} (server_id=$FAILOVER_SERVER_ID) is now active processor" \
            "$CURRENT_BINLOG" "$FAILOVER_POS" "$FAILOVER_GTID"

        log "Database updated — new primary will resume processing on next iteration"
        rm -f "$MAINTENANCE_GTIDS_FILE"

        # This node is no longer the active processor — exit immediately
        trap - EXIT
        exit 10

    else
        # SAFETY: Do NOT filter out maintenance transactions from the SQL file.
        # Text-based GTID block filtering can accidentally capture unrelated events
        # that appear between GTID headers (e.g., COMMIT events from other transactions).
        #
        # Instead, we just log that maintenance was detected and let the transactions
        # apply to the target. Operations like "truncate mysql.slow_log" are harmless.
        
        if [[ -s "$MAINTENANCE_GTIDS_FILE" ]]; then
            log "DEBUG: Foreign maintenance GTIDs detected: $(cat "$MAINTENANCE_GTIDS_FILE" | tr '\n' ' ')"
            log "NOTE: Maintenance transactions will be applied to target (safe/idempotent operations)"
        fi

        rm -f "$MAINTENANCE_GTIDS_FILE"

        log "ANALYSIS COMPLETE - ALL $TRANSACTION_COUNT FOREIGN TRANSACTION(S) WERE MAINTENANCE:"
        log "  Maintenance operations : $MAINTENANCE_COUNT"
        log "  Total transactions     : $TRANSACTION_COUNT"
        log "  Result: No failover detected, maintenance ops will apply (harmless), continuing..."
    fi

else
    log "No foreign server_id transactions found in this binlog — no failover detection needed"
fi

# IMPORTANT: Restore CURRENT_POS after failover detection
# The failover detection loop overwrites CURRENT_POS while analyzing foreign transactions.
# We must restore it to the original extraction position for the apply step.
if [[ -n "${BINLOG_START_POS:-}" ]]; then
    CURRENT_POS="$BINLOG_START_POS"
    log "Restored CURRENT_POS to extraction start position: $CURRENT_POS"
fi

############################################
# PRE-APPLY SQL VALIDATION
############################################
section "PRE-APPLY SQL VALIDATION"

if [[ -s "$SQL_FILE" ]]; then
    BINLOG_OPEN_COUNT=$(grep -c "^BINLOG '" "$SQL_FILE" || true)
    BINLOG_CLOSE_COUNT=$(grep -c "^'/\\*!\\*/;$" "$SQL_FILE" || true)

    if [[ "$BINLOG_OPEN_COUNT" != "$BINLOG_CLOSE_COUNT" ]]; then
        log "ERROR: SQL file appears malformed: BINLOG open/close count mismatch (open=$BINLOG_OPEN_COUNT close=$BINLOG_CLOSE_COUNT)"
        db_log_error "Malformed SQL detected before apply: BINLOG open/close mismatch for $CURRENT_BINLOG (open=$BINLOG_OPEN_COUNT close=$BINLOG_CLOSE_COUNT)" "$CURRENT_BINLOG" "$CURRENT_POS" ""
        log "Keeping generated SQL artifacts for inspection: $SQL_FILE"
        trap - EXIT
        exit 1
    fi
fi



############################################
# APPLY BINLOG TO TARGET (GTID-TRACKED)
############################################
section "APPLY BINLOG TO TARGET"

# Update processing state for crash tracking
PROCESSING_STATE="APPLYING"
PROCESSING_BINLOG="$CURRENT_BINLOG"
PROCESSING_POSITION="$CURRENT_POS"

if [[ "$BINLOG_IN_USE" -eq 1 ]]; then
    log "Binlog is still in use and no failover handoff was triggered — skipping apply and state advancement"
    db_log_info "Reached active binlog: $CURRENT_BINLOG (processed $TOTAL_BINLOGS_PROCESSED binlog(s) this run)" "$CURRENT_BINLOG" "$CURRENT_POS" ""
    rm -f "$SQL_FILE" "$ERR_FILE"
    PROCESSING_STATE="COMPLETED_BINLOG_IN_USE"
    break  # Exit loop - reached active binlog
fi

if [[ -s "$SQL_FILE" ]]; then
    log "Applying SQL file to target..."
    
    if apply_binlog_to_target "$SQL_FILE" "$CURRENT_BINLOG" "$CURRENT_POS"; then
        log "Binlog apply completed successfully"
        PROCESSING_STATE="APPLY_COMPLETE"
    else
        log "ERROR: Binlog apply failed for $CURRENT_BINLOG"
        log "ERROR: Check processing_log table for details. Manual intervention required."
        log "ERROR: Consider restoring from daily backup if data corruption occurred."
        PROCESSING_STATE="APPLY_FAILED"
        
        # Keep SQL file for debugging
        log "Keeping SQL file for debugging: $SQL_FILE"
        rm -f "$ERR_FILE"
        
        # Exit with error - do not advance state
        trap - EXIT
        exit 1
    fi
else
    log "SQL file is empty → no transactions to apply"
    PROCESSING_STATE="APPLY_COMPLETE_EMPTY"
fi

############################################
# ADVANCE STATE
############################################
section "STATE ADVANCEMENT"

NEXT_BINLOG=$(printf "%s.%06d" \
    "${CURRENT_BINLOG%.*}" \
    "$((10#${CURRENT_BINLOG##*.} + 1))")
NEXT_POS=4

log "Next binlog inferred: $NEXT_BINLOG"
log "Next position inferred: $NEXT_POS"

db_query "UPDATE migration_status SET \
    current_binlog_file = '${NEXT_BINLOG//\'/\'\'}', \
    current_binlog_position = $NEXT_POS, \
    last_processed_timestamp = NOW() \
    WHERE config_id = $CONFIG_ID;"

if [[ $? -ne 0 ]]; then
    log "ERROR: Failed to update migration_status with next binlog state"
    trap - EXIT
    exit 1
fi

db_log_info "Advanced processing state to next binlog: $NEXT_BINLOG:$NEXT_POS" "$NEXT_BINLOG" "$NEXT_POS" ""
db_log_info "Binlog iteration completed successfully: $CURRENT_BINLOG processed; continuing with next binlog $NEXT_BINLOG at position $NEXT_POS" "$CURRENT_BINLOG" "$NEXT_POS" ""
log "Control database updated with next processing state"

############################################
# CLEANUP FOR THIS BINLOG
############################################

log "Removing temporary files for $CURRENT_BINLOG"
rm -f "$SQL_FILE" "$ERR_FILE"

# Increment counter
TOTAL_BINLOGS_PROCESSED=$((TOTAL_BINLOGS_PROCESSED + 1))

# Update CURRENT_BINLOG and CURRENT_POS for next iteration
CURRENT_BINLOG="$NEXT_BINLOG"
CURRENT_POS="$NEXT_POS"

log "Continuing to next binlog: $CURRENT_BINLOG"
log ""

done  # End of binlog processing loop

############################################
# SCRIPT END - SUMMARY
############################################
section "SCRIPT END"

LOOP_END_TIME=$(date +%s)
LOOP_DURATION=$((LOOP_END_TIME - LOOP_START_TIME))

PROCESSING_STATE="COMPLETED"
log "Execution completed successfully"
log "Summary:"
log "  Total binlogs processed: $TOTAL_BINLOGS_PROCESSED"
log "  Total duration: ${LOOP_DURATION} seconds"
log "  Next binlog to process: $CURRENT_BINLOG"

if [[ $TOTAL_BINLOGS_PROCESSED -gt 0 ]]; then
    db_log_info "Binlog processing run complete: processed $TOTAL_BINLOGS_PROCESSED binlog(s) in ${LOOP_DURATION}s, next=$CURRENT_BINLOG" "$CURRENT_BINLOG" "$CURRENT_POS" ""
fi

# Disable trap before clean exit to avoid false positive crash logging
trap - EXIT

exit 0
