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
for _var in PORT BINLOG_DIR WORKDIR STATE_FILE LOG_FILE FAILOVER_FILE \
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
CLUSTER_NODE_1="${CLUSTER_NODES[0]}"
CLUSTER_NODE_2="${CLUSTER_NODES[1]}"



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
# LOG CLUSTER CONFIG
############################################

section "CLUSTER CONFIGURATION"

log "Cluster nodes defined:"
log "  Node 1: ${CLUSTER_NODES[0]}"
log "  Node 2: ${CLUSTER_NODES[1]}"
log "Local host: $(hostname)"

############################################
# SCRIPT START
############################################
section "SCRIPT START"
log "PID=$$"
log "WORKDIR=$WORKDIR"
log "STATE_FILE=$STATE_FILE"
log "LOG_FILE=$LOG_FILE"

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
# FAILOVER RESUME DETECTION (NEW PRIMARY)
############################################
section "FAILOVER RESUME CHECK"

if [[ -f "$FAILOVER_FILE" ]]; then
    log "Failover file detected: $FAILOVER_FILE"
    log "Attempting GTID-based resume"

    FAILOVER_GTID=$(grep -oE 'FAILOVER_GTID[:=][[:space:]]*[0-9-]+' "$FAILOVER_FILE" \
        | sed -E 's/.*[:=][[:space:]]*//')

    if [[ -z "$FAILOVER_GTID" ]]; then
        log "ERROR: Could not extract FAILOVER_GTID from file"
        exit 1
    fi

    log "Failover GTID found: $FAILOVER_GTID"
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

    ############################################
    # Extract binlog file and end_log_pos
    ############################################

    # File is before first colon
    CURRENT_BINLOG=$(echo "$FOUND_LINE" | cut -d':' -f1 | xargs basename)

    # Extract end_log_pos value
    CURRENT_POS=$(echo "$FOUND_LINE" \
        | grep -oE 'end_log_pos [0-9]+' \
        | awk '{print $2}')

    if [[ -z "$CURRENT_BINLOG" || -z "$CURRENT_POS" ]]; then
        log "ERROR: Failed to extract binlog name or position"
        exit 1
    fi

    log "Resume binlog determined:"
    log "  Binlog   : $CURRENT_BINLOG"
    log "  Position : $CURRENT_POS"

    ############################################
    # Persist new state
    ############################################
    cat > "$STATE_FILE" <<EOF
LAST_BINLOG=$CURRENT_BINLOG
LAST_POS=$CURRENT_POS
EOF

    log "State updated based on failover:"
    log "$(cat "$STATE_FILE")"

    ############################################
    # Remove failover file (handoff complete)
    ############################################
    rm -f "$FAILOVER_FILE"
    log "Failover file consumed and removed"
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

    log "Initial binlog            : $CURRENT_BINLOG"
    log "Initial position          : $CURRENT_POS"
    log "Processing node_id        : $CURRENT_PROCESSING_NODE_ID"
    log "Processing server_id      : ${_processing_server_id:-unknown}"

    db_query "INSERT INTO migration_status \
        (config_id, current_processing_node_id, current_processing_server_id, \
         current_binlog_file, current_binlog_position, processing_status, \
         process_pid, process_hostname, process_start_time) \
        VALUES ($CONFIG_ID, $CURRENT_PROCESSING_NODE_ID, \
            ${_processing_server_id:-0}, \
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

    log "Resuming from binlog      : $CURRENT_BINLOG"
    log "Resuming from position    : $CURRENT_POS"
    log "Processing node_id        : $CURRENT_PROCESSING_NODE_ID"

    db_query "UPDATE migration_status SET \
        processing_status = 'RUNNING', \
        process_pid = $$, \
        process_hostname = '$(hostname -s)', \
        process_start_time = NOW() \
        WHERE config_id = $CONFIG_ID;" \
        || log "WARNING: Failed to update migration_status to RUNNING"
fi

BINLOG_PATH="$BINLOG_DIR/$CURRENT_BINLOG"
SQL_FILE="$WORKDIR/$CURRENT_BINLOG.sql"
ERR_FILE="$WORKDIR/$CURRENT_BINLOG.err"

log "BINLOG_PATH=$BINLOG_PATH"
log "SQL_FILE=$SQL_FILE"
log "ERR_FILE=$ERR_FILE"

############################################
# BINLOG EXISTENCE CHECK
############################################
section "BINLOG SANITY CHECK"

if [[ ! -f "$BINLOG_PATH" ]]; then
    log "Binlog does not exist → nothing to do  → Exiting"
    db_log_error "Binlog file not found, exiting: $BINLOG_PATH" "$CURRENT_BINLOG" "$CURRENT_POS" ""
    exit 0
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

if grep -q "not closed properly" "$SQL_FILE"; then
    log "WARNING detected in binlog extraction:"
    log "[Not closed properly] line found in $SQL_FILE"
    log "Binlog still in use → skipping this iteration safely"
    db_log_warn "Binlog still in use (not closed properly), skipping iteration: $CURRENT_BINLOG" "$CURRENT_BINLOG" "$CURRENT_POS" ""
    rm -f "$SQL_FILE" "$ERR_FILE"
    log "$SQL_FILE and $ERR_FILE files deleted"
    exit 0
fi

############################################
# FAILOVER DETECTION
############################################
section "FAILOVER DETECTION"

# Find ALL lines from any foreign server_id (pattern covers all non-local nodes)
ALL_FOREIGN_LINES=$(grep -n -E "server id $OTHER_SERVER_ID" "$SQL_FILE" || true)

if [[ -n "$ALL_FOREIGN_LINES" ]]; then
    log "Found $(echo "$ALL_FOREIGN_LINES" | wc -l | tr -d ' ') foreign transaction(s) to analyze"

    MAINTENANCE_COUNT=0
    TRANSACTION_COUNT=0
    FAILOVER_DETECTED=0
    FOREIGN_RANGES_FILE="$WORKDIR/$CURRENT_BINLOG.foreign_ranges.txt"
    : > "$FOREIGN_RANGES_FILE"

    # Save the extraction start position before the loop overwrites CURRENT_POS
    BINLOG_START_POS="$CURRENT_POS"

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

        # Find the next GTID line to bound this transaction block
        NEXT_GTID_LINE_NUM=$(sed -n "${FOREIGN_LINE_NUM},\$p" "$SQL_FILE" | grep -n "GTID [0-9]-[0-9]-[0-9]" | sed -n '2p' | cut -d: -f1)

        if [[ -n "$NEXT_GTID_LINE_NUM" ]]; then
            TXN_END_LINE_NUM=$(( FOREIGN_LINE_NUM + NEXT_GTID_LINE_NUM - 2 ))
            log "  Transaction ends at line $TXN_END_LINE_NUM (next GTID found)"
        else
            TXN_END_LINE_NUM=$(wc -l < "$SQL_FILE")
            log "  Transaction ends at EOF (line $TXN_END_LINE_NUM)"
        fi

        # Extract the complete transaction block
        TRANSACTION_BLOCK=$(sed -n "${FOREIGN_LINE_NUM},${TXN_END_LINE_NUM}p" "$SQL_FILE")

        # Look for the first meaningful SQL statement in the block
        SQL_STATEMENT=$(echo "$TRANSACTION_BLOCK" | grep -vE "^(SET @@session\.|/\*!|#|$)" | \
                       grep -iE "^[[:space:]]*(truncate|insert|update|delete|create|drop|alter|replace|flush|optimize|analyze|repair|show|start|stop|reset|change)" | \
                       head -1 | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//' | sed 's/\/\*!\*\/;*$//')

        # Patterns that are routine cluster maintenance — safe to ignore regardless of which node emits them
        MAINTENANCE_PATTERNS="(truncate[[:space:]]+mysql\.(slow_log|general_log|error_log))|(flush[[:space:]]+(logs|binary[[:space:]]+logs))|(optimize[[:space:]]+table)|(analyze[[:space:]]+table)|(repair[[:space:]]+table)|(show[[:space:]]+(slave|master)[[:space:]]+status)|(start[[:space:]]+slave)|(stop[[:space:]]+slave)|(reset[[:space:]]+slave)"

        if [[ -n "$SQL_STATEMENT" ]]; then
            log "  SQL found: $SQL_STATEMENT"

            if echo "$SQL_STATEMENT" | grep -qE "$MAINTENANCE_PATTERNS"; then
                MAINTENANCE_COUNT=$((MAINTENANCE_COUNT + 1))
                log "  -> MAINTENANCE #$MAINTENANCE_COUNT (server_id=$ACTUAL_FOREIGN_SERVER_ID): $SQL_STATEMENT (IGNORED)"
                echo "$FOREIGN_LINE_NUM $TXN_END_LINE_NUM" >> "$FOREIGN_RANGES_FILE"
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

        # Remove foreign maintenance transactions from pre-failover SQL before apply
        if [[ -s "$PRE_FAILOVER_SQL" && -s "$FOREIGN_RANGES_FILE" ]]; then
            PRE_FAILOVER_FILTERED_SQL="$WORKDIR/$CURRENT_BINLOG.prefailover.filtered.sql"

            awk '
                NR == FNR {
                    range_start[++range_count] = $1
                    range_end[range_count] = $2
                    next
                }
                {
                    drop_line = 0
                    for (i = 1; i <= range_count; i++) {
                        if (NR >= range_start[i] && NR <= range_end[i]) {
                            drop_line = 1
                            break
                        }
                    }
                    if (!drop_line) {
                        print
                    }
                }
            ' "$FOREIGN_RANGES_FILE" "$PRE_FAILOVER_SQL" > "$PRE_FAILOVER_FILTERED_SQL"

            mv "$PRE_FAILOVER_FILTERED_SQL" "$PRE_FAILOVER_SQL"
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
        rm -f "$FOREIGN_RANGES_FILE"

        # This node is no longer the active processor — exit immediately
        exit 10

    else
        if [[ -s "$FOREIGN_RANGES_FILE" ]]; then
            FILTERED_SQL_FILE="$WORKDIR/$CURRENT_BINLOG.filtered.sql"
            REMOVED_LINES=$(awk '{sum += ($2 - $1 + 1)} END {print sum+0}' "$FOREIGN_RANGES_FILE")

            awk '
                NR == FNR {
                    range_start[++range_count] = $1
                    range_end[range_count] = $2
                    next
                }
                {
                    drop_line = 0
                    for (i = 1; i <= range_count; i++) {
                        if (NR >= range_start[i] && NR <= range_end[i]) {
                            drop_line = 1
                            break
                        }
                    }
                    if (!drop_line) {
                        print
                    }
                }
            ' "$FOREIGN_RANGES_FILE" "$SQL_FILE" > "$FILTERED_SQL_FILE"

            mv "$FILTERED_SQL_FILE" "$SQL_FILE"
            log "Removed $TRANSACTION_COUNT foreign maintenance transaction block(s) from SQL file ($REMOVED_LINES line(s) removed)"
        fi

        rm -f "$FOREIGN_RANGES_FILE"

        log "ANALYSIS COMPLETE - ALL FOREIGN TRANSACTIONS WERE MAINTENANCE:"
        log "  Maintenance operations : $MAINTENANCE_COUNT"
        log "  Total transactions     : $TRANSACTION_COUNT"
        log "  Result: No failover detected, continuing binlog processing..."
    fi

else
    log "No foreign server_id transactions found in this binlog"
fi

############################################
# TEST RUN - EARLY EXIT
############################################
log "TEST RUN: Validation checkpoint reached — exiting early for review"
exit 0

############################################
# APPLY BINLOG TO TARGET
############################################
section "APPLY BINLOG TO TARGET"

if [[ -s "$SQL_FILE" ]]; then
    APPLY_CMD=(
        "$MYSQL"
        "--host=$TARGET_HOST"
        "--user=$TARGET_USER"
        "--password=$TARGET_PASS"
        "--port=$TARGET_PORT"
        "--ssl=true"
    )

    log "Applying SQL file to target"
    log "Command:"
    log "${APPLY_CMD[*]} < $SQL_FILE"

    "${APPLY_CMD[@]}" < "$SQL_FILE"
    log "Apply completed successfully"
else
    log "SQL file is empty → no transactions to apply"
fi

############################################
# ADVANCE STATE
############################################
section "STATE ADVANCEMENT"

NEXT_BINLOG=$(printf "%s.%06d" \
    "${CURRENT_BINLOG%.*}" \
    "$((10#${CURRENT_BINLOG##*.} + 1))")

log "Next binlog inferred: $NEXT_BINLOG"

cat > "$STATE_FILE" <<EOF
LAST_BINLOG=$NEXT_BINLOG
LAST_POS=4
EOF

log "State file updated:"
log "$(cat "$STATE_FILE")"

############################################
# CLEANUP
############################################
section "CLEANUP"

log "Removing temporary files"
rm -f "$SQL_FILE" "$ERR_FILE"

############################################
# SCRIPT END
############################################
section "SCRIPT END"
log "Execution completed successfully"


