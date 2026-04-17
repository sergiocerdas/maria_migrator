#!/usr/bin/env bash
set -euo pipefail

############################################
# PARAMETERS - CLUSTER SERVERS
############################################

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <PRIMARY_NODE_NAME> <SECONDARY_NODE_NAME> [other parameters...]"
    exit 1
fi

CLUSTER_NODE_1="$1"
CLUSTER_NODE_2="$2"

shift 2   # Remove the two node names from argument list

CLUSTER_NODES=("$CLUSTER_NODE_1" "$CLUSTER_NODE_2")

############################################
# VALIDATION
############################################

if [[ "$CLUSTER_NODE_1" == "$CLUSTER_NODE_2" ]]; then
    echo "ERROR: Both cluster node names are identical."
    exit 1
fi

############################################
# CONFIGURATION
############################################
PORT=4065
BINLOG_DIR="/instances/mysql_db$PORT/binlogs"
WORKDIR="/instances/mysql_db$PORT/scriptsMig/githubversion/binlog_apply"

STATE_FILE="$WORKDIR/state.env"
LOG_FILE="$WORKDIR/run.log"
FAILOVER_FILE="$WORKDIR/failover_detected.txt"

TARGET_HOST="10-11-180-79.dbaas.intel.com"
TARGET_PORT=3306
TARGET_USER="b00cvzpaccul4dp4z8j5_dbaas"
TARGET_PASS="J_BK=t2o_K3!1hV;tR;j5kgDirndI-Ge"

MYSQL_BINLOG="/usr/bin/mariadb-binlog"
MYSQL="/usr/bin/mariadb"

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
#LOCAL_SERVER_ID="${HOSTNAME_SHORT:1:1}"
LOCAL_SERVER_ID=$(mysql --host $HOSTNAME --user=mysql01 --port=$PORT --skip-column-names --silent -e "SELECT @@server_id;")

log "Hostname detected: $HOSTNAME_SHORT"
log "Inferred local server-id: $LOCAL_SERVER_ID"

if [[ ! "$LOCAL_SERVER_ID" =~ ^[12]$ ]]; then
    log "ERROR: Unable to infer valid server-id from hostname"
    exit 1
fi

#OTHER_SERVER_ID=$([[ "$LOCAL_SERVER_ID" == "1" ]] && echo 2 || echo 1)
OTHER_SERVER_ID=$(mysql --host $CLUSTER_NODE_2 --user=mysql01 --port=$PORT --skip-column-names --silent -e "SELECT @@server_id;")

log "Hostname: $HOSTNAME_SHORT"
log "Local server_id: $LOCAL_SERVER_ID"


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

if [[ ! -f "$STATE_FILE" ]]; then
    log "No state file found → FIRST RUN"

    if [[ $# -ne 2 ]]; then
        log "ERROR: First run requires <binlog_name> <start_position>"
        exit 1
    fi

    CURRENT_BINLOG="$1"
    CURRENT_POS="$2"

    log "Initial binlog provided: $CURRENT_BINLOG"
    log "Initial position provided: $CURRENT_POS"
else
    log "State file found → RESUMING"
    log "Loading state file: $STATE_FILE"

    # shellcheck disable=SC1090
    source "$STATE_FILE"

    CURRENT_BINLOG="$LAST_BINLOG"
    CURRENT_POS="$LAST_POS"

    log "Resuming from binlog: $CURRENT_BINLOG"
    log "Resuming from position: $CURRENT_POS"
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
    log "Binlog does not exist yet → nothing to do"
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

"${BINLOG_CMD[@]}" > "$SQL_FILE" 2> "$ERR_FILE"

############################################
# BINLOG-IN-USE DETECTION
############################################
section "BINLOG IN-USE CHECK"

if grep -q "not closed properly" "$SQL_FILE"; then
    log "WARNING detected in binlog extraction:"
    log "[Not closed properly] line found in $SQL_FILE" 
    log "Binlog still in use → skipping this iteration safely"
    rm -f "$SQL_FILE" "$ERR_FILE"
    log "$SQL_FILE and $ERR_FILE files deleted"
    exit 0
fi

############################################
# FAILOVER DETECTION
############################################
section "FAILOVER DETECTION"

# Find ALL lines with foreign server_id (not just the first one)
ALL_FOREIGN_LINES=$(grep -n -E "server id $OTHER_SERVER_ID" "$SQL_FILE" || true)

if [[ -n "$ALL_FOREIGN_LINES" ]]; then
    log "Found $(echo "$ALL_FOREIGN_LINES" | wc -l) transaction(s) from server_id $OTHER_SERVER_ID"
    
    MAINTENANCE_COUNT=0
    TRANSACTION_COUNT=0
    
    # Process each foreign server_id transaction
    while IFS= read -r FOREIGN_LINE_WITH_NUM; do
        if [[ -z "$FOREIGN_LINE_WITH_NUM" ]]; then
            continue
        fi
        
        TRANSACTION_COUNT=$((TRANSACTION_COUNT + 1))
        FOREIGN_LINE_NUM=$(echo "$FOREIGN_LINE_WITH_NUM" | cut -d: -f1)
        FOREIGN_LINE=$(echo "$FOREIGN_LINE_WITH_NUM" | cut -d: -f2-)
        
        log "Analyzing transaction #$TRANSACTION_COUNT at line $FOREIGN_LINE_NUM..."
        
        # Extract position
        CURRENT_POS=$(echo "$FOREIGN_LINE" | grep -oE 'end_log_pos [0-9]+' | awk '{print $2}')
        
        # Find the next GTID line to determine the end of this transaction
        NEXT_GTID_LINE_NUM=$(sed -n "${FOREIGN_LINE_NUM},\$p" "$SQL_FILE" | grep -n "GTID [0-9]-[0-9]-[0-9]" | sed -n '2p' | cut -d: -f1)
        
        if [[ -n "$NEXT_GTID_LINE_NUM" ]]; then
            # Calculate actual line number in the full file
            END_LINE_NUM=$(($FOREIGN_LINE_NUM + $NEXT_GTID_LINE_NUM - 1))
            log "  Transaction ends at line $END_LINE_NUM (next GTID found)"
        else
            # If no next GTID found, extract to end of file
            END_LINE_NUM=$(wc -l < "$SQL_FILE")
            log "  Transaction ends at EOF (line $END_LINE_NUM)"
        fi
        
        # Extract the complete transaction block until next GTID
        TRANSACTION_BLOCK=$(sed -n "${FOREIGN_LINE_NUM},${END_LINE_NUM}p" "$SQL_FILE")
        
        # Look for actual SQL statements in the transaction block
        SQL_STATEMENT=$(echo "$TRANSACTION_BLOCK" | grep -vE "^(SET @@session\.|/\*!|#|$)" | \
                       grep -iE "^[[:space:]]*(truncate|insert|update|delete|create|drop|alter|replace|flush|optimize|analyze|repair|show|start|stop|reset|change)" | \
                       head -1 | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//' | sed 's/\/\*!\*\/;*$//')
        
        # Define maintenance operation patterns
        MAINTENANCE_PATTERNS="(truncate[[:space:]]+mysql\.(slow_log|general_log|error_log))|(flush[[:space:]]+(logs|binary[[:space:]]+logs))|(optimize[[:space:]]+table)|(analyze[[:space:]]+table)|(repair[[:space:]]+table)|(show[[:space:]]+(slave|master)[[:space:]]+status)|(start[[:space:]]+slave)|(stop[[:space:]]+slave)|(reset[[:space:]]+slave)"
        # Simplified maintenance patterns - test one by one
        #MAINTENANCE_PATTERNS="truncate[[:space:]]+mysql\.slow_log|truncate[[:space:]]+mysql\.general_log|flush[[:space:]]+logs"
        # Check if the SQL statement matches maintenance patterns

        if [[ -n "$SQL_STATEMENT" ]]; then
            log "  Found SQL: $SQL_STATEMENT"
            
            # Check against maintenance patterns (case insensitive)
            if echo "$SQL_STATEMENT" | grep -qiE "$MAINTENANCE_PATTERNS"; then
                MAINTENANCE_COUNT=$((MAINTENANCE_COUNT + 1))
                log "  -> MAINTENANCE #$MAINTENANCE_COUNT: $SQL_STATEMENT (IGNORED)"
                log "  -> Continuing to next transaction..."
            else
                log "  -> REAL FAILOVER DETECTED: $SQL_STATEMENT"
                log "  -> STOPPING ANALYSIS IMMEDIATELY - Failover found!"
                
                # Store failover details
                FAILOVER_POS="$CURRENT_POS"
                FAILOVER_GTID=$(echo "$FOREIGN_LINE" | sed -n 's/.*GTID \([0-9-]\+\).*/\1/p')
                FAILOVER_SERVER_ID="$OTHER_SERVER_ID"
                FAILOVER_TIME=$(date '+%Y-%m-%d %H:%M:%S')
                FAILOVER_SQL="$SQL_STATEMENT"
                FAILOVER_LINE="$FOREIGN_LINE"
                
                # Write failover file immediately
                {
                    echo "FAILOVER DETECTED"
                    echo "Detected server_id: $OTHER_SERVER_ID"
                    echo "Binlog: $CURRENT_BINLOG"
                    echo "Position: $FAILOVER_POS"
                    echo "Timestamp: $(date -u)"
                    echo "FAILOVER_GTID: $FAILOVER_GTID"
                    echo "FAILOVER_POS: $FAILOVER_POS"
                    echo "FAILOVER_SERVER_ID: $FAILOVER_SERVER_ID"
                    echo "FAILOVER_TIME: $FAILOVER_TIME"
                    echo "SQL_STATEMENT: $FAILOVER_SQL"
                    echo "MAINTENANCE_COUNT: $MAINTENANCE_COUNT"
                    echo "TRANSACTION_ANALYZED: $TRANSACTION_COUNT"
                    echo "FULL FAILOVER_LINE: $FAILOVER_LINE"
                } > "$FAILOVER_FILE"

                log "FAILOVER DETECTED - IMMEDIATE STOP"
                log "Foreign server_id $OTHER_SERVER_ID found with non-maintenance SQL"
                log "Failover SQL: $FAILOVER_SQL"
                log "Failover at $CURRENT_BINLOG position $FAILOVER_POS"
                log "Analyzed $TRANSACTION_COUNT transactions ($MAINTENANCE_COUNT maintenance, 1 failover)"
                log "Handoff file written to $FAILOVER_FILE"
                log "  Position : $FAILOVER_POS"
                log "  GTID     : $FAILOVER_GTID"
                log "  ServerID : $FAILOVER_SERVER_ID"
                log "CRITICAL: Stopping processing - other server is now primary"

                ############################################
                # COPY FAILOVER FILE TO NEW PRIMARY
                ############################################

                # Convert server_id to array index (1 → 0, 2 → 1)
                NEW_PRIMARY_INDEX=$((FAILOVER_SERVER_ID - 1))

                NEW_PRIMARY_HOST="${CLUSTER_NODES[$NEW_PRIMARY_INDEX]}"

                if [[ -z "$NEW_PRIMARY_HOST" ]]; then
                    log "ERROR: Unable to resolve new primary host for server_id $FAILOVER_SERVER_ID"
                else
                    section "FAILOVER HANDOFF"

                    log "Copying failover file to new primary"
                    log "  Server ID : $FAILOVER_SERVER_ID"
                    log "  Host      : $NEW_PRIMARY_HOST"
                    log "  File      : $FAILOVER_FILE"

                    SCP_CMD="scp $FAILOVER_FILE $NEW_PRIMARY_HOST:$FAILOVER_FILE"

                    log "Executing: $SCP_CMD"

                    if scp "$FAILOVER_FILE" "$NEW_PRIMARY_HOST:$FAILOVER_FILE"; then
                        log "Failover file successfully copied to $NEW_PRIMARY_HOST"
                        ############################################
                        # Remove failover file (handoff complete)
                        ############################################
                        rm -f "$FAILOVER_FILE"
                        log "Failover file consumed and removed"
                    else
                        log "ERROR: Failed to copy failover file to $NEW_PRIMARY_HOST"
                        log "You may attempt to copy the file manually and remove the failover file locally before moving forward"
                    fi
                fi

                # IMMEDIATE EXIT - DO NOT PROCESS ANY MORE OF THIS BINLOG
                exit 10
            fi
        else
            log "  WARNING: No SQL statement found in transaction block, treating as potential failover"
            log "  -> POTENTIAL FAILOVER DETECTED (no SQL found)"
            log "  -> STOPPING ANALYSIS IMMEDIATELY - Unknown transaction type!"
            
            # Treat unknown transactions as potential failovers for safety
            FAILOVER_POS="$CURRENT_POS"
            FAILOVER_GTID=$(echo "$FOREIGN_LINE" | sed -n 's/.*GTID \([0-9-]\+\).*/\1/p')
            FAILOVER_SERVER_ID="$OTHER_SERVER_ID"
            FAILOVER_TIME=$(date '+%Y-%m-%d %H:%M:%S')
            FAILOVER_SQL="UNKNOWN_TRANSACTION"
            FAILOVER_LINE="$FOREIGN_LINE"
            
            {
                echo "FAILOVER DETECTED"
                echo "Detected server_id: $OTHER_SERVER_ID"
                echo "Binlog: $CURRENT_BINLOG"
                echo "Position: $FAILOVER_POS"
                echo "Timestamp: $(date -u)"
                echo "FAILOVER_GTID: $FAILOVER_GTID"
                echo "FAILOVER_POS: $FAILOVER_POS"
                echo "FAILOVER_SERVER_ID: $FAILOVER_SERVER_ID"
                echo "FAILOVER_TIME: $FAILOVER_TIME"
                echo "SQL_STATEMENT: $FAILOVER_SQL"
                echo "MAINTENANCE_COUNT: $MAINTENANCE_COUNT"
                echo "TRANSACTION_ANALYZED: $TRANSACTION_COUNT"
                echo "FULL FAILOVER_LINE: $FAILOVER_LINE"
                echo "NOTE: Unknown transaction type - treated as failover for safety"
            } > "$FAILOVER_FILE"
            
            log "UNKNOWN TRANSACTION DETECTED - TREATING AS FAILOVER"
            # [Same handoff logic as above...]
            exit 10
        fi
        
    done <<< "$ALL_FOREIGN_LINES"
    
    # If we reach here, all foreign transactions were maintenance operations
    log "ANALYSIS COMPLETE - ALL MAINTENANCE:"
    log "  Maintenance operations: $MAINTENANCE_COUNT"
    log "  Total foreign transactions: $TRANSACTION_COUNT"
    log "  Result: No failover detected, continuing binlog processing..."
    
else
    log "No transactions from foreign server_id $OTHER_SERVER_ID found in this binlog"
fi

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


