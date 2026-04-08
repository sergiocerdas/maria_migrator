#!/bin/bash
# mariadb_export_databases.sh
# Export all MariaDB databases to a single .sql file

set -e

# Default values
MYSQL_USER="mysql"
MYSQL_PASSWORD=""
MYSQL_HOST="$HOSTNAME"
MYSQL_PORT="3306"
OUTPUT_FILE="./all_databases.sql"
EXCLUDE_DBS=()

# Usage function
usage() {
    echo "Usage: $0 [-o output_file] [-h host] [-P port] [-u user] [-p password] [-x db1,db2,...]"
    echo "  -o output_file         Output SQL file (default: ./all_databases.sql)"
    echo "  -h host                MariaDB host (default: $HOSTNAME)"
    echo "  -P port                MariaDB port (default: 3306)"
    echo "  -u user                MariaDB user (default: mysql)"
    echo "  -p password            MariaDB password (default: empty)"
    echo "  -x db1,db2,...         Comma-separated list of databases to exclude from export"
    echo ""
    echo "Example:"
    echo "  ./mariadb_export_databases.sh -o /backup/exported.sql -P 3000 -u mysql01 -x testdb,tempdb"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -h)
            MYSQL_HOST="$2"
            shift 2
            ;;
        -P)
            MYSQL_PORT="$2"
            shift 2
            ;;
        -u)
            MYSQL_USER="$2"
            shift 2
            ;;
        -p)
            MYSQL_PASSWORD="$2"
            shift 2
            ;;
        -x)
            IFS=',' read -ra EXCLUDE_DBS <<< "$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Build ignore-database options (always ignore system DBs and admindb)
DEFAULT_EXCLUDES=(mysql information_schema performance_schema sys admindb)
IGNORE_ARGS=""
for db in "${DEFAULT_EXCLUDES[@]}"; do
    IGNORE_ARGS+=" --ignore-database=$db"
done
for db in "${EXCLUDE_DBS[@]}"; do
    # Only add if not already in default excludes
    if [[ ! " ${DEFAULT_EXCLUDES[@]} " =~ " ${db} " ]]; then
        IGNORE_ARGS+=" --ignore-database=$db"
    fi
done

echo "[INFO] Exporting all databases to $OUTPUT_FILE"
echo "[INFO] Excluding databases: ${DEFAULT_EXCLUDES[*]} ${EXCLUDE_DBS[*]}"

# Execute mysqldump
mysqldump --user="$MYSQL_USER" --host="$MYSQL_HOST" --port="$MYSQL_PORT" \
    --single-transaction --quick --master-data=2 --max_allowed_packet=1GB \
    --events --routines --triggers --all-databases $IGNORE_ARGS  > "$OUTPUT_FILE"

echo "[INFO] Database export completed successfully to $OUTPUT_FILE"

