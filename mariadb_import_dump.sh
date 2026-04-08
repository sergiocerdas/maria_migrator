#!/bin/bash
# mariadb_import_dump.sh
# Import a database dump file into a target MariaDB instance

set -e

# Default values
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_HOST=""
MYSQL_PORT="3306"
DUMP_FILE=""
USE_SSL="true"

# Usage function
usage() {
    echo "Usage: $0 -f dump_file -h host -u user -p password [-P port] [--no-ssl]"
    echo "  -f dump_file           SQL dump file to import (required)"
    echo "  -h host                Target MariaDB host (required)"
    echo "  -u user                Target MariaDB user (required)"
    echo "  -p password            Target MariaDB password (required)"
    echo "  -P port                Target MariaDB port (default: 3306)"
    echo "  --no-ssl               Disable SSL connection (default: SSL enabled)"
    echo ""
    echo "Example:"
    echo "  ./mariadb_import_dump.sh -f /backup/exported.sql -h targethost -u admin -p 'password' -P 3306"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f)
            DUMP_FILE="$2"
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
        --no-ssl)
            USE_SSL="false"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required parameters
if [ -z "$DUMP_FILE" ] || [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_USER" ] || [ -z "$MYSQL_PASSWORD" ]; then
    echo "[ERROR] Missing required parameters"
    usage
fi

# Check if dump file exists
if [ ! -f "$DUMP_FILE" ]; then
    echo "[ERROR] Dump file not found: $DUMP_FILE"
    exit 1
fi

echo "[INFO] Importing dump file: $DUMP_FILE"
echo "[INFO] Target host: $MYSQL_HOST:$MYSQL_PORT"
echo "[INFO] Target user: $MYSQL_USER"
echo "[INFO] SSL enabled: $USE_SSL"

# Build SSL argument
SSL_ARG=""
if [ "$USE_SSL" = "true" ]; then
    SSL_ARG="--ssl=true"
fi

# Import the dump file
mariadb --host="$MYSQL_HOST" --user="$MYSQL_USER" --port="$MYSQL_PORT" \
	--password="$MYSQL_PASSWORD" $SSL_ARG --max_allowed_packet=1G  < "$DUMP_FILE"
#mariadb --host="$RESTORE_HOST" --user="$RESTORE_USER" --port="$RESTORE_PORT" --password="$RESTORE_PASSWORD" --ssl=true --max_allowed_packet=1G --net_buffer_length=16384 < "$OUTPUT_FILE"

echo "[INFO] Database import completed successfully to $MYSQL_HOST:$MYSQL_PORT"

