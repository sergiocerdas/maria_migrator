#!/bin/bash
# mariadb_extract_users_grants.sh
# Extract users and grants from MariaDB instance

set -e

# Default values
MYSQL_USER="mysql"
MYSQL_PASSWORD=""
MYSQL_HOST="$HOSTNAME"
MYSQL_PORT="3306"
OUTPUT_BASE="./grants"
EXCLUDE_DBS=()

# Usage function
usage() {
    echo "Usage: $0 [-o output_base] [-h host] [-P port] [-u user] [-p password] [-x db1,db2,...]"
    echo "  -o output_base         Output file base name (default: ./grants)"
    echo "                         Generates: output_base_show_grants.sql, output_base_grants_selected.sql, output_base_grants_processed.sql"
    echo "  -h host                MariaDB host (default: $HOSTNAME)"
    echo "  -P port                MariaDB port (default: 3306)"
    echo "  -u user                MariaDB user (default: mysql)"
    echo "  -p password            MariaDB password (default: empty)"
    echo "  -x db1,db2,...         Comma-separated list of databases to exclude (their users won't be exported)"
    echo ""
    echo "Example:"
    echo "  ./mariadb_extract_users_grants.sh -o /backup/grants -P 3000 -u mysql01 -x testdb"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o)
            OUTPUT_BASE="$2"
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

# Output file names
SHOW_GRANTS_FILE="${OUTPUT_BASE}_show_grants.sql"
GRANTS_SELECTED_FILE="${OUTPUT_BASE}_grants_selected.sql"
PROCESSED_GRANTS_FILE="${OUTPUT_BASE}_grants_processed.sql"

# Build the exclusion pattern for users
# For each excluded DB, we want to exclude users like: dbname_rw, dbname_ro, dbname_so
EXCLUDE_USER_PATTERN=""
for db in "${EXCLUDE_DBS[@]}"; do
    if [ -z "$EXCLUDE_USER_PATTERN" ]; then
        EXCLUDE_USER_PATTERN="^${db}_"
    else
        EXCLUDE_USER_PATTERN="${EXCLUDE_USER_PATTERN}|^${db}_"
    fi
done

# Build the WHERE clause
if [ -n "$EXCLUDE_USER_PATTERN" ]; then
    WHERE_CLAUSE="User REGEXP '(rw|ro|so)$' AND User NOT REGEXP '$EXCLUDE_USER_PATTERN'"
else
    WHERE_CLAUSE="User REGEXP '(rw|ro|so)$'"
fi

echo "[INFO] Generating SHOW GRANTS statements for users matching (rw|ro|so)$ to $SHOW_GRANTS_FILE"
if [ ${#EXCLUDE_DBS[@]} -gt 0 ]; then
    echo "[INFO] Excluding users from databases: ${EXCLUDE_DBS[*]}"
fi

# Generate show grants file for users matching (rw|ro|so)$
mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" --silent --skip-column-names \
    -e "SELECT CONCAT('SHOW GRANTS FOR ''',  User, '''@''', Host, ''';') FROM mysql.user WHERE $WHERE_CLAUSE;" > "$SHOW_GRANTS_FILE"

echo "[INFO] Executing SHOW GRANTS statements and saving output to $GRANTS_SELECTED_FILE"
mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" < "$SHOW_GRANTS_FILE" > "$GRANTS_SELECTED_FILE"

echo "[INFO] Processing grants file to extract CREATE USER and GRANT statements"

# Process the grants file to extract CREATE USER and GRANT statements
awk '
BEGIN {
    current_user = ""
    current_host = ""
}
/^Grants for/ {
    # Extract user and host from "Grants for user@host"
    match($0, /Grants for (.+)@(.+)/, arr)
    if (arr[1] && arr[2]) {
        current_user = arr[1]
        current_host = arr[2]
        # Remove any quotes
        gsub(/'\''/, "", current_user)
        gsub(/'\''/, "", current_host)
        # Print CREATE USER IF NOT EXISTS
        printf "CREATE USER IF NOT EXISTS '\''%s'\''@'\''%s'\'';\n", current_user, current_host
    }
    next
}
/^GRANT/ {
    # Print the GRANT statement with semicolon
    print $0 ";"
    next
}
' "$GRANTS_SELECTED_FILE" > "$PROCESSED_GRANTS_FILE"

echo "[INFO] Processed grants file saved to $PROCESSED_GRANTS_FILE"
echo "[INFO] Users and grants extraction completed successfully"
echo ""
echo "Generated files:"
echo "  - $SHOW_GRANTS_FILE (SHOW GRANTS statements)"
echo "  - $GRANTS_SELECTED_FILE (raw grants output)"
echo "  - $PROCESSED_GRANTS_FILE (CREATE USER + GRANT statements - ready to import)"

