#!/bin/bash

# Migration Validation Script
# Usage: ./migration_validation.sh [source|target] <host> <port> <user> <password>

ACTION=$1
HOST=$2
PORT=$3
USER=$4
PASSWORD=$5

if [ $# -ne 5 ]; then
    echo "Usage: $0 [source|target] <host> <port> <user> <password>"
    echo "Example: $0 source maria4217-lb-fm-in.dbaas.intel.com 4217 mysql01 password"
    echo "Example: $0 target 10-18-191-149.dbaas.intel.com 3306 admin_user password"
    exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="${ACTION}_metrics_${TIMESTAMP}.txt"

echo "=== DBaaS Migration Validation Report ===" > $OUTPUT_FILE
echo "Instance: $ACTION" >> $OUTPUT_FILE
echo "Host: $HOST" >> $OUTPUT_FILE
echo "Port: $PORT" >> $OUTPUT_FILE
echo "Timestamp: $(date)" >> $OUTPUT_FILE
echo "=========================================" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

# Database connection command
MYSQL_CMD="mariadb --host=$HOST --port=$PORT --user=$USER --password=$PASSWORD --ssl=true --skip-column-names -e"

echo "1. DATABASE INVENTORY" >> $OUTPUT_FILE
echo "=====================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT SCHEMA_NAME as 'Database_Name' FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema','performance_schema','mysql','sys','admindb') ORDER BY SCHEMA_NAME;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "2. DATABASE SIZES" >> $OUTPUT_FILE
echo "=================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    table_schema as 'Database_Name',
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as 'Size_MB',
    COUNT(*) as 'Table_Count'
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
GROUP BY table_schema 
ORDER BY table_schema;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "3. TABLE INVENTORY BY DATABASE" >> $OUTPUT_FILE
echo "==============================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    table_schema as 'Database_Name',
    table_name as 'Table_Name',
    table_type as 'Table_Type',
    table_rows as 'Row_Count'
FROM information_schema.tables 
WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY table_schema, table_name;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "4. USER ACCOUNTS" >> $OUTPUT_FILE
echo "================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    User as 'Username',
    Host as 'Host'
FROM mysql.user 
WHERE User NOT IN ('root','mysql.sys','mysql.session','mysql.infoschema','mariadb.sys')
ORDER BY User, Host;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "5. USER PRIVILEGES SUMMARY" >> $OUTPUT_FILE
echo "==========================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    grantee as 'User_Host',
    table_schema as 'Database_Name',
    privilege_type as 'Privilege'
FROM information_schema.schema_privileges 
WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY grantee, table_schema, privilege_type;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "6. STORED PROCEDURES AND FUNCTIONS" >> $OUTPUT_FILE
echo "===================================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    routine_schema as 'Database_Name',
    routine_name as 'Routine_Name',
    routine_type as 'Type'
FROM information_schema.routines 
WHERE routine_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY routine_schema, routine_type, routine_name;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "7. VIEWS INVENTORY" >> $OUTPUT_FILE
echo "==================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    table_schema as 'Database_Name',
    table_name as 'View_Name'
FROM information_schema.views 
WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY table_schema, table_name;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "8. TRIGGERS INVENTORY" >> $OUTPUT_FILE
echo "=====================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    trigger_schema as 'Database_Name',
    trigger_name as 'Trigger_Name',
    event_manipulation as 'Event',
    event_object_table as 'Table_Name'
FROM information_schema.triggers 
WHERE trigger_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY trigger_schema, trigger_name;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "9. INDEXES SUMMARY" >> $OUTPUT_FILE
echo "==================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    table_schema as 'Database_Name',
    table_name as 'Table_Name',
    index_name as 'Index_Name',
    non_unique as 'Non_Unique',
    seq_in_index as 'Sequence'
FROM information_schema.statistics 
WHERE table_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
ORDER BY table_schema, table_name, index_name, seq_in_index;" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

echo "10. FOREIGN KEY CONSTRAINTS" >> $OUTPUT_FILE
echo "===========================" >> $OUTPUT_FILE
$MYSQL_CMD "SELECT 
    tc.constraint_schema as 'Database_Name',
    tc.table_name as 'Table_Name',
    tc.constraint_name as 'FK_Name',
    kcu.referenced_table_schema as 'Referenced_DB',
    kcu.referenced_table_name as 'Referenced_Table'
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_schema = kcu.constraint_schema 
    AND tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.constraint_schema NOT IN ('information_schema','performance_schema','mysql','sys','admindb')
GROUP BY tc.constraint_schema, tc.table_name, tc.constraint_name, kcu.referenced_table_schema, kcu.referenced_table_name
ORDER BY tc.constraint_schema, tc.table_name, tc.constraint_name;" >> $OUTPUT_FILE

echo "" >> $OUTPUT_FILE
echo "Report generated: $OUTPUT_FILE"
echo "Metrics collection completed for $ACTION instance."