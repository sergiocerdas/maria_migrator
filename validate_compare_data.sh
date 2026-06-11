#!/bin/bash

# Migration Validation Comparison Script
# Usage: ./validate_compare_data.sh <source_file> <target_file>

SOURCE_FILE=$1
TARGET_FILE=$2

if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_metrics_file> <target_metrics_file>"
    echo "Example: $0 source_metrics_20260611_142000.txt target_metrics_20260611_143000.txt"
    exit 1
fi

if [ ! -f "$SOURCE_FILE" ] || [ ! -f "$TARGET_FILE" ]; then
    echo "Error: One or both metric files not found."
    exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
COMPARISON_FILE="migration_comparison_${TIMESTAMP}.txt"
VALIDATION_PASSED=true

echo "=== MIGRATION VALIDATION COMPARISON REPORT ===" > $COMPARISON_FILE
echo "Source File: $SOURCE_FILE" >> $COMPARISON_FILE
echo "Target File: $TARGET_FILE" >> $COMPARISON_FILE
echo "Comparison Date: $(date)" >> $COMPARISON_FILE
echo "===============================================" >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

# Helper function to extract section between two headers
extract_section() {
    local file=$1
    local start_pattern=$2
    local end_pattern=$3
    sed -n "/$start_pattern/,/$end_pattern/p" "$file" | grep -v "$start_pattern\|$end_pattern\|^===\|^---\|^$"
}

#############################################
# 1. DATABASE COMPARISON (Migration Focus)
#############################################
echo "1. DATABASE COMPARISON:" >> $COMPARISON_FILE
echo "=======================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "1. DATABASE INVENTORY" "2. DATABASE SIZES" > /tmp/source_dbs.txt
extract_section "$TARGET_FILE" "1. DATABASE INVENTORY" "2. DATABASE SIZES" > /tmp/target_dbs.txt

# Databases to migrate = all source databases
# Validation: ALL source databases must exist on target
echo "Databases to MIGRATE (from source):" >> $COMPARISON_FILE
cat /tmp/source_dbs.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Source databases MISSING on target (CRITICAL):" >> $COMPARISON_FILE
comm -23 <(sort /tmp/source_dbs.txt) <(sort /tmp/target_dbs.txt) > /tmp/missing_dbs.txt
cat /tmp/missing_dbs.txt >> $COMPARISON_FILE
MISSING_DB_COUNT=$(cat /tmp/missing_dbs.txt | grep -v "^$" | wc -l)
if [ "$MISSING_DB_COUNT" -eq 0 ]; then
    echo "  (None - all source databases exist on target)" >> $COMPARISON_FILE
fi
echo "" >> $COMPARISON_FILE

echo "Extra databases on TARGET (informational only):" >> $COMPARISON_FILE
comm -13 <(sort /tmp/source_dbs.txt) <(sort /tmp/target_dbs.txt) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

# Store migrated databases (those in BOTH) for user filtering
comm -12 <(sort /tmp/source_dbs.txt) <(sort /tmp/target_dbs.txt) > /tmp/migrated_dbs.txt
MIGRATED_DB_COUNT=$(cat /tmp/migrated_dbs.txt | grep -v "^$" | wc -l)
SOURCE_DB_COUNT=$(cat /tmp/source_dbs.txt | grep -v "^$" | wc -l)

#############################################
# 2. DATABASE SIZES COMPARISON
#############################################
echo "2. DATABASE SIZES COMPARISON:" >> $COMPARISON_FILE
echo "=============================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "2. DATABASE SIZES" "3. TABLE INVENTORY" > /tmp/source_sizes.txt
extract_section "$TARGET_FILE" "2. DATABASE SIZES" "3. TABLE INVENTORY" > /tmp/target_sizes.txt

echo "Source Database Sizes:" >> $COMPARISON_FILE
cat /tmp/source_sizes.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE
echo "Target Database Sizes:" >> $COMPARISON_FILE
cat /tmp/target_sizes.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

#############################################
# 3. TABLE ROW COUNT COMPARISON
#############################################
echo "3. TABLE ROW COUNT COMPARISON:" >> $COMPARISON_FILE
echo "==============================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "3. TABLE INVENTORY" "4. USER ACCOUNTS" > /tmp/source_tables.txt
extract_section "$TARGET_FILE" "3. TABLE INVENTORY" "4. USER ACCOUNTS" > /tmp/target_tables.txt

# Extract database.table and row_count for comparison (columns 1,2,4)
awk '{print $1"."$2, $4}' /tmp/source_tables.txt | sort > /tmp/source_table_rows.txt
awk '{print $1"."$2, $4}' /tmp/target_tables.txt | sort > /tmp/target_table_rows.txt

echo "Tables with ROW COUNT DIFFERENCES:" >> $COMPARISON_FILE
diff_found=false
while IFS= read -r line; do
    table=$(echo "$line" | awk '{print $1}')
    source_rows=$(echo "$line" | awk '{print $2}')
    target_rows=$(grep "^$table " /tmp/target_table_rows.txt | awk '{print $2}')
    
    if [ -n "$target_rows" ] && [ "$source_rows" != "$target_rows" ]; then
        echo "  $table: Source=$source_rows, Target=$target_rows (diff: $((target_rows - source_rows)))" >> $COMPARISON_FILE
        diff_found=true
    fi
done < /tmp/source_table_rows.txt

if [ "$diff_found" = false ]; then
    echo "  (No row count differences found)" >> $COMPARISON_FILE
fi
echo "" >> $COMPARISON_FILE

echo "Tables in SOURCE only:" >> $COMPARISON_FILE
comm -23 <(awk '{print $1}' /tmp/source_table_rows.txt | sort) <(awk '{print $1}' /tmp/target_table_rows.txt | sort) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Tables in TARGET only:" >> $COMPARISON_FILE
comm -13 <(awk '{print $1}' /tmp/source_table_rows.txt | sort) <(awk '{print $1}' /tmp/target_table_rows.txt | sort) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_TABLE_COUNT=$(cat /tmp/source_tables.txt | wc -l)
TARGET_TABLE_COUNT=$(cat /tmp/target_tables.txt | wc -l)

#############################################
# 4. USER ACCOUNT COMPARISON (Migration Focus)
# Only compare users whose names start with migrated database names
#############################################
echo "4. USER ACCOUNT COMPARISON (DB-Related Users Only):" >> $COMPARISON_FILE
echo "====================================================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "4. USER ACCOUNTS" "5. USER PRIVILEGES" > /tmp/source_users.txt
extract_section "$TARGET_FILE" "4. USER ACCOUNTS" "5. USER PRIVILEGES" > /tmp/target_users.txt

# Extract user@host (columns 1 and 2)
awk '{print $1"@"$2}' /tmp/source_users.txt | sort > /tmp/source_user_list.txt
awk '{print $1"@"$2}' /tmp/target_users.txt | sort > /tmp/target_user_list.txt

# Build grep pattern from migrated database names
# Users whose username starts with any database name
DB_PATTERN=""
while IFS= read -r db; do
    if [ -n "$db" ]; then
        if [ -z "$DB_PATTERN" ]; then
            DB_PATTERN="^${db}"
        else
            DB_PATTERN="${DB_PATTERN}|^${db}"
        fi
    fi
done < /tmp/migrated_dbs.txt

echo "Filtering users with names starting with: $(cat /tmp/migrated_dbs.txt | tr '\n' ', ' | sed 's/,$//')" >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

# Filter users related to migrated databases
if [ -n "$DB_PATTERN" ]; then
    grep -E "$DB_PATTERN" /tmp/source_user_list.txt > /tmp/source_db_users.txt 2>/dev/null || touch /tmp/source_db_users.txt
    grep -E "$DB_PATTERN" /tmp/target_user_list.txt > /tmp/target_db_users.txt 2>/dev/null || touch /tmp/target_db_users.txt
else
    touch /tmp/source_db_users.txt
    touch /tmp/target_db_users.txt
fi

echo "DB-related users in SOURCE only (MISSING on target):" >> $COMPARISON_FILE
comm -23 <(sort /tmp/source_db_users.txt) <(sort /tmp/target_db_users.txt) > /tmp/missing_users.txt
cat /tmp/missing_users.txt >> $COMPARISON_FILE
MISSING_USER_COUNT=$(cat /tmp/missing_users.txt | grep -v "^$" | wc -l)
if [ "$MISSING_USER_COUNT" -eq 0 ]; then
    echo "  (None - all DB-related source users exist on target)" >> $COMPARISON_FILE
fi
echo "" >> $COMPARISON_FILE

echo "DB-related users in TARGET only (informational):" >> $COMPARISON_FILE
comm -13 <(sort /tmp/source_db_users.txt) <(sort /tmp/target_db_users.txt) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "DB-related users in BOTH:" >> $COMPARISON_FILE
comm -12 <(sort /tmp/source_db_users.txt) <(sort /tmp/target_db_users.txt) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_DB_USER_COUNT=$(cat /tmp/source_db_users.txt | grep -v "^$" | wc -l)
TARGET_DB_USER_COUNT=$(cat /tmp/target_db_users.txt | grep -v "^$" | wc -l)
MATCHED_USER_COUNT=$(comm -12 <(sort /tmp/source_db_users.txt) <(sort /tmp/target_db_users.txt) | wc -l)

#############################################
# 5. STORED PROCEDURES & FUNCTIONS COMPARISON
#############################################
echo "5. STORED PROCEDURES & FUNCTIONS COMPARISON:" >> $COMPARISON_FILE
echo "=============================================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "6. STORED PROCEDURES" "7. VIEWS INVENTORY" > /tmp/source_routines.txt
extract_section "$TARGET_FILE" "6. STORED PROCEDURES" "7. VIEWS INVENTORY" > /tmp/target_routines.txt

# Extract db.routine_name and type
awk '{print $1"."$2, $3}' /tmp/source_routines.txt | sort > /tmp/source_routine_list.txt
awk '{print $1"."$2, $3}' /tmp/target_routines.txt | sort > /tmp/target_routine_list.txt

echo "Routines in SOURCE only:" >> $COMPARISON_FILE
comm -23 <(awk '{print $1}' /tmp/source_routine_list.txt) <(awk '{print $1}' /tmp/target_routine_list.txt) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Routines in TARGET only:" >> $COMPARISON_FILE
comm -13 <(awk '{print $1}' /tmp/source_routine_list.txt) <(awk '{print $1}' /tmp/target_routine_list.txt) >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_ROUTINE_COUNT=$(cat /tmp/source_routine_list.txt | wc -l)
TARGET_ROUTINE_COUNT=$(cat /tmp/target_routine_list.txt | wc -l)

#############################################
# 6. VIEWS COMPARISON
#############################################
echo "6. VIEWS COMPARISON:" >> $COMPARISON_FILE
echo "====================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "7. VIEWS INVENTORY" "8. TRIGGERS INVENTORY" > /tmp/source_views.txt
extract_section "$TARGET_FILE" "7. VIEWS INVENTORY" "8. TRIGGERS INVENTORY" > /tmp/target_views.txt

awk '{print $1"."$2}' /tmp/source_views.txt | sort > /tmp/source_view_list.txt
awk '{print $1"."$2}' /tmp/target_views.txt | sort > /tmp/target_view_list.txt

echo "Views in SOURCE only:" >> $COMPARISON_FILE
comm -23 /tmp/source_view_list.txt /tmp/target_view_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Views in TARGET only:" >> $COMPARISON_FILE
comm -13 /tmp/source_view_list.txt /tmp/target_view_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_VIEW_COUNT=$(cat /tmp/source_view_list.txt | grep -v "^\.$" | wc -l)
TARGET_VIEW_COUNT=$(cat /tmp/target_view_list.txt | grep -v "^\.$" | wc -l)

#############################################
# 7. TRIGGERS COMPARISON
#############################################
echo "7. TRIGGERS COMPARISON:" >> $COMPARISON_FILE
echo "=======================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "8. TRIGGERS INVENTORY" "9. INDEXES SUMMARY" > /tmp/source_triggers.txt
extract_section "$TARGET_FILE" "8. TRIGGERS INVENTORY" "9. INDEXES SUMMARY" > /tmp/target_triggers.txt

awk '{print $1"."$2}' /tmp/source_triggers.txt | sort > /tmp/source_trigger_list.txt
awk '{print $1"."$2}' /tmp/target_triggers.txt | sort > /tmp/target_trigger_list.txt

echo "Triggers in SOURCE only:" >> $COMPARISON_FILE
comm -23 /tmp/source_trigger_list.txt /tmp/target_trigger_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Triggers in TARGET only:" >> $COMPARISON_FILE
comm -13 /tmp/source_trigger_list.txt /tmp/target_trigger_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_TRIGGER_COUNT=$(cat /tmp/source_trigger_list.txt | grep -v "^\.$" | wc -l)
TARGET_TRIGGER_COUNT=$(cat /tmp/target_trigger_list.txt | grep -v "^\.$" | wc -l)

#############################################
# 8. FOREIGN KEY COMPARISON
#############################################
echo "8. FOREIGN KEY COMPARISON:" >> $COMPARISON_FILE
echo "==========================" >> $COMPARISON_FILE

extract_section "$SOURCE_FILE" "10. FOREIGN KEY" "Report generated\|^$" > /tmp/source_fks.txt
extract_section "$TARGET_FILE" "10. FOREIGN KEY" "Report generated\|^$" > /tmp/target_fks.txt

awk '{print $1"."$2"."$3}' /tmp/source_fks.txt | sort > /tmp/source_fk_list.txt
awk '{print $1"."$2"."$3}' /tmp/target_fks.txt | sort > /tmp/target_fk_list.txt

echo "Foreign Keys in SOURCE only:" >> $COMPARISON_FILE
comm -23 /tmp/source_fk_list.txt /tmp/target_fk_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

echo "Foreign Keys in TARGET only:" >> $COMPARISON_FILE
comm -13 /tmp/source_fk_list.txt /tmp/target_fk_list.txt >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

SOURCE_FK_COUNT=$(cat /tmp/source_fk_list.txt | grep -v "^\.\.\.$" | wc -l)
TARGET_FK_COUNT=$(cat /tmp/target_fk_list.txt | grep -v "^\.\.\.$" | wc -l)

#############################################
# VALIDATION SUMMARY
#############################################
echo "==========================================" >> $COMPARISON_FILE
echo "VALIDATION SUMMARY:" >> $COMPARISON_FILE
echo "==========================================" >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

printf "%-25s %10s %10s %10s\n" "Object Type" "Source" "Target" "Status" >> $COMPARISON_FILE
printf "%-25s %10s %10s %10s\n" "-------------------------" "----------" "----------" "----------" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Databases (migrated)" "$SOURCE_DB_COUNT" "$MIGRATED_DB_COUNT" "$([ $MISSING_DB_COUNT -eq 0 ] && echo '✓' || echo '✗ MISSING')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Tables" "$SOURCE_TABLE_COUNT" "$TARGET_TABLE_COUNT" "$([ $SOURCE_TABLE_COUNT -eq $TARGET_TABLE_COUNT ] && echo '✓' || echo '✗')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "DB-related Users" "$SOURCE_DB_USER_COUNT" "$MATCHED_USER_COUNT" "$([ $MISSING_USER_COUNT -eq 0 ] && echo '✓' || echo '✗ MISSING')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Stored Procedures/Funcs" "$SOURCE_ROUTINE_COUNT" "$TARGET_ROUTINE_COUNT" "$([ $SOURCE_ROUTINE_COUNT -eq $TARGET_ROUTINE_COUNT ] && echo '✓' || echo '✗')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Views" "$SOURCE_VIEW_COUNT" "$TARGET_VIEW_COUNT" "$([ $SOURCE_VIEW_COUNT -eq $TARGET_VIEW_COUNT ] && echo '✓' || echo '✗')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Triggers" "$SOURCE_TRIGGER_COUNT" "$TARGET_TRIGGER_COUNT" "$([ $SOURCE_TRIGGER_COUNT -eq $TARGET_TRIGGER_COUNT ] && echo '✓' || echo '✗')" >> $COMPARISON_FILE
printf "%-25s %10d %10d %10s\n" "Foreign Keys" "$SOURCE_FK_COUNT" "$TARGET_FK_COUNT" "$([ $SOURCE_FK_COUNT -eq $TARGET_FK_COUNT ] && echo '✓' || echo '✗')" >> $COMPARISON_FILE
echo "" >> $COMPARISON_FILE

# Determine overall validation status
# Key criteria: All source DBs migrated, all DB-related users migrated, tables/routines/views/triggers/FKs match
if [ "$MISSING_DB_COUNT" -eq 0 ] && \
   [ "$MISSING_USER_COUNT" -eq 0 ] && \
   [ "$SOURCE_TABLE_COUNT" -eq "$TARGET_TABLE_COUNT" ] && \
   [ "$SOURCE_ROUTINE_COUNT" -eq "$TARGET_ROUTINE_COUNT" ] && \
   [ "$SOURCE_VIEW_COUNT" -eq "$TARGET_VIEW_COUNT" ] && \
   [ "$SOURCE_TRIGGER_COUNT" -eq "$TARGET_TRIGGER_COUNT" ] && \
   [ "$SOURCE_FK_COUNT" -eq "$TARGET_FK_COUNT" ]; then
    echo "✅ MIGRATION VALIDATION: PASSED" >> $COMPARISON_FILE
    
    # Check for row count differences
    if [ "$diff_found" = true ]; then
        echo "⚠️  WARNING: Some tables have row count differences (see section 3)" >> $COMPARISON_FILE
    fi
else
    echo "❌ MIGRATION VALIDATION: FAILED" >> $COMPARISON_FILE
    if [ "$MISSING_DB_COUNT" -gt 0 ]; then
        echo "   - $MISSING_DB_COUNT source database(s) missing on target" >> $COMPARISON_FILE
    fi
    if [ "$MISSING_USER_COUNT" -gt 0 ]; then
        echo "   - $MISSING_USER_COUNT DB-related user(s) missing on target" >> $COMPARISON_FILE
    fi
    VALIDATION_PASSED=false
fi

# Cleanup temp files
rm -f /tmp/source_*.txt /tmp/target_*.txt

echo "" >> $COMPARISON_FILE
echo "Detailed comparison report generated: $COMPARISON_FILE"
echo "Migration validation comparison completed."

# Display summary to console
echo ""
echo "=== QUICK SUMMARY ==="
tail -20 "$COMPARISON_FILE"