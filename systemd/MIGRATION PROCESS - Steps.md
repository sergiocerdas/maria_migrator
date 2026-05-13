/**************************************************/

MIGRATION PROCESS WITH CUTOVER DATE.

/**************************************************/

Pre-requisites
	All migration scripts are required to be in the server from which you'll be attempting the migration.  This machine should have access to the source cluster (dbaas 1) and to the target instance (dbaas 2). 
	The system from which you run this must have mariadb client installed.

	Determine your working directory and place there the migration scripts
		- mariadb_extract_users_grants.sh
		- mariadb_export_databases.sh
		- mariadb_import_dump.sh
		- mariadb_load_users_grants.sh
		- apply_incremental_binlogs_failover_aware.sh   (THIS HAS CHANGED AS PER NEW PROCESS - DB CONTROL AND RUN AS A SERVICE - THERE'S A NEW FILE)

	The files are included in the following git repo:
		https://github.com/sergiocerdas/maria_migrator


	Update on each of the machines that participate in the source instance cluster the apply_incremental_binlogs_failover_aware.sh with the PORT, TARGET_HOST, TARGET_USER, TARGET_PORT and TARGET_PASSWORD.  
	Confirm the bin logs directory is correct.
	Make sure the path stated in WORKDIR is empty.
	The user details must be from SUPER user

		This is the section for the update:

		############################################
		# CONFIGURATION
		############################################
		PORT=3902
		BINLOG_DIR="/instances/mysql_db$PORT/binlogs"
		WORKDIR="/mysql/files/scripts/dbaasnew_scripts/binlog_apply"

		STATE_FILE="$WORKDIR/state.env"
		LOG_FILE="$WORKDIR/run.log"
		FAILOVER_FILE="$WORKDIR/failover_detected.txt"


		TARGET_HOST="10-11-227-148.dbaas.intel.com"
		TARGET_PORT=3306
		TARGET_USER="jbyfyzh5cv1iy5rswzfk_dbaas"
		TARGET_PASS="H56iRsIJ9!jWV0cPay4sWZpAP-,CoXvw"

		MYSQL_BINLOG="/usr/bin/mariadb-binlog"
		MYSQL="/usr/bin/mariadb"

		############################################




Step 1.

	Make sure you're on the primary node for the instance being migrated.
		root@d1or1mar043:/mysql/files/scripts/dbaasnew_scripts# /mysql/files/scripts/mysql_replication_check_no_email.sh
		MySQLdb ['3902'] Secondary OK!
		MySQLdb ['3903'] Secondary OK!
		MySQLdb ['3904'] Secondary OK!

	Clear out files prior to start migration and Update Script to target instance
		There might be some files from a previous migration.
		In the path where you have your scripts, you need to update the 


	Create backup image using mariadb_export_databases.sh
		-x parameter allows for exception list of databases, if not needed, simply don't include -x 
	


		root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# time ./mariadb_export_databases.sh -o test2DMP.dmp -P 3000 -u mysql01 -x test_viggu,viggu_test,coverage_dmr_mio_db,sample_test_db

			or

		root@d1or1mar043:/mysql/files/scripts/dbaasnew_scripts# ./mariadb_export_databases.sh -o testSergio.dmp -P 3902 -u mysql01
		
		[INFO] Exporting all databases to test2DMP.dmp
		[INFO] Excluding databases: mysql information_schema performance_schema sys admindb test_viggu viggu_test coverage_dmr_mio_db sample_test_db
		[INFO] Database export completed successfully to test2DMP.dmp
		
		
Step 2.
	Generate User and grants script using mariadb_extract_users_grants.sh
		
		root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# time ./mariadb_extract_users_grants.sh -o test2DMP -P 3000 -u mysql01 
		[INFO] Generating SHOW GRANTS statements for users matching (rw|ro|so)$ to test2DMP_show_grants.sql
		[INFO] Excluding users from databases: test_viggu viggu_test coverage_dmr_mio_db sample_test_db
		[INFO] Executing SHOW GRANTS statements and saving output to test2DMP_grants_selected.sql
		[INFO] Processing grants file to extract CREATE USER and GRANT statements
		[INFO] Processed grants file saved to test2DMP_grants_processed.sql
		[INFO] Users and grants extraction completed successfully
		
		Generated files:
		  - test2DMP_show_grants.sql (SHOW GRANTS statements)
		  - test2DMP_grants_selected.sql (raw grants output)
		  - test2DMP_grants_processed.sql (CREATE USER + GRANT statements - ready to import)
		
		
Step 3
	Capture BIN LOG POSITION
		root@d1or1mar043:/instances/mysql_db3902/scriptsMig# head -n 100 testSergio.dmp | grep "CHANGE MASTER TO MASTER_LOG_FILE="
			-- CHANGE MASTER TO MASTER_LOG_FILE='binlogs_d1or1mar043.000234', MASTER_LOG_POS=856;

		
	Current bin logs;
		L1 (primary)
		root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# /mysql/files/scripts/mysql_replication_check_no_email.sh
		MySQLdb ['3000'] Primary OK!
		
		root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# ls -ltrh /instances/mysql_db3000/binlogs/
		total 6.9G
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:53 binlogs_l1or1mar001.000028
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:55 binlogs_l1or1mar001.000029
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:56 binlogs_l1or1mar001.000030
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:58 binlogs_l1or1mar001.000031
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:59 binlogs_l1or1mar001.000032
		-rw-rw---- 1 mysql3000 mysql  413 Feb  2 05:01 binlogs_l1or1mar001.index
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 05:01 binlogs_l1or1mar001.000033
		-rw-rw---- 1 mysql3000 mysql 821M Feb  2 19:50 binlogs_l1or1mar001.000034
		
		L2 (secondary)
		root@l2or1mar001:~# ls -ltrh /instances/mysql_db3000/binlogs/
		total 6.9G
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:54 binlogs_l2or1mar001.000028
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:55 binlogs_l2or1mar001.000029
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:57 binlogs_l2or1mar001.000030
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 04:59 binlogs_l2or1mar001.000031
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 05:01 binlogs_l2or1mar001.000032
		-rw-rw---- 1 mysql3000 mysql  413 Feb  2 05:02 binlogs_l2or1mar001.index
		-rw-rw---- 1 mysql3000 mysql 1.1G Feb  2 05:02 binlogs_l2or1mar001.000033
		-rw-rw---- 1 mysql3000 mysql 821M Feb  2 19:50 binlogs_l2or1mar001.000034
						
		
Step 4
	Import backup image into target instance using mariadb_import_dump.sh script.

	First remove any reference of DEFINER on dmp file.

	root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# sed -i 's/DEFINER=[^ ]* / /g' test2DMP.dmp
	
	root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# time ./mariadb_import_dump.sh -f test2DMP.dmp -h 10-11-227-148.dbaas.intel.com -u jbyfyzh5cv1iy5rswzfk_admin -p 'AbXRHu7iDzG8r37m;PGfkW,9R7p6gLhw' -P 3306
	[INFO] Importing dump file: test2DMP.dmp
	[INFO] Target host: 10-11-227-148.dbaas.intel.com:3306
	[INFO] Target user: jbyfyzh5cv1iy5rswzfk_admin
	[INFO] SSL enabled: true
	[INFO] Database import completed successfully to 10-11-227-148.dbaas.intel.com:3306
	
	
Step 5
	Disable password check.  Needed to allow initial load up of legacy users that don't comply with current password complexity policy
	Must connect using SUPER user

	root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# mariadb --host='10-11-227-148.dbaas.intel.com' --user=jbyfyzh5cv1iy5rswzfk_dbaas --password='H56iRsIJ9!jWV0cPay4sWZpAP-,CoXvw' --port=3306 --ssl=true
	
	MariaDB [(none)]> uninstall plugin simple_password_check;
	
	
	MariaDB [(none)]> exit
	
	
		

Step 6
	Load users and grants using mariadb_load_users_grants.sh
		File:  the one with processed in the naming.
		Make sure you use SUPER user.

	
	root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# ./mariadb_load_users_grants.sh -f test2DMP_grants_processed.sql -h 10-11-227-148.dbaas.intel.com -u jbyfyzh5cv1iy5rswzfk_admin -p 'AbXRHu7iDzG8r37m;PGfkW,9R7p6gLhw' -P 3306
	[INFO] Loading users and grants from: test2DMP_grants_processed.sql
	[INFO] Target host: 10-11-227-148.dbaas.intel.com:3306
	[INFO] Target user: jbyfyzh5cv1iy5rswzfk_admin
	[INFO] SSL enabled: true

			
Step 7
	Enable password check
	
	root@l1or1mar001:/mysql/files/scripts/dbaasnew_scripts# mariadb --host='10-11-227-148.dbaas.intel.com' --user=jbyfyzh5cv1iy5rswzfk_dbaas --password='H56iRsIJ9!jWV0cPay4sWZpAP-,CoXvw' --port=3306 --ssl=true	
	
	MariaDB [(none)]>  install plugin simple_password_check SONAME 'simple_password_check.so';
	Query OK, 0 rows affected (0.002 sec)
	
	MariaDB [(none)]> exit
	Bye







/**************************************************/

BINLOG APPLY - PROCESS BETWEEN INITIAL MIGRATION AND CUTOVER DATE.

/**************************************************/


# Script Documentation: apply_incremental_binlogs_failover_aware.sh

This document provides detailed documentation for each section of the `apply_incremental_binlogs_failover_aware.sh` script, which handles incremental binlog replication with failover awareness for MariaDB/MySQL clusters.

---

How to execute the binlog apply script:

	RUN INITIAL ITERATION OF BINLOG APPLY

	You MUST make sure that there are no pending files from another migration on the servers of the cluster.
		the working path must be clean to make sure the migration starts as a new one

	The intial run must be executed from the primary server after the backup was taken
	You need the binlog file and position captured in Step #3
	You must provide the list of nodes of the cluster, always with LOCAL server first 


	./apply_incremental_binlogs_failover_aware.sh d1or1mar043 d2or1mar043 'binlogs_l1or1mar001.001808' 645516


	This process must be executed from the server that was primary when the migration was started (backup image taken)

	/*********************************/
	

	THE PROCESS SHOULD USE A DATABASE ON THE TARGET INSTANCE TO KEEP TRACK OF THE CURRENT SERVER FROM THE SOURCE INSTANCE WHERE IT SHOULD BE PROCESSING BIN LOG ACTIVITY.


	

	/*********************************/

	ADD REPETITIVE BINLOG APPLY EXECUTION
	crontab -e
	*/5 * * * * /mysql/files/scripts/dbaasnew_scripts/apply_incremental_binlogs_failover_aware.sh d1or1mar043 d2or1mar043


## Section 1: PARAMETERS - CLUSTER SERVERS

### Input Required
| Parameter | Type | Description |
|-----------|------|-------------|
| `$1` | String (required) | Primary node name (CLUSTER_NODE_1) |
| `$2` | String (required) | Secondary node name (CLUSTER_NODE_2) |
| Additional args | Positional args | Optional parameters passed through after shift |

### Output Generated
- `CLUSTER_NODE_1` - First cluster node hostname
- `CLUSTER_NODE_2` - Second cluster node hostname
- `CLUSTER_NODES` - Array containing both node names

### Action Executed
Validates minimum argument count and assigns cluster node names from positional parameters. Shifts the argument list by 2 to allow additional parameters.

### Exceptions
- **Exit 1**: If fewer than 2 arguments are provided, displays usage message and exits.

---

## Section 2: VALIDATION

### Input Required
- `CLUSTER_NODE_1` - First cluster node name
- `CLUSTER_NODE_2` - Second cluster node name

### Output Generated
None (validation only)

### Action Executed
Compares both cluster node names to ensure they are different.

### Exceptions
- **Exit 1**: If both node names are identical, logs error and exits.

---

## Section 3: CONFIGURATION

### Input Required
None (hardcoded values)

### Output Generated
| Variable | Value | Description |
|----------|-------|-------------|
| `PORT` | 3902 | MySQL/MariaDB port number |
| `BINLOG_DIR` | `/instances/mysql_db$PORT/binlogs` | Directory containing binlog files |
| `WORKDIR` | `/instances/mysql_db$PORT/scriptsMig/binlog_apply` | Working directory for script |
| `STATE_FILE` | `$WORKDIR/state.env` | File tracking replication state |
| `LOG_FILE` | `$WORKDIR/run.log` | Script execution log |
| `FAILOVER_FILE` | `$WORKDIR/failover_detected.txt` | Failover detection marker file |
| `TARGET_HOST` | Target DBaaS hostname | Destination server |
| `TARGET_PORT` | 3306 | Destination port |
| `TARGET_USER` | DBaaS username | Authentication user |
| `TARGET_PASS` | DBaaS password | Authentication password |
| `MYSQL_BINLOG` | `/usr/bin/mariadb-binlog` | Path to binlog utility |
| `MYSQL` | `/usr/bin/mariadb` | Path to MySQL client |

### Action Executed
Initializes all configuration variables required for script operation.

### Exceptions
None

---

## Section 4: LOGGING HELPERS

### Input Required
- `WORKDIR` - Working directory path

### Output Generated
- Creates `WORKDIR` directory if it doesn't exist
- Defines `log()` function for timestamped logging
- Defines `section()` function for section headers

### Action Executed
1. Creates the working directory (`mkdir -p`)
2. Defines helper functions for consistent logging output

### Exceptions
None (mkdir -p will not fail if directory exists)

---

## Section 5: SCRIPT START

### Input Required
- `WORKDIR`, `STATE_FILE`, `LOG_FILE` - Configuration variables

### Output Generated
Log entries showing:
- Process ID (PID)
- Working directory
- State file path
- Log file path

### Action Executed
Logs initialization information to indicate script startup.

### Exceptions
None

---

## Section 6: CLUSTER CONFIGURATION (LOG CLUSTER CONFIG)

### Input Required
- `CLUSTER_NODES` array
- Local hostname

### Output Generated
Log entries showing:
- Node 1 hostname
- Node 2 hostname
- Local hostname

### Action Executed
Logs the cluster topology configuration for debugging purposes.

### Exceptions
None

---

## Section 7: DETERMINE LOCAL SERVER ID

### Input Required
| Variable | Description |
|----------|-------------|
| `HOSTNAME` | System hostname |
| `PORT` | MySQL port number |
| `CLUSTER_NODE_2` | Secondary cluster node |

### Output Generated
| Variable | Description |
|----------|-------------|
| `HOSTNAME_SHORT` | Short hostname |
| `LOCAL_SERVER_ID` | Server ID from MySQL (1 or 2) |
| `OTHER_SERVER_ID` | Server ID of the other cluster node |

### Action Executed
1. Retrieves short hostname using `hostname -s`
2. Queries local MySQL for `@@server_id`
3. Queries secondary node for its `@@server_id`
4. Logs the determined values

### Exceptions
- **Exit 1**: If `LOCAL_SERVER_ID` is not 1 or 2

---

## Section 8: FAILOVER RESUME CHECK

### Input Required
| Variable | Description |
|----------|-------------|
| `FAILOVER_FILE` | Path to failover detection file |
| `BINLOG_DIR` | Directory containing binlog files |

### Output Generated
| Variable | Description |
|----------|-------------|
| `FAILOVER_GTID` | GTID extracted from failover file |
| `FOUND_LINE` | Line containing the GTID in binlogs |
| `CURRENT_BINLOG` | Binlog file to resume from |
| `CURRENT_POS` | Position within binlog to resume from |
| Updated `STATE_FILE` | New state reflecting failover resume point |

### Action Executed
1. Checks if failover file exists
2. Extracts `FAILOVER_GTID` from the file using regex
3. Searches all local binlog files for the GTID
4. Extracts binlog filename and position from the found line
5. Updates state file with new resume point
6. Removes the failover file after processing

### Exceptions
- **Exit 1**: If `FAILOVER_GTID` cannot be extracted from failover file
- **Exit 1**: If GTID is not found in any local binlog
- **Exit 1**: If binlog name or position extraction fails

---

## Section 9: STATE INITIALIZATION

### Input Required
| Variable | Description |
|----------|-------------|
| `STATE_FILE` | Path to state persistence file |
| `$1`, `$2` | Binlog name and position (for first run only) |

### Output Generated
| Variable | Description |
|----------|-------------|
| `CURRENT_BINLOG` | Current binlog file to process |
| `CURRENT_POS` | Current position in binlog |
| `BINLOG_PATH` | Full path to binlog file |
| `SQL_FILE` | Path for extracted SQL statements |
| `ERR_FILE` | Path for error output |

### Action Executed
**First Run (no state file):**
1. Requires 2 positional arguments (binlog name and start position)
2. Initializes `CURRENT_BINLOG` and `CURRENT_POS` from arguments

**Subsequent Runs (state file exists):**
1. Sources the state file
2. Loads `LAST_BINLOG` and `LAST_POS` values
3. Sets `CURRENT_BINLOG` and `CURRENT_POS` from loaded state

### Exceptions
- **Exit 1**: On first run, if binlog name and position are not provided

---

## Section 10: BINLOG SANITY CHECK

### Input Required
- `BINLOG_PATH` - Full path to the binlog file

### Output Generated
None (validation only)

### Action Executed
Checks if the binlog file exists on the filesystem.

### Exceptions
- **Exit 0**: If binlog file does not exist (graceful exit, nothing to do)

---

## Section 11: BINLOG EXTRACTION

### Input Required
| Variable | Description |
|----------|-------------|
| `MYSQL_BINLOG` | Path to mariadb-binlog utility |
| `CURRENT_POS` | Starting position in binlog |
| `BINLOG_PATH` | Full path to binlog file |
| `SQL_FILE` | Output file for SQL statements |
| `ERR_FILE` | Output file for errors |

### Output Generated
- `SQL_FILE` - Contains extracted SQL statements from binlog
- `ERR_FILE` - Contains any errors from extraction

### Action Executed
Executes `mariadb-binlog` with `--start-position` to extract SQL statements from the binlog file starting at the specified position.

### Exceptions
Script will fail if `mariadb-binlog` command fails (due to `set -e`)

---

## Section 12: BINLOG IN-USE CHECK

### Input Required
- `SQL_FILE` - Extracted SQL file to check

### Output Generated
None (or cleanup of temporary files)

### Action Executed
1. Searches for "not closed properly" warning in extracted SQL
2. If found, indicates binlog is still being written to
3. Removes temporary SQL and ERR files
4. Exits gracefully to retry in next iteration

### Exceptions
- **Exit 0**: If binlog is still in use (graceful exit, safe to retry)

---

## Section 13: FAILOVER DETECTION

### Input Required
| Variable | Description |
|----------|-------------|
| `OTHER_SERVER_ID` | Server ID of the other cluster node |
| `SQL_FILE` | Extracted SQL statements |
| `CURRENT_BINLOG` | Current binlog filename |
| `CLUSTER_NODES` | Array of cluster node hostnames |

### Output Generated
| Output | Description |
|--------|-------------|
| `FAILOVER_FILE` | Written with failover details including GTID, position, server ID, and timestamp |
| `FAILOVER_POS` | Position where failover was detected |
| `FAILOVER_GTID` | GTID of the failover transaction |
| `FAILOVER_SERVER_ID` | New primary's server ID |
| `FAILOVER_TIME` | Timestamp of detection |

### Action Executed
1. Searches SQL file for transactions from other server ID
2. If found, extracts position and GTID information
3. Writes comprehensive failover detection file
4. Logs all failover details

### Exceptions
None in detection phase (continues to handoff)

---

## Section 14: FAILOVER HANDOFF

### Input Required
| Variable | Description |
|----------|-------------|
| `FAILOVER_SERVER_ID` | New primary's server ID |
| `CLUSTER_NODES` | Array of cluster hostnames |
| `FAILOVER_FILE` | File to transfer |

### Output Generated
- Failover file copied to new primary node via SCP
- Local failover file deleted after successful transfer

### Action Executed
1. Calculates array index from server ID
2. Resolves new primary hostname from `CLUSTER_NODES` array
3. Copies failover file to new primary using `scp`
4. Removes local failover file on success

### Exceptions
- **Exit 10**: After failover handoff (expected exit code for failover)
- Logs error if new primary host cannot be resolved
- Logs error if SCP transfer fails (but continues)

---

## Section 15: APPLY BINLOG TO TARGET

### Input Required
| Variable | Description |
|----------|-------------|
| `SQL_FILE` | Extracted SQL statements |
| `TARGET_HOST` | Target database host |
| `TARGET_USER` | Target database username |
| `TARGET_PASS` | Target database password |
| `TARGET_PORT` | Target database port |
| `MYSQL` | Path to MySQL client |

### Output Generated
- SQL statements applied to target database
- Log entries confirming application

### Action Executed
1. Checks if SQL file has content (`-s` test)
2. Constructs MySQL command with connection parameters
3. Pipes SQL file contents to MySQL client
4. Logs completion status

### Exceptions
- If file is empty, logs "no transactions to apply" and skips
- Script fails if MySQL application fails (due to `set -e`)

---

## Section 16: STATE ADVANCEMENT

### Input Required
| Variable | Description |
|----------|-------------|
| `CURRENT_BINLOG` | Current binlog filename |
| `STATE_FILE` | State persistence file |

### Output Generated
- Updated `STATE_FILE` with next binlog name and position 4 (header skip)

### Action Executed
1. Calculates next binlog filename by incrementing the numeric suffix
2. Writes new state file with `LAST_BINLOG` and `LAST_POS=4`
3. Logs the updated state

### Exceptions
None

---

## Section 17: CLEANUP

### Input Required
- `SQL_FILE` - Temporary SQL file
- `ERR_FILE` - Temporary error file

### Output Generated
None (files removed)

### Action Executed
Removes temporary SQL and error files created during extraction.

### Exceptions
None (`rm -f` will not fail if files don't exist)

---

## Section 18: SCRIPT END

### Input Required
None

### Output Generated
Log entry confirming successful completion

### Action Executed
Logs script completion message.

### Exceptions
None

---

## Exit Codes Summary

| Exit Code | Meaning |
|-----------|---------|
| **0** | Success or graceful skip (binlog in use, binlog doesn't exist) |
| **1** | Error (validation failure, missing arguments, GTID not found) |
| **10** | Failover detected and handoff completed |

---

## File Dependencies

| File | Purpose |
|------|---------|
| `state.env` | Persists replication progress between runs |
| `failover_detected.txt` | Signals failover event between cluster nodes |
| `*.sql` | Temporary file for extracted SQL statements |
| `*.err` | Temporary file for extraction errors |
| `run.log` | Cumulative execution log |

---

## Flow Diagram

```
START
  │
  ├─► Parse cluster parameters
  │
  ├─► Validate node names
  │
  ├─► Load configuration
  │
  ├─► Determine server IDs
  │
  ├─► Check for failover resume file ──► If exists: locate GTID, update state
  │
  ├─► Initialize state (first run or resume)
  │
  ├─► Check binlog exists ──► If not: exit 0
  │
  ├─► Extract binlog to SQL
  │
  ├─► Check if binlog in use ──► If yes: exit 0
  │
  ├─► Check for failover ──► If detected: handoff and exit 10
  │
  ├─► Apply SQL to target
  │
  ├─► Advance state to next binlog
  │
  ├─► Cleanup temporary files
  │
  └─► END (exit 0)
```
