# MariaDB Migration Systemd Service

Systemd service and timer units for running the binlog migration processor as a scheduled service on Linux servers.

## Files

| File | Description |
|------|-------------|
| `mariadb-migration.service` | Systemd service unit (oneshot) |
| `mariadb-migration.timer` | Systemd timer unit (schedules the service) |
| `install.sh` | Installation script |

## Quick Start

```bash
# On the source cluster node (as root)
cd /path/to/maria_migrator/systemd
chmod +x install.sh
./install.sh

# Edit configuration
vi /opt/mariadb-migration/migration.cfg

# Enable and start
systemctl enable mariadb-migration.timer
systemctl start mariadb-migration.timer
```

## Installation (Manual)

1. **Copy files to target server:**
   ```bash
   scp -r systemd/ root@mariadb-node1:/tmp/
   scp apply_incremental_binlogs_dbstate.sh root@mariadb-node1:/tmp/
   scp migration.cfg root@mariadb-node1:/tmp/
   ```

2. **On the server (as root):**
   ```bash
   # Create directories
   mkdir -p /opt/mariadb-migration/workdir

   # Copy scripts
   cp /tmp/apply_incremental_binlogs_dbstate.sh /opt/mariadb-migration/
   cp /tmp/migration.cfg /opt/mariadb-migration/
   chmod +x /opt/mariadb-migration/*.sh

   # Install systemd units
   cp /tmp/systemd/mariadb-migration.service /etc/systemd/system/
   cp /tmp/systemd/mariadb-migration.timer /etc/systemd/system/
   systemctl daemon-reload
   ```

3. **Configure:**
   ```bash
   vi /opt/mariadb-migration/migration.cfg
   ```

4. **Enable and start:**
   ```bash
   systemctl enable mariadb-migration.timer
   systemctl start mariadb-migration.timer
   ```

## Operations

### Check Status

```bash
# Timer status (shows next/last run times)
systemctl status mariadb-migration.timer

# Service status (shows last run result)
systemctl status mariadb-migration.service

# List all timers
systemctl list-timers --all | grep mariadb
```

### View Logs

```bash
# Follow logs in real-time
journalctl -u mariadb-migration -f

# View last 100 lines
journalctl -u mariadb-migration -n 100

# View logs from last hour
journalctl -u mariadb-migration --since "1 hour ago"

# View logs from specific time range
journalctl -u mariadb-migration --since "2026-04-30 10:00:00" --until "2026-04-30 12:00:00"

# View only errors
journalctl -u mariadb-migration -p err
```

### Manual Trigger

```bash
# Run immediately (independent of timer)
systemctl start mariadb-migration.service

# Watch the run
journalctl -u mariadb-migration -f
```

### Pause/Resume

```bash
# Pause scheduled runs (manual trigger still works)
systemctl stop mariadb-migration.timer

# Resume scheduled runs
systemctl start mariadb-migration.timer

# Disable across reboots
systemctl disable mariadb-migration.timer

# Re-enable
systemctl enable mariadb-migration.timer
```

### Change Schedule

Edit `/etc/systemd/system/mariadb-migration.timer`:

```ini
[Timer]
OnUnitActiveSec=30s    # Every 30 seconds
OnUnitActiveSec=1min   # Every 1 minute (default)
OnUnitActiveSec=5min   # Every 5 minutes
OnUnitActiveSec=1h     # Every hour
```

Then reload:
```bash
systemctl daemon-reload
systemctl restart mariadb-migration.timer
```

## Exit Codes

| Code | Meaning | Timer Behavior |
|------|---------|----------------|
| 0 | Success, binlog processed | Timer reschedules normally |
| 10 | Failover detected, handoff triggered | Timer reschedules normally |
| Other | Error | Timer reschedules normally (check logs) |

## Troubleshooting

### Service fails to start

```bash
# Check detailed error
systemctl status mariadb-migration.service -l
journalctl -u mariadb-migration -n 50

# Common issues:
# - Config file not found: Check /opt/mariadb-migration/migration.cfg exists
# - Permission denied: Ensure scripts are executable
# - MySQL connection failed: Check credentials in config
```

### Timer not running

```bash
# Check if timer is enabled
systemctl is-enabled mariadb-migration.timer

# Check if timer is active
systemctl is-active mariadb-migration.timer

# Check timer status
systemctl list-timers | grep mariadb
```

### Service runs too long

Edit `/etc/systemd/system/mariadb-migration.service`:
```ini
[Service]
TimeoutStartSec=1200  # Increase timeout to 20 minutes
```

Then reload:
```bash
systemctl daemon-reload
```

## Multi-Node Setup

Install on **both** nodes of the source cluster. The script automatically:
- Checks if this node is the current processing node
- Exits gracefully if another node should be processing
- Handles failover handoff between nodes

Each node should have the **same configuration file** pointing to:
- Same control database
- Same target database
- Both nodes listed in `SOURCE_CLUSTER_NODES`

---

## Monitoring & Automation for Operators

The control database provides stored procedures for monitoring migration health and cutover readiness. These can be used in automation scripts, monitoring dashboards, or manual checks.

### Connection Setup

All procedures require connecting to the migration control database:

```bash
# Interactive
mysql -h <DB_HOST> -P <DB_PORT> -u <DB_USER> -p<DB_PASSWORD> <DB_NAME>

# Non-interactive (for scripts)
mysql -h <DB_HOST> -P <DB_PORT> -u <DB_USER> -p<DB_PASSWORD> <DB_NAME> -N -B -e "CALL procedure_name(config_id);"
```

Replace `<DB_HOST>`, `<DB_PORT>`, `<DB_USER>`, `<DB_PASSWORD>`, `<DB_NAME>` with values from your `migration.cfg`.

---

### Check Migration Health: `check_migration_blocked`

**Purpose:** Quick boolean check to determine if a migration is healthy or blocked.

**When to use:** Automation scripts that need a simple pass/fail health check.

```sql
CALL check_migration_blocked(<CONFIG_ID>);
```

**Returns:**

| is_blocked | Meaning |
|------------|---------|
| `0` | Migration is healthy — processing normally |
| `1` | Migration is BLOCKED — requires manual intervention |

**Automation Example (bash):**

```bash
#!/bin/bash
CONFIG_ID=1
BLOCKED=$(mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p"$DB_PASSWORD" $DB_NAME \
    -N -B -e "CALL check_migration_blocked($CONFIG_ID);" 2>/dev/null)

if [[ "$BLOCKED" == "1" ]]; then
    echo "ALERT: Migration $CONFIG_ID is BLOCKED - manual intervention required"
    # Send alert, create ticket, etc.
    exit 1
else
    echo "OK: Migration $CONFIG_ID is healthy"
    exit 0
fi
```

---

### Check Migration Health (Detailed): `check_migration_blocked_extended`

**Purpose:** Get detailed information about a blocked migration, including recovery information.

**When to use:** After `check_migration_blocked` returns `1`, or when you need diagnostic details.

```sql
CALL check_migration_blocked_extended(<CONFIG_ID>);
```

**Returns:**

| Column | Description |
|--------|-------------|
| `is_blocked` | `TRUE` if blocked, `FALSE` if OK |
| `status` | Human-readable status: `OK` or `BLOCKED - MANUAL INTERVENTION REQUIRED` |
| `checkpoint_id` | ID of the last checkpoint |
| `source_binlog_file` | Binlog file being processed when blocked |
| `source_binlog_position` | Position in binlog when blocked |
| `apply_status` | Checkpoint status: `COMPLETED`, `STARTED`, `FAILED`, `INTERRUPTED` |
| `checkpoint_error` | Error message from failed checkpoint |
| `recovery_binlog` | Target binlog file for PITR recovery |
| `recovery_position` | Target position for PITR recovery |
| `recovery_gtid` | Target GTID for PITR recovery |
| `blocking_message` | Message from processing_log if blocked |
| `blocked_at` | Timestamp when blocking condition was logged |

**Blocked Conditions:**

1. **Incomplete checkpoint** — `apply_status` is `STARTED`, `FAILED`, or `INTERRUPTED`
2. **Blocking log entry** — A "Processing blocked" message exists in `processing_log`

**Recovery Actions:**

If blocked, you must either:

1. **Verify and acknowledge** (if target data is OK):
   ```sql
   -- Mark checkpoint as completed
   UPDATE binlog_apply_checkpoints
   SET apply_status = 'COMPLETED', 
       apply_completed_at = NOW(),
       error_message = 'Manually verified by operator'
   WHERE config_id = <CONFIG_ID> AND apply_status != 'COMPLETED';
   
   -- Acknowledge errors
   UPDATE migration_status
   SET error_acknowledged_at = NOW()
   WHERE config_id = <CONFIG_ID>;
   ```

2. **Perform PITR recovery** (if target data is corrupted):
   Use `recovery_binlog`, `recovery_position`, and `recovery_gtid` from the procedure output to restore the target database to the pre-apply state, then reset the migration.

---

### Check Cutover Readiness: `get_migration_sync_status`

**Purpose:** Determine if a migration is caught up and ready for cutover.

**When to use:** Before initiating cutover, or to monitor sync progress.

```sql
CALL get_migration_sync_status(<CONFIG_ID>);
```

**Returns:**

| Column | Description |
|--------|-------------|
| `migration_status` | Overall status (see table below) |
| `incomplete_checkpoints` | Count of non-COMPLETED checkpoints |
| `last_completed_binlog` | Last successfully applied binlog file |
| `last_completed_binlog_num` | Numeric suffix of last completed binlog |
| `active_binlog_file` | Current active binlog on source |
| `active_binlog_num` | Numeric suffix of active binlog |
| `active_binlog_reached` | `1` if script has reached active binlog, `0` if not |

**Status Values:**

| migration_status | Meaning | Cutover Ready? |
|------------------|---------|----------------|
| `CAUGHT_UP` | All binlogs applied, target is in sync | **YES** |
| `INCOMPLETE_CHECKPOINTS` | Some checkpoints failed/interrupted | NO - Fix first |
| `ACTIVE_BINLOG_NOT_REACHED` | Still processing historical binlogs | NO - Wait |
| `BINLOG_GAP_DETECTED` | Gap between completed and active binlog | NO - Investigate |
| `UNKNOWN` | Unable to determine status | NO - Investigate |


