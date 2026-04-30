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

### Trigger Cutover

Connect to the control database and run:
```sql
UPDATE cutover_control
SET trigger_cutover_now = 1,
    trigger_cutover_now_at = NOW()
WHERE config_id = <CONFIG_ID>;
```

## Exit Codes

| Code | Meaning | Timer Behavior |
|------|---------|----------------|
| 0 | Success, binlog processed | Timer reschedules normally |
| 10 | Failover detected, handoff triggered | Timer reschedules normally |
| 20 | Cutover complete | Timer reschedules normally |
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
