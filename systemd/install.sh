#!/bin/bash
############################################
# MariaDB Migration Service Installation Script
# Run as root on each source cluster node
############################################

set -euo pipefail

INSTALL_DIR="/opt/mariadb-migration"
SYSTEMD_DIR="/etc/systemd/system"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== MariaDB Migration Service Installer ==="
echo ""

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    echo "ERROR: This script must be run as root"
    exit 1
fi

# Create mariadb-binlog symlink (required for podman-based MariaDB installations)
if [[ -f /usr/bin/podman_client && ! -e /usr/bin/mariadb-binlog ]]; then
    echo "Creating symbolic link: /usr/bin/mariadb-binlog -> /usr/bin/podman_client"
    ln -s /usr/bin/podman_client /usr/bin/mariadb-binlog
elif [[ -L /usr/bin/mariadb-binlog ]]; then
    echo "Symbolic link /usr/bin/mariadb-binlog already exists"
elif [[ -f /usr/bin/mariadb-binlog ]]; then
    echo "mariadb-binlog binary already exists at /usr/bin/mariadb-binlog"
else
    echo "WARNING: /usr/bin/podman_client not found - mariadb-binlog symlink not created"
fi

# Create installation directory
echo "Creating installation directory: $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
mkdir -p "$INSTALL_DIR/workdir"

# Copy scripts
echo "Copying migration scripts..."
cp "$SCRIPT_DIR/../apply_incremental_binlogs_dbstate.sh" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/../mariadb_export_databases.sh" "$INSTALL_DIR/" 2>/dev/null || true
cp "$SCRIPT_DIR/../mariadb_extract_users_grants.sh" "$INSTALL_DIR/" 2>/dev/null || true
cp "$SCRIPT_DIR/../mariadb_import_dump.sh" "$INSTALL_DIR/" 2>/dev/null || true
cp "$SCRIPT_DIR/../mariadb_load_users_grants.sh" "$INSTALL_DIR/" 2>/dev/null || true

# Make scripts executable
chmod +x "$INSTALL_DIR"/*.sh

# Copy configuration template if it doesn't exist
if [[ ! -f "$INSTALL_DIR/migration.cfg" ]]; then
    echo "Creating configuration template: $INSTALL_DIR/migration.cfg"
    if [[ -f "$SCRIPT_DIR/../migration.cfg" ]]; then
        cp "$SCRIPT_DIR/../migration.cfg" "$INSTALL_DIR/migration.cfg"
    else
        cat > "$INSTALL_DIR/migration.cfg" << 'EOFCFG'
#!/usr/bin/env bash
# MariaDB Migration Configuration
# Edit this file with your environment-specific values

############################################
# SOURCE CLUSTER CONFIGURATION
############################################

PORT=3306
BINLOG_DIR="/var/lib/mysql"
WORKDIR="/opt/mariadb-migration/workdir"
LOG_FILE="/opt/mariadb-migration/workdir/migration.log"

SOURCE_CLUSTER_NODES=(
    "mariadb-node1"
    "mariadb-node2"
)

SOURCE_CLUSTER_IPS=(
    ""
    ""
)

############################################
# TARGET INSTANCE CONFIGURATION
############################################

TARGET_HOST="target-mariadb-host"
TARGET_PORT=3306
TARGET_USER="migration_user"
TARGET_PASS="CHANGE_ME"

############################################
# CONTROL DATABASE CONFIGURATION
############################################

DB_HOST="control-db-host"
DB_PORT=3306
DB_USER="migration_control"
DB_PASSWORD="CHANGE_ME"
DB_NAME="mariaDBaaS_migcontrol"
DB_SSL="--ssl=true"

############################################
# MIGRATION IDENTITY
############################################

MIGRATION_NAME="my-migration"
SOURCE_INSTANCE_NAME="source-cluster"
SOURCE_VIP_PORT=3307
SOURCE_CLUSTER_ID=1
TARGET_INSTANCE_NAME="target-instance"

############################################
# INITIAL BINLOG POSITION (first run only)
############################################

INITIAL_BINLOG="binlogs_mariadb-node1.000001"
INITIAL_POS=4

############################################
# TOOL PATHS
############################################

MYSQL_BINLOG="/usr/bin/mariadb-binlog"
MYSQL="/usr/bin/mysql"
EOFCFG
    fi
    echo ""
    echo "WARNING: Edit $INSTALL_DIR/migration.cfg with your values before starting the service!"
fi

# Install systemd units
echo "Installing systemd service and timer..."
cp "$SCRIPT_DIR/mariadb-migration.service" "$SYSTEMD_DIR/"
cp "$SCRIPT_DIR/mariadb-migration.timer" "$SYSTEMD_DIR/"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

echo ""
echo "=== Installation Complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit configuration:    vi $INSTALL_DIR/migration.cfg - MAKE SURE WORKDIR AND LOG_FILE PATHS ARE CORRECT AND WRITABLE"
echo "  2. Enable the timer:      systemctl enable mariadb-migration.timer"
echo "  3. Start the timer:       systemctl start mariadb-migration.timer"
echo "  4. Check status:          systemctl status mariadb-migration.timer"
echo "  5. View logs:             journalctl -u mariadb-migration -f"
echo ""
echo "Manual trigger:             systemctl start mariadb-migration.service"
echo "Pause scheduled runs:       systemctl stop mariadb-migration.timer"
echo ""
