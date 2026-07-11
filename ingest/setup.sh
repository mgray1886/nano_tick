#!/bin/bash
# Only run on a fresh flash

set -e

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="ingest.service"

# Service identity: whoever runs this script (survives sudo invocation).
RUN_USER="${SUDO_USER:-$USER}"
RUN_GROUP="$(id -gn "$RUN_USER")"

# Dont run setup script if it has already been run
if [ -f "$APP_DIR/.setup_complete" ]; then
    echo "Setup has already been completed. Remove .setup_complete to run again."
    exit 0
fi

sudo apt update
sudo apt install -y python3 python3-pip python3-venv

cd "$APP_DIR"

if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r "$APP_DIR"/requirements.txt

mkdir -p "$APP_DIR"/logs

# Lockdown env vars
if [ -f "$APP_DIR/.env" ]; then
    chmod 600 "$APP_DIR/.env"
fi

# Fill in user/group/paths so nothing is hardcoded to a particular flash username
sed -e "s|__USER__|$RUN_USER|g" \
    -e "s|__GROUP__|$RUN_GROUP|g" \
    -e "s|__APP_DIR__|$APP_DIR|g" \
    "$APP_DIR/$SERVICE_NAME" | sudo tee /etc/systemd/system/$SERVICE_NAME > /dev/null
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl start $SERVICE_NAME
sudo systemctl status $SERVICE_NAME --no-pager

# This box is headless: the GPU doesn't need its default 64MB reservation.
# Suggest (but don't silently apply) reclaiming it for the ingest process.
if [ -f /boot/config.txt ] && ! grep -q "^gpu_mem=" /boot/config.txt; then
    echo ""
    echo "TIP: reclaim ~48MB of RAM on this headless box by adding 'gpu_mem=16'"
    echo "     to /boot/config.txt and rebooting:"
    echo "         echo 'gpu_mem=16' | sudo tee -a /boot/config.txt"
fi

# Since this is a one time startup script, create a marker file to indicate
# that it has been run before
touch "$APP_DIR/.setup_complete"