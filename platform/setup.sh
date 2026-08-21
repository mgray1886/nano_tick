#!/bin/bash
# One-shot provisioning for the rpi4B: broker + recorder.
# Run from the platform/ directory

set -e

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$APP_DIR")"    # recorder.py in ../resources; app.py in platform/

# Service identity: whoever runs this script (survives sudo invocation).
RUN_USER="${SUDO_USER:-$USER}"
RUN_GROUP="$(id -gn "$RUN_USER")"

# Dont run setup script if it has already been run
if [ -f "$APP_DIR/.setup_complete" ]; then
    echo "Setup has already been completed. Remove .setup_complete to run again."
    exit 0
fi

sudo apt update
sudo apt install -y python3 python3-pip python3-venv mosquitto mosquitto-clients

# Broker: bind to the point-to-point link, persistence on
sudo cp "$APP_DIR/mosquitto-nano_tick.conf" /etc/mosquitto/conf.d/nano_tick.conf
sudo systemctl enable mosquitto
sudo systemctl restart mosquitto

cd "$APP_DIR"

if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r "$APP_DIR"/requirements.txt

# --- systemd units ---------------------------------------------------------
# Fill user/group/paths (nothing hardcoded to a particular flash username) + install.
install_unit() {
    sed -e "s|__USER__|$RUN_USER|g" \
        -e "s|__GROUP__|$RUN_GROUP|g" \
        -e "s|__APP_DIR__|$APP_DIR|g" \
        -e "s|__REPO_DIR__|$REPO_DIR|g" \
        "$APP_DIR/$1" | sudo tee "/etc/systemd/system/$1" > /dev/null
}

install_unit recorder.service
install_unit writer.service
sudo systemctl daemon-reload

# Recorder (NDJSON fallback) needs only paho — start it now.
sudo systemctl enable recorder.service
sudo systemctl restart recorder.service

# Writer (KDB-X, app.py) needs pykx's licence; enable it, but only start once the
# licence is in place (see platform/KDBX_SETUP.md).
USER_HOME="$(getent passwd "$RUN_USER" | cut -d: -f6)"
sudo systemctl enable writer.service
if [ -f "$USER_HOME/.kx/kc.lic" ]; then
    sudo systemctl restart writer.service
else
    echo "writer.service enabled but NOT started: no KDB-X licence at $USER_HOME/.kx/kc.lic."
    echo "Install KDB-X (platform/KDBX_SETUP.md), then: sudo systemctl start writer.service"
fi

sudo systemctl status recorder.service --no-pager

# Since this is a one time startup script, create a marker file to indicate
# that it has been run before
touch "$APP_DIR/.setup_complete"
