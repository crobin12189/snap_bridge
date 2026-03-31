#!/bin/bash
set -e

if [ "$EUID" -ne 0 ]; then
    echo "Run with sudo: sudo ./setup.sh"
    exit 1
fi

REAL_USER="${SUDO_USER:-$(logname)}"
REAL_HOME=$(eval echo "~$REAL_USER")
USER_ID=$(id -u "$REAL_USER")

echo "========================================="
echo " Snapcast Server Setup"
echo " User: $REAL_USER"
echo " Home: $REAL_HOME"
echo "========================================="

echo ""
echo "[1/9] Updating package lists..."
apt update

echo ""
echo "[2/9] Installing packages..."
apt install -y \
    snapserver=0.26.0* \
    pipewire=1.2.7* pipewire-pulse=1.2.7* wireplumber \
    alsa-utils \
    python3 python3-pip python3-venv python3-serial

apt-mark hold snapserver pipewire pipewire-pulse

echo ""
echo "[3/9] Configuring boot config and USB gadget audio..."

CONFIG=/boot/firmware/config.txt

sed -i 's/^dtoverlay=vc4-kms-v3d/#dtoverlay=vc4-kms-v3d/' "$CONFIG"

if ! grep -q "dtoverlay=dwc2" "$CONFIG"; then
    cat >> "$CONFIG" << 'CFGEOF'

[all]
dtoverlay=dwc2,dr_mode=peripheral
enable_uart=1
dtoverlay=miniuart-bt
CFGEOF
else
    # Ensure each setting exists
    grep -q "enable_uart=1" "$CONFIG" || echo "enable_uart=1" >> "$CONFIG"
    grep -q "dtoverlay=miniuart-bt" "$CONFIG" || echo "dtoverlay=miniuart-bt" >> "$CONFIG"
fi

grep -q "^dwc2" /etc/modules || echo "dwc2" >> /etc/modules
grep -q "^g_audio" /etc/modules || echo "g_audio" >> /etc/modules

cat > /etc/modprobe.d/g_audio.conf << 'EOF'
options g_audio c_chmask=3 p_chmask=3 c_srate=96000 p_srate=96000 c_ssize=4 p_ssize=4
EOF

echo ""
echo "[4/9] Configuring PipeWire..."

PW_CONF_DIR="$REAL_HOME/.config/pipewire/pipewire.conf.d"
mkdir -p "$PW_CONF_DIR"

cat > "$PW_CONF_DIR/96khz.conf" << 'EOF'
context.properties = {
    default.clock.rate = 96000
    default.clock.allowed-rates = [ 96000 ]
    default.clock.quantum = 1024
    default.clock.min-quantum = 512
}
EOF

chown -R "$REAL_USER:$REAL_USER" "$REAL_HOME/.config"

echo ""
echo "[5/9] Configuring Snapserver..."

cat > /etc/snapserver.conf << 'EOF'
[server]

[http]

[tcp]

[stream]
bind_to_address = 0.0.0.0
port = 1704
source = pipe:///tmp/snapfifo?name=USB_Audio&dryout_ms=2000
sampleformat = 96000:32:2
codec = flac
chunk_ms = 40
buffer = 1500

[logging]
EOF

echo ""
echo "[6/9] Creating audio capture service..."

cat > /etc/systemd/system/snapcast-source.service << SVCEOF
[Unit]
Description=PipeWire audio source to Snapcast FIFO
After=pipewire.service snapserver.service
Wants=pipewire.service

[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        parec \\
            --device=alsa_input.platform-3f980000.usb.stereo-fallback \\
            --format=s32le \\
            --rate=96000 \\
            --channels=2 \\
            --latency-msec=10 \\
            --process-time-msec=5 \\
        > /tmp/snapfifo || true; \\
        sleep 0.5; \\
    done'
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
SVCEOF

echo ""
echo "[7/9] Setting up UART bridge..."

BRIDGE_DIR="$REAL_HOME/uart-bridge"
mkdir -p "$BRIDGE_DIR"

wget -O "$BRIDGE_DIR/snapcast_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/snapcast_bridge.py"

chown -R "$REAL_USER:$REAL_USER" "$BRIDGE_DIR"

sudo -u "$REAL_USER" python3 -m venv "$BRIDGE_DIR/venv"
sudo -u "$REAL_USER" "$BRIDGE_DIR/venv/bin/pip" install pyserial

cat > /etc/systemd/system/uart-bridge.service << SVCEOF
[Unit]
Description=Snapcast UART Bridge (ESP32)
After=snapserver.service
Wants=snapserver.service

[Service]
Type=simple
User=$REAL_USER
ExecStart=$BRIDGE_DIR/venv/bin/python3 $BRIDGE_DIR/snapcast_bridge.py --port /dev/ttyAMA0 --baud 460800
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SVCEOF

echo ""
echo "[8/9] Configuring sudoers..."

echo "$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart snapserver" \
    > /etc/sudoers.d/snapserver-restart
chmod 440 /etc/sudoers.d/snapserver-restart

usermod -aG dialout "$REAL_USER"

echo ""
echo "[9/9] Enabling services..."

systemctl daemon-reload
systemctl enable snapserver
systemctl enable snapcast-source
systemctl enable uart-bridge

echo ""
echo "========================================="
echo " Setup complete!"
echo ""
echo " Services installed:"
echo "   - snapserver (audio streaming)"
echo "   - snapcast-source (USB audio capture)"
echo "   - uart-bridge (ESP32 communication)"
echo ""
echo " UART bridge script location:"
echo "   $BRIDGE_DIR/snapcast_bridge.py"
echo ""
echo " Config files:"
echo "   /etc/snapserver.conf"
echo "   /etc/modprobe.d/g_audio.conf"
echo "   $PW_CONF_DIR/96khz.conf"
echo ""
echo " REBOOT NOW to apply all changes:"
echo "   sudo reboot"
echo "========================================="