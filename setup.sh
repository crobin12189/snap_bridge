#!/bin/bash
set -e

# ── Must run as root ──
if [ "$EUID" -ne 0 ]; then
    echo "Run with sudo: sudo ./setup.sh"
    exit 1
fi

# ── Detect the real user (not root) ──
REAL_USER="${SUDO_USER:-$(logname)}"
REAL_HOME=$(eval echo "~$REAL_USER")
USER_ID=$(id -u "$REAL_USER")

echo "========================================="
echo " Snapcast Server Setup"
echo " User: $REAL_USER"
echo " Home: $REAL_HOME"
echo "========================================="

# ── 1. Update package lists only (no upgrade) ──
echo ""
echo "[1/9] Updating package lists..."
apt update

# ── 2. Install dependencies (pinned versions) ──
echo ""
echo "[2/9] Installing packages..."
apt install -y \
    snapserver=0.26.0* \
    pipewire=1.2.7* pipewire-pulse=1.2.7* wireplumber \
    pulseaudio-utils=16.1+dfsg1-2+rpt1.1 \
    alsa-utils \
    python3 python3-pip python3-venv python3-serial

# Prevent snapserver and pipewire from being auto-upgraded
apt-mark hold snapserver pipewire pipewire-pulse

# ── 3. Configure boot config and kernel modules ──
echo ""
echo "[3/9] Configuring boot config and USB gadget audio..."

CONFIG=/boot/firmware/config.txt

# Disable vc4-kms-v3d (comment it out if uncommented)
sed -i 's/^dtoverlay=vc4-kms-v3d/#dtoverlay=vc4-kms-v3d/' "$CONFIG"

# Remove any existing [all] section and everything after it, we'll rewrite it
# This avoids conflicts with dwc2 in other sections like [cm5]
if grep -q "^\[all\]" "$CONFIG"; then
    sed -i '/^\[all\]/,$d' "$CONFIG"
fi

# Append clean [all] section
cat >> "$CONFIG" << 'CFGEOF'
[all]
dtoverlay=dwc2,dr_mode=peripheral
enable_uart=1
dtoverlay=miniuart-bt
CFGEOF

# Load dwc2 and g_audio modules on boot
grep -q "^dwc2" /etc/modules || echo "dwc2" >> /etc/modules
grep -q "^g_audio" /etc/modules || echo "g_audio" >> /etc/modules

# g_audio config: 96kHz 32bit stereo
cat > /etc/modprobe.d/g_audio.conf << 'EOF'
options g_audio c_chmask=3 p_chmask=3 c_srate=96000 p_srate=96000 c_ssize=4 p_ssize=4
EOF

# ── 4. Configure PipeWire for 96kHz native ──
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

# ── 5. Configure Snapserver ──
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

# Create the FIFO now and make it persistent across reboots
test -p /tmp/snapfifo || mkfifo /tmp/snapfifo
chmod 666 /tmp/snapfifo

# Ensure FIFO is recreated on every boot by systemd-tmpfiles
cat > /etc/tmpfiles.d/snapfifo.conf << 'EOF'
p /tmp/snapfifo 0666 root root -
EOF

# ── 5b. Free up UART — remove serial console ──
echo ""
echo "[5b/9] Freeing UART from serial console..."

# Remove console=serial0,xxxxx from cmdline.txt
sed -i 's/console=serial0,[0-9]* //' /boot/firmware/cmdline.txt

# Disable serial login service
systemctl disable serial-getty@ttyAMA0.service 2>/dev/null || true

# ── 6. Create snapcast-source service ──
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
ExecStartPre=/bin/sleep 5
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

# ── 7. Create UART bridge service ──
echo ""
echo "[7/9] Setting up UART bridge..."

BRIDGE_DIR="$REAL_HOME/uart-bridge"
mkdir -p "$BRIDGE_DIR"

# Download bridge script from GitHub
wget -O "$BRIDGE_DIR/snapcast_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/snapcast_bridge.py"

chown -R "$REAL_USER:$REAL_USER" "$BRIDGE_DIR"

# Create venv and install pyserial
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

# ── 8. Sudoers for snapserver restart ──
echo ""
echo "[8/9] Configuring sudoers..."

echo "$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart snapserver" \
    > /etc/sudoers.d/snapserver-restart
chmod 440 /etc/sudoers.d/snapserver-restart

# Add user to dialout group for UART access
usermod -aG dialout "$REAL_USER"

# Enable linger so PipeWire (user service) starts at boot without login
loginctl enable-linger "$REAL_USER"

# ── 9. Enable services ──
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
