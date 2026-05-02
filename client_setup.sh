#!/bin/bash
set -e

# ── Must run as root ──
if [ "$EUID" -ne 0 ]; then
    echo "Run with sudo: sudo ./setup-client.sh"
    exit 1
fi

# ── Detect the real user (not root) ──
REAL_USER="${SUDO_USER:-$(logname)}"
REAL_HOME=$(eval echo "~$REAL_USER")
USER_ID=$(id -u "$REAL_USER")

echo "========================================="
echo " Snapcast Client + BT Speaker Setup"
echo " User: $REAL_USER"
echo " Home: $REAL_HOME"
echo "========================================="

# ── Prompt for static IP settings ──
echo ""
echo "--- Static IP Configuration for eth0 ---"
read -rp "Static IP address (e.g. 192.168.1.100): " STATIC_IP
read -rp "Subnet prefix length (e.g. 24 for /24): " SUBNET
read -rp "Gateway (e.g. 192.168.1.1): " GATEWAY
read -rp "DNS server (e.g. 192.168.1.1 or 8.8.8.8): " DNS
echo ""
echo "  IP:      $STATIC_IP/$SUBNET"
echo "  Gateway: $GATEWAY"
echo "  DNS:     $DNS"
echo ""
read -rp "Confirm? [y/N]: " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# ── 1. Update package lists ──
echo ""
echo "[1/14] Updating package lists..."
apt update

# ── 2. Install packages (pinned versions) ──
echo ""
echo "[2/14] Installing packages..."
apt install -y \
    pulseaudio=16.1+dfsg1-2+rpt1.1 \
    pulseaudio-module-bluetooth=16.1+dfsg1-2+rpt1.1 \
    pulseaudio-utils=16.1+dfsg1-2+rpt1.1 \
    bluez-tools=2.0~20170911.0.7cb788c-4 \
    snapclient=0.26.0+dfsg1-1+deb12u1 \
    python3-serial=3.5-1.1 \
    avahi-daemon \
    git \
    python3-dev

# Prevent auto-upgrades
apt-mark hold \
    pulseaudio pulseaudio-module-bluetooth pulseaudio-utils \
    bluez-tools snapclient python3-serial

# Add user to bluetooth and dialout groups
usermod -a -G bluetooth "$REAL_USER"
usermod -a -G dialout "$REAL_USER"

# ── 3. config.txt — UART, I2S, disable onboard audio ──
echo ""
echo "[3/14] Configuring /boot/firmware/config.txt..."

CONFIG="/boot/firmware/config.txt"
cp "$CONFIG" "${CONFIG}.bak"

# Enable interfaces
sed -i 's/^#dtparam=i2c_arm=on/dtparam=i2c_arm=on/' "$CONFIG"
sed -i 's/^#dtparam=i2s=on/dtparam=i2s=on/' "$CONFIG"
sed -i 's/^#dtparam=spi=on/dtparam=spi=on/' "$CONFIG"

# Disable onboard audio
sed -i 's/^dtparam=audio=on/#dtparam=audio=on/' "$CONFIG"

# Disable HDMI video overlay
sed -i 's/^dtoverlay=vc4-kms-v3d/#dtoverlay=vc4-kms-v3d/' "$CONFIG"

# Remove existing [all] section and rewrite
if grep -q "^\[all\]" "$CONFIG"; then
    sed -i '/^\[all\]/,$d' "$CONFIG"
fi

cat >> "$CONFIG" << 'CFGEOF'
[all]
# Enable UART for ESP communication
enable_uart=1

# Move Bluetooth to miniuart so ttyAMA0 is free for ESP at 460800 baud
dtoverlay=miniuart-bt

# I2S DAC
dtoverlay=hifiberry-dac
CFGEOF

# ── 4. Free UART from serial console ──
echo ""
echo "[4/14] Freeing UART from serial console..."

sed -i 's/console=serial0,[0-9]* //' /boot/firmware/cmdline.txt
systemctl disable serial-getty@ttyAMA0.service 2>/dev/null || true

# ── 5. ALSA config — 96kHz/32-bit ──
echo ""
echo "[5/14] Configuring ALSA (96kHz/32-bit)..."

cat > /etc/asound.conf << 'EOF'
pcm.!default {
    type plug
    slave {
        pcm "hw:0,0"
        format S32_LE
        rate 96000
        channels 2
    }
}

ctl.!default {
    type hw
    card 0
}
EOF

# ── 6. Configure PulseAudio — 96kHz/32-bit ──
echo ""
echo "[6/14] Configuring PulseAudio..."

# Remove any existing custom config to avoid duplicates
sed -i '/^# Client audio config$/,/^resample-method/d' /etc/pulse/daemon.conf

cat >> /etc/pulse/daemon.conf << 'EOF'

# Client audio config
default-sample-format = s32le
default-sample-rate = 96000
alternate-sample-rate = 48000
default-sample-channels = 2
resample-method = speex-float-5
EOF

# Auto-switch BT profile on connect
grep -q "module-switch-on-connect" /etc/pulse/default.pa || \
    echo "load-module module-switch-on-connect" >> /etc/pulse/default.pa

# Disable PipeWire if installed (we use PulseAudio)
sudo -u "$REAL_USER" systemctl --user disable pipewire.service pipewire.socket \
    pipewire-pulse.service pipewire-pulse.socket wireplumber.service 2>/dev/null || true
sudo -u "$REAL_USER" systemctl --user mask pipewire.service pipewire.socket \
    pipewire-pulse.service pipewire-pulse.socket wireplumber.service 2>/dev/null || true

# Enable PulseAudio
sudo -u "$REAL_USER" systemctl --user unmask pulseaudio.service pulseaudio.socket 2>/dev/null || true
sudo -u "$REAL_USER" systemctl --user enable pulseaudio 2>/dev/null || true

# ── 7. Console autologin ──
echo ""
echo "[7/14] Enabling console autologin..."

raspi-config nonint do_boot_behaviour B2

# ── 8. Bluetooth — always discoverable, auto-pair, no PIN ──
echo ""
echo "[8/14] Configuring Bluetooth..."

# BlueZ main.conf
sed -i 's/^#*Class\s*=.*/Class = 0x41C/' /etc/bluetooth/main.conf
sed -i 's/^#*DiscoverableTimeout\s*=.*/DiscoverableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*PairableTimeout\s*=.*/PairableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*AlwaysPairable\s*=.*/AlwaysPairable = true/' /etc/bluetooth/main.conf
sed -i 's/^#*FastConnectable\s*=.*/FastConnectable = true/' /etc/bluetooth/main.conf
sed -i 's/^#*AutoEnable\s*=.*/AutoEnable = true/' /etc/bluetooth/main.conf

# Always-discoverable service
cat > /etc/systemd/system/bt-discoverable.service << 'EOF'
[Unit]
Description=Bluetooth Always Discoverable
After=bluetooth.service
Requires=bluetooth.service

[Service]
Type=oneshot
ExecStart=/bin/bash -c 'bluetoothctl power on && bluetoothctl discoverable on && bluetoothctl pairable on'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

# Auto-pair agent (no PIN)
cat > /etc/systemd/system/bt-agent.service << 'EOF'
[Unit]
Description=Bluetooth Auth Agent
After=bluetooth.service
PartOf=bluetooth.service

[Service]
Type=simple
ExecStart=/usr/bin/bt-agent -c NoInputNoOutput
Restart=always
RestartSec=3

[Install]
WantedBy=bluetooth.target
EOF

systemctl daemon-reload
systemctl enable bt-discoverable.service
systemctl enable bt-agent.service

# ── 9. Snapclient — 96kHz/32-bit ALSA ──
echo ""
echo "[9/14] Configuring Snapclient..."

cat > /etc/default/snapclient << 'EOF'
START_SNAPCLIENT=true
SNAPCLIENT_OPTS="--sampleformat 96000:32:* --player alsa"
EOF

systemctl enable snapclient

# ── 10. ESP Bridge ──
echo ""
echo "[10/14] Setting up ESP Bridge..."

BRIDGE_DIR="/opt/esp-bridge"
mkdir -p "$BRIDGE_DIR"

# Download bridge script from GitHub
wget -O "$BRIDGE_DIR/client_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/client_bridge.py"

chmod +x "$BRIDGE_DIR/client_bridge.py"

cat > /etc/systemd/system/esp-bridge.service << SVCEOF
[Unit]
Description=ESP UART Bridge (Snapcast + BT)
After=network.target bluetooth.service
Wants=bluetooth.service

[Service]
Type=simple
User=$REAL_USER
ExecStart=/usr/bin/python3 $BRIDGE_DIR/client_bridge.py --port /dev/ttyAMA0 --baud 460800
Restart=always
RestartSec=3
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$USER_ID/bus
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable esp-bridge.service

# ── 11. Sudo permissions for snapclient control ──
echo ""
echo "[11/14] Configuring sudoers..."

echo "$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start snapclient, /bin/systemctl stop snapclient" \
    > /etc/sudoers.d/esp-bridge
chmod 440 /etc/sudoers.d/esp-bridge

# ── 12. Enable user linger ──
echo ""
echo "[12/14] Enabling user linger..."

loginctl enable-linger "$REAL_USER"

# ── 13. Avahi mDNS — IPv4 only ──
echo ""
echo "[13/14] Configuring Avahi for IPv4 only..."

if grep -q "^use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#*use-ipv6\s*=.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
elif grep -q "^#.*use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#.*use-ipv6.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
else
    sed -i '/^\[server\]/a use-ipv6=no' /etc/avahi/avahi-daemon.conf
fi

# ── 14. SigmaDSP Backend (ADAU1466 over SPI) ──
echo ""
echo "[14/14] Installing sigmadsp backend..."


# Clone fork and run install.sh
sudo -u "$REAL_USER" git clone https://github.com/crobin12189/sigmadsp.git "$REAL_HOME/sigmadsp" || true
cd "$REAL_HOME/sigmadsp"
set +e
(sudo -u "$REAL_USER" bash install.sh <<< "n")
set -e

# Fix gpiozero FIRST before creating service — 1.6.2 has pkg_resources bug, 2.0.1 works fine
# despite version constraint warning from pipx
echo "Upgrading gpiozero to 2.0.1..."
VENV_PIP="$REAL_HOME/.local/pipx/venvs/sigmadsp/bin/python"
sudo -u "$REAL_USER" $VENV_PIP -m pip install "gpiozero==2.0.1" --force-reinstall

# Create systemd service manually (install.sh may fail due to PATH timing)
SIGMADSP_BIN="$REAL_HOME/.local/bin/sigmadsp-backend"

cat > /etc/systemd/system/sigmadsp-backend.service << SIGEOF
[Unit]
Description=sigmadsp backend service
After=network.target

[Service]
ExecStart=$SIGMADSP_BIN --settings /var/lib/sigmadsp/config.yaml
Restart=always
RestartSec=5
User=$REAL_USER

[Install]
WantedBy=multi-user.target
SIGEOF

# Write config for ADAU1466 over SPI0, reset on GPIO25
mkdir -p /var/lib/sigmadsp
cat > /var/lib/sigmadsp/config.yaml << 'YAMLEOF'
host:
  ip: "0.0.0.0"
  port: 8087

backend:
  port: 50051

parameters:
  path: "/var/lib/sigmadsp/current.params"

dsp:
  type: "adau14xx"
  protocol: "spi"
  bus_number: "0"
  device_address: "0"
  pins:
    reset:
      number: 25
      active_high: false
      initial_state: true
      mode: "output"
    self_boot:
      number: 22
      active_high: true
      initial_state: false
      mode: "output"
YAMLEOF

systemctl daemon-reload
systemctl enable sigmadsp-backend
systemctl start sigmadsp-backend

# ── Static IP for eth0 ──
echo ""
echo "Configuring static IP for eth0..."

cat >> /etc/dhcpcd.conf << DHCPEOF

# Static IP for USB ethernet
interface eth0
static ip_address=$STATIC_IP/$SUBNET
static routers=$GATEWAY
static domain_name_servers=$DNS
DHCPEOF

# ── Clean up PipeWire leftovers ──
rm -rf "$REAL_HOME/.config/wireplumber" 2>/dev/null || true
rm -rf "$REAL_HOME/.config/pipewire" 2>/dev/null || true
rm -rf "$REAL_HOME/.config/systemd/user/wireplumber.service.d" 2>/dev/null || true

echo ""
echo "========================================="
echo " Setup complete!"
echo ""
echo " Services installed:"
echo "   - pulseaudio (audio + BT A2DP sink)"
echo "   - bt-discoverable (always discoverable)"
echo "   - bt-agent (auto-pair, no PIN)"
echo "   - snapclient (multiroom audio)"
echo "   - esp-bridge (ESP32 UART communication)"
echo "   - sigmadsp-backend (ADAU1466 SPI control, port 8087)"
echo ""
echo " Config files:"
echo "   /etc/asound.conf"
echo "   /etc/pulse/daemon.conf"
echo "   /etc/pulse/default.pa"
echo "   /etc/bluetooth/main.conf"
echo "   /etc/default/snapclient"
echo "   /etc/avahi/avahi-daemon.conf"
echo "   /var/lib/sigmadsp/config.yaml"
echo ""
echo " Bridge script: $BRIDGE_DIR/client_bridge.py"
echo " SigmaDSP repo: $REAL_HOME/sigmadsp"
echo ""
echo " Wiring:"
echo "   GPIO14 (TX) → ESP RX"
echo "   GPIO15 (RX) → ESP TX"
echo "   I2S: GPIO18 (BCLK), GPIO19 (LRCLK), GPIO21 (DOUT)"
echo "   DAC SD_MODE → 3.3V"
echo "   SPI0: GPIO10 (MOSI), GPIO9 (MISO), GPIO11 (SCLK), GPIO8 (CE0)"
echo "   ADAU1466 RES → GPIO25"
echo "   ADAU1466 5V → Pi Pin 2/4, GND → Pi GND"
echo ""
echo " SigmaStudio: connect to this Pi's IP on port 8087"
echo ""
echo " Network:"
echo "   eth0 static IP: $STATIC_IP/$SUBNET"
echo "   Gateway:        $GATEWAY"
echo "   DNS:            $DNS"
echo ""
echo " REBOOT NOW to apply all changes:"
echo "   sudo reboot"
echo "========================================="
