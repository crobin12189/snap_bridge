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

# ── Prompt for static IP settings ──
echo ""
echo "--- Static IP Configuration for eth0 ---"
read -rp "Static IP address (e.g. 192.168.1.100): " STATIC_IP
read -rp "Subnet prefix length (e.g. 24 for /24): " SUBNET
read -rp "Gateway (e.g. 192.168.1.1): " GATEWAY
read -rp "DNS server (e.g. 192.168.1.1 or 8.8.8.8): " DNS
read -rp "Disable WiFi? [y/N]: " DISABLE_WIFI
echo ""
echo "  IP:      $STATIC_IP/$SUBNET"
echo "  Gateway: $GATEWAY"
echo "  DNS:     $DNS"
if [[ "$DISABLE_WIFI" =~ ^[Yy]$ ]]; then
    echo "  WiFi:    disabled"
else
    echo "  WiFi:    enabled"
fi
echo ""
read -rp "Confirm? [y/N]: " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

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
    gpiod \
    python3 python3-pip python3-venv python3-serial \
    python3-dbus python3-gi

# Prevent snapserver and pipewire from being auto-upgraded
apt-mark hold snapserver pipewire pipewire-pulse

# ── 3. Configure boot config and kernel modules ──
echo ""
echo "[3/9] Configuring boot config and USB gadget audio..."

CONFIG=/boot/firmware/config.txt

# Enable SPI for W5500
sed -i 's/^#dtparam=spi=on/dtparam=spi=on/' "$CONFIG"

# Download and install W5500 overlay
wget -O /tmp/w5500-overlay.dts \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/w5500-overlay.dts"
dtc -I dts -O dtb -o /boot/overlays/w5500-driver.dtbo /tmp/w5500-overlay.dts
rm /tmp/w5500-overlay.dts

# Disable vc4-kms-v3d (comment it out if uncommented)
sed -i 's/^dtoverlay=vc4-kms-v3d/#dtoverlay=vc4-kms-v3d/' "$CONFIG"

# Remove any existing [all] section and everything after it, we'll rewrite it
# This avoids conflicts with dwc2 in other sections like [cm5]
if grep -q "^\[all\]" "$CONFIG"; then
    sed -i '/^\[all\]/,$d' "$CONFIG"
fi

# Append clean [all] section
# NOTE: miniuart-bt keeps onboard BT enabled (needed for BT input mode)
cat >> "$CONFIG" << 'CFGEOF'
[all]
dtoverlay=dwc2,dr_mode=peripheral
enable_uart=1
dtoverlay=miniuart-bt
dtoverlay=w5500-driver
CFGEOF

# Load dwc2 and g_audio modules on boot
grep -q "^dwc2" /etc/modules || echo "dwc2" >> /etc/modules
grep -q "^g_audio" /etc/modules || echo "g_audio" >> /etc/modules

# g_audio config: 96kHz 32bit stereo
cat > /etc/modprobe.d/g_audio.conf << 'EOF'
options g_audio c_chmask=3 p_chmask=0 c_srate=96000 p_srate=96000 c_ssize=4 p_ssize=4
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

# ── 6. Create snapcast source services ──
echo ""
echo "[6/9] Creating audio capture services..."

# UAC source service — original unchanged
cat > /etc/systemd/system/snapcast-source.service << SVCEOF
[Unit]
Description=PipeWire UAC audio source to Snapcast FIFO
After=pipewire.service snapserver.service
Wants=pipewire.service
 
[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        DEVICE=\$(pactl list sources short | grep "alsa_input.platform-.*usb" | awk "{print \\\$2}" | head -1); \\
        if [ -n "\$DEVICE" ]; then \\
            parec \\
                --device=\$DEVICE \\
                --format=s32le \\
                --rate=96000 \\
                --channels=2 \\
                --latency-msec=10 \\
                --process-time-msec=5 \\
            > /tmp/snapfifo || true; \\
        else \\
            sleep 1; \\
        fi; \\
    done'
Restart=always
RestartSec=2
 
[Install]
WantedBy=multi-user.target
SVCEOF

# Mic source service — original unchanged
cat > /etc/systemd/system/snapcast-sourcemic.service << SVCEOF
[Unit]
Description=PipeWire mic audio source to Snapcast FIFO
After=pipewire.service snapserver.service
Wants=pipewire.service
 
[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        DEVICE=\$(pactl list sources short | grep alsa_input | grep -v monitor | grep -v "platform-" | awk "{print \\\$2}" | head -1); \\
        if [ -n "\$DEVICE" ]; then \\
            parec \\
                --device=\$DEVICE \\
                --format=s32le \\
                --rate=96000 \\
                --channels=2 \\
                --latency-msec=10 \\
                --process-time-msec=5 \\
            > /tmp/snapfifo || true; \\
        else \\
            sleep 1; \\
        fi; \\
    done'
Restart=always
RestartSec=2
 
[Install]
WantedBy=multi-user.target
SVCEOF

# BT source service — NEW, same pattern as above
cat > /etc/systemd/system/snapcast-sourcebt.service << SVCEOF
[Unit]
Description=PipeWire BT audio source to Snapcast FIFO
After=pipewire.service snapserver.service
Wants=pipewire.service
 
[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        DEVICE=\$(pactl list sources short | grep "bluez_source" | grep -v monitor | awk "{print \\\$2}" | head -1); \\
        if [ -n "\$DEVICE" ]; then \\
            parec \\
                --device=\$DEVICE \\
                --format=s32le \\
                --rate=96000 \\
                --channels=2 \\
                --latency-msec=10 \\
                --process-time-msec=5 \\
            > /tmp/snapfifo || true; \\
        else \\
            sleep 1; \\
        fi; \\
    done'
Restart=on-failure
RestartSec=2
 
[Install]
WantedBy=multi-user.target
SVCEOF

# All disabled at boot — server_bridge controls which one runs
systemctl disable snapcast-source    2>/dev/null || true
systemctl disable snapcast-sourcemic 2>/dev/null || true
systemctl disable snapcast-sourcebt  2>/dev/null || true

# ── 6b. Bluetooth — auto-pair agent (NEW) ──
echo ""
echo "[6b/9] Configuring Bluetooth..."

# BlueZ main.conf
sed -i 's/^#*Class\s*=.*/Class = 0x41C/'                        /etc/bluetooth/main.conf
sed -i 's/^#*DiscoverableTimeout\s*=.*/DiscoverableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*PairableTimeout\s*=.*/PairableTimeout = 0/'         /etc/bluetooth/main.conf
sed -i 's/^#*AlwaysPairable\s*=.*/AlwaysPairable = true/'        /etc/bluetooth/main.conf
sed -i 's/^#*FastConnectable\s*=.*/FastConnectable = true/'       /etc/bluetooth/main.conf
sed -i 's/^#*AutoEnable\s*=.*/AutoEnable = true/'                /etc/bluetooth/main.conf
sed -i '/^Disable=Headset/d'                                      /etc/bluetooth/main.conf
sed -i 's/^#*JustWorksRepairing\s*=.*/JustWorksRepairing = always/' /etc/bluetooth/main.conf
grep -q "JustWorksRepairing" /etc/bluetooth/main.conf || \
    sed -i '/^\[General\]/a JustWorksRepairing = always' /etc/bluetooth/main.conf

BRIDGE_DIR="$REAL_HOME/server_bridge"
mkdir -p "$BRIDGE_DIR"

cat > "$BRIDGE_DIR/bt_agent.py" << 'PYEOF'
#!/usr/bin/env python3
"""
bt_agent.py — Bluetooth auto-pair agent for server BT input mode.
NoInputNoOutput: auto-pairs any device, sets adapter name to hostname.
"""
import dbus
import dbus.service
import dbus.mainloop.glib
from gi.repository import GLib
import logging
import socket
import time
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bt-agent")

AGENT_PATH       = "/org/bluez/AutoAgent"
AGENT_CAPABILITY = "NoInputNoOutput"
BUS_NAME         = "org.bluez"
ADAPTER_IFACE    = "org.bluez.Adapter1"
AGENT_MGR_IFACE  = "org.bluez.AgentManager1"
DEVICE_IFACE     = "org.bluez.Device1"


class BTAgent(dbus.service.Object):
    def __init__(self, bus):
        super().__init__(bus, AGENT_PATH)

    @dbus.service.method("org.bluez.Agent1", in_signature="", out_signature="")
    def Release(self): log.info("Agent released")

    @dbus.service.method("org.bluez.Agent1", in_signature="os", out_signature="")
    def AuthorizeService(self, device, uuid):
        log.info("AuthorizeService: %s uuid=%s — auto-authorized", device, uuid)

    @dbus.service.method("org.bluez.Agent1", in_signature="o", out_signature="s")
    def RequestPinCode(self, device):
        log.info("RequestPinCode: %s — returning 0000", device)
        return "0000"

    @dbus.service.method("org.bluez.Agent1", in_signature="o", out_signature="u")
    def RequestPasskey(self, device):
        log.info("RequestPasskey: %s — returning 0", device)
        return dbus.UInt32(0)

    @dbus.service.method("org.bluez.Agent1", in_signature="ouq", out_signature="")
    def DisplayPasskey(self, device, passkey, entered):
        log.info("DisplayPasskey: %s passkey=%06d entered=%d", device, passkey, entered)

    @dbus.service.method("org.bluez.Agent1", in_signature="os", out_signature="")
    def DisplayPinCode(self, device, pincode):
        log.info("DisplayPinCode: %s pin=%s", device, pincode)

    @dbus.service.method("org.bluez.Agent1", in_signature="ou", out_signature="")
    def RequestConfirmation(self, device, passkey):
        log.info("RequestConfirmation: %s passkey=%06d — auto-confirmed", device, passkey)

    @dbus.service.method("org.bluez.Agent1", in_signature="o", out_signature="")
    def RequestAuthorization(self, device):
        log.info("RequestAuthorization: %s — auto-authorized", device)

    @dbus.service.method("org.bluez.Agent1", in_signature="", out_signature="")
    def Cancel(self): log.info("Pairing cancelled")


def get_adapter(bus):
    manager = dbus.Interface(bus.get_object(BUS_NAME, "/"),
                             "org.freedesktop.DBus.ObjectManager")
    for path, ifaces in manager.GetManagedObjects().items():
        if ADAPTER_IFACE in ifaces:
            return path, dbus.Interface(bus.get_object(BUS_NAME, path),
                                        "org.freedesktop.DBus.Properties")
    return None, None


def setup_adapter(bus):
    path, props = get_adapter(bus)
    if not props:
        log.error("No Bluetooth adapter found!")
        return False
    try:
        props.Set(ADAPTER_IFACE, "Alias",               dbus.String(socket.gethostname()))
        props.Set(ADAPTER_IFACE, "Powered",             dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "Discoverable",        dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "DiscoverableTimeout", dbus.UInt32(0))
        props.Set(ADAPTER_IFACE, "Pairable",            dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "PairableTimeout",     dbus.UInt32(0))
        log.info("Adapter %s: name=%s powered=on discoverable=on", path, socket.gethostname())
        return True
    except Exception as e:
        log.error("Adapter setup failed: %s", e)
        return False


def register_agent(bus):
    agent_mgr = dbus.Interface(bus.get_object(BUS_NAME, "/org/bluez"), AGENT_MGR_IFACE)
    agent_mgr.RegisterAgent(AGENT_PATH, AGENT_CAPABILITY)
    agent_mgr.RequestDefaultAgent(AGENT_PATH)
    log.info("Agent registered as default (%s)", AGENT_CAPABILITY)


def trust_device(bus, device_path):
    try:
        props = dbus.Interface(bus.get_object(BUS_NAME, device_path),
                               "org.freedesktop.DBus.Properties")
        props.Set(DEVICE_IFACE, "Trusted", dbus.Boolean(True))
        log.info("Trusted device: %s", device_path)
    except Exception as e:
        log.warning("Could not trust %s: %s", device_path, e)


def on_properties_changed(interface, changed, invalidated, path, bus):
    if interface != DEVICE_IFACE:
        return
    if "Paired" in changed and changed["Paired"]:
        log.info("Device paired: %s — trusting", path)
        trust_device(bus, path)
    if "Connected" in changed:
        log.info("Device %s: %s", path, "connected" if changed["Connected"] else "disconnected")


def watchdog(bus):
    while True:
        time.sleep(30)
        try:
            setup_adapter(bus)
        except Exception as e:
            log.warning("Watchdog adapter check failed: %s", e)


def main():
    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
    bus = dbus.SystemBus()

    for i in range(10):
        try:
            bus.get_object(BUS_NAME, "/org/bluez")
            break
        except Exception:
            log.info("Waiting for BlueZ... (%d/10)", i + 1)
            time.sleep(1)

    for i in range(5):
        if setup_adapter(bus):
            break
        log.warning("Adapter not ready, retrying... (%d/5)", i + 1)
        time.sleep(2)

    agent = BTAgent(bus)
    register_agent(bus)

    bus.add_signal_receiver(
        lambda iface, changed, invalidated, path=None: on_properties_changed(
            iface, changed, invalidated, path, bus
        ),
        signal_name="PropertiesChanged",
        dbus_interface="org.freedesktop.DBus.Properties",
        path_keyword="path"
    )

    threading.Thread(target=watchdog, args=(bus,), daemon=True).start()

    log.info("Bluetooth agent running — waiting for connections")
    loop = GLib.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.quit()


if __name__ == "__main__":
    main()
PYEOF

chmod +x "$BRIDGE_DIR/bt_agent.py"
chown "$REAL_USER:$REAL_USER" "$BRIDGE_DIR/bt_agent.py"

cat > /etc/systemd/system/bt-agent.service << 'EOF'
[Unit]
Description=Bluetooth Auth Agent (server BT input mode)
After=bluetooth.service
Requires=bluetooth.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 BRIDGE_DIR_PLACEHOLDER/bt_agent.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

# Substitute the actual path (can't use $BRIDGE_DIR inside single-quoted heredoc)
sed -i "s|BRIDGE_DIR_PLACEHOLDER|$BRIDGE_DIR|g" /etc/systemd/system/bt-agent.service

systemctl daemon-reload
systemctl enable bt-agent.service

# ── 7. Create Server bridge service ──
echo ""
echo "[7/9] Setting up Server bridge..."

# Download bridge script from GitHub
wget -O "$BRIDGE_DIR/server_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/server_bridge.py"

chown -R "$REAL_USER:$REAL_USER" "$BRIDGE_DIR"

# Create venv and install pyserial
sudo -u "$REAL_USER" python3 -m venv "$BRIDGE_DIR/venv"
sudo -u "$REAL_USER" "$BRIDGE_DIR/venv/bin/pip" install pyserial

# ── Password hash file ──
touch /etc/zone_password.hash
chown "$REAL_USER:$REAL_USER" /etc/zone_password.hash
chmod 640 /etc/zone_password.hash

cat > /etc/systemd/system/server_bridge.service << SVCEOF
[Unit]
Description=Snapcast UART Bridge (ESP32)
After=snapserver.service
Wants=snapserver.service

[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
ExecStart=$BRIDGE_DIR/venv/bin/python3 $BRIDGE_DIR/server_bridge.py --port /dev/ttyAMA0 --baud 460800
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SVCEOF

# ── 8. Sudoers for snapserver restart ──
echo ""
echo "[8/9] Configuring sudoers..."

cat > /etc/sudoers.d/snapserver-restart << SUDOEOF
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart snapserver
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl start snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl start snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl start snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl start bt-agent
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start bt-agent
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/systemctl stop bt-agent
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop bt-agent
SUDOEOF
chmod 440 /etc/sudoers.d/snapserver-restart

# Add user to dialout group for UART access
usermod -aG dialout "$REAL_USER"
usermod -aG gpio "$REAL_USER"
usermod -aG bluetooth "$REAL_USER"

# Enable linger so PipeWire (user service) starts at boot without login
loginctl enable-linger "$REAL_USER"

# ── 9. Enable services ──
echo ""
echo "[9/9] Enabling services..."

systemctl daemon-reload
systemctl enable snapserver
systemctl enable server_bridge

cat > /etc/sysctl.d/99-tcp-retries.conf << 'EOF'
net.ipv4.tcp_retries2 = 3
net.ipv4.tcp_keepalive_time = 5
net.ipv4.tcp_keepalive_intvl = 2
net.ipv4.tcp_keepalive_probes = 3
EOF
sudo sysctl -p /etc/sysctl.d/99-tcp-retries.conf

# ── Static IP for eth0 ──
echo ""
echo "Configuring static IP for eth0..."

nmcli con add type ethernet ifname eth0 con-name ethernet \
  ip4 "$STATIC_IP/$SUBNET" gw4 "$GATEWAY"
nmcli con mod ethernet ipv4.dns "$DNS"
nmcli con mod ethernet ipv4.method manual

if [[ "$DISABLE_WIFI" =~ ^[Yy]$ ]]; then
    nmcli radio wifi off
fi

echo ""
echo "========================================="
echo " Setup complete!"
echo ""
echo " Services installed:"
echo "   - snapserver (audio streaming)"
echo "   - snapcast-source (USB audio capture)"
echo "   - snapcast-sourcemic (mic audio capture)"
echo "   - snapcast-sourcebt (Bluetooth audio capture) [NEW]"
echo "   - bt-agent (Bluetooth auto-pair agent) [NEW]"
echo "   - server_bridge (ESP32 communication)"
echo ""
echo " UART bridge script location:"
echo "   $BRIDGE_DIR/server_bridge.py"
echo ""
echo " Config files:"
echo "   /etc/snapserver.conf"
echo "   /etc/modprobe.d/g_audio.conf"
echo "   $PW_CONF_DIR/96khz.conf"
echo ""
echo " Network:"
echo "   eth0 static IP: $STATIC_IP/$SUBNET"
echo "   Gateway:        $GATEWAY"
echo "   DNS:            $DNS"
echo ""
echo " REBOOT NOW to apply all changes:"
echo "   sudo reboot"
echo "========================================="
