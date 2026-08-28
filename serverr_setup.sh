#!/bin/bash
set -e

# ── Must run as root ──
if [ "$EUID" -ne 0 ]; then
    echo "Run with sudo: sudo ./server_setup.sh"
    exit 1
fi

REAL_USER="${SUDO_USER:-$(logname)}"
REAL_HOME=$(eval echo "~$REAL_USER")
USER_ID=$(id -u "$REAL_USER")

echo "========================================="
echo " Snapcast SERVER + ESP32 Bridge Setup"
echo " User: $REAL_USER (uid=$USER_ID)"
echo " Home: $REAL_HOME"
echo "========================================="

echo ""
echo "--- Static IP Configuration for eth0 ---"
read -rp "Static IP address (e.g. 192.168.1.50): "  STATIC_IP
read -rp "Subnet prefix length (e.g. 24):           " SUBNET
read -rp "Gateway (e.g. 192.168.1.1):               " GATEWAY
read -rp "DNS server (e.g. 192.168.1.1 or 8.8.8.8):" DNS
read -rp "Disable WiFi? [y/N]: "                      DISABLE_WIFI
echo ""
echo "  IP:      $STATIC_IP/$SUBNET"
echo "  Gateway: $GATEWAY"
echo "  DNS:     $DNS"
echo ""
read -rp "Confirm? [y/N]: " CONFIRM
[[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }

# ── 1. Update ─────────────────────────────────────────────────────────────
echo ""
echo "[1/14] Updating package lists..."
apt update

# ── 2. Install packages ───────────────────────────────────────────────────
echo ""
echo "[2/14] Installing packages..."
apt install -y \
    snapserver \
    pulseaudio \
    pulseaudio-module-bluetooth \
    pulseaudio-utils \
    python3-serial \
    python3-dbus \
    python3-gi \
    avahi-daemon \
    git \
    gpiod \
    python3-dev \
    ffmpeg

apt-mark hold pulseaudio pulseaudio-module-bluetooth pulseaudio-utils snapserver

usermod -a -G bluetooth "$REAL_USER"
usermod -a -G dialout   "$REAL_USER"
usermod -a -G pulse     "$REAL_USER"
usermod -a -G pulse-access "$REAL_USER"
usermod -a -G gpio      "$REAL_USER"

# ── 3. config.txt ─────────────────────────────────────────────────────────
echo ""
echo "[3/14] Configuring /boot/firmware/config.txt..."

CONFIG="/boot/firmware/config.txt"
cp "$CONFIG" "${CONFIG}.bak"

sed -i 's/^#dtparam=i2c_arm=on/dtparam=i2c_arm=on/'   "$CONFIG"
sed -i 's/^#dtparam=i2s=on/dtparam=i2s=on/'           "$CONFIG"
sed -i 's/^#dtparam=spi=on/dtparam=spi=on/'           "$CONFIG"
sed -i 's/^dtparam=audio=on/#dtparam=audio=on/'        "$CONFIG"
sed -i 's/^dtoverlay=vc4-kms-v3d/#dtoverlay=vc4-kms-v3d/' "$CONFIG"

# Remove existing [all] section and rewrite cleanly
if grep -q "^\[all\]" "$CONFIG"; then
    sed -i '/^\[all\]/,$d' "$CONFIG"
fi

cat >> "$CONFIG" << 'CFGEOF'
[all]
# UART for ESP32 communication
enable_uart=1

# USB-C UAC gadget mode (phone → Pi as USB audio device)
dtoverlay=dwc2,dr_mode=peripheral

# Bluetooth — keep onboard BT enabled for BT input mode
# (do NOT add dtoverlay=disable-bt here — BT input mode needs it)
# USB BT dongle is also supported via bt-init.service below.

# Hardware PWM on GPIO12 for fan control (optional)
# dtoverlay=pwm,pin=12,func=4
CFGEOF

# ── 4. UAC gadget (g_audio) ───────────────────────────────────────────────
echo ""
echo "[4/14] Configuring USB Audio Class gadget..."

mkdir -p /etc/modprobe.d
cat > /etc/modprobe.d/g_audio.conf << 'EOF'
options g_audio c_chmask=3 p_chmask=0 c_srate=96000 p_srate=96000 c_ssize=4 p_ssize=4
EOF

# Ensure g_audio loads after dwc2 at boot
grep -q "^dwc2$"   /etc/modules 2>/dev/null || echo "dwc2"   >> /etc/modules
grep -q "^g_audio$" /etc/modules 2>/dev/null || echo "g_audio" >> /etc/modules

# ── 5. Free UART from serial console ──────────────────────────────────────
echo ""
echo "[5/14] Freeing UART from serial console..."
sed -i 's/console=serial0,[0-9]* //' /boot/firmware/cmdline.txt
systemctl disable serial-getty@ttyAMA0.service 2>/dev/null || true

# ── 6. Snapserver ─────────────────────────────────────────────────────────
echo ""
echo "[6/14] Configuring Snapserver..."

SNAPFIFO="/tmp/snapfifo"

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

cat > /etc/tmpfiles.d/snapfifo.conf << 'EOF'
p /tmp/snapfifo 0666 root root -
EOF

systemctl enable snapserver

# ── 7. Source services (all disabled at boot — bridge controls them) ───────
echo ""
echo "[7/14] Installing Snapcast source services..."

BRIDGE_DIR="/opt/esp-bridge"
mkdir -p "$BRIDGE_DIR"

# ── snapcast-source (USB UAC → FIFO) — original unchanged ──
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
Restart=on-failure
RestartSec=2
 
[Install]
WantedBy=multi-user.target
SVCEOF
 
# ── snapcast-sourcemic (mic → FIFO) — original unchanged ──
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
Restart=on-failure
RestartSec=2
 
[Install]
WantedBy=multi-user.target
SVCEOF
 
# ── snapcast-sourcebt (BT → FIFO) — new addition ──
# Same pattern as USB and mic services above — finds bluez_source.* directly
# via pactl (PipeWire compatibility layer) and pipes straight to FIFO.
# No PulseAudio daemon or null-sink needed — PipeWire handles BT A2DP natively.
cat > /etc/systemd/system/snapcast-sourcebt.service << SVCEOF
[Unit]
Description=PipeWire BT audio source to Snapcast FIFO
After=pipewire.service snapserver.service bt-agent.service
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

# Both disabled at boot — server_bridge starts them based on input mode
systemctl disable snapcast-source    2>/dev/null || true
systemctl disable snapcast-sourcemic 2>/dev/null || true
systemctl disable snapcast-sourcebt  2>/dev/null || true

# ── 8. Bluetooth — USB dongle + auto-pair agent ───────────────────────────
echo ""
echo "[9/14] Configuring Bluetooth..."

# BlueZ main.conf
sed -i 's/^#*Class\s*=.*/Class = 0x41C/'                   /etc/bluetooth/main.conf
sed -i 's/^#*DiscoverableTimeout\s*=.*/DiscoverableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*PairableTimeout\s*=.*/PairableTimeout = 0/'   /etc/bluetooth/main.conf
sed -i 's/^#*AlwaysPairable\s*=.*/AlwaysPairable = true/'  /etc/bluetooth/main.conf
sed -i 's/^#*FastConnectable\s*=.*/FastConnectable = true/' /etc/bluetooth/main.conf
sed -i 's/^#*AutoEnable\s*=.*/AutoEnable = true/'          /etc/bluetooth/main.conf
sed -i '/^Disable=Headset/d'                                /etc/bluetooth/main.conf
sed -i 's/^#*JustWorksRepairing\s*=.*/JustWorksRepairing = always/' /etc/bluetooth/main.conf
grep -q "JustWorksRepairing" /etc/bluetooth/main.conf || \
    sed -i '/^\[General\]/a JustWorksRepairing = always' /etc/bluetooth/main.conf

# USB BT dongle init (same pattern as client)
cat > /etc/systemd/system/bt-init.service << 'EOF'
[Unit]
Description=Bluetooth USB dongle init
After=bluetooth.service
Requires=bluetooth.service

[Service]
Type=oneshot
ExecStartPre=/bin/sleep 2
ExecStart=/usr/sbin/rfkill unblock bluetooth
ExecStart=/usr/bin/hciconfig hci0 up
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

# bt_agent.py — identical logic to client, keeps Pi always discoverable
# and auto-pairs any device without PIN confirmation.
cat > "$BRIDGE_DIR/bt_agent.py" << 'PYEOF'
#!/usr/bin/env python3
"""
bt_agent.py — Bluetooth pairing agent for server Pi BT input mode.
Auto-pairs NoInputNoOutput, auto-trusts, keeps adapter discoverable.
Removes device bonding on disconnect so next pair is always fresh.
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
        hostname = socket.gethostname()
        props.Set(ADAPTER_IFACE, "Alias",             dbus.String(hostname))
        props.Set(ADAPTER_IFACE, "Powered",           dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "Discoverable",      dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "DiscoverableTimeout", dbus.UInt32(0))
        props.Set(ADAPTER_IFACE, "Pairable",          dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "PairableTimeout",   dbus.UInt32(0))
        log.info("Adapter %s: name=%s powered=on discoverable=on pairable=on",
                 path, hostname)
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
        state = "connected" if changed["Connected"] else "disconnected"
        log.info("Device %s: %s", state, path)


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

cat > /etc/systemd/system/bt-agent.service << EOF
[Unit]
Description=Bluetooth Auth Agent (Python DBus) — server BT input mode
After=bluetooth.service bt-init.service
Requires=bluetooth.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 ${BRIDGE_DIR}/bt_agent.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable bt-init.service
systemctl enable bt-agent.service

# ── 10. Console autologin ─────────────────────────────────────────────────
echo ""
echo "[10/14] Enabling console autologin..."
raspi-config nonint do_boot_behaviour B2

# ── 11. ESP Bridge (server_bridge.py) ─────────────────────────────────────
echo ""
echo "[11/14] Setting up server ESP bridge..."

# Download bridge script from GitHub (or copy from this repo)
wget -O "$BRIDGE_DIR/server_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/server_bridge.py" || \
    echo "WARNING: download failed — place server_bridge.py in $BRIDGE_DIR manually"

chmod +x "$BRIDGE_DIR/server_bridge.py"

touch /etc/zone_password.hash
chown "$REAL_USER:$REAL_USER" /etc/zone_password.hash
chmod 640 /etc/zone_password.hash

cat > /etc/systemd/system/esp-bridge-server.service << EOF
[Unit]
Description=Server ESP UART Bridge (Snapcast + BT input)
After=network.target snapserver.service bluetooth.service bt-init.service

[Service]
Type=simple
User=$REAL_USER
ExecStart=/usr/bin/python3 ${BRIDGE_DIR}/server_bridge.py \
    --port /dev/ttyAMA0 \
    --baud 460800
Restart=always
RestartSec=3
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${USER_ID}/bus
Environment=XDG_RUNTIME_DIR=/run/user/${USER_ID}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable esp-bridge-server.service

# ── 12. Sudoers ───────────────────────────────────────────────────────────
echo ""
echo "[12/14] Configuring sudoers..."

cat > /etc/sudoers.d/esp-bridge-server << EOF
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start  snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop   snapcast-source
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start  snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop   snapcast-sourcemic
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start  snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop   snapcast-sourcebt
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl restart snapserver
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start  bt-agent
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop   bt-agent
EOF
chmod 440 /etc/sudoers.d/esp-bridge-server

# ── 13. Avahi mDNS — IPv4 only ───────────────────────────────────────────
echo ""
echo "[13/14] Configuring Avahi..."

if grep -q "^use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#*use-ipv6\s*=.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
elif grep -q "^#.*use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#.*use-ipv6.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
else
    sed -i '/^\[server\]/a use-ipv6=no' /etc/avahi/avahi-daemon.conf
fi

# ── 14. Network — static IP for eth0 ─────────────────────────────────────
echo ""
echo "[14/14] Configuring static IP for eth0..."

nmcli con add type ethernet ifname eth0 con-name ethernet \
    ip4 "$STATIC_IP/$SUBNET" gw4 "$GATEWAY"
nmcli con mod ethernet ipv4.dns "$DNS"
nmcli con mod ethernet ipv4.method manual

[[ "$DISABLE_WIFI" =~ ^[Yy]$ ]] && nmcli radio wifi off

loginctl enable-linger "$REAL_USER"

# ── Clean up PipeWire leftovers ───────────────────────────────────────────
rm -rf "$REAL_HOME/.config/wireplumber"  2>/dev/null || true
rm -rf "$REAL_HOME/.config/pipewire"     2>/dev/null || true

echo ""
echo "========================================="
echo " Setup complete!"
echo ""
echo " Services:"
echo "   snapserver       — Snapcast server (creates and reads /tmp/snapfifo)"
echo "   snapserver       — Snapcast server (reads FIFO)"
echo "   bt-init          — USB BT dongle init"
echo "   bt-agent         — auto-pair agent (started by bridge in BT mode)"
echo "   esp-bridge-server — server_bridge.py"
echo ""
echo " Input mode services (bridge-controlled, all disabled at boot):"
echo "   snapcast-source    — USB/UAC phone input"
echo "   snapcast-sourcemic — microphone input"
echo "   snapcast-sourcebt  — Bluetooth A2DP input (via PulseAudio)"
echo ""
echo " BT input mode flow:"
echo "   ESP sends MSG_INPUT_SET(2) → bridge starts PA + bt-agent + snapcast-sourcebt"
echo "   Phone pairs (auto) → A2DP source appears in PA"
echo "   Bridge polls every 2s, detects source, loads module-loopback:"
echo "     bluez_source.* → bt_snapcast_sink → parec → /tmp/snapfifo → snapserver"
echo ""
echo " Key files:"
echo "   /etc/snapserver.conf"
echo "   /etc/modprobe.d/g_audio.conf"
echo "   /etc/bluetooth/main.conf"
echo "   /etc/pulse/daemon.conf"
echo "   /etc/zone_password.hash"
echo "   ${BRIDGE_DIR}/"
echo ""
echo " Wiring:"
echo "   GPIO14 (TX) → ESP RX"
echo "   GPIO15 (RX) → ESP TX"
echo "   USB-C data splitter → phone (UAC gadget)"
echo ""
echo " Network:"
echo "   eth0 static: $STATIC_IP/$SUBNET  gw $GATEWAY"
echo ""
echo " REBOOT NOW to apply all changes:"
echo "   sudo reboot"
echo "========================================="
