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
read -rp "Disable WiFi? [y/N]: " DISABLE_WIFI
read -rp "Server Pi IP address (e.g. 192.168.1.100): " SERVER_IP
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

# ── 1. Update package lists ──
echo ""
echo "[1/12] Updating package lists..."
apt update

# ── 2. Install packages (pinned versions) ──
echo ""
echo "[2/12] Installing packages..."
apt install -y \
    pulseaudio=16.1+dfsg1-2+rpt1.1 \
    pulseaudio-module-bluetooth=16.1+dfsg1-2+rpt1.1 \
    pulseaudio-utils=16.1+dfsg1-2+rpt1.1 \
    snapclient=0.26.0+dfsg1-1+deb12u1 \
    python3-serial=3.5-1.1 \
    python3-dbus \
    python3-gi \
    avahi-daemon \
    git \
    gpiod \
    python3-dev

# Prevent auto-upgrades
apt-mark hold \
    pulseaudio pulseaudio-module-bluetooth pulseaudio-utils \
    snapclient python3-serial

# Add user to bluetooth, pulse and dialout groups
usermod -a -G bluetooth "$REAL_USER"
usermod -a -G dialout "$REAL_USER"
usermod -a -G pulse "$REAL_USER"
usermod -a -G pulse-access "$REAL_USER"
usermod -a -G gpio "$REAL_USER"

# ── 3. config.txt — UART, I2S, disable onboard BT ──
echo ""
echo "[3/12] Configuring /boot/firmware/config.txt..."

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

# Disable onboard Bluetooth — using USB dongle only
dtoverlay=disable-bt
dtoverlay=dwc2,dr_mode=host
CFGEOF

# ── 4. Free UART from serial console ──
echo ""
echo "[4/12] Freeing UART from serial console..."

sed -i 's/console=serial0,[0-9]* //' /boot/firmware/cmdline.txt
systemctl disable serial-getty@ttyAMA0.service 2>/dev/null || true


# ── 5. Configure PulseAudio — user mode, 96kHz/32-bit ──
echo ""
echo "[5/12] Configuring PulseAudio..."

# Remove any existing custom config to avoid duplicates
sed -i '/^# Client audio config$/,/^resample-method/d' /etc/pulse/daemon.conf

# Disable autospawn — bridge controls PA start/stop
echo "autospawn = no" >> /etc/pulse/client.conf

cat >> /etc/pulse/daemon.conf << 'EOF'

# Client audio config
default-sample-format = s32le
default-sample-rate = 96000
alternate-sample-rate = 48000
default-sample-channels = 2
resample-method = speex-float-5
EOF

# Remove loopback from default.pa — bridge handles it dynamically
sed -i '/module-loopback/d' /etc/pulse/default.pa
sed -i '/module-switch-on-connect/d' /etc/pulse/default.pa

# Disable PipeWire if installed (we use PulseAudio)
sudo -u "$REAL_USER" systemctl --user disable pipewire.service pipewire.socket \
    pipewire-pulse.service pipewire-pulse.socket wireplumber.service 2>/dev/null || true
sudo -u "$REAL_USER" systemctl --user mask pipewire.service pipewire.socket \
    pipewire-pulse.service pipewire-pulse.socket wireplumber.service 2>/dev/null || true

# Enable user PulseAudio
sudo -u "$REAL_USER" systemctl --user unmask pulseaudio.service pulseaudio.socket 2>/dev/null || true
sudo -u "$REAL_USER" systemctl --user enable pulseaudio 2>/dev/null || true

# Enable linger so user services start at boot without login
loginctl enable-linger "$REAL_USER"

# ── 6. Console autologin ──
echo ""
echo "[6/12] Enabling console autologin..."

raspi-config nonint do_boot_behaviour B2

# ── 7. Bluetooth — USB dongle, always discoverable, auto-pair, A2DP ──
echo ""
echo "[7/12] Configuring Bluetooth..."

# BlueZ main.conf
sed -i 's/^#*Class\s*=.*/Class = 0x41C/' /etc/bluetooth/main.conf
sed -i 's/^#*DiscoverableTimeout\s*=.*/DiscoverableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*PairableTimeout\s*=.*/PairableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*AlwaysPairable\s*=.*/AlwaysPairable = true/' /etc/bluetooth/main.conf
sed -i 's/^#*FastConnectable\s*=.*/FastConnectable = true/' /etc/bluetooth/main.conf
sed -i 's/^#*AutoEnable\s*=.*/AutoEnable = true/' /etc/bluetooth/main.conf

# Remove Disable=Headset if present (breaks pairing on some phones)
sed -i '/^Disable=Headset/d' /etc/bluetooth/main.conf

# Add Justworksrepairing always so bt pairing is seamless
sed -i 's/^#*JustWorksRepairing\s*=.*/JustWorksRepairing = always/' /etc/bluetooth/main.conf
grep -q "JustWorksRepairing" /etc/bluetooth/main.conf || sed -i '/^\[General\]/a JustWorksRepairing = always' /etc/bluetooth/main.conf

# USB dongle init service — bring hci0 up after bluetooth service
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

# Python DBus BT agent — replaces bt-agent binary
mkdir -p /opt/esp-bridge

cat > /opt/esp-bridge/bt_agent.py << 'PYEOF'
#!/usr/bin/env python3
"""
bt_agent.py — Reliable Bluetooth pairing agent using DBus directly.
Auto-pair NoInputNoOutput, auto-trust, keeps adapter discoverable.
Removes device bonding on disconnect so next pair is always fresh.
"""

import dbus
import dbus.service
import dbus.mainloop.glib
from gi.repository import GLib
import logging
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

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="", out_signature="")
    def Release(self):
        log.info("Agent released")

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="os", out_signature="")
    def AuthorizeService(self, device, uuid):
        log.info("AuthorizeService: %s uuid=%s — auto-authorized", device, uuid)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="o", out_signature="s")
    def RequestPinCode(self, device):
        log.info("RequestPinCode: %s — returning 0000", device)
        return "0000"

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="o", out_signature="u")
    def RequestPasskey(self, device):
        log.info("RequestPasskey: %s — returning 0", device)
        return dbus.UInt32(0)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="ouq", out_signature="")
    def DisplayPasskey(self, device, passkey, entered):
        log.info("DisplayPasskey: %s passkey=%d entered=%d", device, passkey, entered)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="os", out_signature="")
    def DisplayPinCode(self, device, pincode):
        log.info("DisplayPinCode: %s pin=%s", device, pincode)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="ou", out_signature="")
    def RequestConfirmation(self, device, passkey):
        log.info("RequestConfirmation: %s passkey=%d — auto-confirmed", device, passkey)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="o", out_signature="")
    def RequestAuthorization(self, device):
        log.info("RequestAuthorization: %s — auto-authorized", device)

    @dbus.service.method(dbus_interface="org.bluez.Agent1",
                         in_signature="", out_signature="")
    def Cancel(self):
        log.info("Pairing cancelled")


def get_adapter(bus):
    manager = dbus.Interface(
        bus.get_object(BUS_NAME, "/"),
        "org.freedesktop.DBus.ObjectManager"
    )
    objects = manager.GetManagedObjects()
    for path, ifaces in objects.items():
        if ADAPTER_IFACE in ifaces:
            return path, dbus.Interface(
                bus.get_object(BUS_NAME, path),
                "org.freedesktop.DBus.Properties"
            )
    return None, None


def setup_adapter(bus):
    path, props = get_adapter(bus)
    if not props:
        log.error("No Bluetooth adapter found!")
        return False
    try:
        props.Set(ADAPTER_IFACE, "Powered", dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "Discoverable", dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "DiscoverableTimeout", dbus.UInt32(0))
        props.Set(ADAPTER_IFACE, "Pairable", dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "PairableTimeout", dbus.UInt32(0))
        log.info("Adapter %s: powered=on, discoverable=on, pairable=on", path)
        return True
    except Exception as e:
        log.error("Adapter setup failed: %s", e)
        return False


def register_agent(bus):
    agent_mgr = dbus.Interface(
        bus.get_object(BUS_NAME, "/org/bluez"),
        AGENT_MGR_IFACE
    )
    agent_mgr.RegisterAgent(AGENT_PATH, AGENT_CAPABILITY)
    agent_mgr.RequestDefaultAgent(AGENT_PATH)
    log.info("Agent registered as default (%s)", AGENT_CAPABILITY)


def trust_device(bus, device_path):
    try:
        props = dbus.Interface(
            bus.get_object(BUS_NAME, device_path),
            "org.freedesktop.DBus.Properties"
        )
        props.Set(DEVICE_IFACE, "Trusted", dbus.Boolean(True))
        log.info("Trusted device: %s", device_path)
    except Exception as e:
        log.warning("Could not trust device %s: %s", device_path, e)


def remove_device(bus, device_path):
    try:
        adapter = dbus.Interface(
            bus.get_object(BUS_NAME, "/org/bluez/hci0"),
            "org.bluez.Adapter1"
        )
        adapter.RemoveDevice(device_path)
        log.info("Removed device %s after disconnect", device_path)
    except Exception as e:
        log.warning("Could not remove device %s: %s", device_path, e)


def on_properties_changed(interface, changed, invalidated, path, bus):
    if interface != DEVICE_IFACE:
        return
    if "Paired" in changed and changed["Paired"]:
        log.info("Device paired: %s — trusting", path)
        trust_device(bus, path)
    if "Connected" in changed:
        if changed["Connected"]:
            log.info("Device connected: %s", path)
        else:
            log.info("Device disconnected: %s", path)


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
        log.info("Shutting down")
        loop.quit()


if __name__ == "__main__":
    main()
PYEOF

chmod +x /opt/esp-bridge/bt_agent.py

# bt-agent service using Python DBus agent
cat > /etc/systemd/system/bt-agent.service << 'EOF'
[Unit]
Description=Bluetooth Auth Agent (Python DBus)
After=bluetooth.service bt-init.service
Requires=bluetooth.service bt-init.service

[Service]
Type=simple
ExecStart=/usr/bin/python3 /opt/esp-bridge/bt_agent.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable bt-init.service
systemctl enable bt-agent.service

# ── 8. Snapclient — 96kHz/32-bit ALSA ──
echo ""
echo "[8/12] Configuring Snapclient..."

cat > /etc/default/snapclient << 'EOF'
START_SNAPCLIENT=true
SNAPCLIENT_OPTS="--sampleformat 96000:32:* --player alsa"
EOF

systemctl enable snapclient

# ── 9. ESP Bridge ──
echo ""
echo "[9/12] Setting up ESP Bridge..."

BRIDGE_DIR="/opt/esp-bridge"
mkdir -p "$BRIDGE_DIR"

# Download bridge script from GitHub
wget -O "$BRIDGE_DIR/client_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/client_bridge.py"

chmod +x "$BRIDGE_DIR/client_bridge.py"

# ── Password hash file ──
touch /etc/zone_password.hash
chown "$REAL_USER:$REAL_USER" /etc/zone_password.hash
chmod 640 /etc/zone_password.hash

cat > /etc/systemd/system/esp-bridge.service << SVCEOF
[Unit]
Description=ESP UART Bridge (Snapcast + BT)
After=network.target bluetooth.service bt-init.service
Wants=bluetooth.service

[Service]
Type=simple
User=$REAL_USER
ExecStart=/usr/bin/python3 $BRIDGE_DIR/client_bridge.py --port /dev/ttyAMA0 --baud 460800 --server-ip $SERVER_IP
Restart=always
RestartSec=3
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$USER_ID/bus
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable esp-bridge.service

# ── 10. Sudo permissions ──
echo ""
echo "[10/12] Configuring sudoers..."

cat > /etc/sudoers.d/esp-bridge << SUDOEOF
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl start snapclient
$REAL_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop snapclient
$REAL_USER ALL=(ALL) NOPASSWD: /usr/bin/hostnamectl set-hostname *
SUDOEOF
chmod 440 /etc/sudoers.d/esp-bridge

# ── 11. Enable user linger (already done above, ensure it's set) ──
echo ""
echo "[11/12] Enabling user linger..."
loginctl enable-linger "$REAL_USER"

# ── 12. Avahi mDNS — IPv4 only ──
echo ""
echo "[12/12] Configuring Avahi for IPv4 only..."

if grep -q "^use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#*use-ipv6\s*=.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
elif grep -q "^#.*use-ipv6" /etc/avahi/avahi-daemon.conf; then
    sed -i 's/^#.*use-ipv6.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf
else
    sed -i '/^\[server\]/a use-ipv6=no' /etc/avahi/avahi-daemon.conf
fi

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

# ── Clean up PipeWire leftovers ──
rm -rf "$REAL_HOME/.config/wireplumber" 2>/dev/null || true
rm -rf "$REAL_HOME/.config/pipewire" 2>/dev/null || true
rm -rf "$REAL_HOME/.config/systemd/user/wireplumber.service.d" 2>/dev/null || true

echo ""
echo "========================================="
echo " Setup complete!"
echo ""
echo " Services installed:"
echo "   - pulseaudio (user mode, BT mode only — bridge controls)"
echo "   - bt-init (USB dongle init)"
echo "   - bt-agent (Python DBus auto-pair, no PIN, auto-removes on disconnect)"
echo "   - snapclient (multiroom audio, sync mode)"
echo "   - esp-bridge (ESP32 UART communication)"
echo ""
echo " Key behaviors:"
echo "   - Sync mode: PulseAudio OFF, snapclient ON (ALSA direct at 96kHz)"
echo "   - BT mode:   PulseAudio ON, snapclient OFF, loopback loaded by bridge"
echo "   - BT agent auto-pairs any device, removes bonding on disconnect"
echo ""
echo " Config files:"
echo "   /etc/pulse/daemon.conf"
echo "   /etc/pulse/client.conf"
echo "   /etc/bluetooth/main.conf"
echo "   /etc/default/snapclient"
echo "   /etc/avahi/avahi-daemon.conf"
echo "   /etc/zone_password.hash"
echo ""
echo " Bridge scripts: $BRIDGE_DIR/"
echo ""
echo " Wiring:"
echo "   GPIO14 (TX) → ESP RX"
echo "   GPIO15 (RX) → ESP TX"
echo "   I2S: GPIO18 (BCLK), GPIO19 (LRCLK), GPIO21 (DOUT)"
echo "   DAC SD_MODE → 3.3V"
echo ""
echo " Network:"
echo "   eth0 static IP: $STATIC_IP/$SUBNET"
echo "   Gateway:        $GATEWAY"
echo "   DNS:            $DNS"
echo ""
echo " REBOOT NOW to apply all changes:"
echo "   sudo reboot"
echo "========================================="
