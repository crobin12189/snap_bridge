#!/bin/bash
set -e

# ── Must run as root ──
if [ "$EUID" -ne 0 ]; then
    echo "Run with sudo: sudo ./opi_server_setup.sh"
    exit 1
fi

ORIG_USER="${SUDO_USER:-$(logname)}"

echo "========================================="
echo " Snapcast SERVER + ESP32 Bridge Setup"
echo " Orange Pi Zero 3 Edition"
echo "========================================="

echo ""
echo "--- New Admin User ---"
read -rp "New username: " NEW_USER
while true; do
    read -rsp "New password: " NEW_PASS; echo
    read -rsp "Confirm password: " NEW_PASS2; echo
    [ "$NEW_PASS" = "$NEW_PASS2" ] && break
    echo "Passwords do not match, try again."
done

echo ""
echo "--- Static IP Configuration for eth0 ---"
read -rp "Static IP address (e.g. 192.168.1.50): "  STATIC_IP
read -rp "Subnet prefix length (e.g. 24):           " SUBNET
read -rp "Gateway (e.g. 192.168.1.1):               " GATEWAY
read -rp "DNS server (e.g. 192.168.1.1 or 8.8.8.8):" DNS
read -rp "Disable WiFi after setup? [y/N]: "          DISABLE_WIFI
echo ""
echo "  New user: $NEW_USER"
echo "  IP:       $STATIC_IP/$SUBNET"
echo "  Gateway:  $GATEWAY"
echo "  DNS:      $DNS"
echo "  Disable WiFi: ${DISABLE_WIFI:-N}"
echo ""
read -rp "Confirm? [y/N]: " CONFIRM
[[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }

# ── Create new user with full sudo ────────────────────────────────────────
echo ""
echo "[0/13] Creating new admin user: $NEW_USER..."

useradd -m -s /bin/bash "$NEW_USER"
echo "$NEW_USER:$NEW_PASS" | chpasswd
usermod -aG sudo,bluetooth,dialout "$NEW_USER"
# Add pulse/gpio groups only if they exist (created after apt install)
for grp in pulse pulse-access gpio; do
    getent group "$grp" &>/dev/null && usermod -aG "$grp" "$NEW_USER" || true
done

# Give full passwordless sudo
echo "$NEW_USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/99-newadmin
chmod 440 /etc/sudoers.d/99-newadmin

# Copy SSH authorized keys if any
if [ -f "/home/$ORIG_USER/.ssh/authorized_keys" ]; then
    mkdir -p "/home/$NEW_USER/.ssh"
    cp "/home/$ORIG_USER/.ssh/authorized_keys" "/home/$NEW_USER/.ssh/"
    chown -R "$NEW_USER:$NEW_USER" "/home/$NEW_USER/.ssh"
    chmod 700 "/home/$NEW_USER/.ssh"
    chmod 600 "/home/$NEW_USER/.ssh/authorized_keys"
fi

REAL_USER="$NEW_USER"
REAL_HOME="/home/$NEW_USER"
USER_ID=$(id -u "$NEW_USER")

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
    ffmpeg \
    sox \
    dbus-user-session \
    dbus-x11

apt-mark hold pulseaudio pulseaudio-module-bluetooth pulseaudio-utils snapserver

# Now pulse groups exist — add new user to them
for grp in pulse pulse-access gpio; do
    getent group "$grp" &>/dev/null && usermod -aG "$grp" "$NEW_USER" || true
done

usermod -a -G bluetooth "$REAL_USER"
usermod -a -G dialout   "$REAL_USER"
usermod -a -G audio     "$REAL_USER"

# ── 3. D-Bus user session (fixes PulseAudio BT on OPi) ───────────────────
echo ""
echo "[3/14] Configuring D-Bus user session..."
loginctl enable-linger "$REAL_USER"

# Ensure systemd user session starts on boot and D-Bus socket exists
systemctl enable "user@${USER_ID}.service" 2>/dev/null || true

# Startup script that waits for D-Bus before starting PulseAudio
cat > /usr/local/bin/pulse-start.sh << PULSEEOF
#!/bin/bash
# Wait for D-Bus user session socket
for i in \$(seq 1 30); do
    if [ -S "/run/user/${USER_ID}/bus" ]; then
        break
    fi
    sleep 1
done

# Clean stale PID
rm -f "/run/user/${USER_ID}/pulse/pid"

export XDG_RUNTIME_DIR="/run/user/${USER_ID}"
export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/${USER_ID}/bus"
export PULSE_RUNTIME_PATH="/run/user/${USER_ID}/pulse"

# Start PulseAudio if not already running
if ! pulseaudio --check 2>/dev/null; then
    pulseaudio --start --log-target=journal
fi
PULSEEOF
chmod +x /usr/local/bin/pulse-start.sh

# Systemd service to reliably start PulseAudio after D-Bus
cat > /etc/systemd/system/pulseaudio-init.service << EOF
[Unit]
Description=PulseAudio Init (D-Bus aware)
After=user@${USER_ID}.service dbus.service
Wants=user@${USER_ID}.service

[Service]
Type=oneshot
User=$REAL_USER
ExecStart=/usr/local/bin/pulse-start.sh
Environment=XDG_RUNTIME_DIR=/run/user/${USER_ID}
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${USER_ID}/bus
Environment=PULSE_RUNTIME_PATH=/run/user/${USER_ID}/pulse
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable pulseaudio-init.service

# Add env vars to new user bashrc so manual SSH sessions also work
cat >> "$REAL_HOME/.bashrc" << 'BASHEOF'

# PulseAudio / D-Bus env
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$(id -u)/bus"
export PULSE_RUNTIME_PATH="/run/user/$(id -u)/pulse"
BASHEOF
chown "$REAL_USER:$REAL_USER" "$REAL_HOME/.bashrc"

# ── 4. PulseAudio ─────────────────────────────────────────────────────────
echo ""
echo "[4/14] Configuring PulseAudio..."

# Remove duplicate BT module lines if any
sed -i '/^load-module module-bluetooth-policy/d' /etc/pulse/default.pa
sed -i '/^load-module module-bluetooth-discover/d' /etc/pulse/default.pa

# Add BT modules properly at end
cat >> /etc/pulse/default.pa << 'EOF'
load-module module-bluetooth-policy
load-module module-bluetooth-discover
EOF

# PulseAudio systemd user service env fix for OPi
mkdir -p /etc/systemd/system/pulseaudio.service.d
cat > /etc/systemd/system/pulseaudio.service.d/override.conf << EOF
[Service]
Environment=XDG_RUNTIME_DIR=/run/user/${USER_ID}
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${USER_ID}/bus
Environment=PULSE_RUNTIME_PATH=/run/user/${USER_ID}/pulse
EOF

# ── 5. UAC gadget (libcomposite) ──────────────────────────────────────────
echo ""
echo "[5/14] Configuring USB Audio Class gadget..."

grep -q "^libcomposite$" /etc/modules 2>/dev/null || echo "libcomposite" >> /etc/modules

cat > /usr/local/bin/uac-gadget.sh << 'EOF'
#!/bin/bash
modprobe libcomposite

cd /sys/kernel/config/usb_gadget/
mkdir -p uac_gadget
cd uac_gadget

echo 0x1d6b > idVendor
echo 0x0104 > idProduct
echo 0x0100 > bcdDevice
echo 0x0200 > bcdUSB

mkdir -p strings/0x409
echo "12345678" > strings/0x409/serialnumber
echo "OrangePi" > strings/0x409/manufacturer
echo "USB Audio" > strings/0x409/product

mkdir -p configs/c.1/strings/0x409
echo "UAC" > configs/c.1/strings/0x409/configuration
echo 120 > configs/c.1/MaxPower

mkdir -p functions/uac2.usb0
echo 96000 > functions/uac2.usb0/c_srate
echo 3 > functions/uac2.usb0/c_chmask
echo 3 > functions/uac2.usb0/p_chmask
echo 4 > functions/uac2.usb0/c_ssize
echo 4 > functions/uac2.usb0/p_ssize

ln -sf functions/uac2.usb0 configs/c.1/ 2>/dev/null || true

UDC=$(ls /sys/class/udc | head -1)
echo $UDC > UDC

echo "UAC gadget enabled on $UDC"
EOF

chmod +x /usr/local/bin/uac-gadget.sh

cat > /etc/systemd/system/uac-gadget.service << 'EOF'
[Unit]
Description=USB UAC Audio Gadget
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/uac-gadget.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable uac-gadget

# ── 6. Snapserver ─────────────────────────────────────────────────────────
echo ""
echo "[6/14] Configuring Snapserver..."

test -p /tmp/snapfifo || mkfifo /tmp/snapfifo
chmod 777 /tmp/snapfifo
chown "$REAL_USER:$REAL_USER" /tmp/snapfifo

cat > /etc/tmpfiles.d/snapfifo.conf << EOF
p /tmp/snapfifo 0777 $REAL_USER $REAL_USER -
EOF

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

systemctl enable snapserver

# ── 7. Snapcast source services ───────────────────────────────────────────
echo ""
echo "[7/14] Installing Snapcast source services..."

# USB UAC source
cat > /etc/systemd/system/snapcast-source.service << SVCEOF
[Unit]
Description=PulseAudio UAC audio source to Snapcast FIFO
After=pulseaudio-init.service snapserver.service uac-gadget.service
Wants=pulseaudio-init.service

[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
Environment=PULSE_RUNTIME_PATH=/run/user/$USER_ID/pulse
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$USER_ID/bus
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo && chown $REAL_USER /tmp/snapfifo && chmod 777 /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        DEVICE=\$(pactl list sources short | grep "alsa_input.platform-musb" | awk "{print \\\$2}" | head -1); \\
        if [ -n "\$DEVICE" ]; then \\
            parec \\
                --device=\$DEVICE \\
                --format=s32le \\
                --rate=96000 \\
                --channels=2 \\
                --latency-msec=10 \\
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

# USB Mic source
cat > /etc/systemd/system/snapcast-sourcemic.service << SVCEOF
[Unit]
Description=PulseAudio mic audio source to Snapcast FIFO
After=pulseaudio-init.service snapserver.service
Wants=pulseaudio-init.service

[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
Environment=PULSE_RUNTIME_PATH=/run/user/$USER_ID/pulse
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$USER_ID/bus
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo && chown $REAL_USER /tmp/snapfifo && chmod 777 /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    while true; do \\
        DEVICE=\$(pactl list sources short | grep "alsa_input.usb" | grep -v monitor | grep -v "platform-" | awk "{print \\\$2}" | head -1); \\
        if [ -n "\$DEVICE" ]; then \\
            parec \\
                --device=\$DEVICE \\
                --format=s32le \\
                --rate=96000 \\
                --channels=2 \\
                --latency-msec=10 \\
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

# BT source
cat > /etc/systemd/system/snapcast-sourcebt.service << SVCEOF
[Unit]
Description=PulseAudio BT audio source to Snapcast FIFO
After=pulseaudio-init.service snapserver.service
Wants=pulseaudio-init.service

[Service]
Type=simple
User=$REAL_USER
Environment=XDG_RUNTIME_DIR=/run/user/$USER_ID
Environment=PULSE_RUNTIME_PATH=/run/user/$USER_ID/pulse
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$USER_ID/bus
ExecStartPre=/bin/bash -c 'test -p /tmp/snapfifo || mkfifo /tmp/snapfifo && chown $REAL_USER /tmp/snapfifo && chmod 777 /tmp/snapfifo'
ExecStart=/bin/bash -c '\\
    parec \\
        --device=bt_mix.monitor \\
        --format=s32le \\
        --rate=96000 \\
        --channels=2 \\
        --latency-msec=200 \\
    > /tmp/snapfifo'
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
SVCEOF

# All disabled at boot — bridge controls them
systemctl disable snapcast-source    2>/dev/null || true
systemctl disable snapcast-sourcemic 2>/dev/null || true
systemctl disable snapcast-sourcebt  2>/dev/null || true

# ── 8. Bluetooth ──────────────────────────────────────────────────────────
echo ""
echo "[8/14] Configuring Bluetooth..."

# Disable onboard BT (hci0) — keep only USB dongle (hci1)
# NOTE: Do NOT rfkill block 0 — it kills WiFi on OPi Zero 3!
# Instead disable via bluetoothd config
cat > /etc/udev/rules.d/99-bt-disable-onboard.rules << 'EOF'
# Disable onboard UART bluetooth, keep USB dongle only
ACTION=="add", SUBSYSTEM=="bluetooth", KERNEL=="hci0", RUN+="/usr/bin/hciconfig hci0 down"
EOF

udevadm control --reload-rules

# BlueZ main.conf
sed -i 's/^#*Class\s*=.*/Class = 0x41C/'                         /etc/bluetooth/main.conf
sed -i 's/^#*DiscoverableTimeout\s*=.*/DiscoverableTimeout = 0/' /etc/bluetooth/main.conf
sed -i 's/^#*PairableTimeout\s*=.*/PairableTimeout = 0/'         /etc/bluetooth/main.conf
sed -i 's/^#*AlwaysPairable\s*=.*/AlwaysPairable = true/'        /etc/bluetooth/main.conf
sed -i 's/^#*FastConnectable\s*=.*/FastConnectable = true/'       /etc/bluetooth/main.conf
sed -i 's/^#*AutoEnable\s*=.*/AutoEnable = true/'                 /etc/bluetooth/main.conf
sed -i 's/^#*JustWorksRepairing\s*=.*/JustWorksRepairing = always/' /etc/bluetooth/main.conf
grep -q "JustWorksRepairing" /etc/bluetooth/main.conf || \
    sed -i '/^\[General\]/a JustWorksRepairing = always' /etc/bluetooth/main.conf

# bt-init service — targets hci1 (USB dongle)
cat > /etc/systemd/system/bt-init.service << 'EOF'
[Unit]
Description=Bluetooth USB dongle init
After=bluetooth.service
Requires=bluetooth.service

[Service]
Type=oneshot
ExecStartPre=/bin/sleep 2
ExecStart=/usr/sbin/rfkill unblock bluetooth
ExecStart=/usr/bin/hciconfig hci1 up
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

# bt_agent.py — modified to prefer hci1
BRIDGE_DIR="/opt/esp-bridge"
mkdir -p "$BRIDGE_DIR"

cat > "$BRIDGE_DIR/bt_agent.py" << 'PYEOF'
#!/usr/bin/env python3
"""
bt_agent.py — Bluetooth pairing agent for OPi Zero 3.
Prefers hci1 (USB dongle). Auto-pairs NoInputNoOutput.
Removes bonding on disconnect for fresh pair every time.
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
    """Prefer hci1 (USB dongle) over hci0 (onboard)."""
    manager = dbus.Interface(bus.get_object(BUS_NAME, "/"),
                             "org.freedesktop.DBus.ObjectManager")
    objects = manager.GetManagedObjects()
    # First try hci1
    for path, ifaces in objects.items():
        if ADAPTER_IFACE in ifaces and "hci1" in path:
            return path, dbus.Interface(bus.get_object(BUS_NAME, path),
                                        "org.freedesktop.DBus.Properties")
    # Fallback to any adapter
    for path, ifaces in objects.items():
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
        props.Set(ADAPTER_IFACE, "Alias",               dbus.String(hostname))
        props.Set(ADAPTER_IFACE, "Powered",             dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "Discoverable",        dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "DiscoverableTimeout", dbus.UInt32(0))
        props.Set(ADAPTER_IFACE, "Pairable",            dbus.Boolean(True))
        props.Set(ADAPTER_IFACE, "PairableTimeout",     dbus.UInt32(0))
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


def remove_device(bus, device_path):
    """Remove bonding so next pair is always fresh."""
    try:
        adapter_path, _ = get_adapter(bus)
        if not adapter_path:
            return
        adapter = dbus.Interface(bus.get_object(BUS_NAME, adapter_path),
                                 "org.bluez.Adapter1")
        adapter.RemoveDevice(device_path)
        log.info("Removed device bonding: %s", device_path)
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
            log.info("Device disconnected: %s — removing bonding for fresh pair", path)
            threading.Thread(target=remove_device, args=(bus, path), daemon=True).start()


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
Description=Bluetooth Auth Agent (Python DBus)
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

# ── 9. ESP Bridge (server_bridge.py) ──────────────────────────────────────
echo ""
echo "[9/14] Setting up ESP32 UART bridge..."

wget -O "$BRIDGE_DIR/server_bridge.py" \
    "https://raw.githubusercontent.com/crobin12189/snap_bridge/main/server_bridge.py" || \
    echo "WARNING: download failed — place server_bridge.py in $BRIDGE_DIR manually"

chmod +x "$BRIDGE_DIR/server_bridge.py"

touch /etc/zone_password.hash
chown "$REAL_USER:$REAL_USER" /etc/zone_password.hash
chmod 640 /etc/zone_password.hash

# OPi Zero 3 UART: use UART5 on PH2/PH3 = /dev/ttyS5
# Make sure to enable it in orangepiEnv.txt if not already
grep -q "uart5" /boot/orangepiEnv.txt 2>/dev/null || \
    echo "overlays=uart5" >> /boot/orangepiEnv.txt

cat > /etc/systemd/system/esp-bridge-server.service << EOF
[Unit]
Description=Server ESP UART Bridge (Snapcast + BT input)
After=network.target snapserver.service bluetooth.service bt-init.service

[Service]
Type=simple
User=$REAL_USER
ExecStart=/usr/bin/python3 ${BRIDGE_DIR}/server_bridge.py \
    --port /dev/ttyS5 \
    --baud 460800
Restart=always
RestartSec=3
Environment=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/${USER_ID}/bus
Environment=XDG_RUNTIME_DIR=/run/user/${USER_ID}
Environment=PULSE_RUNTIME_PATH=/run/user/${USER_ID}/pulse

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable esp-bridge-server.service

# ── 10. Sudoers ───────────────────────────────────────────────────────────
echo ""
echo "[10/14] Configuring sudoers..."

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

# ── 11. Avahi mDNS ────────────────────────────────────────────────────────
echo ""
echo "[11/14] Configuring Avahi..."

sed -i 's/^#*use-ipv6\s*=.*/use-ipv6=no/' /etc/avahi/avahi-daemon.conf || \
    sed -i '/^\[server\]/a use-ipv6=no' /etc/avahi/avahi-daemon.conf

# ── 12. Static IP via NetworkManager ─────────────────────────────────────
echo ""
echo "[12/14] Configuring static IP for eth0..."

nmcli con add type ethernet ifname eth0 con-name ethernet \
    ip4 "$STATIC_IP/$SUBNET" gw4 "$GATEWAY" 2>/dev/null || \
nmcli con mod ethernet \
    ipv4.addresses "$STATIC_IP/$SUBNET" \
    ipv4.gateway "$GATEWAY" \
    ipv4.method manual

nmcli con mod ethernet ipv4.dns "$DNS"

# ── 13. PulseAudio linger + cleanup ──────────────────────────────────────
echo ""
echo "[13/14] Finalizing PulseAudio and linger..."

loginctl enable-linger "$REAL_USER"
rm -rf "$REAL_HOME/.config/wireplumber" 2>/dev/null || true
rm -rf "$REAL_HOME/.config/pipewire"    2>/dev/null || true

# ── 14. Disable WiFi + remove orangepi user ───────────────────────────────
echo ""
echo "[14/14] Cleanup..."

# Disable WiFi if requested
if [[ "$DISABLE_WIFI" =~ ^[Yy]$ ]]; then
    echo "Disabling WiFi..."
    nmcli radio wifi off
    # Persist via NetworkManager
    cat > /etc/NetworkManager/conf.d/disable-wifi.conf << 'EOF'
[device]
wifi.scan-rand-mac-address=no

[main]
wifi.backend=none
EOF
    # Also remove wlan0 from /etc/network/interfaces if present
    if [ -f /etc/network/interfaces ]; then
        sed -i '/auto wlan0/d' /etc/network/interfaces
        sed -i '/iface wlan0/d' /etc/network/interfaces
        sed -i '/wpa-conf/d' /etc/network/interfaces
    fi
    # Remove wpa_supplicant conf
    rm -f /etc/wpa_supplicant/wpa_supplicant.conf
    echo "WiFi disabled."
fi

# Remove original orangepi user (but NOT if it's the same as new user)
if [ "$ORIG_USER" != "$REAL_USER" ] && id "$ORIG_USER" &>/dev/null; then
    echo "Removing original user: $ORIG_USER..."
    # Kill any running processes by that user
    pkill -u "$ORIG_USER" 2>/dev/null || true
    sleep 1
    userdel -r "$ORIG_USER" 2>/dev/null || userdel "$ORIG_USER" 2>/dev/null || true
    # Remove sudoers entry if any
    rm -f "/etc/sudoers.d/$ORIG_USER" 2>/dev/null || true
    echo "User $ORIG_USER removed."
fi

echo ""
echo "========================================="
echo " Setup complete! Orange Pi Zero 3"
echo ""
echo " Admin user:  $REAL_USER (orangepi user removed)"
echo " SSH login:   ssh $REAL_USER@$STATIC_IP"
echo ""
echo " Services enabled:"
echo "   uac-gadget         — USB UAC audio gadget"
echo "   snapserver         — Snapcast server (reads /tmp/snapfifo)"
echo "   bt-init            — USB BT dongle (hci1) init"
echo "   bt-agent           — auto-pair agent"
echo "   esp-bridge-server  — server_bridge.py on /dev/ttyS5"
echo ""
echo " Input mode services (bridge-controlled, disabled at boot):"
echo "   snapcast-source    — USB/UAC phone input"
echo "   snapcast-sourcemic — microphone input"
echo "   snapcast-sourcebt  — Bluetooth A2DP input"
echo ""
echo " Key OPi Zero 3 differences vs RPi:"
echo "   - No /boot/firmware/config.txt — uses /boot/orangepiEnv.txt"
echo "   - UART5 on PH2(TX)/PH3(RX) = /dev/ttyS5"
echo "   - hci0 = onboard (disabled via udev), hci1 = USB dongle"
echo "   - UAC via libcomposite (musb-hdrc OTG), not dwc2"
echo "   - dbus-user-session required for PulseAudio BT"
echo ""
echo " Wiring (OPi Zero 3 pinout):"
echo "   PH2 (TX, pin on header) → ESP32 RX"
echo "   PH3 (RX, pin on header) → ESP32 TX"
echo "   GND → ESP32 GND"
echo "   USB-C → phone (UAC gadget)"
echo ""
echo " Network:"
echo "   eth0 static: $STATIC_IP/$SUBNET  gw $GATEWAY"
if [[ "$DISABLE_WIFI" =~ ^[Yy]$ ]]; then
echo "   WiFi: DISABLED"
fi
echo ""
echo " REBOOT NOW — then SSH as $REAL_USER:"
echo "   sudo reboot"
echo "========================================="
