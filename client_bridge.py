#!/usr/bin/env python3
"""
client_bridge.py — Pi CLIENT-side daemon (460800 baud)
"""

import argparse
import hashlib
import json
import logging
import os
import re
import select
import socket
import subprocess
import threading
import time
from typing import Optional

import serial
import dbus
import dbus.mainloop.glib
try:
    from gi.repository import GObject as gobject
except ImportError:
    import gobject

import shutil
GPIO_AVAILABLE = bool(shutil.which("gpioset") and shutil.which("gpioget"))
if not GPIO_AVAILABLE:
    logging.warning("gpioset/gpioget not found — GPIO control disabled")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bridge")

SYNC_0, SYNC_1 = 0xAA, 0x55
MAX_PAYLOAD     = 2048

MSG_INIT            = 0x01
MSG_VOL_SET         = 0x02
MSG_PING            = 0x04
MSG_MODE_SYNC       = 0x05
MSG_MODE_BT         = 0x06
MSG_POWER_SET       = 0x07
MSG_DSP_SET         = 0x08
MSG_AMP_SET         = 0x09
MSG_ACK             = 0x10
MSG_CLIENT_VOL_UPD  = 0x12
MSG_PONG            = 0x13
MSG_STATE_UPDATE    = 0x20
MSG_MODE_SWITCHING  = 0x21
MSG_PW_CHECK        = 0x35
MSG_PW_RESULT       = 0x36
MSG_RENAME          = 0x33

MODE_SYNC, MODE_BT = 0, 1
DEVICE_NAME_LEN    = 32
STATE_UPDATE_SIZE  = 68
ACK_PAYLOAD_SIZE   = 33

SNAPCLIENT_SERVICE = "snapclient"
SNAP_RPC_PORT      = 1705
PW_BROADCAST_PORT  = 7700
SERVER_CTRL_PORT   = 7702
PW_HASH_FILE       = "/etc/zone_password.hash"
PW_DEFAULT         = "anjay1234"

GPIO_DSP   = 17
GPIO_AMP   = 27
GPIO_LED   = 26
GPIO_DELAY = 30.0
GPIO_CHIP  = "gpiochip0"

_gpio_state: dict = {GPIO_DSP: False, GPIO_AMP: False}

REGISTER_RETRY_S = 5
PING_TIMEOUT_S   = 15


def gpio_set(pin: int, state: bool):
    if not GPIO_AVAILABLE:
        log.info("GPIO%d -> %s (simulated)", pin, "HIGH" if state else "LOW")
        _gpio_state[pin] = state
        return
    val = 1 if state else 0
    try:
        subprocess.run(["gpioset", GPIO_CHIP, f"{pin}={val}"],
                       timeout=3, capture_output=True, check=True)
        _gpio_state[pin] = state
        log.info("GPIO%d -> %s", pin, "HIGH" if state else "LOW")
    except Exception as e:
        log.error("gpio_set GPIO%d failed: %s", pin, e)
    
    # Write power state for fan controller                                                                        # Write power state for fan controller
    try:
        dsp = _gpio_state.get(GPIO_DSP, False)
        amp = _gpio_state.get(GPIO_AMP, False)
        with open("/tmp/power_state", "w") as f:
            f.write(f"{int(dsp)} {int(amp)}\n")
    except Exception:
        pass

    try:
        dsp = _gpio_state.get(GPIO_DSP, False)
        amp = _gpio_state.get(GPIO_AMP, False)
        led_val = 1 if (dsp and amp) else 0
        subprocess.run(["gpioset", GPIO_CHIP, f"{GPIO_LED}={led_val}"],
                       timeout=3, capture_output=True)
    except Exception:
        pass


def gpio_get(pin: int) -> bool:
    return _gpio_state.get(pin, False)


def gpio_cleanup():
    if not GPIO_AVAILABLE:
        return
    for pin in (GPIO_DSP, GPIO_AMP, GPIO_LED):
        try:
            subprocess.run(["gpioset", GPIO_CHIP, f"{pin}=0"],
                           timeout=3, capture_output=True)
        except Exception:
            pass


def crc8(data: bytes) -> int:
    crc = 0x00
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = ((crc << 1) ^ 0x31) & 0xFF if crc & 0x80 else (crc << 1) & 0xFF
    return crc


def build_frame(msg_type: int, payload: bytes = b"") -> bytes:
    plen = len(payload)
    header = bytes([SYNC_0, SYNC_1, msg_type, plen & 0xFF, (plen >> 8) & 0xFF])
    crc_data = bytes([msg_type, plen & 0xFF, (plen >> 8) & 0xFF]) + payload
    return header + payload + bytes([crc8(crc_data)])


def pad(s: str, length: int) -> bytes:
    return s.encode("utf-8")[:length].ljust(length, b"\x00")


def sha256_hex(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def load_or_init_password() -> str:
    if os.path.exists(PW_HASH_FILE):
        with open(PW_HASH_FILE, "r") as f:
            h = f.read().strip()
        if len(h) == 64:
            return h
    h = sha256_hex(PW_DEFAULT)
    save_password_hash(h)
    return h


def save_password_hash(h: str):
    with open(PW_HASH_FILE, "w") as f:
        f.write(h)


def fetch_hash_from_server(server_ip: str) -> Optional[str]:
    try:
        with socket.create_connection((server_ip, 7701), timeout=5) as s:
            data = s.recv(64)
        if len(data) == 64:
            return data.decode("ascii", errors="replace")
    except OSError as e:
        log.warning("Hash pull failed: %s", e)
    return None


def get_hostname() -> str:
    return socket.gethostname()


class UARTReceiver:
    SYNC_0_ST = 0; SYNC_1_ST = 1; HEADER_ST = 2; PAYLOAD_ST = 3; CRC_ST = 4

    def __init__(self):
        self.state       = self.SYNC_0_ST
        self.header_buf  = bytearray(3)
        self.header_idx  = 0
        self.msg_type    = 0
        self.payload_len = 0
        self.payload_buf = bytearray(MAX_PAYLOAD)
        self.payload_idx = 0

    def feed(self, data: bytes):
        for byte in data:
            if self.state == self.SYNC_0_ST:
                if byte == SYNC_0: self.state = self.SYNC_1_ST
            elif self.state == self.SYNC_1_ST:
                if byte == SYNC_1:
                    self.state = self.HEADER_ST; self.header_idx = 0
                elif byte != SYNC_0:
                    self.state = self.SYNC_0_ST
            elif self.state == self.HEADER_ST:
                self.header_buf[self.header_idx] = byte
                self.header_idx += 1
                if self.header_idx == 3:
                    self.msg_type    = self.header_buf[0]
                    self.payload_len = self.header_buf[1] | (self.header_buf[2] << 8)
                    if self.payload_len > MAX_PAYLOAD:
                        self.state = self.SYNC_0_ST
                    elif self.payload_len == 0:
                        self.state = self.CRC_ST
                    else:
                        self.payload_idx = 0; self.state = self.PAYLOAD_ST
            elif self.state == self.PAYLOAD_ST:
                self.payload_buf[self.payload_idx] = byte
                self.payload_idx += 1
                if self.payload_idx == self.payload_len:
                    self.state = self.CRC_ST
            elif self.state == self.CRC_ST:
                crc_data = bytes([self.msg_type,
                                  self.payload_len & 0xFF,
                                  (self.payload_len >> 8) & 0xFF,
                                  ]) + bytes(self.payload_buf[:self.payload_len])
                if byte == crc8(crc_data):
                    yield (self.msg_type, bytes(self.payload_buf[:self.payload_len]))
                else:
                    log.warning("CRC mismatch")
                self.state = self.SYNC_0_ST


class SnapcastRPC:
    def __init__(self):
        self.sock: Optional[socket.socket] = None
        self._recv_buf = b""
        self._req_id   = 0
        self._lock     = threading.Lock()
        self.pending_notifications = []

    def connect(self, host: str, port: int = SNAP_RPC_PORT) -> bool:
        self.disconnect()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(3)
            self.sock.connect((host, port))
            self.sock.setblocking(False)
            self._recv_buf = b""
            log.info("RPC connected to %s:%d", host, port)
            return True
        except OSError as e:
            log.warning("RPC connect failed: %s", e)
            self.sock = None
            return False

    def disconnect(self):
        if self.sock:
            try: self.sock.close()
            except Exception: pass
            self.sock = None
        self._recv_buf = b""
        self.pending_notifications.clear()

    @property
    def connected(self):
        return self.sock is not None

    def _send_request(self, method: str, params: dict) -> Optional[dict]:
        sock = self.sock
        if not sock:
            return None
        with self._lock:
            self._req_id += 1
            req_id = self._req_id
        msg = json.dumps({"id": req_id, "jsonrpc": "2.0",
                          "method": method, "params": params}) + "\r\n"
        try:
            sock.sendall(msg.encode())
        except OSError as e:
            log.error("RPC send failed: %s", e)
            self.disconnect()
            return None
        deadline = time.time() + 5.0
        while time.time() < deadline:
            try:
                ready, _, _ = select.select([sock], [], [], 0.5)
            except (ValueError, OSError):
                self.disconnect()
                return None
            if ready:
                try:
                    data = sock.recv(4096)
                except OSError as e:
                    log.error("RPC recv failed: %s", e)
                    self.disconnect()
                    return None
                if not data:
                    self.disconnect()
                    return None
                self._recv_buf += data
                while b"\n" in self._recv_buf:
                    line, self._recv_buf = self._recv_buf.split(b"\n", 1)
                    line = line.strip()
                    if not line: continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("id") == req_id:
                        return obj.get("result", {})
                    if "method" in obj and "id" not in obj:
                        self.pending_notifications.append(obj)
        log.warning("RPC timeout for %s", method)
        return None

    def get_status(self) -> Optional[dict]:
        return self._send_request("Server.GetStatus", {})

    def set_volume(self, client_id: str, percent: int):
        return self._send_request("Client.SetVolume", {
            "id": client_id,
            "volume": {"percent": max(0, min(100, percent))},
        })

    def find_client_id_by_hostname(self, hostname: str) -> Optional[str]:
        status = self.get_status()
        if not status:
            return None
        for group in status.get("server", {}).get("groups", []):
            for c in group.get("clients", []):
                if not c.get("connected", False):
                    continue
                if c.get("host", {}).get("name", "").lower() == hostname.lower():
                    cid = c.get("id", "")
                    log.info("Found client ID for '%s': %s", hostname, cid)
                    return cid
        log.warning("No connected client found for hostname '%s'", hostname)
        return None

    def get_volume_for_client(self, client_id: str) -> Optional[int]:
        status = self.get_status()
        if not status:
            return None
        for group in status.get("server", {}).get("groups", []):
            for c in group.get("clients", []):
                if c.get("id") == client_id:
                    return c.get("config", {}).get("volume", {}).get("percent", 0)
        return None

    def read_notifications(self) -> list:
        notifications = list(self.pending_notifications)
        self.pending_notifications.clear()
        if not self.sock:
            return notifications
        try:
            ready, _, _ = select.select([self.sock], [], [], 0)
            if ready:
                data = self.sock.recv(4096)
                if not data:
                    self.disconnect()
                    return notifications
                self._recv_buf += data
        except (BlockingIOError, OSError):
            pass
        while b"\n" in self._recv_buf:
            line, self._recv_buf = self._recv_buf.split(b"\n", 1)
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "method" in obj and "id" not in obj:
                notifications.append(obj)
        return notifications


def get_snapserver_ip() -> Optional[str]:
    try:
        result = subprocess.run(
            ["journalctl", "-u", SNAPCLIENT_SERVICE, "--no-pager", "-o", "cat"],
            capture_output=True, text=True, timeout=10)
        server_ip = None
        for line in result.stdout.splitlines():
            m = re.search(r"Connected to\s+(\d+\.\d+\.\d+\.\d+)", line)
            if m:
                server_ip = m.group(1)
        if server_ip:
            log.info("Found snapserver IP: %s", server_ip)
        return server_ip
    except Exception as e:
        log.error("Failed to read snapclient journal: %s", e)
        return None


def bt_disconnect_all():
    try:
        result = subprocess.run(["bluetoothctl", "devices", "Connected"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                subprocess.run(["bluetoothctl", "disconnect", parts[1]],
                               timeout=5, capture_output=True)
    except Exception as e:
        log.error("bt_disconnect_all error: %s", e)


def bt_start_discoverable():
    for cmd in [["bluetoothctl", "power", "on"],
                ["bluetoothctl", "discoverable", "on"],
                ["bluetoothctl", "pairable", "on"]]:
        try:
            subprocess.run(cmd, timeout=5, capture_output=True)
        except Exception:
            pass


def bt_stop_discoverable():
    try:
        subprocess.run(["bluetoothctl", "discoverable", "off"],
                       timeout=5, capture_output=True)
    except Exception:
        pass


def bt_get_connected_device() -> tuple[bool, str]:
    """Returns (connected, name)."""
    try:
        result = subprocess.run(["bluetoothctl", "info"],
                                capture_output=True, text=True, timeout=5)
        if "Connected: yes" in result.stdout:
            for line in result.stdout.splitlines():
                if "Name:" in line:
                    return True, line.split("Name:", 1)[1].strip()
            return True, ""
        return False, ""
    except Exception:
        return False, ""


def _find_bt_source_name() -> Optional[str]:
    try:
        result = subprocess.run(["pactl", "list", "sources", "short"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2 and "bluez_source" in parts[1] and "a2dp_source" in parts[1]:
                return parts[1]
    except Exception as e:
        log.warning("Could not list pactl sources: %s", e)
    return None


def bt_get_source_volume() -> Optional[int]:
    source = _find_bt_source_name()
    if not source:
        return None
    try:
        result = subprocess.run(["pactl", "get-source-volume", source],
                                capture_output=True, text=True, timeout=3)
        m = re.search(r"(\d+)%", result.stdout)
        if m:
            return int(m.group(1))
    except Exception as e:
        log.warning("bt_get_source_volume error: %s", e)
    return None


def bt_set_source_volume(percent: int) -> bool:
    source = _find_bt_source_name()
    if not source:
        log.warning("bt_set_source_volume: no A2DP source found (paused?)")
        return False
    try:
        subprocess.run(["pactl", "set-source-volume", source, f"{percent}%"],
                       capture_output=True, timeout=3, check=True)
        log.info("AVRCP volume set: %s -> %d%%", source, percent)
        return True
    except Exception as e:
        log.warning("bt_set_source_volume error: %s", e)
        return False


def _find_bt_transport_path() -> Optional[str]:
    """Find the current A2DP MediaTransport1 object path in BlueZ DBus tree."""
    try:
        bus = dbus.SystemBus()
        mgr = dbus.Interface(
            bus.get_object("org.bluez", "/"),
            "org.freedesktop.DBus.ObjectManager")
        objects = mgr.GetManagedObjects()
        for path, ifaces in objects.items():
            if "org.bluez.MediaTransport1" in ifaces:
                return str(path)
    except Exception as e:
        log.warning("_find_bt_transport_path error: %s", e)
    return None


def bt_dbus_get_volume() -> Optional[int]:
    """
    Read AVRCP volume directly from BlueZ MediaTransport1.
    Works even when transport is idle (paused).
    Returns 0-100 percent or None if no transport found.
    """
    path = _find_bt_transport_path()
    if not path:
        return None
    try:
        bus = dbus.SystemBus()
        obj   = bus.get_object("org.bluez", path)
        props = dbus.Interface(obj, "org.freedesktop.DBus.Properties")
        bluez_vol = int(props.Get("org.bluez.MediaTransport1", "Volume"))
        return round(bluez_vol / BlueZWatcher.BLUEZ_VOL_MAX * 100)
    except Exception as e:
        log.warning("bt_dbus_get_volume error: %s", e)
    return None


def bt_dbus_set_volume(percent: int) -> bool:
    """
    Set AVRCP volume via BlueZ MediaTransport1 DBus.
    Only works when transport is active (not paused).
    Returns True on success.
    """
    path = _find_bt_transport_path()
    if not path:
        log.warning("bt_dbus_set_volume: no transport found")
        return False
    try:
        bus   = dbus.SystemBus()
        obj   = bus.get_object("org.bluez", path)
        props = dbus.Interface(obj, "org.freedesktop.DBus.Properties")
        bluez_vol = dbus.UInt16(round(max(0, min(100, percent)) / 100 * BlueZWatcher.BLUEZ_VOL_MAX))
        props.Set("org.bluez.MediaTransport1", "Volume", bluez_vol)
        log.info("BlueZ AVRCP volume set: %d%% -> BlueZ %d", percent, int(bluez_vol))
        return True
    except Exception as e:
        log.warning("bt_dbus_set_volume error (transport may be idle): %s", e)
        return False


def pa_set_volume(percent: int):
    try:
        subprocess.run(["pactl", "set-sink-volume", "@DEFAULT_SINK@", f"{percent}%"],
                       timeout=3, capture_output=True)
    except Exception as e:
        log.error("pactl error: %s", e)


def pulseaudio_stop():
    try:
        subprocess.run(["systemctl", "--user", "stop",
                        "pulseaudio.service", "pulseaudio.socket"],
                       timeout=10, capture_output=True)
        log.info("PulseAudio stopped")
    except Exception as e:
        log.error("pulseaudio stop: %s", e)


def pulseaudio_start():
    try:
        subprocess.run(["systemctl", "--user", "start", "pulseaudio.service"],
                       timeout=10, capture_output=True)
        log.info("PulseAudio started")
    except Exception as e:
        log.error("pulseaudio start: %s", e)


def snapclient_start():
    try:
        subprocess.run(["sudo", "systemctl", "start", SNAPCLIENT_SERVICE],
                       timeout=10, capture_output=True)
    except Exception as e:
        log.error("snapclient start: %s", e)


def snapclient_stop():
    try:
        subprocess.run(["sudo", "systemctl", "stop", SNAPCLIENT_SERVICE],
                       timeout=10, capture_output=True)
    except Exception as e:
        log.error("snapclient stop: %s", e)


def snapclient_is_running() -> bool:
    try:
        r = subprocess.run(["systemctl", "is-active", SNAPCLIENT_SERVICE],
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip() == "active"
    except Exception:
        return False


def bt_agent_start():
    try:
        subprocess.run(["sudo", "systemctl", "restart", "bt-agent"],
                       timeout=10, capture_output=True)
        log.info("bt-agent restarted")
    except Exception as e:
        log.error("bt-agent restart failed: %s", e)


def bt_agent_stop():
    try:
        subprocess.run(["sudo", "systemctl", "stop", "bt-agent"],
                       timeout=10, capture_output=True)
        log.info("bt-agent stopped")
    except Exception as e:
        log.error("bt-agent stop failed: %s", e)


class PasswordListener(threading.Thread):
    def __init__(self, on_hash_received):
        super().__init__(daemon=True)
        self._cb = on_hash_received

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", PW_BROADCAST_PORT))
        srv.listen(5)
        log.info("Password listener on port %d", PW_BROADCAST_PORT)
        while True:
            try:
                conn, addr = srv.accept()
                with conn:
                    data = conn.recv(64)
                if len(data) == 64:
                    self._cb(data.decode("ascii", errors="replace"))
                    log.info("Password broadcast from %s", addr[0])
            except Exception as e:
                log.error("Password listener error: %s", e)


class BlueZWatcher(threading.Thread):
    """
    Watches org.bluez.MediaTransport1 PropertiesChanged signals via DBus.

    Fires:
      on_volume_change(percent 0-100) — phone changed AVRCP volume.
                                        Fires even when transport is idle/paused.
      on_source_appeared(percent)     — transport went active (resume from pause
                                        or fresh connect). Also watches pactl
                                        for 'new source' so fresh pair is caught.
      on_source_removed()             — transport went idle / source removed.

    Volume conversion: BlueZ uses 0-127, we use 0-100.
    """

    BLUEZ_VOL_MAX = 127

    def __init__(self, on_volume_change, on_source_appeared, on_source_removed):
        super().__init__(daemon=True)
        self._on_vol      = on_volume_change
        self._on_appeared = on_source_appeared
        self._on_removed  = on_source_removed
        self._active      = False
        self._lock        = threading.Lock()
        self._mainloop    = None
        self._bus         = None

    def activate(self):
        with self._lock:
            self._active = True
        log.info("BlueZWatcher activated")

    def deactivate(self):
        with self._lock:
            self._active = False
        # Quit the GLib mainloop so the thread can restart cleanly next time
        if self._mainloop and self._mainloop.is_running():
            self._mainloop.quit()
        log.info("BlueZWatcher deactivated")

    @staticmethod
    def bluez_to_percent(vol: int) -> int:
        """Convert BlueZ 0-127 volume to 0-100 percent."""
        return round(vol / BlueZWatcher.BLUEZ_VOL_MAX * 100)

    @staticmethod
    def percent_to_bluez(percent: int) -> int:
        """Convert 0-100 percent to BlueZ 0-127."""
        return round(max(0, min(100, percent)) / 100 * BlueZWatcher.BLUEZ_VOL_MAX)

    def _on_properties_changed(self, interface, changed, invalidated, path):
        """DBus signal handler — runs in GLib mainloop thread."""
        with self._lock:
            active = self._active
        if not active:
            return

        if interface != "org.bluez.MediaTransport1":
            return

        if "Volume" in changed:
            bluez_vol = int(changed["Volume"])
            percent   = self.bluez_to_percent(bluez_vol)
            log.info("BlueZ AVRCP Volume changed: %d (%.0f%%)", bluez_vol, percent)
            self._on_vol(percent)

        if "State" in changed:
            state = str(changed["State"])
            log.info("BlueZ transport state: %s (%s)", state, path)
            if state == "active":
                # Transport just opened — read current volume from BlueZ
                try:
                    bus = dbus.SystemBus()
                    obj = bus.get_object("org.bluez", path)
                    props = dbus.Interface(obj, "org.freedesktop.DBus.Properties")
                    bluez_vol = int(props.Get("org.bluez.MediaTransport1", "Volume"))
                    percent   = self.bluez_to_percent(bluez_vol)
                    log.info("Transport active — current BlueZ vol: %d (%.0f%%)",
                             bluez_vol, percent)
                    self._on_appeared(percent)
                except Exception as e:
                    log.warning("BlueZWatcher: failed to read vol on active: %s", e)
                    self._on_appeared(0)
            elif state == "idle":
                self._on_removed()

    def _pactl_source_thread(self):
        """
        Runs alongside the DBus watcher to catch fresh pair events.
        pactl subscribe fires 'new source' when the A2DP source first appears
        (fresh pair), which doesn't always produce a DBus 'active' state event.
        """
        while True:
            with self._lock:
                active = self._active
            if not active:
                time.sleep(0.5)
                continue
            try:
                proc = subprocess.Popen(["pactl", "subscribe"],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL,
                                        text=True)
                for line in proc.stdout:
                    with self._lock:
                        active = self._active
                    if not active:
                        break
                    if "new" in line and "source" in line and "bluez" in line.lower():
                        time.sleep(0.5)
                        # Fresh pair — read BlueZ vol directly
                        try:
                            bus = dbus.SystemBus()
                            mgr = dbus.Interface(
                                bus.get_object("org.bluez", "/"),
                                "org.freedesktop.DBus.ObjectManager")
                            objects = mgr.GetManagedObjects()
                            for path, ifaces in objects.items():
                                if "org.bluez.MediaTransport1" in ifaces:
                                    props = ifaces["org.bluez.MediaTransport1"]
                                    if "Volume" in props:
                                        bluez_vol = int(props["Volume"])
                                        percent   = self.bluez_to_percent(bluez_vol)
                                        log.info("pactl: new BT source, BlueZ vol %d (%.0f%%)",
                                                 bluez_vol, percent)
                                        self._on_appeared(percent)
                                        break
                            else:
                                self._on_appeared(0)
                        except Exception as e:
                            log.warning("pactl source thread vol read failed: %s", e)
                            self._on_appeared(0)
                proc.terminate()
                proc.wait()
            except Exception as e:
                log.warning("pactl source thread error: %s", e)
                time.sleep(2)

    def run(self):
        # Start pactl source watcher in a sibling thread
        threading.Thread(target=self._pactl_source_thread, daemon=True).start()

        while True:
            with self._lock:
                active = self._active
            if not active:
                time.sleep(0.5)
                continue

            log.info("Starting BlueZ DBus watcher")
            try:
                dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
                bus = dbus.SystemBus()
                self._bus = bus

                bus.add_signal_receiver(
                    self._on_properties_changed,
                    bus_name="org.bluez",
                    signal_name="PropertiesChanged",
                    dbus_interface="org.freedesktop.DBus.Properties",
                    path_keyword="path",
                )

                self._mainloop = gobject.MainLoop()
                self._mainloop.run()

            except Exception as e:
                log.warning("BlueZWatcher error: %s", e)
                time.sleep(2)
            finally:
                self._bus = None
                self._mainloop = None


class ClientBridge:
    ESP_TIMEOUT_S   = 20
    RPC_RETRY_S     = 5.0
    POLL_INTERVAL_S = 5.0   # BT periodic fallback poll

    def __init__(self, serial_port: str, baud: int, server_ip: str):
        self.ser       = serial.Serial(serial_port, baud, timeout=0.05)
        self.rx        = UARTReceiver()
        self._running  = True
        self.server_ip = server_ip

        self.mode     = MODE_SYNC
        self.hostname = get_hostname()
        self.volume   = 0

        self.dsp_on = False
        self.amp_on = False

        self._power_lock    = threading.Lock()
        self._power_on_done = threading.Event()

        self.rpc              = SnapcastRPC()
        self.client_id:  Optional[str] = None
        self._snap_server_ip: Optional[str] = None
        self._esp_vol_set_time = 0.0
        self._last_rpc_attempt = 0.0

        self.bt_connected  = False
        self.bt_dev_name   = ""

        # Echo guard: suppress pactl vol events for 1 s after we set volume.
        self._bt_vol_set_time = 0.0
        self._bt_ignore_next_vol = False  # suppress phone vol after fresh pair

        # Desired BT volume — always updated even when source is absent (paused).
        # Applied to the source the moment it re-appears.
        self._bt_desired_vol: int = 0

        self._last_poll_time = 0.0

        self._esp_connected     = False
        self._last_esp_msg_time = time.time()
        self._last_state_sent   = None

        self._pw_hash = load_or_init_password()
        self._pw_listener = PasswordListener(self._on_pw_broadcast)
        self._pw_listener.start()

        self._vol_lock          = threading.Lock()
        self._pending_rpc_vol: Optional[int] = None
        self._rpc_lock          = threading.Lock()
        self._vol_flush_running = threading.Event()

        self._srv_sock: Optional[socket.socket] = None
        self._srv_lock       = threading.Lock()
        self._last_ping_time = time.time()

        self._source_watcher = BlueZWatcher(
            self._on_bt_source_volume_changed,
            self._on_bt_source_appeared,
            self._on_bt_source_removed,
        )
        self._source_watcher.start()

    @property
    def powered(self) -> bool:
        return self.dsp_on and self.amp_on

    # ── Server connection ──────────────────────────────────────────────────
    def _server_connect_loop(self):
        self._power_on_done.wait()
        while self._running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((self.server_ip, SERVER_CTRL_PORT))
                sock.settimeout(None)
                sock.setblocking(False)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,  10)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT,   3)
                with self._srv_lock:
                    self._srv_sock       = sock
                    self._last_ping_time = time.time()
                self._send_register(sock)
                h = fetch_hash_from_server(self.server_ip)
                if h:
                    save_password_hash(h)
                    self._pw_hash = h
                self._server_recv_loop(sock)
            except Exception as e:
                log.warning("Server connect failed: %s — retry in %ds", e, REGISTER_RETRY_S)
                with self._srv_lock:
                    self._srv_sock = None
            time.sleep(REGISTER_RETRY_S)

    def _send_register(self, sock: socket.socket):
        reg = {
            "type":           "register",
            "snap_id":        self.client_id or "",
            "name":           self.hostname,
            "mode":           self.mode,
            "volume":         self.volume,
            "muted":          False,
            "snap_connected": self.rpc.connected and self.client_id is not None,
            "bt_connected":   self.bt_connected,
            "powered":        self.powered,
        }
        sock.sendall((json.dumps(reg) + "\n").encode())
        log.info("Registered with server (snap_id=%s name=%s powered=%s)",
                 self.client_id or "pending", self.hostname, self.powered)

    def _server_recv_loop(self, sock: socket.socket):
        buf = b""
        while self._running:
            with self._srv_lock:
                last_ping = self._last_ping_time
            if time.time() - last_ping > PING_TIMEOUT_S:
                log.warning("Server ping timeout — reconnecting")
                break
            try:
                data = sock.recv(1024)
            except BlockingIOError:
                time.sleep(0.01)
                continue
            except Exception:
                break
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    self._on_server_message(json.loads(line))
                except json.JSONDecodeError:
                    pass
        with self._srv_lock:
            self._srv_sock = None
        log.info("Server connection closed")

    def _on_server_message(self, msg: dict):
        mtype = msg.get("type", "")
        if mtype == "ping":
            with self._srv_lock:
                self._last_ping_time = time.time()
            self._srv_send({"type": "pong"})
        elif mtype == "set_volume":
            vol = msg.get("volume", self.volume)
            self._esp_vol_set_time = time.time()
            if self.mode == MODE_BT:
                self._set_bt_volume(vol, broadcast=False)
            else:
                self.volume = vol
                self._pending_rpc_vol = vol
                if not self._vol_flush_running.is_set():
                    self._vol_flush_running.set()
                    threading.Thread(target=self._flush_vol, daemon=True).start()
            self.send_vol_update(vol)
        elif mtype == "set_mode":
            new_mode = msg.get("mode", self.mode)
            if new_mode != self.mode:
                threading.Thread(target=self._do_mode_switch,
                                 args=(new_mode,), daemon=True).start()
        elif mtype == "set_powered":
            powered = msg.get("powered", self.powered)
            threading.Thread(target=self._do_power_sequence,
                             args=(powered,), daemon=True).start()
        elif mtype == "set_name":
            new_name = msg.get("name", "")
            if new_name:
                threading.Thread(
                    target=self._handle_rename,
                    args=(new_name.encode(),),
                    daemon=True
                ).start()

    def _srv_send(self, msg: dict):
        with self._srv_lock:
            sock = self._srv_sock
        if not sock:
            return
        try:
            sock.sendall((json.dumps(msg) + "\n").encode())
        except BlockingIOError:
            log.warning("srv_send: send buffer busy, skipping this cycle")
        except Exception as e:
            log.warning("srv_send failed: %s", e)
            with self._srv_lock:
                self._srv_sock = None

    def broadcast_ctrl_state(self):
        snap_conn = (self.rpc.connected and self.client_id is not None) \
                    if self.mode == MODE_SYNC else False
        self._srv_send({
            "type":           "state",
            "snap_id":        self.client_id or "",
            "client_name":    self.hostname,
            "mode":           self.mode,
            "volume":         self.volume,
            "snap_connected": snap_conn,
            "bt_connected":   self.bt_connected,
            "bt_dev_name":    self.bt_dev_name,
            "powered":        self.powered,
        })

    def _broadcast_power(self):
        self._srv_send({"type": "power_state", "powered": self.powered})

    def _broadcast_switching(self):
        self._srv_send({"type": "switching"})

    # ── UART ──────────────────────────────────────────────────────────────
    def send_frame(self, msg_type: int, payload: bytes = b""):
        frame = build_frame(msg_type, payload)
        self.ser.write(frame)
        self.ser.flush()

    def send_ack(self):
        payload = pad(self.hostname, DEVICE_NAME_LEN) + bytes([self.mode])
        self.send_frame(MSG_ACK, payload)

    def send_vol_update(self, vol: int):
        self.send_frame(MSG_CLIENT_VOL_UPD, bytes([vol]))
        log.info("Sent VOL_UPD: %d", vol)

    def send_state(self, force: bool = False):
        srv_conn = (self.rpc.connected and self.client_id is not None) \
                   if self.mode == MODE_SYNC else False
        bt_conn  = self.bt_connected if self.mode == MODE_BT else False
        bt_name  = self.bt_dev_name  if self.mode == MODE_BT else ""

        payload = bytearray()
        payload.append(self.mode)
        payload.append(self.volume & 0xFF)
        payload.append(1 if srv_conn else 0)
        payload.append(1 if bt_conn  else 0)
        payload += pad(bt_name,       DEVICE_NAME_LEN)
        payload += pad(self.hostname, DEVICE_NAME_LEN)
        payload.append(1 if self.dsp_on else 0)
        payload.append(1 if self.amp_on else 0)
        payload = bytes(payload)

        if not force and payload == self._last_state_sent:
            return
        self._last_state_sent = payload
        self.send_frame(MSG_STATE_UPDATE, payload)

    # ── Relay helpers ──────────────────────────────────────────────────────
    def _power_on_sequence(self):
        with self._power_lock:
            log.info("Power ON: turning DSP on")
            gpio_set(GPIO_DSP, True)
            self.dsp_on = True
        # Broadcast DSP=on immediately so ESP/server see it light up
        self.broadcast_ctrl_state()
        self._broadcast_power()
        self.send_state(force=True)
        time.sleep(GPIO_DELAY)
        with self._power_lock:
            log.info("Power ON: turning AMP on")
            gpio_set(GPIO_AMP, True)
            self.amp_on = True
            log.info("Power ON: complete — powered=%s", self.powered)
        self._power_on_done.set()
        self.broadcast_ctrl_state()
        self._broadcast_power()
        self.send_state(force=True)

    def _power_off_all(self):
        if self.amp_on:
            log.info("Power OFF: turning AMP off")
            gpio_set(GPIO_AMP, False)
            self.amp_on = False
            self.send_state(force=True)     # update this zone's own ESP screen only
        time.sleep(GPIO_DELAY)
        if self.dsp_on:
            log.info("Power OFF: turning DSP off")
            gpio_set(GPIO_DSP, False)
            self.dsp_on = False
            self.broadcast_ctrl_state()     # ← now the ONLY point the server hears about it
            self.send_state(force=True)

    def _power_on_missing(self):
        with self._power_lock:
            if not self.dsp_on:
                log.info("Power ON: turning DSP on")
                gpio_set(GPIO_DSP, True)
                self.dsp_on = True
            else:
                log.info("Power ON: DSP already on")
        # Broadcast DSP state immediately before the delay
        self.broadcast_ctrl_state()
        self._broadcast_power()
        self.send_state(force=True)
        if not self.amp_on:
            time.sleep(GPIO_DELAY)
            with self._power_lock:
                log.info("Power ON: turning AMP on")
                gpio_set(GPIO_AMP, True)
                self.amp_on = True
            log.info("Power ON: complete — powered=%s", self.powered)
            self.broadcast_ctrl_state()
            self._broadcast_power()
            self.send_state(force=True)
        else:
            log.info("Power ON: AMP already on — complete")

    # ── Audio start/stop ───────────────────────────────────────────────────
    def _stop_audio(self):
        log.info("Powered off — stopping audio services")
        if self.mode == MODE_BT:
            self._source_watcher.deactivate()
            bt_disconnect_all()
            bt_stop_discoverable()
            bt_agent_stop()
            pulseaudio_stop()
            self.bt_connected = False
            self.bt_dev_name  = ""
        else:
            snapclient_stop()
            self.rpc.disconnect()
            self.client_id       = None
            self._snap_server_ip = None
            self._last_rpc_attempt = 0.0

    def _start_audio(self):
        log.info("Powered on — starting audio services")
        if self.mode == MODE_BT:
            pulseaudio_start()
            time.sleep(2)
            try:
                subprocess.run(["pactl", "load-module", "module-loopback",
                                "latency_msec=500"],
                               timeout=5, capture_output=True)
                log.info("Loopback module loaded")
            except Exception as e:
                log.error("loopback load failed: %s", e)
            bt_agent_start()
            self._source_watcher.activate()
        else:
            if not snapclient_is_running():
                snapclient_start()
                time.sleep(2)
            self._snap_server_ip = None
            self.client_id = None
            self._last_rpc_attempt = 0.0

    # ── Power sequences ────────────────────────────────────────────────────
    def _do_power_sequence(self, powered: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            log.info("Power sequence: %s", "ON" if powered else "OFF")
            if powered:
                self._power_lock.release()
                self._power_on_missing()
                self._start_audio()
                return
            else:
                self._power_off_all()
                self._stop_audio()
            self.broadcast_ctrl_state()
            self._broadcast_power()
            self.send_state(force=True)
        finally:
            if self._power_lock.locked():
                self._power_lock.release()

    def _do_dsp_sequence(self, on: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            gpio_set(GPIO_DSP, on)
            self.dsp_on = gpio_get(GPIO_DSP)
        finally:
            self._power_lock.release()
        log.info("DSP -> %s, powered=%s", on, self.powered)
        if not self.powered:
            self._stop_audio()
        else:
            self._start_audio()
        self.broadcast_ctrl_state()
        self._broadcast_power()
        self.send_state(force=True)

    def _do_amp_sequence(self, on: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            gpio_set(GPIO_AMP, on)
            self.amp_on = gpio_get(GPIO_AMP)
        finally:
            self._power_lock.release()
        log.info("AMP -> %s, powered=%s", on, self.powered)
        if not self.powered:
            self._stop_audio()
        else:
            self._start_audio()
        self.broadcast_ctrl_state()
        self._broadcast_power()
        self.send_state(force=True)

    # ── AVRCP / BT volume ─────────────────────────────────────────────────
    def _on_bt_source_appeared(self, percent: int):
        if self.mode != MODE_BT:
            return
        pa_set_volume(100)
        if not self.bt_connected:
            # Fresh pair — set to 0
            log.info("BT source appeared (fresh pair) — setting volume to 0")
            self.bt_connected    = True
            conn, name = bt_get_connected_device()
            self.bt_dev_name     = name
            self.volume          = 0
            self._bt_desired_vol = 0
            self._bt_vol_set_time = time.time()
            # Phone sends its own vol notification right after pairing —
            # ignore just that one event so our 0 push sticks.
            self._bt_ignore_next_vol = True
            bt_dbus_set_volume(0)
            self.send_vol_update(0)
        else:
            # Same device re-appeared after pause — apply desired vol
            desired = self._bt_desired_vol
            log.info("BT source re-appeared (pause resume) — applying vol %d%%", desired)
            self._bt_vol_set_time = time.time()
            bt_dbus_set_volume(desired)
            self.volume = desired
            self.send_vol_update(desired)
        self.send_state(force=True)
        self.broadcast_ctrl_state()


    def _on_bt_source_removed(self):
        if self.mode != MODE_BT:
            return
        log.info("BT source removed — waiting for BT stack to settle")
        # Poll multiple times — some stacks are slow to update bluetoothctl
        for wait in (1.0, 1.0, 1.0):
            time.sleep(wait)
            conn, name = bt_get_connected_device()
            if conn:
                log.info("Device still connected ('%s') — source removal was pause, ignoring", name)
                return
        log.info("Device genuinely disconnected — clearing BT state")
        self.bt_connected = False
        self.bt_dev_name  = ""
        self.send_state(force=True)
        self.broadcast_ctrl_state()

    def _on_bt_source_volume_changed(self, percent: int):
        """Phone changed volume while source is live."""
        if self.mode != MODE_BT:
            return
        if self._bt_ignore_next_vol:
            log.info("BT vol event ignored (post-pair suppress): %d%%", percent)
            self._bt_ignore_next_vol = False
            return
        if time.time() - self._bt_vol_set_time < 0.3:
            log.debug("BT source vol suppressed (echo guard): %d%%", percent)
            return
        if percent == self.volume:
            return
        log.info("Phone changed BT volume: %d%% -> %d%%", self.volume, percent)
        self.volume          = percent
        self._bt_desired_vol = percent
        self.send_vol_update(percent)
        self.send_state()
        self.broadcast_ctrl_state()

    def _set_bt_volume(self, percent: int, broadcast: bool = True):
        percent = max(0, min(100, percent))
        self._bt_vol_set_time = time.time()
        self._bt_desired_vol  = percent
        self.volume           = percent
        if not bt_dbus_set_volume(percent):
            log.info("Transport idle (paused) — vol %d%% queued for resume", percent)
        if broadcast:
            self.broadcast_ctrl_state()

    def poll_bt(self):
        """
        Periodic fallback — catches state/volume drift the watcher may miss,
        especially while the A2DP source is absent during pause.
        """
        conn, name = bt_get_connected_device()
        changed = False

        if conn != self.bt_connected:
            self.bt_connected = conn
            changed = True
            if conn:
                # Device just connected during a poll cycle (watcher missed it)
                # Reset desired vol to 0 and push it — same as fresh pair
                log.info("BT poll: new device connected — pushing volume 0")
                self._bt_desired_vol = 0
                self.volume          = 0
                self._bt_vol_set_time = time.time()
                bt_dbus_set_volume(0)
                self.send_vol_update(0)

        if name != self.bt_dev_name:
            self.bt_dev_name = name
            changed = True

        # Sync volume from BlueZ DBus (works even when paused).
        # If DBus returns None fall back to pactl.
        if conn and time.time() - self._bt_vol_set_time >= 1.0:
            src_vol = bt_dbus_get_volume()
            if src_vol is not None and src_vol != self.volume:
                log.info("BT poll: volume drift %d%% -> %d%%", self.volume, src_vol)
                self.volume          = src_vol
                self._bt_desired_vol = src_vol
                self.send_vol_update(src_vol)
                changed = True

        if changed:
            self.send_state()
            self.broadcast_ctrl_state()

    # ── Password ──────────────────────────────────────────────────────────
    def _on_pw_broadcast(self, h: str):
        save_password_hash(h)
        self._pw_hash = h
        log.info("Password updated from broadcast")

    def _handle_pw_check(self, payload: bytes):
        raw   = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
        match = (sha256_hex(raw) == self._pw_hash)
        self.send_frame(MSG_PW_RESULT, bytes([1 if match else 0]))
        log.info("PW_CHECK: %s", "correct" if match else "wrong")

    # ── Rename ────────────────────────────────────────────────────────────
    def _handle_rename(self, payload: bytes):
        new_name = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
        new_name = re.sub(r"[^a-zA-Z0-9\-]", "", new_name)[:32]
        if not new_name:
            log.warning("RENAME: empty or invalid name, ignoring")
            return
        log.info("RENAME: setting hostname to '%s'", new_name)
        self.hostname = new_name
        self.send_state(force=True)
        self.broadcast_ctrl_state()
        threading.Thread(target=self._apply_rename_system, args=(new_name,), daemon=True).start()

    def _apply_rename_system(self, new_name: str):
        try:
            subprocess.run(["sudo", "hostnamectl", "set-hostname", new_name],
                        timeout=5, check=True, capture_output=True)
            try:
                new_hosts_lines = []
                found = False
                with open("/etc/hosts", "r") as f:
                    for line in f:
                        if "127.0.1.1" in line:
                            if not found:
                                new_hosts_lines.append(f"127.0.1.1\t{new_name}\n")
                                found = True
                        else:
                            new_hosts_lines.append(line)
                if not found:
                    new_hosts_lines.append(f"127.0.1.1\t{new_name}\n")
                tmp_path = "/tmp/hosts.new"
                with open(tmp_path, "w") as f:
                    f.writelines(new_hosts_lines)
                subprocess.run(["sudo", "mv", tmp_path, "/etc/hosts"],
                            timeout=5, check=True, capture_output=True)
                log.info("Updated /etc/hosts -> 127.0.1.1 %s", new_name)
            except (OSError, subprocess.CalledProcessError) as e:
                log.warning("Could not update /etc/hosts: %s", e)
            subprocess.run(["sudo", "systemctl", "restart", "bluetooth"],
                        timeout=10, capture_output=True)
            time.sleep(1)
            if self.mode == MODE_BT:
                bt_agent_start()
                if not self.bt_connected:
                    bt_start_discoverable()
            if self.mode == MODE_SYNC and snapclient_is_running():
                snapclient_stop()
                time.sleep(1)
                snapclient_start()
                time.sleep(2)
                self.client_id = None
                self._last_rpc_attempt = 0.0
            self.send_state(force=True)
            self.broadcast_ctrl_state()
        except subprocess.CalledProcessError as e:
            log.error("hostnamectl failed: %s", e)
            
    # ── RPC ───────────────────────────────────────────────────────────────
    def ensure_rpc(self):
        now = time.time()
        if now - self._last_rpc_attempt < self.RPC_RETRY_S:
            return
        self._last_rpc_attempt = now

        if not self._snap_server_ip:
            self._snap_server_ip = get_snapserver_ip()
            if not self._snap_server_ip:
                return

        if not self.rpc.connected:
            if not self.rpc.connect(self._snap_server_ip):
                return

        if not self.client_id:
            self.client_id = self.rpc.find_client_id_by_hostname(self.hostname)
            if self.client_id:
                self.volume = 0
                self.rpc.set_volume(self.client_id, 0)
                self.send_state(force=True)
                self.send_vol_update(0)
                self.broadcast_ctrl_state()

    # ── Mode transitions ──────────────────────────────────────────────────
    def enter_sync_mode(self):
        log.info("=== ENTERING SYNC MODE ===")
        self.mode = MODE_SYNC
        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_SYNC]))
        self._source_watcher.deactivate()
        bt_disconnect_all()
        bt_stop_discoverable()
        bt_agent_stop()
        pulseaudio_stop()
        time.sleep(1)
        if not snapclient_is_running():
            snapclient_start()
            time.sleep(2)
        self.rpc.disconnect()
        self._snap_server_ip   = None
        self.client_id         = None
        self._last_rpc_attempt = 0.0
        self.bt_connected      = False
        self.bt_dev_name       = ""
        self.volume            = 0
        for _ in range(5):
            self._snap_server_ip = get_snapserver_ip()
            if self._snap_server_ip and self.rpc.connect(self._snap_server_ip):
                self.client_id = self.rpc.find_client_id_by_hostname(self.hostname)
                if self.client_id:
                    self.rpc.set_volume(self.client_id, 0)
                    break
            time.sleep(1)
        self.send_state(force=True)
        self.broadcast_ctrl_state()

    def enter_bt_mode(self):
        log.info("=== ENTERING BT MODE ===")
        self.mode = MODE_BT
        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_BT]))
        self.rpc.disconnect()
        self.client_id         = None
        self._snap_server_ip   = None
        snapclient_stop()
        time.sleep(1)
        pulseaudio_start()
        time.sleep(2)
        try:
            subprocess.run(["pactl", "load-module", "module-loopback",
                            "latency_msec=500"],
                           timeout=5, capture_output=True)
            log.info("Loopback module loaded")
        except Exception as e:
            log.error("loopback load failed: %s", e)
        bt_agent_start()
        self.volume            = 0
        self._bt_desired_vol   = 0
        pa_set_volume(100)
        self._bt_vol_set_time  = time.time()
        self._source_watcher.activate()
        self.send_state(force=True)
        self.broadcast_ctrl_state()

    def _do_mode_switch(self, new_mode: int):
        if new_mode == self.mode:
            self.send_state(force=True)
            self.broadcast_ctrl_state()
            return
        self._broadcast_switching()
        if new_mode == MODE_BT:
            self.enter_bt_mode()
        else:
            self.enter_sync_mode()

    def _flush_vol(self):
        try:
            with self._vol_lock:
                while self._pending_rpc_vol is not None:
                    vol = self._pending_rpc_vol
                    self._pending_rpc_vol = None
                    with self._rpc_lock:
                        if self.rpc.connected and self.client_id:
                            self.rpc.set_volume(self.client_id, vol)
        finally:
            self._vol_flush_running.clear()

    # ── ESP message handling ───────────────────────────────────────────────
    def handle_esp_message(self, msg_type: int, payload: bytes):
        self._last_esp_msg_time = time.time()
        if not self._esp_connected:
            self._esp_connected = True
            log.info("ESP connected")

        if msg_type == MSG_INIT:
            log.info("ESP INIT → ACK + state")
            self._esp_connected = True
            self.send_ack()
            time.sleep(0.05)
            self.send_state(force=True)

        elif msg_type == MSG_PING:
            self.send_frame(MSG_PONG)

        elif msg_type == MSG_VOL_SET:
            if len(payload) < 1:
                return
            vol = payload[0]
            self._esp_vol_set_time = time.time()
            self.volume = vol
            if self.mode == MODE_SYNC:
                if self.rpc.connected and self.client_id:
                    self._pending_rpc_vol = vol
                    if not self._vol_flush_running.is_set():
                        self._vol_flush_running.set()
                        threading.Thread(target=self._flush_vol, daemon=True).start()
            else:
                self._set_bt_volume(vol)

        elif msg_type == MSG_MODE_SYNC:
            if self.mode != MODE_SYNC:
                threading.Thread(target=self._do_mode_switch,
                                 args=(MODE_SYNC,), daemon=True).start()

        elif msg_type == MSG_MODE_BT:
            if self.mode != MODE_BT:
                threading.Thread(target=self._do_mode_switch,
                                 args=(MODE_BT,), daemon=True).start()

        elif msg_type == MSG_POWER_SET:
            if len(payload) < 1:
                return
            threading.Thread(target=self._do_power_sequence,
                             args=(payload[0] != 0,), daemon=True).start()

        elif msg_type == MSG_DSP_SET:
            if len(payload) < 1:
                return
            threading.Thread(target=self._do_dsp_sequence,
                             args=(payload[0] != 0,), daemon=True).start()

        elif msg_type == MSG_AMP_SET:
            if len(payload) < 1:
                return
            threading.Thread(target=self._do_amp_sequence,
                             args=(payload[0] != 0,), daemon=True).start()

        elif msg_type == MSG_PW_CHECK:
            self._handle_pw_check(payload)

        elif msg_type == MSG_RENAME:
            self._handle_rename(payload)

        else:
            log.warning("Unknown ESP msg: 0x%02X", msg_type)

    # ── Snap notifications ─────────────────────────────────────────────────
    def handle_snap_notifications(self):
        if not self.rpc.connected:
            return
        with self._rpc_lock:
            try:
                notifications = self.rpc.read_notifications()
            except (ConnectionError, OSError):
                log.error("RPC lost during notification read")
                self.rpc.disconnect()
                self.client_id = None
                self._last_rpc_attempt = 0.0
                self.send_state(force=True)
                self.broadcast_ctrl_state()
                return
        for n in notifications:
            method = n.get("method", "")
            if method == "Client.OnVolumeChanged":
                params = n.get("params", {})
                if params.get("id") == self.client_id:
                    vol = params.get("volume", {}).get("percent", self.volume)
                    if time.time() - self._esp_vol_set_time >= 1.0:
                        self.volume = vol
                        self.send_vol_update(vol)
            elif method in ("Client.OnConnect", "Client.OnDisconnect"):
                self.client_id = None
                self._last_rpc_attempt = 0.0
                self.send_state(force=True)
                self.broadcast_ctrl_state()

    # ── Main loop ──────────────────────────────────────────────────────────
    def run(self):
        log.info("Client bridge — %s @ %d — hostname: %s",
                 self.ser.port, self.ser.baudrate, self.hostname)

        threading.Thread(target=self._power_on_sequence, daemon=True).start()
        threading.Thread(target=self._server_connect_loop, daemon=True).start()

        self.enter_sync_mode()

        last_heavy_poll = 0.0

        while self._running:
            data = self.ser.read(256)
            if data:
                for msg_type, payload in self.rx.feed(data):
                    self.handle_esp_message(msg_type, payload)

            if self._esp_connected:
                if time.time() - self._last_esp_msg_time > self.ESP_TIMEOUT_S:
                    log.warning("ESP silent — marking disconnected")
                    self._esp_connected = False

            # SYNC mode RPC runs regardless of ESP state so the zone works
            # even when the ESP UART is disconnected/silent.
            now = time.time()
            if self.mode == MODE_SYNC:
                self.ensure_rpc()
                self.handle_snap_notifications()
                if now - last_heavy_poll >= 10.0:
                    last_heavy_poll = now
                    if self.rpc.connected and self.client_id:
                        try:
                            vol = self.rpc.get_volume_for_client(self.client_id)
                            if vol is not None and vol != self.volume:
                                if time.time() - self._esp_vol_set_time >= 1.0:
                                    self.volume = vol
                                    self.send_vol_update(vol)
                        except Exception:
                            self.rpc.disconnect()
                            self.client_id = None
                            self._last_rpc_attempt = 0.0
                            self.send_state(force=True)
                            self.broadcast_ctrl_state()

            # BT periodic fallback — runs regardless of ESP state
            if self.mode == MODE_BT:
                if now - self._last_poll_time >= self.POLL_INTERVAL_S:
                    self._last_poll_time = now
                    self.poll_bt()


def main():
    ap = argparse.ArgumentParser(description="ESP ↔ Snapclient bridge (client Pi)")
    ap.add_argument("--port",      default="/dev/ttyAMA0")
    ap.add_argument("--baud",      type=int, default=460800)
    ap.add_argument("--server-ip", required=True,
                    help="Server Pi IP address e.g. 192.168.1.10")
    args = ap.parse_args()

    bridge = ClientBridge(args.port, args.baud, args.server_ip)
    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("Shutting down")
        bridge.rpc.disconnect()
        gpio_cleanup()


if __name__ == "__main__":
    main()
