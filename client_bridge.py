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

# gpioset/gpioget CLI tools are used — no Python GPIO library needed.
# Requires: sudo apt install gpiod  (usually pre-installed on Raspbian)
# User must be in the gpio group: sudo usermod -aG gpio $USER
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

# ── Protocol constants ──
SYNC_0, SYNC_1  = 0xAA, 0x55
MAX_PAYLOAD      = 2048

MSG_INIT             = 0x01
MSG_VOL_SET          = 0x02
MSG_PING             = 0x04
MSG_MODE_SYNC        = 0x05
MSG_MODE_BT          = 0x06
MSG_POWER_SET        = 0x07
MSG_DSP_SET          = 0x08
MSG_AMP_SET          = 0x09
MSG_ACK              = 0x10
MSG_CLIENT_VOL_UPD   = 0x12
MSG_PONG             = 0x13
MSG_STATE_UPDATE     = 0x20
MSG_MODE_SWITCHING   = 0x21
MSG_PW_CHECK         = 0x35
MSG_PW_RESULT        = 0x36
MSG_RENAME           = 0x33
MSG_BOOT_STATUS      = 0x40   # Pi → ESP: 0=initializing, 1=done
MSG_BOOT_STATUS_REQ  = 0x41   # ESP → Pi: request boot sequence

MODE_SYNC, MODE_BT = 0, 1
DEVICE_NAME_LEN    = 32
STATE_UPDATE_SIZE  = 68
ACK_PAYLOAD_SIZE   = 33

SNAPCLIENT_SERVICE = "snapclient"
SNAP_RPC_PORT      = 1705
PW_BROADCAST_PORT  = 7700
CTRL_PORT          = 7702
DISCOVERY_PORT     = 7703
PW_HASH_FILE       = "/etc/zone_password.hash"
PW_DEFAULT         = "anjay1234"

# ── GPIO pin definitions ──
GPIO_DSP   = 17    # DSP power relay (BCM numbering)
GPIO_AMP   = 27    # AMP power relay (BCM numbering)
GPIO_DELAY = 10.0  # seconds between DSP and AMP
GPIO_CHIP  = "gpiochip0"

# Track output state in software since gpioget reads actual pin level
# and we drive outputs, so we just remember what we last set.
_gpio_state: dict = {GPIO_DSP: False, GPIO_AMP: False}


# ── GPIO helpers (gpioset/gpioget CLI — no root, user needs gpio group) ──
def gpio_init():
    if not GPIO_AVAILABLE:
        return
    # Drive both pins LOW on startup via gpioset.
    # gpioset holds the line only for the duration of the call when using
    # --mode=exit, but the default mode sets and exits which is fine for
    # output relays — the kernel keeps the last value.
    for pin in (GPIO_DSP, GPIO_AMP):
        try:
            subprocess.run(
                ["gpioset", GPIO_CHIP, f"{pin}=0"],
                timeout=3, capture_output=True, check=True
            )
        except Exception as e:
            log.error("gpio_init pin %d failed: %s", pin, e)
    _gpio_state[GPIO_DSP] = False
    _gpio_state[GPIO_AMP] = False
    log.info("GPIO initialised via gpioset — DSP=GPIO%d AMP=GPIO%d", GPIO_DSP, GPIO_AMP)


def gpio_get(pin: int) -> bool:
    """Return last-set state from our software cache (we drive outputs)."""
    if not GPIO_AVAILABLE:
        return False
    return _gpio_state.get(pin, False)


def gpio_set(pin: int, state: bool):
    if not GPIO_AVAILABLE:
        log.info("GPIO%d -> %s (simulated)", pin, "HIGH" if state else "LOW")
        _gpio_state[pin] = state
        return
    val = 1 if state else 0
    try:
        subprocess.run(
            ["gpioset", GPIO_CHIP, f"{pin}={val}"],
            timeout=3, capture_output=True, check=True
        )
        _gpio_state[pin] = state
        log.info("GPIO%d -> %s", pin, "HIGH" if state else "LOW")
    except Exception as e:
        log.error("gpio_set GPIO%d failed: %s", pin, e)


def gpio_cleanup():
    """Drive both relay pins LOW on shutdown."""
    if not GPIO_AVAILABLE:
        return
    for pin in (GPIO_DSP, GPIO_AMP):
        try:
            subprocess.run(
                ["gpioset", GPIO_CHIP, f"{pin}=0"],
                timeout=3, capture_output=True
            )
        except Exception:
            pass


# ── CRC-8 ──
def crc8(data: bytes) -> int:
    crc = 0x00
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x80:
                crc = ((crc << 1) ^ 0x31) & 0xFF
            else:
                crc = (crc << 1) & 0xFF
    return crc


def build_frame(msg_type: int, payload: bytes = b"") -> bytes:
    plen = len(payload)
    header = bytes([SYNC_0, SYNC_1, msg_type, plen & 0xFF, (plen >> 8) & 0xFF])
    crc_data = bytes([msg_type, plen & 0xFF, (plen >> 8) & 0xFF]) + payload
    return header + payload + bytes([crc8(crc_data)])


def pad(s: str, length: int) -> bytes:
    return s.encode("utf-8")[:length].ljust(length, b"\x00")


# ── Password helpers ──
def sha256_hex(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def load_or_init_password() -> str:
    if os.path.exists(PW_HASH_FILE):
        with open(PW_HASH_FILE, "r") as f:
            h = f.read().strip()
        if len(h) == 64:
            log.info("Password hash loaded from %s", PW_HASH_FILE)
            return h
    log.info("No valid password file — seeding default")
    h = sha256_hex(PW_DEFAULT)
    save_password_hash(h)
    return h


def save_password_hash(h: str):
    with open(PW_HASH_FILE, "w") as f:
        f.write(h)
    log.info("Password hash saved to %s", PW_HASH_FILE)


def fetch_hash_from_server(server_ip: str) -> Optional[str]:
    try:
        with socket.create_connection((server_ip, 7701), timeout=5) as s:
            data = s.recv(64)
        if len(data) == 64:
            h = data.decode("ascii", errors="replace")
            log.info("Fetched hash from server %s", server_ip)
            return h
        log.warning("Hash pull got wrong length: %d", len(data))
        return None
    except OSError as e:
        log.warning("Hash pull failed: %s", e)
        return None


def get_own_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ── UART frame receiver ──
class UARTReceiver:
    SYNC_0_ST  = 0
    SYNC_1_ST  = 1
    HEADER_ST  = 2
    PAYLOAD_ST = 3
    CRC_ST     = 4

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
                if byte == SYNC_0:
                    self.state = self.SYNC_1_ST
            elif self.state == self.SYNC_1_ST:
                if byte == SYNC_1:
                    self.state = self.HEADER_ST
                    self.header_idx = 0
                elif byte == SYNC_0:
                    pass
                else:
                    self.state = self.SYNC_0_ST
            elif self.state == self.HEADER_ST:
                self.header_buf[self.header_idx] = byte
                self.header_idx += 1
                if self.header_idx == 3:
                    self.msg_type    = self.header_buf[0]
                    self.payload_len = self.header_buf[1] | (self.header_buf[2] << 8)
                    if self.payload_len > MAX_PAYLOAD:
                        log.warning("Payload too large: %d", self.payload_len)
                        self.state = self.SYNC_0_ST
                    elif self.payload_len == 0:
                        self.state = self.CRC_ST
                    else:
                        self.payload_idx = 0
                        self.state = self.PAYLOAD_ST
            elif self.state == self.PAYLOAD_ST:
                self.payload_buf[self.payload_idx] = byte
                self.payload_idx += 1
                if self.payload_idx == self.payload_len:
                    self.state = self.CRC_ST
            elif self.state == self.CRC_ST:
                crc_data = bytes([
                    self.msg_type,
                    self.payload_len & 0xFF,
                    (self.payload_len >> 8) & 0xFF,
                ]) + bytes(self.payload_buf[:self.payload_len])
                expected = crc8(crc_data)
                if byte == expected:
                    yield (self.msg_type, bytes(self.payload_buf[:self.payload_len]))
                else:
                    log.warning("CRC mismatch: got 0x%02X expected 0x%02X",
                                byte, expected)
                self.state = self.SYNC_0_ST


# ── Snapcast JSON-RPC ──
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
            try:
                self.sock.close()
            except Exception:
                pass
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

        msg = json.dumps({
            "id": req_id, "jsonrpc": "2.0",
            "method": method, "params": params,
        }) + "\r\n"

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
                    if not line:
                        continue
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
                    return c.get("config", {}).get("volume", {}).get("percent", 100)
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
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "method" in obj and "id" not in obj:
                notifications.append(obj)
        return notifications


# ── Snapserver IP from snapclient journal ──
def get_snapserver_ip() -> Optional[str]:
    try:
        result = subprocess.run(
            ["journalctl", "-u", SNAPCLIENT_SERVICE, "--no-pager", "-o", "cat"],
            capture_output=True, text=True, timeout=10
        )
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


# ── BT helpers ──
def get_hostname() -> str:
    return socket.gethostname()


def bt_disconnect_all():
    try:
        result = subprocess.run(
            ["bluetoothctl", "devices", "Connected"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                mac = parts[1]
                subprocess.run(["bluetoothctl", "disconnect", mac],
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


# ── A2DP source (AVRCP) volume helpers ──

def _find_bt_source_name() -> Optional[str]:
    """Return the PulseAudio source name for the connected A2DP device, if any."""
    try:
        result = subprocess.run(
            ["pactl", "list", "sources", "short"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            # Lines look like: 7\tbluez_source.AA_BB_CC_DD_EE_FF.a2dp_source\t...
            parts = line.split()
            if len(parts) >= 2 and "bluez_source" in parts[1] and "a2dp_source" in parts[1]:
                return parts[1]
    except Exception as e:
        log.warning("Could not list pactl sources: %s", e)
    return None


def bt_get_source_volume() -> Optional[int]:
    """
    Read volume from the A2DP source (phone side).
    Returns 0-100 percent, or None if no source found.
    """
    source = _find_bt_source_name()
    if not source:
        return None
    try:
        result = subprocess.run(
            ["pactl", "get-source-volume", source],
            capture_output=True, text=True, timeout=3
        )
        m = re.search(r"(\d+)%", result.stdout)
        if m:
            return int(m.group(1))
    except Exception as e:
        log.warning("bt_get_source_volume error: %s", e)
    return None


def bt_set_source_volume(percent: int) -> bool:
    """
    Set volume on the A2DP source — this sends AVRCP volume to the phone.
    Returns True on success.
    """
    source = _find_bt_source_name()
    if not source:
        log.warning("bt_set_source_volume: no A2DP source found")
        return False
    try:
        subprocess.run(
            ["pactl", "set-source-volume", source, f"{percent}%"],
            capture_output=True, timeout=3, check=True
        )
        log.info("AVRCP volume set: %s -> %d%%", source, percent)
        return True
    except Exception as e:
        log.warning("bt_set_source_volume error: %s", e)
        return False


def pa_get_volume() -> int:
    """Read output sink volume (for the loopback/speaker side)."""
    try:
        result = subprocess.run(["pactl", "get-sink-volume", "@DEFAULT_SINK@"],
                                capture_output=True, text=True, timeout=3)
        m = re.search(r"(\d+)%", result.stdout)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return 100


def pa_set_volume(percent: int):
    """Set output sink volume."""
    try:
        subprocess.run(["pactl", "set-sink-volume", "@DEFAULT_SINK@",
                        f"{percent}%"], timeout=3, capture_output=True)
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


# ── Password broadcast listener — port 7700 ──
class PasswordListener(threading.Thread):
    def __init__(self, on_hash_received):
        super().__init__(daemon=True)
        self._cb = on_hash_received

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", PW_BROADCAST_PORT))
        srv.listen(5)
        log.info("Password listener started on port %d", PW_BROADCAST_PORT)
        while True:
            try:
                conn, addr = srv.accept()
                with conn:
                    data = conn.recv(64)
                if len(data) == 64:
                    h = data.decode("ascii", errors="replace")
                    log.info("Received password broadcast from %s", addr[0])
                    self._cb(h)
                else:
                    log.warning("Password broadcast wrong length: %d", len(data))
            except Exception as e:
                log.error("Password listener error: %s", e)


# ── pactl subscribe watcher for BT source volume changes ──
class PactlSourceWatcher(threading.Thread):
    """
    Runs `pactl subscribe` and:
      - fires on_volume_change(percent) on source 'change' events
        (phone adjusted volume while already connected)
      - fires on_source_appeared(percent) on source 'new' events
        (A2DP source just registered — device finished pairing/connecting)

    Runs only while BT mode is active.
    """

    def __init__(self, on_volume_change, on_source_appeared):
        super().__init__(daemon=True)
        self._on_vol      = on_volume_change
        self._on_appeared = on_source_appeared
        self._active      = False
        self._proc: Optional[subprocess.Popen] = None
        self._lock        = threading.Lock()

    def activate(self):
        """Call when entering BT mode."""
        with self._lock:
            self._active = True
        log.info("PactlSourceWatcher activated")

    def deactivate(self):
        """Call when leaving BT mode."""
        with self._lock:
            self._active = False
        log.info("PactlSourceWatcher deactivated")

    def run(self):
        while True:
            with self._lock:
                active = self._active
            if not active:
                time.sleep(0.5)
                continue

            log.info("Starting pactl subscribe watcher")
            try:
                proc = subprocess.Popen(
                    ["pactl", "subscribe"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
                with self._lock:
                    self._proc = proc

                for line in proc.stdout:
                    with self._lock:
                        active = self._active
                    if not active:
                        break

                    # 'new' on source  → A2DP source just appeared (device connected/paired)
                    if "new" in line and "source" in line:
                        # Give PulseAudio a moment to fully register the source
                        time.sleep(0.5)
                        vol = bt_get_source_volume()
                        if vol is not None:
                            log.info("A2DP source appeared, initial volume: %d%%", vol)
                            self._on_appeared(vol)

                    # 'change' on source → phone adjusted volume
                    elif "change" in line and "source" in line:
                        # Debounce so volume has settled in PulseAudio
                        time.sleep(0.15)
                        vol = bt_get_source_volume()
                        if vol is not None:
                            self._on_vol(vol)

                proc.terminate()
                proc.wait()
            except Exception as e:
                log.warning("PactlSourceWatcher error: %s", e)
                time.sleep(2)
            finally:
                with self._lock:
                    self._proc = None


# ── Main bridge ──
class ClientBridge:
    ESP_TIMEOUT_S   = 20
    POLL_INTERVAL_S = 5.0   # BT poll interval (reduced — watcher handles most events)
    RPC_RETRY_S     = 5.0

    def __init__(self, serial_port: str, baud: int):
        self.ser      = serial.Serial(serial_port, baud, timeout=0.05)
        self.rx       = UARTReceiver()
        self._running = True

        self.mode     = MODE_SYNC
        self.hostname = get_hostname()
        self.volume   = 0

        # ── Per-relay state ──
        self.dsp_on = False
        self.amp_on = False

        # Boot sequence state
        self._boot_done  = False
        self._power_lock = threading.Lock()

        # SYNC state
        self.rpc               = SnapcastRPC()
        self.server_ip:  Optional[str] = None
        self.client_id:  Optional[str] = None
        self._esp_vol_set_time = 0.0
        self._last_rpc_attempt = 0.0

        # BT state
        self.bt_connected = False
        self.bt_dev_name  = ""
        # Guard to prevent echo: when we set the source volume ourselves,
        # ignore the resulting pactl subscribe event for a short window.
        self._bt_vol_set_time = 0.0

        # ESP tracking
        self._esp_connected     = False
        self._last_esp_msg_time = time.time()
        self._last_poll_time    = 0.0
        self._last_state_sent   = None

        # Password
        self._pw_hash = load_or_init_password()
        self._pw_listener = PasswordListener(self._on_pw_broadcast)
        self._pw_listener.start()

        # Control socket clients
        self._ctrl_clients: list[socket.socket] = []
        self._ctrl_lock = threading.Lock()

        # pactl subscribe watcher for AVRCP volume from phone
        self._source_watcher = PactlSourceWatcher(
            self._on_bt_source_volume_changed,
            self._on_bt_source_appeared,
        )
        self._source_watcher.start()

    # ── Convenience property ──────────────────────────────────────────────────
    @property
    def powered(self) -> bool:
        return self.dsp_on and self.amp_on

    # ── Relay sync helpers ────────────────────────────────────────────────────
    def _read_relay_state(self):
        self.dsp_on = gpio_get(GPIO_DSP)
        self.amp_on = gpio_get(GPIO_AMP)

    def _power_on_missing(self):
        if not self.dsp_on:
            log.info("Power ON: DSP was OFF — turning DSP on")
            gpio_set(GPIO_DSP, True)
            self.dsp_on = True
            self.broadcast_ctrl_state()
            self.send_state(force=True)
            time.sleep(GPIO_DELAY)
        else:
            log.info("Power ON: DSP already ON — skipping DSP delay")

        if not self.amp_on:
            log.info("Power ON: turning AMP on")
            gpio_set(GPIO_AMP, True)
            self.amp_on = True
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        else:
            log.info("Power ON: AMP already ON — nothing to do")

    def _power_off_all(self):
        if self.amp_on:
            log.info("Power OFF: turning AMP off")
            gpio_set(GPIO_AMP, False)
            self.amp_on = False
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        else:
            log.info("Power OFF: AMP already OFF — skipping")

        time.sleep(GPIO_DELAY)

        if self.dsp_on:
            log.info("Power OFF: turning DSP off")
            gpio_set(GPIO_DSP, False)
            self.dsp_on = False
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        else:
            log.info("Power OFF: DSP already OFF — skipping")

    # ────────────────────────────────────────────
    # Frame sending
    # ────────────────────────────────────────────
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

    # ────────────────────────────────────────────
    # AVRCP / A2DP source volume callbacks
    # ────────────────────────────────────────────
    def _on_bt_source_appeared(self, percent: int):
        """
        Called by PactlSourceWatcher when an A2DP source is newly registered
        (i.e. a device just finished pairing/connecting).
        Syncs initial volume from the phone and propagates everywhere.
        """
        if self.mode != MODE_BT:
            return
        log.info("BT source appeared — syncing initial volume: %d%%", percent)
        self.volume = percent
        # Sync sink so loopback level matches
        pa_set_volume(percent)
        self.send_vol_update(percent)
        self.send_state(force=True)
        self.broadcast_ctrl_state()

    def _on_bt_source_volume_changed(self, percent: int):
        """
        Called by PactlSourceWatcher when the phone changes volume.
        Ignored for 1 second after we ourselves set the volume (echo guard).
        """
        if self.mode != MODE_BT:
            return
        if time.time() - self._bt_vol_set_time < 1.0:
            log.debug("BT source vol event suppressed (echo guard): %d%%", percent)
            return
        if percent == self.volume:
            return
        log.info("Phone changed BT volume: %d%% -> %d%%", self.volume, percent)
        self.volume = percent
        self.send_vol_update(percent)
        self.send_state()
        self.broadcast_ctrl_state()

    def _set_bt_volume(self, percent: int):
        """
        Set volume in BT mode: update A2DP source (AVRCP → phone) and
        also the sink (loopback speaker). Marks the echo-guard timer.
        """
        percent = max(0, min(100, percent))
        self._bt_vol_set_time = time.time()
        bt_set_source_volume(percent)   # → phone via AVRCP
        pa_set_volume(percent)          # → local loopback/speaker
        self.volume = percent

    # ────────────────────────────────────────────
    # Boot sequence
    # ────────────────────────────────────────────
    def _do_boot_sequence(self):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Boot sequence already in progress")
            return
        try:
            if self._boot_done:
                log.info("Boot already complete — sending done status")
                self.send_frame(MSG_BOOT_STATUS, bytes([1]))
                return

            log.info("=== BOOT SEQUENCE START ===")
            self.send_frame(MSG_BOOT_STATUS, bytes([0]))
            log.info("Sent BOOT_STATUS: initializing")

            self._read_relay_state()
            log.info("Pre-boot relay state: DSP=%s AMP=%s", self.dsp_on, self.amp_on)

            self._power_on_missing()

            self._boot_done = True
            log.info("=== BOOT SEQUENCE DONE — DSP=%s AMP=%s powered=%s ===",
                     self.dsp_on, self.amp_on, self.powered)

            self.send_frame(MSG_BOOT_STATUS, bytes([1]))
            log.info("Sent BOOT_STATUS: done")

            self.broadcast_ctrl_state()
        finally:
            self._power_lock.release()

    # ────────────────────────────────────────────
    # Power sequences
    # ────────────────────────────────────────────
    def _do_power_sequence(self, powered: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            log.info("Power sequence requested: %s (DSP=%s AMP=%s)",
                     "ON" if powered else "OFF", self.dsp_on, self.amp_on)
            if powered:
                self._power_on_missing()
            else:
                self._power_off_all()
            log.info("Power sequence complete — DSP=%s AMP=%s powered=%s",
                     self.dsp_on, self.amp_on, self.powered)
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        finally:
            self._power_lock.release()

    def _do_dsp_sequence(self, on: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            log.info("DSP sequence: %s", "ON" if on else "OFF")
            gpio_set(GPIO_DSP, on)
            self.dsp_on = gpio_get(GPIO_DSP)
            log.info("DSP sequence complete — dsp=%s amp=%s powered=%s",
                     self.dsp_on, self.amp_on, self.powered)
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        finally:
            self._power_lock.release()

    def _do_amp_sequence(self, on: bool):
        if not self._power_lock.acquire(blocking=False):
            log.warning("Power sequence already in progress")
            return
        try:
            log.info("AMP sequence: %s", "ON" if on else "OFF")
            gpio_set(GPIO_AMP, on)
            self.amp_on = gpio_get(GPIO_AMP)
            log.info("AMP sequence complete — dsp=%s amp=%s powered=%s",
                     self.dsp_on, self.amp_on, self.powered)
            self.broadcast_ctrl_state()
            self.send_state(force=True)
        finally:
            self._power_lock.release()

    def _reconnect_after_remove(self):
        log.info("Reconnect after remove: restarting snapclient")
        snapclient_stop()
        self.rpc.disconnect()
        self.client_id = None
        self.server_ip = None
        self._last_rpc_attempt = 0.0
        time.sleep(1)
        snapclient_start()
        log.info("Reconnect after remove: snapclient restarted")

    # ────────────────────────────────────────────
    # UDP discovery listener
    # ────────────────────────────────────────────
    def _discovery_thread(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", DISCOVERY_PORT))
        log.info("UDP discovery listener on port %d", DISCOVERY_PORT)
        while True:
            try:
                data, addr = sock.recvfrom(512)
                try:
                    msg = json.loads(data.decode())
                except Exception:
                    continue
                if msg.get("type") == "discover":
                    reply = json.dumps({
                        "type": "announce",
                        "ip":   get_own_ip(),
                        "name": self.hostname,
                    }).encode()
                    sock.sendto(reply, addr)
                    log.info("Discovery reply sent to %s", addr[0])
            except Exception as e:
                log.error("Discovery listener error: %s", e)

    # ────────────────────────────────────────────
    # Control socket — port 7702
    # ────────────────────────────────────────────
    def _ctrl_server_thread(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", CTRL_PORT))
        srv.listen(5)
        log.info("Control server listening on port %d", CTRL_PORT)
        while True:
            try:
                conn, addr = srv.accept()
                log.info("Server Pi connected from %s", addr[0])
                with self._ctrl_lock:
                    self._ctrl_clients.append(conn)
                threading.Thread(
                    target=self._ctrl_client_handler,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except Exception as e:
                log.error("Control server accept error: %s", e)

    def _ctrl_client_handler(self, conn: socket.socket, addr):
        buf = b""
        try:
            self._send_ctrl_state(conn)
            conn.settimeout(60)
            while True:
                try:
                    data = conn.recv(1024)
                except socket.timeout:
                    try:
                        self._send_ctrl_msg(conn, {"type": "ping"})
                    except Exception:
                        break
                    continue
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        self._handle_ctrl_msg(conn, msg)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            log.warning("Control client %s disconnected: %s", addr[0], e)
        finally:
            with self._ctrl_lock:
                if conn in self._ctrl_clients:
                    self._ctrl_clients.remove(conn)
            try:
                conn.close()
            except Exception:
                pass

    def _send_ctrl_msg(self, conn: socket.socket, msg: dict):
        conn.sendall((json.dumps(msg) + "\n").encode())

    def _send_ctrl_state(self, conn: socket.socket):
        srv_conn = (self.rpc.connected and self.client_id is not None) \
                   if self.mode == MODE_SYNC else False
        try:
            self._send_ctrl_msg(conn, {
                "type":           "state",
                "mode":           self.mode,
                "volume":         self.volume,
                "snap_connected": srv_conn,
                "bt_connected":   self.bt_connected,
                "bt_dev_name":    self.bt_dev_name,
                "client_name":    self.hostname,
                "powered":        self.powered,
                "dsp_on":         self.dsp_on,
                "amp_on":         self.amp_on,
            })
        except Exception as e:
            log.warning("Failed to send ctrl state: %s", e)

    def broadcast_ctrl_state(self):
        with self._ctrl_lock:
            dead = []
            for conn in self._ctrl_clients:
                try:
                    self._send_ctrl_state(conn)
                except Exception:
                    dead.append(conn)
            for conn in dead:
                self._ctrl_clients.remove(conn)

    def _handle_ctrl_msg(self, conn: socket.socket, msg: dict):
        mtype = msg.get("type", "")

        if mtype == "set_mode":
            new_mode = msg.get("mode", MODE_SYNC)
            log.info("Control: set_mode -> %d", new_mode)
            try:
                self._send_ctrl_msg(conn, {"type": "switching"})
            except Exception:
                pass
            threading.Thread(
                target=self._do_mode_switch,
                args=(new_mode,),
                daemon=True,
            ).start()

        elif mtype == "set_volume":
            vol = msg.get("volume", self.volume)
            if self.mode == MODE_BT:
                self._set_bt_volume(vol)
            else:
                self.volume = vol
            self.send_vol_update(vol)
            self.send_state(force=True)

        elif mtype == "set_powered":
            powered = msg.get("powered", True)
            log.info("Control: set_powered -> %s", powered)
            threading.Thread(
                target=self._do_power_sequence,
                args=(powered,),
                daemon=True,
            ).start()

        elif mtype == "get_state":
            self._send_ctrl_state(conn)

        elif mtype == "ping":
            try:
                self._send_ctrl_msg(conn, {"type": "pong"})
            except Exception:
                pass

        elif mtype == "removed":
            log.info("Server removed us — restarting snapclient to re-register")
            if self.mode == MODE_SYNC:
                threading.Thread(
                    target=self._reconnect_after_remove,
                    daemon=True,
                ).start()

    # ────────────────────────────────────────────
    # Password
    # ────────────────────────────────────────────
    def _on_pw_broadcast(self, h: str):
        save_password_hash(h)
        self._pw_hash = h
        log.info("Password updated from server broadcast")

    def _handle_pw_check(self, payload: bytes):
        raw = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
        attempt_hash = sha256_hex(raw)
        match = (attempt_hash == self._pw_hash)
        self.send_frame(MSG_PW_RESULT, bytes([1 if match else 0]))
        log.info("PW_CHECK: %s", "correct" if match else "wrong")

    # ────────────────────────────────────────────
    # Rename
    # ────────────────────────────────────────────
    def _handle_rename(self, payload: bytes):
        new_name = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
        new_name = re.sub(r"[^a-zA-Z0-9\-]", "", new_name)[:32]
        if not new_name:
            log.warning("RENAME: empty or invalid name, ignoring")
            return
        log.info("RENAME: setting hostname to '%s'", new_name)
        try:
            subprocess.run(
                ["sudo", "hostnamectl", "set-hostname", new_name],
                timeout=5, check=True, capture_output=True
            )
            try:
                with open("/etc/hosts", "r") as f:
                    lines = f.readlines()
                with open("/etc/hosts", "w") as f:
                    for line in lines:
                        if "127.0.1.1" in line:
                            f.write(f"127.0.1.1\t{new_name}\n")
                        else:
                            f.write(line)
            except OSError as e:
                log.warning("Could not update /etc/hosts: %s", e)

            self.hostname = new_name
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

    # ────────────────────────────────────────────
    # RPC
    # ────────────────────────────────────────────
    def ensure_rpc(self):
        now = time.time()
        if now - self._last_rpc_attempt < self.RPC_RETRY_S:
            return
        self._last_rpc_attempt = now

        if not self.server_ip:
            self.server_ip = get_snapserver_ip()
            if not self.server_ip:
                return

        if not self.rpc.connected:
            if not self.rpc.connect(self.server_ip):
                return

        if not self.client_id:
            self.client_id = self.rpc.find_client_id_by_hostname(self.hostname)
            if self.client_id:
                vol = self.rpc.get_volume_for_client(self.client_id)
                if vol is not None:
                    self.volume = vol
                self.send_state(force=True)
                self.broadcast_ctrl_state()

    # ────────────────────────────────────────────
    # Mode transitions
    # ────────────────────────────────────────────
    def enter_sync_mode(self):
        log.info("=== ENTERING SYNC MODE ===")
        self.mode = MODE_SYNC
        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_SYNC]))

        self._source_watcher.deactivate()

        bt_disconnect_all()
        bt_stop_discoverable()
        pulseaudio_stop()
        time.sleep(1)

        if not snapclient_is_running():
            snapclient_start()
            time.sleep(2)

        self.rpc.disconnect()
        self.server_ip = None
        self.client_id = None
        self._last_rpc_attempt = 0.0
        self.bt_connected = False
        self.bt_dev_name  = ""

        for _ in range(5):
            self.server_ip = get_snapserver_ip()
            if self.server_ip and self.rpc.connect(self.server_ip):
                self.client_id = self.rpc.find_client_id_by_hostname(self.hostname)
                if self.client_id:
                    vol = self.rpc.get_volume_for_client(self.client_id)
                    if vol is not None:
                        self.volume = vol
                    fetched = fetch_hash_from_server(self.server_ip)
                    if fetched:
                        save_password_hash(fetched)
                        self._pw_hash = fetched
                    break
            time.sleep(1)

        self.send_state(force=True)

    def enter_bt_mode(self):
        log.info("=== ENTERING BT MODE ===")
        self.mode = MODE_BT
        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_BT]))

        self.rpc.disconnect()
        self.client_id = None
        self.server_ip = None
        snapclient_stop()
        time.sleep(1)
        pulseaudio_start()
        time.sleep(2)

        try:
            subprocess.run(
                ["pactl", "load-module", "module-loopback", "latency_msec=500"],
                timeout=5, capture_output=True
            )
            log.info("Loopback module loaded")
        except Exception as e:
            log.error("loopback load failed: %s", e)

        bt_start_discoverable()

        # Read initial volume from the A2DP source (phone's current volume)
        # rather than the sink, so we start in sync with the phone.
        bt_vol = bt_get_source_volume()
        if bt_vol is not None:
            log.info("Initial BT source volume from phone: %d%%", bt_vol)
            self.volume = bt_vol
            # Sync sink to match so loopback level is consistent
            pa_set_volume(bt_vol)
        else:
            log.info("No A2DP source yet — using last known volume: %d%%", self.volume)

        # Activate the pactl watcher so we catch phone-side changes
        self._source_watcher.activate()

        self.send_state(force=True)

    def _do_mode_switch(self, new_mode: int):
        if new_mode == self.mode:
            self.send_state(force=True)
            self.broadcast_ctrl_state()
            return

        with self._ctrl_lock:
            dead = []
            for conn in self._ctrl_clients:
                try:
                    self._send_ctrl_msg(conn, {"type": "switching"})
                except Exception:
                    dead.append(conn)
            for conn in dead:
                self._ctrl_clients.remove(conn)

        if new_mode == MODE_BT:
            self.enter_bt_mode()
        else:
            self.enter_sync_mode()
        self.broadcast_ctrl_state()

    # ────────────────────────────────────────────
    # ESP message handling
    # ────────────────────────────────────────────
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

        elif msg_type == MSG_BOOT_STATUS_REQ:
            log.info("ESP requested boot sequence")
            threading.Thread(
                target=self._do_boot_sequence,
                daemon=True,
            ).start()

        elif msg_type == MSG_PING:
            self.send_frame(MSG_PONG)

        elif msg_type == MSG_VOL_SET:
            if len(payload) < 1:
                return
            vol = payload[0]
            self._esp_vol_set_time = time.time()
            if self.mode == MODE_SYNC:
                if self.rpc.connected and self.client_id:
                    self.rpc.set_volume(self.client_id, vol)
                    self.volume = vol
                else:
                    log.warning("VOL_SET ignored — RPC not ready")
            else:
                # BT mode: set both source (AVRCP → phone) and sink
                self._set_bt_volume(vol)

        elif msg_type == MSG_MODE_SYNC:
            if self.mode != MODE_SYNC:
                threading.Thread(
                    target=self._do_mode_switch,
                    args=(MODE_SYNC,),
                    daemon=True,
                ).start()

        elif msg_type == MSG_MODE_BT:
            if self.mode != MODE_BT:
                threading.Thread(
                    target=self._do_mode_switch,
                    args=(MODE_BT,),
                    daemon=True,
                ).start()

        elif msg_type == MSG_POWER_SET:
            if len(payload) < 1:
                return
            powered = payload[0] != 0
            log.info("MSG_POWER_SET from ESP: %s", powered)
            threading.Thread(
                target=self._do_power_sequence,
                args=(powered,),
                daemon=True,
            ).start()

        elif msg_type == MSG_DSP_SET:
            if len(payload) < 1:
                return
            on = payload[0] != 0
            log.info("MSG_DSP_SET from ESP: %s", on)
            threading.Thread(
                target=self._do_dsp_sequence,
                args=(on,),
                daemon=True,
            ).start()

        elif msg_type == MSG_AMP_SET:
            if len(payload) < 1:
                return
            on = payload[0] != 0
            log.info("MSG_AMP_SET from ESP: %s", on)
            threading.Thread(
                target=self._do_amp_sequence,
                args=(on,),
                daemon=True,
            ).start()

        elif msg_type == MSG_PW_CHECK:
            self._handle_pw_check(payload)

        elif msg_type == MSG_RENAME:
            self._handle_rename(payload)

        else:
            log.warning("Unknown ESP msg: 0x%02X", msg_type)

    # ────────────────────────────────────────────
    # Snap notifications
    # ────────────────────────────────────────────
    def handle_snap_notifications(self):
        if not self.rpc.connected:
            return
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

    # ────────────────────────────────────────────
    # BT polling (periodic fallback — watcher handles real-time events)
    # ────────────────────────────────────────────
    def poll_bt(self):
        conn, name = bt_get_connected_device()
        changed = False

        if conn != self.bt_connected:
            self.bt_connected = conn
            changed = True
            if conn:
                # Device just connected — read its current volume immediately
                bt_vol = bt_get_source_volume()
                if bt_vol is not None:
                    log.info("BT device connected, initial volume: %d%%", bt_vol)
                    if bt_vol != self.volume:
                        self.volume = bt_vol
                        pa_set_volume(bt_vol)
                        self.send_vol_update(bt_vol)
        if name != self.bt_dev_name:
            self.bt_dev_name = name
            changed = True

        # Periodic source volume check as fallback
        # (in case a subscribe event was missed)
        if conn and time.time() - self._bt_vol_set_time >= 1.0:
            src_vol = bt_get_source_volume()
            if src_vol is not None and src_vol != self.volume:
                log.info("BT poll: volume drift detected %d%% -> %d%%",
                         self.volume, src_vol)
                self.volume = src_vol
                pa_set_volume(src_vol)
                self.send_vol_update(src_vol)
                changed = True

        if changed:
            self.send_state()
            self.broadcast_ctrl_state()

    # ────────────────────────────────────────────
    # Main loop
    # ────────────────────────────────────────────
    def run(self):
        log.info("Client bridge — %s @ %d — hostname: %s",
                 self.ser.port, self.ser.baudrate, self.hostname)

        gpio_init()
        self._read_relay_state()
        log.info("Startup relay state: DSP=%s AMP=%s", self.dsp_on, self.amp_on)

        threading.Thread(target=self._discovery_thread,   daemon=True).start()
        threading.Thread(target=self._ctrl_server_thread, daemon=True).start()

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

            if self._esp_connected:
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

            if self.mode == MODE_BT:
                now = time.time()
                if now - self._last_poll_time >= self.POLL_INTERVAL_S:
                    self._last_poll_time = now
                    self.poll_bt()


def main():
    ap = argparse.ArgumentParser(description="ESP ↔ Snapclient bridge (client Pi)")
    ap.add_argument("--port", default="/dev/ttyAMA0")
    ap.add_argument("--baud", type=int, default=460800)
    args = ap.parse_args()

    bridge = ClientBridge(args.port, args.baud)
    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("Shutting down")
        bridge.rpc.disconnect()
        gpio_cleanup()


if __name__ == "__main__":
    main()
