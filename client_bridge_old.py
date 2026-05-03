#!/usr/bin/env python3
"""
esp_bridge.py — Pi CLIENT-side daemon (460800 baud)
Two modes:
  SYNC: snapclient runs, auto-connects to snapserver.
        Volume controlled via snapserver JSON-RPC (same approach as server bridge).
        Client ID resolved by hostname matching.
  BT:   stops snapclient, BT discoverable. Volume via pactl.
"""

import argparse
import json
import logging
import re
import select
import socket
import subprocess
import threading
import time
from typing import Optional

import serial

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bridge")

# ── Protocol constants ───────────────────────────────────────────────────────

SYNC_0, SYNC_1 = 0xAA, 0x55
MAX_PAYLOAD     = 2048

MSG_INIT            = 0x01
MSG_VOL_SET         = 0x02
MSG_PING            = 0x04
MSG_MODE_SYNC       = 0x05
MSG_MODE_BT         = 0x06

MSG_ACK             = 0x10
MSG_CLIENT_VOL_UPD  = 0x12
MSG_PONG            = 0x13
MSG_STATE_UPDATE    = 0x20
MSG_MODE_SWITCHING  = 0x21

MODE_SYNC, MODE_BT  = 0, 1
DEVICE_NAME_LEN     = 32
STATE_UPDATE_SIZE   = 68
ACK_PAYLOAD_SIZE    = 33

SNAPCLIENT_SERVICE  = "snapclient"
SNAP_RPC_PORT       = 1705


# ── CRC-8 ────────────────────────────────────────────────────────────────────

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


# ── Frame builder ────────────────────────────────────────────────────────────

def build_frame(msg_type: int, payload: bytes = b"") -> bytes:
    plen = len(payload)
    header = bytes([SYNC_0, SYNC_1, msg_type, plen & 0xFF, (plen >> 8) & 0xFF])
    crc_data = bytes([msg_type, plen & 0xFF, (plen >> 8) & 0xFF]) + payload
    return header + payload + bytes([crc8(crc_data)])


def pad(s: str, length: int) -> bytes:
    return s.encode("utf-8")[:length].ljust(length, b'\x00')


# ── UART frame receiver ───────────────────────────────────────────────────────

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


# ── Snapcast JSON-RPC (server-bridge style) ───────────────────────────────────

class SnapcastRPC:
    """Direct persistent connection to snapserver JSON-RPC, same as server bridge."""

    def __init__(self):
        self.sock: Optional[socket.socket] = None
        self._recv_buf = b""
        self._req_id = 0
        self._lock = threading.Lock()
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
        if not self.sock:
            return None
        with self._lock:
            self._req_id += 1
            req_id = self._req_id

        msg = json.dumps({
            "id": req_id, "jsonrpc": "2.0",
            "method": method, "params": params,
        }) + "\r\n"

        try:
            self.sock.sendall(msg.encode())
        except OSError as e:
            log.error("RPC send failed: %s", e)
            self.disconnect()
            return None

        deadline = time.time() + 5.0
        while time.time() < deadline:
            try:
                ready, _, _ = select.select([self.sock], [], [], 0.5)
            except (ValueError, OSError):
                self.disconnect()
                return None
            if ready:
                try:
                    data = self.sock.recv(4096)
                except OSError as e:
                    log.error("RPC recv failed: %s", e)
                    self.disconnect()
                    return None
                if not data:
                    log.warning("RPC connection closed by server")
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
                host = c.get("host", {})
                if host.get("name", "").lower() == hostname.lower():
                    cid = c.get("id", "")
                    log.info("Found client ID for hostname '%s': %s", hostname, cid)
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
                    log.warning("RPC connection closed")
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


# ── Get snapserver IP from snapclient journal ─────────────────────────────────

def get_snapserver_ip() -> Optional[str]:
    """Read ALL snapclient journal logs to find the last known server IP."""
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
            log.info("Found snapserver IP from journal: %s", server_ip)
        return server_ip
    except Exception as e:
        log.error("Failed to read snapclient journal: %s", e)
        return None


# ── BT helpers ────────────────────────────────────────────────────────────────

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
                log.info("Disconnecting BT device: %s", mac)
                subprocess.run(["bluetoothctl", "disconnect", mac],
                               timeout=5, capture_output=True)
    except Exception as e:
        log.error("bt_disconnect_all error: %s", e)


def bt_start_discoverable():
    log.info("Starting BT discoverable")
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


def bt_get_connected_device():
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


def pa_get_volume() -> int:
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
    try:
        subprocess.run(["pactl", "set-sink-volume", "@DEFAULT_SINK@",
                        f"{percent}%"], timeout=3, capture_output=True)
    except Exception as e:
        log.error("pactl error: %s", e)


def snapclient_start():
    log.info("Starting snapclient service")
    try:
        subprocess.run(["sudo", "systemctl", "start", SNAPCLIENT_SERVICE],
                       timeout=10, capture_output=True)
    except Exception as e:
        log.error("snapclient start: %s", e)


def snapclient_stop():
    log.info("Stopping snapclient service")
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


# ── Main bridge ──────────────────────────────────────────────────────────────

class ClientBridge:
    ESP_TIMEOUT_S       = 20
    POLL_INTERVAL_S     = 2.0
    RPC_RETRY_S         = 5.0

    def __init__(self, serial_port: str, baud: int):
        self.ser      = serial.Serial(serial_port, baud, timeout=0.05)
        self.rx       = UARTReceiver()
        self._running = True

        self.mode     = MODE_SYNC
        self.hostname = get_hostname()
        self.volume   = 0

        # SYNC state — server-bridge style
        self.rpc                = SnapcastRPC()
        self.server_ip:  Optional[str] = None
        self.client_id:  Optional[str] = None
        self._esp_vol_set_time  = 0.0
        self._last_rpc_attempt  = 0.0

        # BT state
        self.bt_connected = False
        self.bt_dev_name  = ""

        # ESP tracking
        self._esp_connected     = False
        self._last_esp_msg_time = time.time()
        self._last_poll_time    = 0.0
        self._last_state_sent   = None

    # ── Frame sending ──

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
        srv_conn = (self.rpc.connected and self.client_id is not None) if self.mode == MODE_SYNC else False
        bt_conn  = self.bt_connected if self.mode == MODE_BT else False
        bt_name  = self.bt_dev_name  if self.mode == MODE_BT else ""

        payload = bytearray()
        payload.append(self.mode)
        payload.append(self.volume & 0xFF)
        payload.append(1 if srv_conn else 0)
        payload.append(1 if bt_conn  else 0)
        payload += pad(bt_name,       DEVICE_NAME_LEN)
        payload += pad(self.hostname, DEVICE_NAME_LEN)
        payload = bytes(payload)

        if not force and payload == self._last_state_sent:
            return

        self._last_state_sent = payload
        self.send_frame(MSG_STATE_UPDATE, payload)
        log.debug("Sent STATE_UPDATE: mode=%d vol=%d srv=%d bt=%d",
                  self.mode, self.volume, srv_conn, bt_conn)

    # ── RPC connect + client ID resolve (server-bridge style) ──

    def ensure_rpc(self):
        now = time.time()
        if now - self._last_rpc_attempt < self.RPC_RETRY_S:
            return
        self._last_rpc_attempt = now

        if not self.server_ip:
            self.server_ip = get_snapserver_ip()
            if not self.server_ip:
                log.warning("Snapserver IP not found in journal")
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
                    log.info("Initial volume from server: %d", vol)
                self.send_state(force=True)
            else:
                log.warning("Could not find our client ID yet")

    # ── Mode transitions ──
    def enter_sync_mode(self):
        log.info("=== ENTERING SYNC MODE ===")
        self.mode = MODE_SYNC

        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_SYNC]))

        bt_disconnect_all()
        bt_stop_discoverable()

        # Reset hardware volume so snapclient isn't quiet from BT's last setting
        pa_set_volume(80)

        if not snapclient_is_running():
            snapclient_start()
            time.sleep(2)

        self.rpc.disconnect()
        self.server_ip = None
        self.client_id = None
        self._last_rpc_attempt = 0
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
                    break
            time.sleep(1)

        self.send_state(force=True)
    
    def enter_bt_mode(self):
        log.info("=== ENTERING BT MODE ===")
        self.mode = MODE_BT

        # Tell ESP to show loading spinner immediately
        self.send_frame(MSG_MODE_SWITCHING, bytes([MODE_BT]))

        self.rpc.disconnect()
        self.client_id = None
        self.server_ip = None
        snapclient_stop()

        bt_start_discoverable()
        self.volume = pa_get_volume()

        self.send_state(force=True)

    # ── ESP message handling ──

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
            log.info("VOL_SET: %d (mode=%s)", vol,
                     "SYNC" if self.mode == MODE_SYNC else "BT")

            self._esp_vol_set_time = time.time()

            if self.mode == MODE_SYNC:
                if self.rpc.connected and self.client_id:
                    self.rpc.set_volume(self.client_id, vol)
                    self.volume = vol
                else:
                    log.warning("VOL_SET ignored — RPC not ready (connected=%s client_id=%s)",
                                self.rpc.connected, self.client_id is not None)
            else:
                pa_set_volume(vol)
                self.volume = vol

        elif msg_type == MSG_MODE_SYNC:
            if self.mode != MODE_SYNC:
                self.enter_sync_mode()

        elif msg_type == MSG_MODE_BT:
            if self.mode != MODE_BT:
                self.enter_bt_mode()

        else:
            log.warning("Unknown ESP msg: 0x%02X", msg_type)

    # ── Snap notification handling (server-bridge style) ──

    def handle_snap_notifications(self):
        if not self.rpc.connected:
            return
        try:
            notifications = self.rpc.read_notifications()
        except (ConnectionError, OSError):
            log.error("RPC connection lost during notification read")
            self.rpc.disconnect()
            self.client_id = None
            self._last_rpc_attempt = 0
            self.send_state(force=True)
            return

        for n in notifications:
            method = n.get("method", "")
            if method == "Client.OnVolumeChanged":
                params = n.get("params", {})
                cid = params.get("id", "")
                if cid == self.client_id:
                    vol = params.get("volume", {}).get("percent", self.volume)
                    if time.time() - self._esp_vol_set_time < 1.0:
                        log.debug("Suppressing echo vol notification")
                    else:
                        log.info("External vol change: %d → %d", self.volume, vol)
                        self.volume = vol
                        self.send_vol_update(vol)

            elif method in ("Client.OnConnect", "Client.OnDisconnect"):
                log.info("Snapcast event: %s — re-resolving client ID", method)
                self.client_id = None
                self._last_rpc_attempt = 0

    # ── Periodic polling ──

    def poll_bt(self):
        conn, name = bt_get_connected_device()
        hw_vol     = pa_get_volume()

        changed = False
        if conn != self.bt_connected:
            self.bt_connected = conn
            changed = True
        if name != self.bt_dev_name:
            self.bt_dev_name = name
            changed = True
        if hw_vol != self.volume:
            if time.time() - self._esp_vol_set_time >= 1.0:
                self.send_vol_update(hw_vol)
            self.volume = hw_vol
            changed = True

        if changed:
            self.send_state()

    # ── Main loop ──

    def run(self):
        log.info("Client bridge — %s @ %d — hostname: %s",
                 self.ser.port, self.ser.baudrate, self.hostname)

        self.enter_sync_mode()

        last_heavy_poll = 0.0

        while self._running:
            data = self.ser.read(256)
            if data:
                for msg_type, payload in self.rx.feed(data):
                    self.handle_esp_message(msg_type, payload)

            if self._esp_connected:
                if time.time() - self._last_esp_msg_time > self.ESP_TIMEOUT_S:
                    log.warning("ESP silent for %.0fs — marking disconnected",
                                self.ESP_TIMEOUT_S)
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
                                        log.info("Polled vol change: %d → %d", self.volume, vol)
                                        self.volume = vol
                                        self.send_vol_update(vol)
                            except Exception:
                                log.error("RPC health check failed — reconnecting")
                                self.rpc.disconnect()
                                self.client_id = None
                                self._last_rpc_attempt = 0
                                self.send_state(force=True)
                else:
                    if now - self._last_poll_time >= self.POLL_INTERVAL_S:
                        self._last_poll_time = now
                        self.poll_bt()


def main():
    ap = argparse.ArgumentParser(
        description="ESP ↔ Snapclient bridge (client Pi)")
    ap.add_argument("--port", default="/dev/ttyAMA0")
    ap.add_argument("--baud", type=int, default=460800)
    args = ap.parse_args()

    bridge = ClientBridge(args.port, args.baud)
    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("Shutting down")
        bridge.rpc.disconnect()


if __name__ == "__main__":
    main()
