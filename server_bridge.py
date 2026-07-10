import argparse
import json
import logging
import select
import socket
import subprocess
import threading
import time
import hashlib
import os
import re
from typing import Optional

import serial

import shutil
GPIO_AVAILABLE = bool(shutil.which("gpioset"))
GPIO_CHIP = "gpiochip0"
GPIO_LED  = 26

SYNC_0 = 0xAA
SYNC_1 = 0x55
MAX_PAYLOAD = 2048

MSG_INIT            = 0x01
MSG_VOL_SET         = 0x02
MSG_VOL_MUTE        = 0x03
MSG_PING            = 0x04
MSG_MODE_SYNC       = 0x05
MSG_MODE_BT         = 0x06
MSG_POWER_SET       = 0x07
MSG_RESTART_SERVER  = 0x09
MSG_INPUT_SET       = 0x0A   # 0=usb, 1=mic
MSG_MIC_GAIN_SET    = 0x0B
MSG_ACK             = 0x10
MSG_CLIENT_LIST     = 0x11
MSG_CLIENT_VOL_UPD  = 0x12
MSG_PONG            = 0x13
MSG_STATE_UPDATE    = 0x20
MSG_MODE_SWITCHING  = 0x21
MSG_POWER_STATE     = 0x22
MSG_POWER_PENDING   = 0x23 
MSG_MIC_STATUS      = 0x24
MSG_PW_SET          = 0x30
MSG_RENAME          = 0x33  # add at top with other constants
MSG_PW_ACK          = 0x34

MODE_SYNC = 0
MODE_BT   = 1

CLIENT_ID_LEN     = 36
CLIENT_NAME_LEN   = 32
CLIENT_ENTRY_SIZE = CLIENT_ID_LEN + CLIENT_NAME_LEN + 8

CTRL_PORT         = 7702
PW_HASH_FILE      = "/etc/zone_password.hash"
PW_DEFAULT        = "anjay1234"
PW_BROADCAST_PORT = 7700

PING_INTERVAL_S  = 5
PING_MISS_MAX    = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server_bridge")


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

def _find_mic_source() -> Optional[str]:
    """Same selection rule as snapcast-sourcemic.service's DEVICE lookup."""
    try:
        result = subprocess.run(["pactl", "list", "sources", "short"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            name = parts[1]
            if "alsa_input" in name and "monitor" not in name and "platform-" not in name:
                return name
    except Exception as e:
        log.warning("_find_mic_source error: %s", e)
    return None


def _get_mic_gain(source: str) -> Optional[int]:
    try:
        result = subprocess.run(["pactl", "get-source-volume", source],
                                capture_output=True, text=True, timeout=3)
        m = re.search(r"(\d+)%", result.stdout)
        if m:
            return int(m.group(1))
    except Exception as e:
        log.warning("_get_mic_gain error: %s", e)
    return None

class HashPullServer(threading.Thread):
    def __init__(self, get_hash_fn):
        super().__init__(daemon=True)
        self._get_hash = get_hash_fn

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", 7701))
        srv.listen(10)
        log.info("Hash pull server on port 7701")
        while True:
            try:
                conn, addr = srv.accept()
                with conn:
                    conn.sendall(self._get_hash().encode("ascii"))
                log.info("Sent hash to %s", addr[0])
            except Exception as e:
                log.error("Hash pull server error: %s", e)


class SnapcastClient:
    def __init__(self, host="127.0.0.1", port=1705):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self._req_id = 1
        self._lock = threading.Lock()
        self._recv_buf = b""
        self.pending_notifications = []

    def connect(self):
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, self.port))
                self.sock.setblocking(False)
                log.info("Connected to Snapcast at %s:%d", self.host, self.port)
                return
            except OSError as e:
                log.warning("Snapcast connect failed: %s — retrying in 3s", e)
                time.sleep(3)

    def _send_request(self, method: str, params: dict) -> dict:
        with self._lock:
            req_id = self._req_id
            self._req_id += 1
        msg = json.dumps({"id": req_id, "jsonrpc": "2.0",
                          "method": method, "params": params}) + "\r\n"
        self.sock.sendall(msg.encode())
        deadline = time.time() + 5.0
        while time.time() < deadline:
            ready, _, _ = select.select([self.sock], [], [], 0.5)
            if ready:
                if not self.sock:
                    return {}
                try:
                    data = self.sock.recv(4096)
                except OSError as e:
                    log.error("RPC recv failed: %s", e)
                    self.sock = None
                    self._recv_buf = b""
                    return {}
                if not data:
                    raise ConnectionError("Snapcast closed")
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
        raise TimeoutError(f"No response for {method}")

    def get_status(self) -> dict:
        return self._send_request("Server.GetStatus", {})

    def set_volume(self, client_id: str, volume: int, muted: bool = False):
        self._send_request("Client.SetVolume", {
            "id": client_id,
            "volume": {"percent": volume, "muted": muted},
        })

    def read_notifications(self) -> list:
        notifications = list(self.pending_notifications)
        self.pending_notifications.clear()
        try:
            ready, _, _ = select.select([self.sock], [], [], 0)
            if ready:
                if not self.sock:
                    return notifications
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("Snapcast closed")
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


class UARTReceiver:
    SYNC_0_ST, SYNC_1_ST, HEADER_ST, PAYLOAD_ST, CRC_ST = range(5)

    def __init__(self):
        self.state = self.SYNC_0_ST
        self.header_buf = bytearray(3)
        self.header_idx = 0
        self.msg_type = 0
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
                elif byte != SYNC_0:
                    self.state = self.SYNC_0_ST
            elif self.state == self.HEADER_ST:
                self.header_buf[self.header_idx] = byte
                self.header_idx += 1
                if self.header_idx == 3:
                    self.msg_type = self.header_buf[0]
                    self.payload_len = self.header_buf[1] | (self.header_buf[2] << 8)
                    if self.payload_len > MAX_PAYLOAD:
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
                crc_data = bytes([self.msg_type,
                                  self.payload_len & 0xFF,
                                  (self.payload_len >> 8) & 0xFF
                                  ]) + bytes(self.payload_buf[:self.payload_len])
                if byte == crc8(crc_data):
                    yield (self.msg_type, bytes(self.payload_buf[:self.payload_len]))
                else:
                    log.warning("CRC mismatch")
                self.state = self.SYNC_0_ST


class ClientRecord:
    def __init__(self, snap_id: str, name: str):
        self.snap_id        = snap_id
        self.name           = name
        self.volume         = 0
        self.muted          = False
        self.snap_connected = False
        self.mode           = MODE_SYNC
        self.powered        = False
        self.power_pending  = False 
        self.power_target   = False  
        self.bt_connected   = False
        self.bt_dev_name    = ""
        self.client_ip      = ""

        self._sock: Optional[socket.socket] = None
        self._sock_lock = threading.Lock()
        self._last_pong = time.time()
        self._ping_misses = 0

    def set_sock(self, sock: socket.socket, addr):
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
            self._sock = sock
            self.client_ip = addr[0]
            self._last_pong = time.time()
            self._ping_misses = 0

    def ctrl_send(self, msg: dict):
        with self._sock_lock:
            sock = self._sock
        if not sock:
            return
        try:
            sock.sendall((json.dumps(msg) + "\n").encode())
        except BlockingIOError:
            # send buffer momentarily full — transient, don't tear down a healthy socket
            log.warning("ctrl_send to %s: send buffer busy, skipping this cycle", self.name)
        except Exception as e:
            log.warning("ctrl_send to %s failed: %s", self.name, e)
            with self._sock_lock:
                self._sock = None

    def disconnect(self):
        with self._sock_lock:
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None

    def disconnect_if(self, sock) -> bool:
        """Only closes/clears if `sock` is still this record's active socket.
        Returns True if it actually cleaned up (i.e. this was the live session)."""
        with self._sock_lock:
            if self._sock is sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
                return True
            return False
        
    def connected(self) -> bool:
        with self._sock_lock:
            return self._sock is not None

    def record_pong(self):
        self._last_pong = time.time()
        self._ping_misses = 0

    def tick_ping(self) -> bool:
        if time.time() - self._last_pong > PING_INTERVAL_S:
            self._ping_misses += 1
            if self._ping_misses >= PING_MISS_MAX:
                return True
        return False


def sha256_hex(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def load_or_init_password() -> tuple[str, bool]:
    if os.path.exists(PW_HASH_FILE):
        with open(PW_HASH_FILE, "r") as f:
            h = f.read().strip()
        if len(h) == 64:
            return h, True
    h = sha256_hex(PW_DEFAULT)
    with open(PW_HASH_FILE, "w") as f:
        f.write(h)
    return h, False


def _write_hash_file(h: str):
    with open(PW_HASH_FILE, "w") as f:
        f.write(h)


def _broadcast_hash_to_one(ip: str, h: str):
    try:
        with socket.create_connection((ip, PW_BROADCAST_PORT), timeout=5) as s:
            s.sendall(h.encode("ascii"))
        log.info("PW broadcast sent to %s", ip)
    except OSError as e:
        log.warning("PW broadcast failed for %s: %s", ip, e)


class ServerBridge:
    ESP_TIMEOUT_S          = 20
    SNAP_HEALTH_INTERVAL_S = 10
    MIC_CHECK_INTERVAL_S   = 2
    CLIENT_LIST_RESEND_S   = 5

    def __init__(self, serial_port, baud, snap_host, snap_port):
        self.ser  = serial.Serial(serial_port, baud, timeout=0.05)
        self.snap = SnapcastClient(snap_host, snap_port)
        self.rx   = UARTReceiver()

        self._clients: dict[str, ClientRecord] = {}
        self._clients_lock = threading.Lock()

        self._esp_connected        = False
        self._last_esp_msg_time    = time.time()
        self._last_snap_health     = time.time()
        self._last_client_list_send = 0.0
        self._esp_vol_set_time     = {}
        self._running              = True
        self._input_mode           = 0  # 0=usb 1=mic; default usb, both services start disabled
        self._mic_present          = False   # ← add
        self._mic_gain              = 50     # ← add
        self._last_mic_check        = 0.0    # ← add

        self._pw_hash, self._pw_user_set = load_or_init_password()
        self._hash_pull_server = HashPullServer(lambda: self._pw_hash)
        self._hash_pull_server.start()

    # ── Client lookup — by snap_id first, then name fallback ──────────────
    def _find_client(self, snap_id: str) -> Optional[ClientRecord]:
        """
        Look up a client by snap_id. Falls back to name match so commands
        from the ESP still work during the window before Snapcast resolves
        the real snap_id (MAC address).
        """
        with self._clients_lock:
            rec = self._clients.get(snap_id)
            if rec:
                return rec
            # Fallback: match by name (snap_id sent by ESP may still be the
            # hostname placeholder used before Snapcast resolution)
            name_lower = snap_id.lower()
            for r in self._clients.values():
                if r.name.lower() == name_lower:
                    return r
        return None

    # ── Registration listener ──────────────────────────────────────────────
    def _registration_listener(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", CTRL_PORT))
        srv.listen(24)
        log.info("Registration listener on port %d", CTRL_PORT)
        while True:
            try:
                conn, addr = srv.accept()
                conn.setblocking(False)
                log.info("Inbound connection from %s", addr[0])
                threading.Thread(
                    target=self._client_session,
                    args=(conn, addr),
                    daemon=True,
                ).start()
            except Exception as e:
                log.error("Registration listener error: %s", e)

    def _client_session(self, sock: socket.socket, addr):
        buf = b""
        rec: Optional[ClientRecord] = None

        sock.settimeout(10)
        try:
            while b"\n" not in buf:
                chunk = sock.recv(1024)
                if not chunk:
                    return
                buf += chunk
        except Exception:
            log.warning("No register message from %s — dropping", addr[0])
            sock.close()
            return

        sock.settimeout(None)
        sock.setblocking(False)

        line, buf = buf.split(b"\n", 1)
        try:
            msg = json.loads(line.strip())
        except json.JSONDecodeError:
            log.warning("Bad register JSON from %s", addr[0])
            sock.close()
            return

        if msg.get("type") != "register":
            log.warning("First message from %s was not register", addr[0])
            sock.close()
            return

        snap_id = msg.get("snap_id", "")[:CLIENT_ID_LEN]
        name    = msg.get("name", addr[0])

        if not snap_id:
            snap_id = name  # placeholder until Snapcast resolves real snap_id

        with self._clients_lock:
            rec = self._clients.get(snap_id)
            if rec is None:
                # Also check by name in case it was registered under a different key
                for r in self._clients.values():
                    if r.name.lower() == name.lower():
                        rec = r
                        # Re-key under new snap_id if different
                        if rec.snap_id != snap_id:
                            old_id = rec.snap_id
                            self._clients.pop(old_id, None)
                            rec.snap_id = snap_id
                            self._clients[snap_id] = rec
                        break
            if rec is None:
                rec = ClientRecord(snap_id, name)
                self._clients[snap_id] = rec
                log.info("New client registered: %s (%s)", name, snap_id)
            else:
                log.info("Client re-registered: %s (%s)", name, snap_id)

        rec.set_sock(sock, addr)
        rec.name           = name
        rec.mode           = msg.get("mode",           MODE_SYNC)
        rec.volume         = msg.get("volume",         0)
        rec.muted          = msg.get("muted",          False)
        rec.snap_connected = msg.get("snap_connected", False)
        rec.bt_connected   = msg.get("bt_connected",   False)
        rec.powered        = msg.get("powered",        False)

        log.info("Registered %s: mode=%d vol=%d powered=%s",
                 name, rec.mode, rec.volume, rec.powered)

        if self._esp_connected:
            self._send_client_list()
            threading.Thread(
                target=self._send_single_state,
                args=(rec,),
                daemon=True,
            ).start()

        try:
            while True:
                try:
                    data = sock.recv(1024)
                except BlockingIOError:
                    time.sleep(0.01)
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
                        m = json.loads(line)
                        self._on_ctrl_message(rec, m)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            log.warning("Client %s session error: %s", rec.name, e)
        finally:
            was_active = rec.disconnect_if(sock)
            if was_active:
                self._on_client_gone(rec)
            else:
                log.info("Stale session for %s closed (already superseded by a newer connection)", rec.name)

    def _send_single_state(self, rec: ClientRecord):
        time.sleep(0.3)
        if not self._esp_connected:
            return
        payload = self._id_bytes(rec.snap_id) + bytes([
            rec.mode,
            1 if rec.snap_connected else 0,
            1 if rec.bt_connected   else 0,
            rec.volume,
        ])
        self.send_frame(MSG_STATE_UPDATE, payload)
        self.send_frame(MSG_POWER_STATE,
                        self._id_bytes(rec.snap_id) + bytes([1 if rec.powered else 0]))

    def _on_client_gone(self, rec: ClientRecord):
        log.info("Client gone: %s (%s)", rec.name, rec.snap_id)
        with self._clients_lock:
            self._clients.pop(rec.snap_id, None)
        if self._esp_connected:
            self._send_client_list()

    # ── Ping/pong keepalive ───────────────────────────────────────────────
    def _keepalive_thread(self):
        while True:
            time.sleep(PING_INTERVAL_S)
            with self._clients_lock:
                records = list(self._clients.values())

            dead = []
            for rec in records:
                if not rec.connected():
                    continue
                if rec.tick_ping():
                    log.warning("Client %s missed %d pings — removing",
                                rec.name, PING_MISS_MAX)
                    dead.append(rec)
                else:
                    rec.ctrl_send({"type": "ping"})

            for rec in dead:
                rec.disconnect()
                self._on_client_gone(rec)

    # ── UART helpers ──────────────────────────────────────────────────────
    def send_frame(self, msg_type: int, payload: bytes = b""):
        frame = build_frame(msg_type, payload)
        self.ser.write(frame)
        self.ser.flush()

    def _id_bytes(self, snap_id: str) -> bytes:
        return snap_id.encode("ascii")[:CLIENT_ID_LEN].ljust(CLIENT_ID_LEN, b"\x00")

    # ── CLIENT_LIST ───────────────────────────────────────────────────────
    def _build_client_list_payload(self) -> bytes:
        with self._clients_lock:
            records = list(self._clients.values())

        count = min(len(records), 24)
        payload = bytes([count])

        for rec in records[:count]:
            id_bytes   = rec.snap_id.encode("ascii", errors="replace")[:CLIENT_ID_LEN]
            id_bytes   = id_bytes.ljust(CLIENT_ID_LEN, b"\x00")
            name_bytes = rec.name.encode("utf-8", errors="replace")[:CLIENT_NAME_LEN]
            name_bytes = name_bytes.ljust(CLIENT_NAME_LEN, b"\x00")
            payload   += (id_bytes + name_bytes
                          + bytes([
                              rec.volume,
                              1 if rec.muted         else 0,
                              1 if rec.snap_connected else 0,
                              rec.mode,
                              1 if rec.powered        else 0,
                              1 if rec.bt_connected   else 0,
                              1 if rec.power_pending  else 0,
                              1 if rec.power_target   else 0,
                          ]))
        return payload

    def _send_client_list(self):
        self._last_client_list_send = time.time()
        payload = self._build_client_list_payload()
        self.send_frame(MSG_CLIENT_LIST, payload)
        log.info("Sent CLIENT_LIST with %d clients", payload[0])
        threading.Thread(target=self._resend_all_states, daemon=True).start()

    def _resend_all_states(self):
        time.sleep(0.3)
        with self._clients_lock:
            records = list(self._clients.values())
        for rec in records:
            if not self._esp_connected:
                return
            payload = self._id_bytes(rec.snap_id) + bytes([
                rec.mode,
                1 if rec.snap_connected else 0,
                1 if rec.bt_connected   else 0,
                rec.volume,
            ])
            self.send_frame(MSG_STATE_UPDATE, payload)
            self.send_frame(MSG_POWER_STATE,
                            self._id_bytes(rec.snap_id) + bytes([1 if rec.powered else 0]))

    # ── Snapcast status ───────────────────────────────────────────────────
    def _update_from_snap_status(self, status: dict) -> bool:
        changed = False
        with self._clients_lock:
            for group in status.get("server", {}).get("groups", []):
                for c in group.get("clients", []):
                    snap_id   = c.get("id", "")[:CLIENT_ID_LEN]
                    config    = c.get("config", {})
                    host      = c.get("host", {})
                    name      = (config.get("name", "")
                                 or host.get("friendlyName", "")
                                 or host.get("name", "")
                                 or snap_id)
                    vol_info  = config.get("volume", {})
                    volume    = vol_info.get("percent", 0)
                    muted     = vol_info.get("muted", False)
                    connected = c.get("connected", False)

                    rec = self._clients.get(snap_id)
                    if rec is None:
                        # try match by name for placeholder-registered clients
                        for r in self._clients.values():
                            if r.name.lower() == name.lower() and r.snap_id == r.name:
                                old_id = r.snap_id
                                self._clients.pop(old_id, None)
                                r.snap_id = snap_id
                                self._clients[snap_id] = r
                                rec = r
                                log.info("Resolved snap_id for %s: %s", name, snap_id)
                                break
                        if rec is None:
                            continue

                    rec.name  = name
                    rec.muted = muted

                    just_connected = connected and not rec.snap_connected
                    if rec.snap_connected != connected:
                        changed = True
                    rec.snap_connected = connected

                    if rec.mode == MODE_SYNC and just_connected:
                        rec.volume = volume
        return changed
    
    # ── Control message handler ───────────────────────────────────────────
    def _on_ctrl_message(self, rec: ClientRecord, msg: dict):
        mtype = msg.get("type", "")

        if mtype == "pong":
            rec.record_pong()
            return

        if mtype == "state":
            new_snap_id = msg.get("snap_id", "")
            if new_snap_id and new_snap_id != rec.snap_id and len(new_snap_id) == CLIENT_ID_LEN:
                with self._clients_lock:
                    self._clients.pop(rec.snap_id, None)
                    rec.snap_id = new_snap_id
                    self._clients[new_snap_id] = rec
                log.info("snap_id resolved for %s: %s", rec.name, new_snap_id)

            rec.mode           = msg.get("mode",           rec.mode)
            rec.volume         = msg.get("volume",         rec.volume)
            rec.snap_connected = msg.get("snap_connected", rec.snap_connected)
            rec.bt_connected   = msg.get("bt_connected",   rec.bt_connected)
            rec.bt_dev_name    = msg.get("bt_dev_name",    "")
            rec.powered        = msg.get("powered",        rec.powered)
            
            new_name = msg.get("client_name", "")
            if new_name:
                old_name = rec.name
                rec.name = new_name
                if old_name != new_name and self._esp_connected:
                    self._send_client_list()   # ← name changed, push new list
                    return                     # _send_client_list already calls _resend_all_states

            if self._esp_connected:
                payload = self._id_bytes(rec.snap_id) + bytes([
                    rec.mode,
                    1 if rec.snap_connected else 0,
                    1 if rec.bt_connected   else 0,
                    rec.volume,
                ])
                self.send_frame(MSG_STATE_UPDATE, payload)
                self.send_frame(MSG_POWER_STATE,
                                self._id_bytes(rec.snap_id) + bytes([1 if rec.powered else 0]))
                
        elif mtype == "switching":
            if self._esp_connected:
                self.send_frame(MSG_MODE_SWITCHING, self._id_bytes(rec.snap_id))

        elif mtype == "power_state":
            powered = msg.get("powered", rec.powered)
            rec.powered = powered
            if rec.power_pending and powered == rec.power_target:   #
                rec.power_pending = False    
            if self._esp_connected:
                self.send_frame(MSG_POWER_STATE,
                                self._id_bytes(rec.snap_id) + bytes([1 if powered else 0]))
        
        elif mtype == "power_pending":                 
            target = msg.get("target", rec.powered)
            rec.power_pending = True  
            rec.power_target  = target
            if self._esp_connected:
                self.send_frame(MSG_POWER_PENDING,
                                self._id_bytes(rec.snap_id) + bytes([1 if target else 0]))

    # ── ESP message handlers ──────────────────────────────────────────────
    def handle_esp_message(self, msg_type: int, payload: bytes):
        self._last_esp_msg_time = time.time()

        if msg_type == MSG_INIT:
            log.info("ESP INIT received")
            self._esp_connected = True
            self.send_frame(MSG_ACK)
            time.sleep(0.1)
            try:
                status = self.snap.get_status()
                self._update_from_snap_status(status)
            except Exception as e:
                log.error("Snap status on INIT failed: %s", e)
            self._send_client_list()
            self.send_frame(MSG_PW_ACK, bytes([1 if self._pw_user_set else 0]))
            # On ESP connect, start whichever input was last active
            threading.Thread(
                target=self._apply_input_mode,
                args=(self._input_mode,),
                daemon=True,
            ).start()

        elif msg_type == MSG_PING:
            self.send_frame(MSG_PONG)

        elif msg_type == MSG_VOL_SET:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            snap_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            volume  = payload[CLIENT_ID_LEN]
            self._esp_vol_set_time[snap_id] = time.time()
            rec = self._find_client(snap_id)
            if rec:
                rec.volume = volume
                if rec.mode == MODE_SYNC:
                    try:
                        self.snap.set_volume(rec.snap_id, volume)
                    except (ConnectionError, OSError):
                        self._snap_reconnect()
                    except Exception as e:
                        log.error("VOL_SET snap failed: %s", e)
                if rec.connected():
                    rec.ctrl_send({"type": "set_volume", "volume": volume})

        elif msg_type in (MSG_MODE_SYNC, MSG_MODE_BT):
            if len(payload) < CLIENT_ID_LEN:
                return
            snap_id  = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            new_mode = MODE_SYNC if msg_type == MSG_MODE_SYNC else MODE_BT
            rec = self._find_client(snap_id)
            if rec and rec.connected():
                rec.ctrl_send({"type": "set_mode", "mode": new_mode})
                log.info("Mode switch -> %s sent to %s", "SYNC" if new_mode == MODE_SYNC else "BT", rec.name)
            else:
                log.warning("No control connection to %s", snap_id)

        elif msg_type == MSG_POWER_SET:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            snap_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            powered = payload[CLIENT_ID_LEN] != 0
            rec = self._find_client(snap_id)
            if rec and rec.connected():
                rec.ctrl_send({"type": "set_powered", "powered": powered})
                log.info("Power -> %s sent to %s", powered, rec.name)
            else:
                log.warning("No control connection to %s", snap_id)

        elif msg_type == MSG_INPUT_SET:
            if len(payload) < 1:
                return
            mode = payload[0]  # 0=usb, 1=mic
            self._input_mode = mode
            log.info("Input mode set: %s", "mic" if mode else "usb")
            threading.Thread(
                target=self._apply_input_mode,
                args=(mode,),
                daemon=True,
            ).start()
            if mode == 1:
                threading.Thread(target=self._poll_mic_status, args=(True,), daemon=True).start()  # ← add

        elif msg_type == MSG_RESTART_SERVER:
            log.info("ESP requested snapserver restart")
            threading.Thread(target=self._restart_snapserver, daemon=True).start()

        elif msg_type == MSG_PW_SET:
            if len(payload) < 64:
                return
            old_raw = payload[:64].rstrip(b"\x00").decode("utf-8", errors="replace")
            new_raw = payload[64:].rstrip(b"\x00").decode("utf-8", errors="replace")
            if not new_raw:
                return
            if self._pw_user_set and sha256_hex(old_raw) != self._pw_hash:
                log.warning("Password change rejected: wrong current password")
                self.send_frame(MSG_PW_ACK, bytes([0]))
                return
            new_hash = sha256_hex(new_raw)
            _write_hash_file(new_hash)
            self._pw_hash     = new_hash
            self._pw_user_set = True
            self.send_frame(MSG_PW_ACK, bytes([1]))
            log.info("Password updated")
            with self._clients_lock:
                records = list(self._clients.values())
            for rec in records:
                if rec.client_ip:
                    threading.Thread(
                        target=_broadcast_hash_to_one,
                        args=(rec.client_ip, new_hash),
                        daemon=True,
                    ).start()
        
        elif msg_type == MSG_RENAME:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            snap_id  = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            new_name = payload[CLIENT_ID_LEN:CLIENT_ID_LEN + CLIENT_NAME_LEN].rstrip(b"\x00").decode("utf-8", errors="replace")
            if not new_name:
                return
            rec = self._find_client(snap_id)
            if rec:
                old_name = rec.name
                rec.name = new_name
                log.info("Renamed %s -> %s", old_name, new_name)
                if rec.connected():
                    rec.ctrl_send({"type": "set_name", "name": new_name})
                self._send_client_list()
        
        elif msg_type == MSG_MIC_GAIN_SET:
            if len(payload) < 1:
                return
            gain = payload[0]
            source = _find_mic_source()
            if source:
                try:
                    subprocess.run(["pactl", "set-source-volume", source, f"{gain}%"],
                                timeout=3, capture_output=True)
                    self._mic_gain = gain
                    log.info("Mic gain set via ESP: %d%%", gain)
                except Exception as e:
                    log.error("Mic gain set failed: %s", e)
            else:
                log.warning("MIC_GAIN_SET received but no mic source found")
            self._poll_mic_status(force=True)

        else:
            log.warning("Unknown ESP msg: 0x%02X", msg_type)

    # ── Snapcast notification handler ─────────────────────────────────────
    def handle_snap_notification(self, notification: dict):
        method = notification.get("method", "")
        relevant = {
            "Client.OnConnect", "Client.OnDisconnect",
            "Client.OnNameChanged", "Server.OnUpdate",
        }
        if method not in relevant and method != "Client.OnVolumeChanged":
            return

        if method == "Client.OnVolumeChanged":
            params   = notification.get("params", {})
            snap_id  = params.get("id", "")[:CLIENT_ID_LEN]
            volume   = params.get("volume", {}).get("percent", 0)
            last_set = self._esp_vol_set_time.get(snap_id, 0)
            if time.time() - last_set < 1.0:
                return
            rec = self._find_client(snap_id)
            if rec:
                if rec.mode == MODE_BT:
                    return
                rec.volume = volume
            if self._esp_connected:
                self.send_frame(MSG_CLIENT_VOL_UPD,
                                self._id_bytes(snap_id) + bytes([volume]))
        else:
            time.sleep(0.5)
            try:
                status = self.snap.get_status()
                self._update_from_snap_status(status)
            except Exception as e:
                log.error("Snap refresh failed: %s", e)

    # ── Input mode switch ────────────────────────────────────────────────
    def _apply_input_mode(self, mode: int):
        """Switch audio input source."""
        try:
            if mode == 0:
                log.info("Switching input to USB/UAC")
                subprocess.run(["sudo", "systemctl", "stop",  "snapcast-sourcemic"],
                               timeout=10, check=False, capture_output=True)
                subprocess.run(["sudo", "systemctl", "start", "snapcast-source"],
                               timeout=10, check=False, capture_output=True)
                log.info("Input: USB/UAC active")
            else:
                log.info("Switching input to mic")
                subprocess.run(["sudo", "systemctl", "stop",  "snapcast-source"],
                               timeout=10, check=False, capture_output=True)
                subprocess.run(["sudo", "systemctl", "start", "snapcast-sourcemic"],
                               timeout=10, check=False, capture_output=True)
                log.info("Input: mic active")
        except Exception as e:
            log.error("Input mode switch failed: %s", e)
    
    def _poll_mic_status(self, force: bool = False):
        source  = _find_mic_source()
        present = source is not None
        gain    = self._mic_gain
        if present:
            g = _get_mic_gain(source)
            if g is not None:
                gain = g
        changed = (present != self._mic_present) or (gain != self._mic_gain)
        self._mic_present = present
        self._mic_gain    = gain
        if (changed or force) and self._esp_connected:
            self._send_mic_status()

    def _send_mic_status(self):
        self.send_frame(MSG_MIC_STATUS,
                        bytes([1 if self._mic_present else 0, self._mic_gain & 0xFF]))

    # ── Restart Snapserver ────────────────────────────────────────────────
    def _restart_snapserver(self):
        log.info("Restarting snapserver...")
        try:
            subprocess.run(["sudo", "systemctl", "restart", "snapserver"],
                           timeout=15, check=False)
            time.sleep(4)
            self.snap.connect()
            status = self.snap.get_status()
            if status:
                self._update_from_snap_status(status)
                if self._esp_connected:
                    self._send_client_list()
        except Exception as e:
            log.error("Snapserver restart failed: %s", e)

    # ── Snap reconnect ────────────────────────────────────────────────────
    def _snap_reconnect(self):
        log.error("Snapcast lost")
        try:
            if self.snap.sock:
                self.snap.sock.close()
        except Exception:
            pass
        self.snap.sock = None
        self.snap._recv_buf = b""
        try:
            subprocess.run(["sudo", "systemctl", "restart", "snapserver"],
                           timeout=10, check=False)
            time.sleep(3)
        except Exception:
            pass
        self.snap.connect()
        try:
            status = self.snap.get_status()
            self._update_from_snap_status(status)
            if self._esp_connected:
                self._send_client_list()
        except Exception as e:
            log.error("Snap reconnect status failed: %s", e)

    # ── Main loop ─────────────────────────────────────────────────────────
    def run(self):
        log.info("Server bridge starting — %s @ %d", self.ser.port, self.ser.baudrate)
        if GPIO_AVAILABLE:
            try:
                subprocess.run(["gpioset", GPIO_CHIP, f"{GPIO_LED}=1"],
                               timeout=3, capture_output=True)
                log.info("LED GPIO%d ON", GPIO_LED)
            except Exception as e:
                log.warning("LED set failed: %s", e)
        self.snap.connect()
        threading.Thread(target=self._registration_listener, daemon=True).start()
        threading.Thread(target=self._keepalive_thread,      daemon=True).start()

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
                if now - self._last_snap_health >= self.SNAP_HEALTH_INTERVAL_S:
                    self._last_snap_health = now
                    try:
                        status = self.snap.get_status()
                        changed = self._update_from_snap_status(status)
                        if changed:
                            self._resend_all_states()
                    except (ConnectionError, OSError, TimeoutError):
                        self._snap_reconnect()
                    except Exception as e:
                        log.error("Snap health error: %s", e)

            if self._esp_connected:
                try:
                    for n in self.snap.read_notifications():
                        self.handle_snap_notification(n)
                except (ConnectionError, OSError):
                    self._snap_reconnect()
                except Exception as e:
                    log.error("Snap notification error: %s", e)

            if self._esp_connected:
                now_mic = time.time()
                if self._input_mode == 1 and now_mic - self._last_mic_check >= self.MIC_CHECK_INTERVAL_S:
                    self._last_mic_check = now_mic
                    self._poll_mic_status()
            
            if self._esp_connected:
                now_list = time.time()
                if now_list - self._last_client_list_send >= self.CLIENT_LIST_RESEND_S:
                    self._send_client_list()


def main():
    parser = argparse.ArgumentParser(description="Snapcast ↔ ESP32 Server Bridge")
    parser.add_argument("--port",      default="/dev/ttyAMA0")
    parser.add_argument("--baud",      type=int, default=460800)
    parser.add_argument("--snap-host", default="127.0.0.1")
    parser.add_argument("--snap-port", type=int, default=1705)
    args = parser.parse_args()

    bridge = ServerBridge(args.port, args.baud, args.snap_host, args.snap_port)
    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
