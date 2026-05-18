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
from typing import Optional

import serial

# ── Protocol constants ──
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
MSG_REMOVE_CLIENT   = 0x08
MSG_RESTART_SERVER  = 0x09
MSG_ACK             = 0x10
MSG_CLIENT_LIST     = 0x11
MSG_CLIENT_VOL_UPD  = 0x12
MSG_PONG            = 0x13
MSG_STATE_UPDATE    = 0x20
MSG_MODE_SWITCHING  = 0x21
MSG_POWER_STATE     = 0x22
MSG_CLIENT_REMOVED  = 0x23
MSG_PW_SET          = 0x30
MSG_PW_ACK          = 0x34

MODE_SYNC = 0
MODE_BT   = 1

CLIENT_ID_LEN     = 36
CLIENT_NAME_LEN   = 32
CLIENT_ENTRY_SIZE = CLIENT_ID_LEN + CLIENT_NAME_LEN + 6

CTRL_PORT            = 7702
DISCOVERY_PORT       = 7703
DISCOVERY_INTERVAL_S = 10
PW_HASH_FILE         = "/etc/zone_password.hash"
PW_DEFAULT           = "anjay1234"
PW_BROADCAST_PORT    = 7700

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server_bridge")


# ── CRC-8 ──
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


# ── Hash pull server — port 7701 ──
class HashPullServer(threading.Thread):
    def __init__(self, get_hash_fn):
        super().__init__(daemon=True)
        self._get_hash = get_hash_fn

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", 7701))
        srv.listen(10)
        log.info("Hash pull server listening on port 7701")
        while True:
            try:
                conn, addr = srv.accept()
                with conn:
                    h = self._get_hash()
                    conn.sendall(h.encode("ascii"))
                log.info("Sent hash to %s on pull request", addr[0])
            except Exception as e:
                log.error("Hash pull server error: %s", e)


# ── Snapcast JSON-RPC ──
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

    def delete_client(self, client_id: str) -> bool:
        """Remove a client from Snapcast permanently."""
        try:
            self._send_request("Server.DeleteClient", {"id": client_id})
            log.info("Snapcast DeleteClient: %s", client_id)
            return True
        except Exception as e:
            log.error("DeleteClient failed: %s", e)
            return False

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


# ── UART frame receiver ──
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


# ── Per-client state ──
class ClientRecord:
    def __init__(self, snap_id: str, name: str):
        self.snap_id        = snap_id
        self.name           = name
        self.volume         = 0
        self.muted          = False
        self.snap_connected = False
        self.mode           = MODE_SYNC
        self.powered        = True
        self.bt_connected   = False
        self.bt_dev_name    = ""
        self.client_ip      = ""
        self.reachable      = True   # False if control socket can't connect

        self._ctrl_sock: Optional[socket.socket] = None
        self._ctrl_lock  = threading.Lock()
        self._connecting = False

    def ctrl_connected(self) -> bool:
        return self._ctrl_sock is not None

    def ctrl_connect(self, on_message_cb):
        if not self.client_ip:
            return
        with self._ctrl_lock:
            if self._connecting or self._ctrl_sock:
                return
            self._connecting = True
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.client_ip, CTRL_PORT))
            sock.settimeout(None)
            sock.setblocking(False)
            with self._ctrl_lock:
                self._ctrl_sock = sock
                self._connecting = False
            self.reachable = True
            log.info("Connected to client Pi %s (%s)", self.name, self.client_ip)
            self._ctrl_recv_loop(on_message_cb)
        except Exception as e:
            log.warning("Cannot connect to client Pi %s: %s", self.name, e)
            self.reachable = False
            with self._ctrl_lock:
                self._ctrl_sock = None
                self._connecting = False

    def _ctrl_recv_loop(self, on_message_cb):
        with self._ctrl_lock:
            sock = self._ctrl_sock
        if not sock:
            return
        buf = b""
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
                        msg = json.loads(line)
                        on_message_cb(self, msg)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            log.warning("Client Pi %s ctrl recv error: %s", self.name, e)
        finally:
            self.reachable = False
            with self._ctrl_lock:
                self._ctrl_sock = None
            log.info("Client Pi %s control socket closed", self.name)

    def ctrl_send(self, msg: dict):
        with self._ctrl_lock:
            sock = self._ctrl_sock
        if not sock:
            return
        try:
            sock.sendall((json.dumps(msg) + "\n").encode())
        except Exception as e:
            log.warning("ctrl_send to %s failed: %s", self.name, e)
            with self._ctrl_lock:
                self._ctrl_sock = None

    def ensure_connected(self, on_message_cb):
        with self._ctrl_lock:
            if self._ctrl_sock or self._connecting or not self.client_ip:
                return
        threading.Thread(
            target=self.ctrl_connect,
            args=(on_message_cb,),
            daemon=True,
        ).start()


# ── Password helpers ──
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


# ── Main server bridge ──
class ServerBridge:
    ESP_TIMEOUT_S          = 20
    SNAP_HEALTH_INTERVAL_S = 10
    CTRL_RECONNECT_S       = 10

    def __init__(self, serial_port, baud, snap_host, snap_port):
        self.ser  = serial.Serial(serial_port, baud, timeout=0.05)
        self.snap = SnapcastClient(snap_host, snap_port)
        self.rx   = UARTReceiver()

        self._clients: dict[str, ClientRecord] = {}
        self._clients_lock = threading.Lock()
        self._clients_by_name: dict[str, str] = {}
        self._known_snap_ids: set[str] = set()

        self._esp_connected        = False
        self._last_esp_msg_time    = time.time()
        self._last_snap_health     = time.time()
        self._last_ctrl_reconnect  = time.time()
        self._esp_vol_set_time     = {}
        self._running              = True

        self._pw_hash, self._pw_user_set = load_or_init_password()
        self._hash_pull_server = HashPullServer(lambda: self._pw_hash)
        self._hash_pull_server.start()

    # ────────────────────────────────────────────
    # UDP discovery broadcaster
    # ────────────────────────────────────────────
    def _discovery_thread(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(1.0)
        log.info("UDP discovery broadcaster started")
        discover_msg = json.dumps({"type": "discover"}).encode()

        while True:
            try:
                sock.sendto(discover_msg, ("255.255.255.255", DISCOVERY_PORT))
            except Exception as e:
                log.warning("Discovery broadcast error: %s", e)

            deadline = time.time() + DISCOVERY_INTERVAL_S
            while time.time() < deadline:
                try:
                    data, addr = sock.recvfrom(512)
                    try:
                        msg = json.loads(data.decode())
                    except Exception:
                        continue
                    if msg.get("type") == "announce":
                        ip   = msg.get("ip", addr[0])
                        name = msg.get("name", "")
                        log.info("Discovery: %s at %s", name, ip)
                        self._on_client_discovered(name, ip)
                except socket.timeout:
                    continue
                except Exception as e:
                    log.error("Discovery recv error: %s", e)

    def _on_client_discovered(self, name: str, ip: str):
        with self._clients_lock:
            matched = None
            for rec in self._clients.values():
                if rec.name.lower() == name.lower():
                    matched = rec
                    break
            if matched:
                if matched.client_ip != ip:
                    log.info("Updated IP for %s: %s -> %s", name, matched.client_ip, ip)
                    matched.client_ip = ip
            else:
                self._clients_by_name[name] = ip
                return
        matched.ensure_connected(self._on_ctrl_message)

    # ────────────────────────────────────────────
    # UART helpers
    # ────────────────────────────────────────────
    def send_frame(self, msg_type: int, payload: bytes = b""):
        frame = build_frame(msg_type, payload)
        self.ser.write(frame)
        self.ser.flush()

    def _id_bytes(self, snap_id: str) -> bytes:
        return snap_id.encode("ascii")[:CLIENT_ID_LEN].ljust(CLIENT_ID_LEN, b"\x00")

    # ────────────────────────────────────────────
    # Build CLIENT_LIST payload
    # ────────────────────────────────────────────
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
                          ]))
        return payload

    def _send_client_list(self):
        payload = self._build_client_list_payload()
        self.send_frame(MSG_CLIENT_LIST, payload)
        log.info("Sent CLIENT_LIST with %d clients", payload[0])

    # ────────────────────────────────────────────
    # Snapcast status → ClientRecord map
    # ────────────────────────────────────────────
    def _update_from_snap_status(self, status: dict) -> bool:
        set_changed = False
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
                    ip        = host.get("ip", "")

                    if snap_id not in self._clients:
                        rec = ClientRecord(snap_id, name)
                        self._clients[snap_id] = rec
                        self._known_snap_ids.add(snap_id)
                        set_changed = True
                        log.info("New client: %s (%s)", name, snap_id)
                        cached_ip = self._clients_by_name.pop(name, None)
                        if cached_ip:
                            rec.client_ip = cached_ip
                        elif ip:
                            rec.client_ip = ip
                    else:
                        rec = self._clients[snap_id]

                    rec.name           = name
                    rec.muted          = muted
                    rec.snap_connected = connected
                    if ip and not rec.client_ip:
                        rec.client_ip = ip
                    if rec.mode == MODE_SYNC:
                        rec.volume = volume

        return set_changed

    # ────────────────────────────────────────────
    # Control message handler from client Pi
    # ────────────────────────────────────────────
    def _on_ctrl_message(self, rec: ClientRecord, msg: dict):
        mtype = msg.get("type", "")

        if mtype == "state":
            rec.mode           = msg.get("mode",           rec.mode)
            rec.volume         = msg.get("volume",         rec.volume)
            rec.snap_connected = msg.get("snap_connected", rec.snap_connected)
            rec.bt_connected   = msg.get("bt_connected",   rec.bt_connected)
            rec.bt_dev_name    = msg.get("bt_dev_name",    "")
            rec.powered        = msg.get("powered",        rec.powered)
            rec.reachable      = True
            new_name = msg.get("client_name", "")
            if new_name:
                rec.name = new_name

            log.info("State from %s: mode=%d snap=%s bt=%s vol=%d powered=%s",
                     rec.name, rec.mode, rec.snap_connected,
                     rec.bt_connected, rec.volume, rec.powered)

            if self._esp_connected:
                # Send state update
                payload = self._id_bytes(rec.snap_id) + bytes([
                    rec.mode,
                    1 if rec.snap_connected else 0,
                    1 if rec.bt_connected   else 0,
                    rec.volume,
                ])
                self.send_frame(MSG_STATE_UPDATE, payload)

                # Send power state
                self.send_frame(MSG_POWER_STATE,
                                self._id_bytes(rec.snap_id) + bytes([1 if rec.powered else 0]))

        elif mtype == "switching":
            if self._esp_connected:
                self.send_frame(MSG_MODE_SWITCHING, self._id_bytes(rec.snap_id))
                log.info("Forwarded MODE_SWITCHING for %s to server ESP", rec.name)

        elif mtype == "power_state":
            # Client reports its powered state after completing GPIO sequence
            powered = msg.get("powered", rec.powered)
            rec.powered = powered
            log.info("Power state from %s: powered=%s", rec.name, powered)
            if self._esp_connected:
                self.send_frame(MSG_POWER_STATE,
                                self._id_bytes(rec.snap_id) + bytes([1 if powered else 0]))

        elif mtype == "pong":
            pass

    # ────────────────────────────────────────────
    # Ensure control connections
    # ────────────────────────────────────────────
    def _ensure_ctrl_connections(self):
        with self._clients_lock:
            records = list(self._clients.values())
        for rec in records:
            rec.ensure_connected(self._on_ctrl_message)
            # Update reachability — if no IP and not connected, mark unreachable
            if not rec.client_ip and not rec.ctrl_connected():
                if rec.reachable:
                    rec.reachable = False
                    if self._esp_connected:
                        # Send power state with unreachable flag via powered=False
                        self.send_frame(MSG_POWER_STATE,
                                        self._id_bytes(rec.snap_id) + bytes([0xFF]))  # 0xFF = unreachable

    # ────────────────────────────────────────────
    # ESP message handlers
    # ────────────────────────────────────────────
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

        elif msg_type == MSG_PING:
            self.send_frame(MSG_PONG)

        elif msg_type == MSG_VOL_SET:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            snap_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            volume  = payload[CLIENT_ID_LEN]
            log.info("VOL_SET: %s -> %d", snap_id, volume)
            self._esp_vol_set_time[snap_id] = time.time()
            with self._clients_lock:
                rec = self._clients.get(snap_id)
            if rec:
                rec.volume = volume
                if rec.mode == MODE_SYNC:
                    try:
                        self.snap.set_volume(snap_id, volume)
                    except (ConnectionError, OSError):
                        self._snap_reconnect()
                    except Exception as e:
                        log.error("VOL_SET snap failed: %s", e)
                if rec.ctrl_connected():
                    rec.ctrl_send({"type": "set_volume", "volume": volume})

        elif msg_type in (MSG_MODE_SYNC, MSG_MODE_BT):
            if len(payload) < CLIENT_ID_LEN:
                return
            snap_id  = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            new_mode = MODE_SYNC if msg_type == MSG_MODE_SYNC else MODE_BT
            log.info("Mode switch for %s -> %d", snap_id, new_mode)
            with self._clients_lock:
                rec = self._clients.get(snap_id)
            if rec and rec.ctrl_connected():
                rec.ctrl_send({"type": "set_mode", "mode": new_mode})
            else:
                log.warning("No control connection to %s", snap_id)

        elif msg_type == MSG_POWER_SET:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            snap_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            powered = payload[CLIENT_ID_LEN] != 0
            log.info("POWER_SET: %s -> %s", snap_id, powered)
            with self._clients_lock:
                rec = self._clients.get(snap_id)
            if rec and rec.ctrl_connected():
                rec.ctrl_send({"type": "set_powered", "powered": powered})
            else:
                log.warning("No control connection to %s — cannot set power", snap_id)

        elif msg_type == MSG_REMOVE_CLIENT:
            if len(payload) < CLIENT_ID_LEN:
                return
            snap_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            log.info("REMOVE_CLIENT: %s", snap_id)
            self._handle_remove_client(snap_id)

        elif msg_type == MSG_RESTART_SERVER:
            log.info("ESP requested snapserver restart")
            threading.Thread(target=self._restart_snapserver, daemon=True).start()
            
        elif msg_type == MSG_PW_SET:
            raw = payload.rstrip(b"\x00").decode("utf-8", errors="replace")
            if not raw:
                return
            new_hash = sha256_hex(raw)
            _write_hash_file(new_hash)
            self._pw_hash     = new_hash
            self._pw_user_set = True
            self.send_frame(MSG_PW_ACK, bytes([1]))
            log.info("Password updated")
            try:
                status = self.snap.get_status()
                for group in status.get("server", {}).get("groups", []):
                    for c in group.get("clients", []):
                        ip = c.get("host", {}).get("ip", "")
                        if ip:
                            threading.Thread(
                                target=_broadcast_hash_to_one,
                                args=(ip, new_hash),
                                daemon=True,
                            ).start()
            except Exception as e:
                log.error("PW broadcast failed: %s", e)

        else:
            log.warning("Unknown ESP msg: 0x%02X", msg_type)

    # ────────────────────────────────────────────
    # Restart Snapserver
    # ────────────────────────────────────────────
    def _restart_snapserver(self):
        log.info("Restarting snapserver...")
        try:
            subprocess.run(["sudo", "systemctl", "restart", "snapserver"],
                        timeout=15, check=False)
            log.info("Snapserver restarted — waiting for it to come back")
            time.sleep(4)
            self.snap.connect()
            status = self.snap.get_status()
            if status:
                self._update_from_snap_status(status)
                if self._esp_connected:
                    self._send_client_list()
        except Exception as e:
            log.error("Snapserver restart failed: %s", e)
            
    # ────────────────────────────────────────────
    # Remove client
    # ────────────────────────────────────────────
    def _handle_remove_client(self, snap_id: str):
        # Remove from Snapcast
        try:
            self.snap.delete_client(snap_id)
        except Exception as e:
            log.error("Snapcast delete_client failed: %s", e)

        # Remove from our map
        with self._clients_lock:
            rec = self._clients.pop(snap_id, None)
            self._known_snap_ids.discard(snap_id)

        if rec:
            # Close control socket if open
            rec.ctrl_send({"type": "removed"})  # notify client Pi
            log.info("Removed client %s (%s)", rec.name, snap_id)

        # Tell ESP the client was removed and send updated list
        if self._esp_connected:
            self.send_frame(MSG_CLIENT_REMOVED, self._id_bytes(snap_id))
            self._send_client_list()

    # ────────────────────────────────────────────
    # Snapcast notification handler
    # ────────────────────────────────────────────
    def handle_snap_notification(self, notification: dict):
        method = notification.get("method", "")
        relevant = {
            "Client.OnConnect", "Client.OnDisconnect",
            "Client.OnNameChanged", "Server.OnUpdate",
        }
        if method not in relevant and method != "Client.OnVolumeChanged":
            return

        log.info("Snapcast event: %s", method)

        if method == "Client.OnVolumeChanged":
            params   = notification.get("params", {})
            snap_id  = params.get("id", "")[:CLIENT_ID_LEN]
            volume   = params.get("volume", {}).get("percent", 0)
            last_set = self._esp_vol_set_time.get(snap_id, 0)
            if time.time() - last_set < 1.0:
                return
            with self._clients_lock:
                rec = self._clients.get(snap_id)
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
                set_changed = self._update_from_snap_status(status)
                if set_changed and self._esp_connected:
                    self._send_client_list()
            except Exception as e:
                log.error("Snap refresh failed: %s", e)

    # ────────────────────────────────────────────
    # Snap reconnect
    # ────────────────────────────────────────────
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

    # ────────────────────────────────────────────
    # Main loop
    # ────────────────────────────────────────────
    def run(self):
        log.info("Server bridge starting — %s @ %d", self.ser.port, self.ser.baudrate)
        self.snap.connect()
        threading.Thread(target=self._discovery_thread, daemon=True).start()

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
                        set_changed = self._update_from_snap_status(status)
                        if set_changed:
                            self._send_client_list()
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

            now = time.time()
            if now - self._last_ctrl_reconnect >= self.CTRL_RECONNECT_S:
                self._last_ctrl_reconnect = now
                self._ensure_ctrl_connections()


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
