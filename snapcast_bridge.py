#!/usr/bin/env python3
import argparse
import json
import logging
import select
import socket
import struct
import subprocess
import threading
import time
from typing import Optional

import serial

# ── Protocol constants (must match protocol.h) ──
SYNC_0 = 0xAA
SYNC_1 = 0x55
HEADER_SIZE = 5
MAX_PAYLOAD = 2048

# Message types
MSG_INIT           = 0x01
MSG_VOL_SET        = 0x02
MSG_VOL_MUTE       = 0x03
MSG_PING           = 0x04
MSG_ACK            = 0x10
MSG_CLIENT_LIST    = 0x11
MSG_CLIENT_VOL_UPD = 0x12
MSG_PONG           = 0x13

# Client entry sizes
CLIENT_ID_LEN   = 36
CLIENT_NAME_LEN = 32
CLIENT_ENTRY_SIZE = CLIENT_ID_LEN + CLIENT_NAME_LEN + 3  # id + name + vol + muted + connected

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bridge")


# ── CRC-8 (polynomial 0x31, init 0x00) ──
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


# ── Frame builder ──
def build_frame(msg_type: int, payload: bytes = b"") -> bytes:
    plen = len(payload)
    header = bytes([SYNC_0, SYNC_1, msg_type, plen & 0xFF, (plen >> 8) & 0xFF])
    crc_data = bytes([msg_type, plen & 0xFF, (plen >> 8) & 0xFF]) + payload
    return header + payload + bytes([crc8(crc_data)])


# ── Snapcast JSON-RPC helper ──
class SnapcastClient:
    """Connects to Snapcast server JSON-RPC TCP interface."""

    def __init__(self, host: str = "127.0.0.1", port: int = 1705):
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

        msg = json.dumps({
            "id": req_id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }) + "\r\n"

        self.sock.sendall(msg.encode())

        # Read response (blocking with timeout)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            ready, _, _ = select.select([self.sock], [], [], 0.5)
            if ready:
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("Snapcast closed connection")
                self._recv_buf += data

                # Process line-delimited JSON
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
                    # Store notification for later processing
                    if "method" in obj and "id" not in obj:
                        self.pending_notifications.append(obj)

        raise TimeoutError(f"No response for {method} (id={req_id})")

    def get_status(self) -> dict:
        return self._send_request("Server.GetStatus", {})

    def set_volume(self, client_id: str, volume: int, muted: bool = False):
        self._send_request("Client.SetVolume", {
            "id": client_id,
            "volume": {"percent": volume, "muted": muted},
        })

    def read_notifications(self) -> list:
        """Non-blocking read of any pending notifications."""
        # Grab any notifications captured during _send_request
        notifications = list(self.pending_notifications)
        self.pending_notifications.clear()

        try:
            ready, _, _ = select.select([self.sock], [], [], 0)
            if ready:
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("Snapcast closed connection")
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


# ── Build CLIENT_LIST payload from Snapcast status ──
def build_client_list_payload(status: dict) -> bytes:
    """Parse Snapcast Server.GetStatus and build binary CLIENT_LIST payload."""
    clients = []
    for group in status.get("server", {}).get("groups", []):
        for c in group.get("clients", []):
            # Only send clients that are currently connected
            if not c.get("connected", False):
                continue

            client_id = c.get("id", "")[:CLIENT_ID_LEN]
            # Use friendly name, fall back to hostname, then ID
            config = c.get("config", {})
            host = c.get("host", {})
            name = config.get("name", "") or host.get("friendlyName", "") or host.get("name", "") or client_id
            name = name[:CLIENT_NAME_LEN]

            vol_info = config.get("volume", {})
            volume = vol_info.get("percent", 100)
            muted = 1 if vol_info.get("muted", False) else 0

            clients.append((client_id, name, volume, muted))

    count = min(len(clients), 24)
    payload = bytes([count])

    for i in range(count):
        cid, cname, vol, muted = clients[i]

        id_bytes = cid.encode("ascii", errors="replace")[:CLIENT_ID_LEN]
        id_bytes = id_bytes.ljust(CLIENT_ID_LEN, b"\x00")

        name_bytes = cname.encode("utf-8", errors="replace")[:CLIENT_NAME_LEN]
        name_bytes = name_bytes.ljust(CLIENT_NAME_LEN, b"\x00")

        payload += id_bytes + name_bytes + bytes([vol, muted, 1])  # connected=1 always

    return payload


# ── UART frame receiver ──
class UARTReceiver:
    """State-machine UART frame receiver."""

    SYNC_0_ST = 0
    SYNC_1_ST = 1
    HEADER_ST = 2
    PAYLOAD_ST = 3
    CRC_ST = 4

    def __init__(self):
        self.state = self.SYNC_0_ST
        self.header_buf = bytearray(3)
        self.header_idx = 0
        self.msg_type = 0
        self.payload_len = 0
        self.payload_buf = bytearray(MAX_PAYLOAD)
        self.payload_idx = 0

    def feed(self, data: bytes):
        """Feed bytes, yields (msg_type, payload) for complete valid frames."""
        for byte in data:
            if self.state == self.SYNC_0_ST:
                if byte == SYNC_0:
                    self.state = self.SYNC_1_ST

            elif self.state == self.SYNC_1_ST:
                if byte == SYNC_1:
                    self.state = self.HEADER_ST
                    self.header_idx = 0
                elif byte == SYNC_0:
                    pass  # stay
                else:
                    self.state = self.SYNC_0_ST

            elif self.state == self.HEADER_ST:
                self.header_buf[self.header_idx] = byte
                self.header_idx += 1
                if self.header_idx == 3:
                    self.msg_type = self.header_buf[0]
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
                    log.warning("CRC mismatch: got 0x%02X expected 0x%02X", byte, expected)
                self.state = self.SYNC_0_ST


# ── Main bridge ──
class SnapcastBridge:
    ESP_TIMEOUT_S = 20  # If no message from ESP in this many seconds, consider it lost
    SNAP_HEALTH_INTERVAL_S = 10  # Check snapserver health every N seconds

    def __init__(self, serial_port: str, baud: int, snap_host: str, snap_port: int):
        self.ser = serial.Serial(serial_port, baud, timeout=0.05)
        self.snap = SnapcastClient(snap_host, snap_port)
        self.rx = UARTReceiver()
        self._running = True
        self._last_status = None
        self._last_esp_msg_time = time.time()
        self._esp_connected = False
        self._last_snap_health_check = time.time()
        self._esp_vol_set_time = {}  # client_id -> timestamp, for echo suppression

    def send_frame(self, msg_type: int, payload: bytes = b""):
        frame = build_frame(msg_type, payload)
        self.ser.write(frame)
        self.ser.flush()

    def fetch_and_send_clients(self):
        """Fetch client list from Snapcast and send to ESP."""
        try:
            status = self.snap.get_status()
            self._last_status = status
            payload = build_client_list_payload(status)
            count = payload[0]
            self.send_frame(MSG_CLIENT_LIST, payload)
            log.info("Sent CLIENT_LIST with %d clients", count)
        except Exception as e:
            log.error("Failed to fetch/send client list: %s", e)

    def handle_esp_message(self, msg_type: int, payload: bytes):
        self._last_esp_msg_time = time.time()

        if msg_type == MSG_INIT:
            log.info("Received INIT from ESP")
            self._esp_connected = True
            self.send_frame(MSG_ACK)
            log.info("Sent ACK")
            time.sleep(0.1)
            self.fetch_and_send_clients()

        elif msg_type == MSG_PING:
            self.send_frame(MSG_PONG)

        elif msg_type == MSG_VOL_SET:
            if len(payload) < CLIENT_ID_LEN + 1:
                log.warning("VOL_SET payload too short")
                return
            client_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            volume = payload[CLIENT_ID_LEN]
            log.info("VOL_SET: %s -> %d", client_id, volume)
            self._esp_vol_set_time[client_id] = time.time()
            try:
                self.snap.set_volume(client_id, volume)
            except (ConnectionError, OSError):
                log.error("Snapcast connection lost during VOL_SET")
                self._snap_reconnect()
            except Exception as e:
                log.error("Failed to set volume: %s", e)

        elif msg_type == MSG_VOL_MUTE:
            if len(payload) < CLIENT_ID_LEN + 1:
                return
            client_id = payload[:CLIENT_ID_LEN].rstrip(b"\x00").decode("ascii", errors="replace")
            muted = bool(payload[CLIENT_ID_LEN])
            log.info("VOL_MUTE: %s -> %s", client_id, muted)
            try:
                self.snap.set_volume(client_id, -1, muted)
            except (ConnectionError, OSError):
                log.error("Snapcast connection lost during VOL_MUTE")
                self._snap_reconnect()
            except Exception as e:
                log.error("Failed to set mute: %s", e)

        else:
            log.warning("Unknown ESP message type: 0x%02X", msg_type)

    def handle_snap_notification(self, notification: dict):
        """Handle Snapcast server notifications — refetch on relevant changes."""
        method = notification.get("method", "")
        relevant_events = {
            "Client.OnConnect",
            "Client.OnDisconnect",
            "Client.OnNameChanged",
            "Client.OnVolumeChanged",
            "Group.OnStreamChanged",
            "Server.OnUpdate",
        }

        if method in relevant_events:
            log.info("Snapcast event: %s", method)

            if method == "Client.OnVolumeChanged":
                params = notification.get("params", {})
                client_id = params.get("id", "")[:CLIENT_ID_LEN]
                vol_info = params.get("volume", {})
                volume = vol_info.get("percent", 0)

                # Suppress echo — if ESP just set this client's volume, skip
                last_set = self._esp_vol_set_time.get(client_id, 0)
                if time.time() - last_set < 1.0:
                    log.debug("Suppressing echo VOL_UPD for %s", client_id)
                else:
                    id_bytes = client_id.encode("ascii")[:CLIENT_ID_LEN]
                    id_bytes = id_bytes.ljust(CLIENT_ID_LEN, b"\x00")
                    self.send_frame(MSG_CLIENT_VOL_UPD, id_bytes + bytes([volume]))
                    log.info("Sent VOL_UPD to ESP: %s -> %d", client_id, volume)
            else:
                # For connect/disconnect/rename: refetch full list
                time.sleep(0.5)  # Let Snapcast settle
                self.fetch_and_send_clients()

    def _snap_reconnect(self):
        """Snapserver died — tell ESP to go to splash, restart snapserver, reconnect."""
        log.error("Snapcast server lost")

        # Close dead socket
        try:
            if self.snap.sock:
                self.snap.sock.close()
        except Exception:
            pass
        self.snap.sock = None
        self.snap._recv_buf = b""

        # Tell ESP: no clients → triggers splash + "Server not found"
        if self._esp_connected:
            self.send_frame(MSG_CLIENT_LIST, bytes([0]))
            log.info("Sent empty CLIENT_LIST to ESP — splash screen")

        # Try to restart snapserver
        log.info("Attempting to restart snapserver...")
        try:
            subprocess.run(["sudo", "systemctl", "restart", "snapserver"],
                           timeout=10, check=False)
            time.sleep(3)  # Give snapserver time to start
        except Exception as e:
            log.error("Failed to restart snapserver: %s", e)

        # Reconnect loop
        log.info("Reconnecting to Snapcast...")
        self.snap.connect()

        # Re-send client list to ESP
        if self._esp_connected:
            self.fetch_and_send_clients()

    def run(self):
        log.info("Bridge starting — UART: %s @ %d", self.ser.port, self.ser.baudrate)

        self.snap.connect()

        log.info("Waiting for ESP INIT...")

        while self._running:
            # Read UART
            data = self.ser.read(256)
            if data:
                for msg_type, payload in self.rx.feed(data):
                    self.handle_esp_message(msg_type, payload)

            # Check ESP timeout
            if self._esp_connected:
                elapsed = time.time() - self._last_esp_msg_time
                if elapsed > self.ESP_TIMEOUT_S:
                    log.warning("ESP silent for %.0fs — marking disconnected", elapsed)
                    self._esp_connected = False

            # Periodic snapserver health check
            if self._esp_connected:
                now = time.time()
                if now - self._last_snap_health_check >= self.SNAP_HEALTH_INTERVAL_S:
                    self._last_snap_health_check = now
                    try:
                        self.snap.get_status()
                    except (ConnectionError, OSError, TimeoutError):
                        log.error("Snapserver health check failed")
                        self._snap_reconnect()
                        continue
                    except Exception as e:
                        log.error("Snapserver health check error: %s", e)

            # Read Snapcast notifications (only if ESP is connected)
            if self._esp_connected:
                try:
                    notifications = self.snap.read_notifications()
                    for n in notifications:
                        self.handle_snap_notification(n)
                except (ConnectionError, OSError):
                    log.error("Snapcast connection lost — reconnecting")
                    self._snap_reconnect()
                except Exception as e:
                    log.error("Snapcast notification error: %s", e)


def main():
    parser = argparse.ArgumentParser(description="Snapcast ↔ ESP32 UART Bridge")
    parser.add_argument("--port", default="/dev/ttyAMA0", help="Serial port (default: /dev/ttyAMA0)")
    parser.add_argument("--baud", type=int, default=460800, help="Baud rate (default: 460800)")
    parser.add_argument("--snap-host", default="127.0.0.1", help="Snapcast server host")
    parser.add_argument("--snap-port", type=int, default=1705, help="Snapcast JSON-RPC port")
    args = parser.parse_args()

    bridge = SnapcastBridge(args.port, args.baud, args.snap_host, args.snap_port)

    try:
        bridge.run()
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
