#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import base64
import json
import os
import re
import ssl
import struct
import time
import urllib.request
import uuid
import socket
import aiohttp
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

PORT = int(os.environ.get("PORT", 3000))
MONGO_URI = os.environ.get("MONGO_URI", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "18102010")

SERVER_INVITE_CODE = "rAx625W8Gn"
DEFAULT_GUILD_ID = os.environ.get("GUILD_ID", "1059780215720648725")
DEFAULT_CHANNEL_ID = "1569527599426707546"

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "discord/1.0.9168 Chrome/128.0.6613.186 Electron/32.2.2 Safari/537.36"
)

STREAM_BADGE_MILESTONES = {
    1: (1.0, "Tier 1: Newcomer (1 hour)"),
    2: (5.0, "Tier 2: Fledgling (5 hours)"),
    3: (20.0, "Tier 3: Breakout (20 hours)"),
    4: (75.0, "Tier 4: Standout (75 hours)"),
    5: (150.0, "Tier 5: Trendsetter (150 hours)"),
    6: (300.0, "Tier 6: Headliner (300 hours)"),
    7: (500.0, "Tier 7: Star (500 hours)"),
    8: (1000.0, "Tier 8: Sensation (1,000 hours)"),
    9: (2000.0, "Tier 9: Visionary (2,000 hours)"),
    10: (5000.0, "Tier 10: Phenomenon (5,000+ hours)"),
}

def make_super_properties() -> str:
    props = {
        "os": "Windows", "browser": "Discord Client", "release_channel": "stable",
        "client_version": "1.0.9168", "os_version": "10.0.22631", "os_arch": "x64",
        "app_arch": "x64", "system_locale": "en-US", "browser_user_agent": USER_AGENT,
        "browser_version": "32.2.2", "client_build_number": 343349, "native_build_number": 53915,
        "client_event_source": None,
    }
    return base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()

class AsyncWebSocket:
    def __init__(self, url: str) -> None:
        self.url = url
        self.reader = None
        self.writer = None
        self.closed = False
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        parsed = re.match(r"^(wss?|https?)://([^/:]+)(?::(\d+))?(/.*)?$", self.url)
        if not parsed:
            raise ValueError(f"Invalid URL: {self.url}")
        scheme, host, port_str, path = parsed.groups()
        use_ssl = scheme in ("wss", "https")
        port = int(port_str) if port_str else (443 if use_ssl else 80)
        ssl_ctx = ssl.create_default_context() if use_ssl else None
        self.reader, self.writer = await asyncio.open_connection(
            host, port, ssl=ssl_ctx, server_hostname=host if use_ssl else None
        )
        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        host_header = host if port in (80, 443) else f"{host}:{port}"
        handshake = (
            f"GET {path or '/'} HTTP/1.1\r\n"
            f"Host: {host_header}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\nSec-WebSocket-Version: 13\r\n"
            f"User-Agent: {USER_AGENT}\r\nOrigin: https://discord.com\r\n\r\n"
        )
        self.writer.write(handshake.encode("utf-8"))
        await self.writer.drain()
        line = await self.reader.readline()
        if not line.startswith(b"HTTP/1.1 101"):
            raise ConnectionError("Handshake failed")
        while True:
            hl = await self.reader.readline()
            if hl in (b"\r\n", b"\n", b""):
                break

    async def send_json(self, payload: dict) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        await self._send_frame(0x1, data)

    async def _send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed or not self.writer:
            return
        async with self._write_lock:
            header = bytearray([0x80 | (opcode & 0x0F)])
            length = len(payload)
            if length < 126:
                header.append(0x80 | length)
            elif length < 65536:
                header.append(0x80 | 126)
                header.extend(struct.pack("!H", length))
            else:
                header.append(0x80 | 127)
                header.extend(struct.pack("!Q", length))
            mask_key = os.urandom(4)
            header.extend(mask_key)
            masked = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
            self.writer.write(header + masked)
            await self.writer.drain()

    async def recv_json(self) -> dict | None:
        if not self.reader or self.closed:
            return None
        fragments = bytearray()
        message_opcode = None
        while True:
            try:
                first_two = await self.reader.readexactly(2)
            except Exception:
                return None
            b1, b2 = first_two[0], first_two[1]
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack("!H", await self.reader.readexactly(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", await self.reader.readexactly(8))[0]
            mask_key = await self.reader.readexactly(4) if masked else b""
            payload = await self.reader.readexactly(length) if length > 0 else b""
            if masked:
                payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))

            if opcode == 0x8:
                self.closed = True
                return None
            elif opcode == 0x9:
                await self._send_frame(0xA, payload)
                continue
            elif opcode in (0x1, 0x2):
                message_opcode = opcode
                fragments.extend(payload)
            elif opcode == 0x0:
                fragments.extend(payload)

            if fin:
                if message_opcode == 0x1:
                    try:
                        return json.loads(fragments.decode("utf-8", errors="replace"))
                    except:
                        return None
                fragments.clear()
                message_opcode = None

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.writer:
            try:
                await self._send_frame(0x8, struct.pack("!H", 1000))
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass

class RTCVoiceSession:
    def __init__(self, endpoint: str, server_id: str, channel_id: str, user_id: str, session_id: str, voice_token: str, rtc_connection_id: str, is_stream: bool = False):
        self.endpoint = endpoint
        self.server_id = server_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.session_id = session_id
        self.voice_token = voice_token
        self.rtc_connection_id = rtc_connection_id
        self.is_stream = is_stream
        self.ws = None
        self.udp_socket = None
        self.secret_key = None
        self.aesgcm = None
        self.video_ssrc = 0
        self._task = None
        self._hb_task = None
        self._media_task = None
        self.running = False

    def start(self):
        self.running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self.running = False
        if self._hb_task: self._hb_task.cancel()
        if self._media_task: self._media_task.cancel()
        if self.udp_socket:
            try: self.udp_socket.close()
            except: pass
        if self.ws: await self.ws.close()
        if self._task: self._task.cancel()

    async def _udp_media_loop(self, ip: str, port: int):
        seq = 0
        nonce_seq = 0
        # Valid H.264 Annex-B Minimal Black Frame (SPS + PPS + IDR slice)
        h264_black_frame = (
            b"\x00\x00\x00\x01\x67\x42\x00\x1f\x96\x54\x05\x01\xe9\x80"
            b"\x00\x00\x00\x01\x68\xce\x38\x80"
            b"\x00\x00\x00\x01\x65\x88\x84\x00\x10\x00\x00\x03\x00\x00\x03\x00\x78"
        )
        try:
            while self.running and self.udp_socket and self.aesgcm:
                timestamp = int(time.time() * 90000) & 0xFFFFFFFF
                
                # 1. Real Encrypted Video RTP Packet (Payload Type 101)
                rtp_header = bytearray(12)
                rtp_header[0] = 0x80
                rtp_header[1] = 101 # PT 101 = H264
                struct.pack_into(">H", rtp_header, 2, seq % 65535)
                struct.pack_into(">I", rtp_header, 4, timestamp)
                struct.pack_into(">I", rtp_header, 8, self.video_ssrc)
                
                nonce_seq = (nonce_seq + 1) % 0xFFFFFFFF
                nonce = struct.pack(">I", nonce_seq) + (b"\x00" * 8)
                
                # AES-256-GCM encryption with RTP Header as AAD
                ciphertext = self.aesgcm.encrypt(nonce, h264_black_frame, bytes(rtp_header))
                final_pkt = bytes(rtp_header) + ciphertext + struct.pack(">I", nonce_seq)
                
                try:
                    self.udp_socket.sendto(final_pkt, (ip, port))
                except BlockingIOError:
                    pass
                
                # 2. Discord Keepalive Ping
                if seq % 5 == 0:
                    ka_pkt = struct.pack('<Q', int(time.time() * 1000))
                    try:
                        self.udp_socket.sendto(ka_pkt, (ip, port))
                    except BlockingIOError:
                        pass
                
                seq += 1
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"⚠️ [Media Loop] Error: {e}", flush=True)

    async def _run(self):
        url = self.endpoint
        if not url.startswith("wss://") and not url.startswith("ws://"):
            url = f"wss://{url}"
        if "?v=" not in url:
            url = f"{url.rstrip('/')}/?v=8"
        try:
            self.ws = AsyncWebSocket(url)
            await self.ws.connect()
            await self.ws.send_json({
                "op": 0,
                "d": {
                    "server_id": str(self.server_id), "channel_id": str(self.channel_id),
                    "user_id": str(self.user_id), "session_id": self.session_id,
                    "token": self.voice_token, "video": True,
                    "streams": [{"type": "screen" if self.is_stream else "video", "rid": "100", "quality": 100}]
                }
            })
            while self.running:
                msg = await self.ws.recv_json()
                if not msg:
                    break
                op, d = msg.get("op"), msg.get("d") or {}
                
                if op == 8:
                    interval = d.get("heartbeat_interval", 15000) / 1000.0
                    if self._hb_task: self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._hb(interval))
                
                elif op == 2:
                    ip = d.get("ip")
                    port = d.get("port")
                    ssrc = d.get("ssrc", 1)
                    self.video_ssrc = ssrc + 1
                    rtx_ssrc = ssrc + 2
                    modes = d.get("modes", [])
                    selected_mode = "aead_aes256_gcm_rtpsize" if "aead_aes256_gcm_rtpsize" in modes else modes[0]
                    
                    ext_ip = "127.0.0.1"
                    ext_port = 1337
                    
                    # IP Discovery
                    try:
                        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        self.udp_socket.setblocking(False)
                        discovery_pkt = bytearray(74)
                        struct.pack_into(">H", discovery_pkt, 0, 1)
                        struct.pack_into(">H", discovery_pkt, 2, 70)
                        struct.pack_into(">I", discovery_pkt, 4, ssrc)
                        self.udp_socket.sendto(discovery_pkt, (ip, port))
                        
                        loop = asyncio.get_running_loop()
                        resp, _ = await asyncio.wait_for(loop.sock_recv(self.udp_socket, 74), timeout=5.0)
                        ext_ip = resp[8:72].decode('utf-8').strip('\x00')
                        ext_port = struct.unpack_from(">H", resp, 72)[0]
                    except:
                        pass

                    await self.ws.send_json({
                        "op": 1,
                        "d": {
                            "protocol": "udp",
                            "data": {"address": ext_ip, "port": ext_port, "mode": selected_mode},
                            "codecs": [
                                {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120},
                                {"name": "H264", "type": "video", "priority": 1000, "payload_type": 101, "rtx_payload_type": 102, "encode": True, "decode": True}
                            ],
                            "rtc_connection_id": self.rtc_connection_id
                        }
                    })
                    
                    await self.ws.send_json({
                        "op": 12,
                        "d": {
                            "audio_ssrc": ssrc, "video_ssrc": self.video_ssrc, "rtx_ssrc": rtx_ssrc,
                            "streams": [{
                                "type": "video", "rid": "100", "ssrc": self.video_ssrc, "active": True,
                                "quality": 100, "rtx_ssrc": rtx_ssrc, "max_bitrate": 1500000,
                                "max_framerate": 30, "max_resolution": {"type": "fixed", "width": 1280, "height": 720}
                            }]
                        }
                    })

                # Opcode 4: The missing Session Description event carrying the real secret_key!
                elif op == 4:
                    raw_key = d.get("secret_key", [])
                    if raw_key and len(raw_key) == 32:
                        self.secret_key = bytes(raw_key)
                        self.aesgcm = AESGCM(self.secret_key)
                        print(f"🔑 [Encryption Ready] AEAD AES-256-GCM initialized for {self.user_id}!", flush=True)
                        if self.is_stream and not self._media_task:
                            self._media_task = asyncio.create_task(self._udp_media_loop(ip, port))
        except:
            pass

    async def _hb(self, interval: float):
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": -1}})
                await asyncio.sleep(interval)
        except: pass

class ManagedAccount:
    def __init__(self, acc_id: str, token: str, role: str, guild_id: str, channel_id: str, owner_id: str = "global", scope: str = "community", username="Loading...", avatar_url=None, tier_num=0, raw_hours=None, total_hours=0.0, current_tier="No Badge (0h)", next_tier="Tier 1: Newcomer (1 hour)", remaining_hours=1.0, progress_percent=0.0, analytics_token=""):
        self.acc_id = acc_id
        self.token = token.strip()
        self.role = role
        self.guild_id = guild_id.strip()
        self.channel_id = channel_id.strip()
        self.owner_id = owner_id
        self.scope = scope
        self.username = username
        self.avatar_url = avatar_url or "https://cdn.discordapp.com/embed/avatars/0.png"
        self.analytics_token = analytics_token
        
        self.tier_num = tier_num
        self.raw_hours = raw_hours
        self.total_hours = total_hours
        self.current_tier = current_tier
        self.next_tier = next_tier
        self.remaining_hours = remaining_hours
        self.progress_percent = progress_percent

        self.status = "Idle 🛑"
        self.telemetry_status = "Waiting..."
        self.is_connected = False
        self.connected_since = None
        self.user_id = None
        self.stream_key = None
        self.voice_session_id = None
        self.rtc_connection_id = str(uuid.uuid4())
        self.client_session_id = str(uuid.uuid4())
        
        self.watching_streams = set()
        self.ws = None
        self.voice_rtc = None
        self.stream_rtc = None
        self.running = False
        self.task = None
        self.telemetry_task = None
        self.cooldown_active = False

    def mask_token(self):
        if len(self.token) > 10:
            return f"{self.token[:4]}••••••••{self.token[-4:]}"
        return "••••••••"

    def build_stream_key(self, target_uid=None):
        uid = target_uid or self.user_id
        return f"guild:{self.guild_id}:{self.channel_id}:{uid}" if self.guild_id else f"call:{self.channel_id}:{uid}"

    def get_uptime_string(self):
        if not self.is_connected or not self.connected_since:
            return "Offline"
        elapsed = int(time.time() - self.connected_since)
        hrs = elapsed // 3600
        mins = (elapsed % 3600) // 60
        return f"{hrs}h {mins}m"

    # --- SCIENCE TELEMETRY ENGINE (With duration_tracked_ms) ---
    async def _send_telemetry(self, events):
        if not self.analytics_token or not self.is_connected:
            return
        payload_events = []
        for evt in events:
            payload_events.append({
                "type": evt,
                "properties": {
                    "client_track_timestamp": int(time.time() * 1000),
                    "client_heartbeat_session_id": self.client_session_id,
                    "guild_id": self.guild_id,
                    "channel_id": self.channel_id,
                    "media_session_id": self.voice_session_id,
                    "rtc_connection_id": self.rtc_connection_id,
                    "duration": 60000,
                    "duration_tracked_ms": 60000, # The exact field Discord pipeline reads!
                    "is_video_blank": False,
                    "connection_type": "webrtc"
                }
            })
        payload = {"token": self.analytics_token, "events": payload_events}
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties()
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://discord.com/api/v9/science", json=payload, headers=headers) as r:
                    if r.status in (200, 204):
                        self.telemetry_status = "Tracking Active 🟢"
                    else:
                        self.telemetry_status = f"Err: {r.status} 🔴"
        except:
            self.telemetry_status = "Network Err 🟡"

    async def _telemetry_loop(self):
        try:
            while not self.is_connected or not self.stream_key:
                await asyncio.sleep(1)
            
            if self.role == "streamer":
                await self._send_telemetry(["media_session_joined", "video_stream_started"])
                print(f"📡 [Science] Tracking started for {self.username}!")
            
            while self.running:
                await asyncio.sleep(60)
                if self.role == "streamer" and self.is_connected and not self.cooldown_active:
                    await self._send_telemetry(["video_stream_heartbeat"])
        except asyncio.CancelledError:
            pass

    async def start(self):
        if self.running: return
        self.running = True
        self.status = "Connecting... ⏳"
        self.telemetry_status = "Initializing..."
        self.rtc_connection_id = str(uuid.uuid4())
        self.client_session_id = str(uuid.uuid4())
        self.task = asyncio.create_task(self._loop())
        if self.role == "streamer":
            self.telemetry_task = asyncio.create_task(self._telemetry_loop())

    async def stop(self):
        self.running = False
        if self.role == "streamer":
            await self._send_telemetry(["video_stream_ended"])
        if self.task: self.task.cancel()
        if self.telemetry_task: self.telemetry_task.cancel()
        if self.ws and not self.ws.closed:
            try:
                if self.role == "streamer" and self.stream_key:
                    await self.ws.send_json({"op": 19, "d": {"stream_key": self.stream_key}})
                await self.ws.send_json({"op": 4, "d": {"guild_id": self.guild_id, "channel_id": None, "self_mute": True, "self_deaf": True}})
            except: pass
            await self.ws.close()
        if self.voice_rtc: await self.voice_rtc.stop()
        if self.stream_rtc: await self.stream_rtc.stop()
        self.is_connected = False
        self.connected_since = None
        self.status = "Idle 🛑"
        self.telemetry_status = "Offline"

    async def _loop(self):
        while self.running:
            try:
                self.status = "Connecting... ⏳"
                self.ws = AsyncWebSocket(GATEWAY_URL)
                await self.ws.connect()
                hb_t = None
                try:
                    while self.running:
                        msg = await self.ws.recv_json()
                        if not msg: break
                        op, t, d = msg.get("op"), msg.get("t"), msg.get("d") or {}
                        if op == 10:
                            interval = d.get("heartbeat_interval", 41250) / 1000.0
                            hb_t = asyncio.create_task(self._hb(interval))
                            await self.ws.send_json({
                                "op": 2,
                                "d": {
                                    "token": self.token, "capabilities": 16381,
                                    "properties": json.loads(base64.b64decode(make_super_properties())),
                                    "presence": {"status": "online", "since": 0, "activities": [], "afk": False}
                                }
                            })
                        elif op == 1:
                            await self.ws.send_json({"op": 1, "d": None})
                        elif op == 0:
                            await self._handle_dispatch(t, d)
                finally:
                    if hb_t: hb_t.cancel()
                    if self.voice_rtc: await self.voice_rtc.stop()
                    if self.stream_rtc: await self.stream_rtc.stop()
                    if self.ws: await self.ws.close()
            except Exception as e:
                if not self.running: break
                self.status = f"Error: {e}"
                await asyncio.sleep(6)

    async def _hb(self, interval: float):
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 1, "d": None})
                await asyncio.sleep(interval)
        except: pass

    async def _handle_dispatch(self, t: str, d: dict):
        if t == "READY":
            user = d.get("user") or {}
            self.user_id = user.get("id")
            self.username = user.get("username", "User")
            av = user.get("avatar")
            self.avatar_url = f"https://cdn.discordapp.com/avatars/{self.user_id}/{av}.png?size=128" if av else f"https://cdn.discordapp.com/embed/avatars/{int(self.user_id or 0) % 5}.png"
            self.voice_session_id = d.get("session_id")
            self.is_connected = True
            self.connected_since = time.time()
            
            await self.ws.send_json({"op": 4, "d": {"guild_id": self.guild_id, "channel_id": self.channel_id, "self_mute": True, "self_deaf": True, "self_video": False}})
            
            if self.role == "streamer":
                await asyncio.sleep(1.5)
                await self.ws.send_json({"op": 18, "d": {"type": "guild" if self.guild_id else "call", "guild_id": self.guild_id, "channel_id": self.channel_id, "preferred_region": None}})
                self.stream_key = self.build_stream_key()
                await asyncio.sleep(0.5)
                await self.ws.send_json({"op": 22, "d": {"stream_key": self.stream_key, "paused": False}})
                self.status = "Live Streaming 📺"
            else:
                self.status = "Watching VC 👀"

        elif t == "VOICE_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            v_token = d.get("token")
            srv_id = d.get("guild_id") or self.channel_id
            if endpoint and v_token and self.user_id and self.voice_session_id:
                if self.voice_rtc: await self.voice_rtc.stop()
                self.voice_rtc = RTCVoiceSession(endpoint, srv_id, self.channel_id, self.user_id, self.voice_session_id, v_token, False)
                self.voice_rtc.start()

        elif t == "STREAM_CREATE":
            if self.role == "streamer":
                s_key = d.get("stream_key")
                if s_key:
                    self.stream_key = s_key
                    await self.ws.send_json({"op": 22, "d": {"stream_key": self.stream_key, "paused": False}})

        elif t == "STREAM_DELETE":
            if self.role == "watcher":
                s_key = d.get("stream_key")
                if s_key in self.watching_streams:
                    self.watching_streams.discard(s_key)
                    self.status = f"Watching {len(self.watching_streams)} Stream(s) 👀"

        elif t == "STREAM_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            s_token = d.get("token")
            s_key = d.get("stream_key") or self.stream_key
            if endpoint and s_token and self.user_id and self.voice_session_id and s_key:
                if self.stream_rtc: await self.stream_rtc.stop()
                self.stream_rtc = RTCVoiceSession(endpoint, self.guild_id or self.channel_id, self.channel_id, self.user_id, self.voice_session_id, s_token, self.rtc_connection_id, True)
                self.stream_rtc.start()

    async def watch_target(self, target_stream_key: str):
        if self.ws and not self.ws.closed:
            await self.ws.send_json({"op": 20, "d": {"stream_key": target_stream_key}})
            self.watching_streams.add(target_stream_key)
            self.status = f"Watching {len(self.watching_streams)} Stream(s) 👀🔥"

class CloudSystemManager:
    def __init__(self):
        self.accounts: dict[str, ManagedAccount] = {}
        self.is_running = False
        self.db = None
        self.mongo_client = None
        self.guild_id = DEFAULT_GUILD_ID
        self.guild_name = "Official Community"
        self.sync_db_task = None
        self.cycle_task = None
        self.rebalance_task = None
        self.badge_refresh_task = None

    def ensure_background_tasks(self):
        if not self.sync_db_task or self.sync_db_task.done():
            self.sync_db_task = asyncio.create_task(self._auto_save_loop())
        if not self.rebalance_task or self.rebalance_task.done():
            self.rebalance_task = asyncio.create_task(self._periodic_rebalance_loop())
        if not self.cycle_task or self.cycle_task.done():
            self.cycle_task = asyncio.create_task(self._three_hour_cooldown_loop())
        if not self.badge_refresh_task or self.badge_refresh_task.done():
            self.badge_refresh_task = asyncio.create_task(self._badge_refresh_loop())

    async def resolve_guild(self):
        url = f"https://discord.com/api/v10/invites/{SERVER_INVITE_CODE}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"User-Agent": USER_AGENT}, timeout=7) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        g = data.get("guild", {})
                        if g.get("id"):
                            self.guild_id = g["id"]
                            self.guild_name = g.get("name", "Official Community")
                            return
        except: pass
        self.guild_id = DEFAULT_GUILD_ID

    async def update_account_badge_from_discord(self, acc: ManagedAccount):
        if not acc.user_id or not acc.token: return
        url = f"https://discord.com/api/v9/users/{acc.user_id}/profile?with_mutual_guilds=false"
        headers = {"Authorization": acc.token, "User-Agent": USER_AGENT}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=8) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        badges = data.get("badges", [])
                        matched_tier = 0
                        raw_hrs = None

                        for b in badges:
                            b_id = str(b.get("id", "")).lower()
                            desc = str(b.get("description", ""))
                            if "streaming" in b_id or "stream" in b_id:
                                match = re.search(r"streaming(?:_tier)?_(\d+)", b_id, re.I)
                                if match: matched_tier = int(match.group(1))
                                hrs_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)", desc, re.I)
                                if hrs_match:
                                    try: raw_hrs = float(hrs_match.group(1))
                                    except: pass
                                break

                        acc.tier_num = matched_tier
                        acc.raw_hours = raw_hrs

                        if matched_tier == 0:
                            acc.current_tier = "No Badge (0h)"
                            acc.total_hours = raw_hrs if raw_hrs is not None else 0.0
                            acc.next_tier = "Tier 1: Newcomer (1 hour)"
                            acc.remaining_hours = max(0.0, 1.0 - acc.total_hours)
                            acc.progress_percent = min(100.0, (acc.total_hours / 1.0) * 100.0)
                        elif matched_tier in STREAM_BADGE_MILESTONES:
                            curr_base_hrs, curr_name = STREAM_BADGE_MILESTONES[matched_tier]
                            acc.current_tier = curr_name
                            acc.total_hours = raw_hrs if raw_hrs is not None else float(curr_base_hrs)
                            if matched_tier < 10:
                                next_base_hrs, next_name = STREAM_BADGE_MILESTONES[matched_tier + 1]
                                acc.next_tier = next_name
                                acc.remaining_hours = max(0.0, float(next_base_hrs) - acc.total_hours)
                                span = float(next_base_hrs - curr_base_hrs)
                                offset = acc.total_hours - float(curr_base_hrs)
                                acc.progress_percent = min(100.0, max(0.0, (offset / span) * 100.0)) if span > 0 else 100.0
                            else:
                                acc.next_tier = "Max Level Reached! 👑"
                                acc.remaining_hours = 0.0
                                acc.progress_percent = 100.0
        except: pass

    async def _badge_refresh_loop(self):
        try:
            while True:
                await asyncio.sleep(300)
                for acc in list(self.accounts.values()):
                    if acc.user_id and acc.token:
                        await self.update_account_badge_from_discord(acc)
                        await self.save_account_to_db(acc)
        except: pass

    async def verify_token_and_membership(self, token: str):
        headers = {"Authorization": token, "User-Agent": USER_AGENT}
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v10/users/@me?with_analytics_token=true", headers=headers, timeout=8) as r1:
                if r1.status == 401: return False, "Invalid Discord Token.", None, False
                if r1.status != 200: return False, f"API Error ({r1.status})", None, False
                user_info = await r1.json()

            target_gid = str(self.guild_id)
            async with session.get("https://discord.com/api/v10/users/@me/guilds", headers=headers, timeout=8) as r2:
                if r2.status == 200:
                    user_guilds = await r2.json()
                    if str(self.guild_id) not in [str(g.get("id")) for g in user_guilds if "id" in g]:
                        return False, "You must join our server first!", user_info, True
                else: return False, "Could not verify server membership.", user_info, False
        return True, "Valid", user_info, False

    async def init_mongo(self):
        if not MONGO_URI: return
        try:
            self.mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client["streamer_badge_db"]
            async for doc in self.db.accounts.find({}):
                acc_id = doc["acc_id"]
                self.accounts[acc_id] = ManagedAccount(
                    acc_id=acc_id, token=doc["token"], role=doc["role"],
                    guild_id=self.guild_id, channel_id=DEFAULT_CHANNEL_ID,
                    owner_id=doc.get("owner_id", "global"), scope=doc.get("scope", "private"),
                    username=doc.get("username", "Saved Account"), avatar_url=doc.get("avatar_url"),
                    analytics_token=doc.get("analytics_token", ""), tier_num=doc.get("tier_num", 0),
                    total_hours=doc.get("total_hours", 0.0), current_tier=doc.get("current_tier", "No Badge (0h)"),
                    next_tier=doc.get("next_tier", "Tier 1: Newcomer (1 hour)"), remaining_hours=doc.get("remaining_hours", 1.0),
                    progress_percent=doc.get("progress_percent", 0.0)
                )
        except: self.db = None

    async def save_account_to_db(self, acc: ManagedAccount):
        if self.db is not None:
            try:
                await self.db.accounts.update_one({"acc_id": acc.acc_id}, {"$set": {
                    "acc_id": acc.acc_id, "token": acc.token, "role": acc.role,
                    "guild_id": acc.guild_id, "channel_id": acc.channel_id, "owner_id": acc.owner_id,
                    "scope": acc.scope, "username": acc.username, "avatar_url": acc.avatar_url,
                    "analytics_token": acc.analytics_token, "tier_num": acc.tier_num,
                    "total_hours": acc.total_hours, "current_tier": acc.current_tier,
                    "next_tier": acc.next_tier, "remaining_hours": acc.remaining_hours,
                    "progress_percent": acc.progress_percent
                }}, upsert=True)
            except: pass

    async def remove_account_from_db(self, acc_id: str):
        if self.db is not None:
            try: await self.db.accounts.delete_one({"acc_id": acc_id})
            except: pass

    async def balance_and_pair_streams(self):
        await asyncio.sleep(3)
        streamers = [a for a in self.accounts.values() if a.role == "streamer" and a.is_connected and a.stream_key]
        watchers = [a for a in self.accounts.values() if a.role == "watcher" and a.is_connected]

        for pw in [w for w in watchers if w.scope == "private"]:
            for s in [s for s in streamers if s.owner_id == pw.owner_id]:
                s_key = s.stream_key or s.build_stream_key()
                if s_key not in pw.watching_streams:
                    await pw.watch_target(s_key)
                    await asyncio.sleep(4)

        comm_watchers = [w for w in watchers if w.scope == "community"]
        if comm_watchers and streamers:
            for idx, s in enumerate(streamers):
                assigned = comm_watchers[idx % len(comm_watchers)]
                s_key = s.stream_key or s.build_stream_key()
                if s_key not in assigned.watching_streams:
                    await assigned.watch_target(s_key)
                    await asyncio.sleep(4)

    async def start_all(self):
        self.is_running = True
        self.ensure_background_tasks()
        for acc in self.accounts.values():
            if not acc.running:
                await acc.start()
                await asyncio.sleep(4)

    async def stop_all(self):
        self.is_running = False
        for acc in self.accounts.values():
            if acc.running:
                await acc.stop()
                await self.save_account_to_db(acc)

    async def _periodic_rebalance_loop(self):
        try:
            while True:
                await self.balance_and_pair_streams()
                await asyncio.sleep(60)
        except: pass

    async def _three_hour_cooldown_loop(self):
        try:
            while True:
                await asyncio.sleep(3 * 3600)
                if not any(a.is_connected for a in self.accounts.values()): continue
                for acc in self.accounts.values():
                    if acc.is_connected and acc.ws and not acc.ws.closed:
                        acc.cooldown_active = True
                        acc.status = "Cooldown Rest (3 mins) ☕"
                        acc.watching_streams.clear()
                        await acc.ws.send_json({"op": 4, "d": {"guild_id": acc.guild_id, "channel_id": None, "self_mute": True, "self_deaf": True}})
                await asyncio.sleep(180)
                for acc in self.accounts.values():
                    if acc.running and acc.ws and not acc.ws.closed:
                        acc.cooldown_active = False
                        await acc.ws.send_json({"op": 4, "d": {"guild_id": acc.guild_id, "channel_id": acc.channel_id, "self_mute": True, "self_deaf": True, "self_video": False}})
                        if acc.role == "streamer":
                            await asyncio.sleep(1.5)
                            await acc.ws.send_json({"op": 18, "d": {"type": "guild", "guild_id": acc.guild_id, "channel_id": acc.channel_id, "preferred_region": None}})
                            await asyncio.sleep(0.5)
                            acc.stream_key = acc.build_stream_key()
                            await acc.ws.send_json({"op": 22, "d": {"stream_key": acc.stream_key, "paused": False}})
                            acc.status = "Live Streaming 📺"
                await self.balance_and_pair_streams()
        except: pass

    async def _auto_save_loop(self):
        try:
            while True:
                await asyncio.sleep(60)
                for acc in self.accounts.values():
                    if acc.running: await self.save_account_to_db(acc)
        except: pass

manager = CloudSystemManager()
routes = web.RouteTableDef()

@web.middleware
async def session_cookie_middleware(request, handler):
    sess_id = request.cookies.get("spoofer_session")
    is_new = not sess_id
    if is_new: sess_id = "sess_" + uuid.uuid4().hex
    request["session_id"] = sess_id
    resp = await handler(request)
    if is_new: resp.set_cookie("spoofer_session", sess_id, max_age=31536000, httponly=True, samesite="Lax")
    return resp

@routes.get("/")
async def handle_index(request): return web.FileResponse("public/index.html")

@routes.post("/api/admin/login")
async def handle_admin_login(request):
    data = await request.json()
    if data.get("password", "") == ADMIN_SECRET:
        resp = web.json_response({"success": True})
        resp.set_cookie("admin_token", ADMIN_SECRET, max_age=86400*7, httponly=True, samesite="Lax")
        return resp
    return web.json_response({"success": False, "error": "Invalid admin password!"})

@routes.post("/api/admin/logout")
async def handle_admin_logout(request):
    resp = web.json_response({"success": True})
    resp.del_cookie("admin_token")
    return resp

@routes.get("/api/status")
async def handle_status(request):
    sess_id = request.get("session_id", "")
    is_admin = (request.cookies.get("admin_token") == ADMIN_SECRET or request.headers.get("X-Admin-Token") == ADMIN_SECRET)
    accs = [{
        "id": a.acc_id, "role": a.role, "scope": a.scope, "username": a.username, "avatar": a.avatar_url,
        "maskedToken": a.mask_token(), "status": a.status, "connected": a.is_connected, "running": a.running,
        "uptime": a.get_uptime_string(), "totalHours": round(a.total_hours, 2), "currentTier": a.current_tier,
        "nextTier": a.next_tier, "remainingHours": round(a.remaining_hours, 2), "progressPercent": round(a.progress_percent, 1),
        "telemetryStatus": a.telemetry_status,
        "isOwner": is_admin or (a.owner_id == sess_id)
    } for a in manager.accounts.values()]
    accs.sort(key=lambda x: (0 if x["role"] == "streamer" else 1, -x["totalHours"]))
    return web.json_response({
        "isRunning": manager.is_running, "mongoConnected": manager.db is not None, "guildName": manager.guild_name,
        "guildId": manager.guild_id, "channelId": DEFAULT_CHANNEL_ID, "serverInvite": f"https://discord.gg/{SERVER_INVITE_CODE}",
        "isAdmin": is_admin, "accounts": accs
    })

@routes.post("/api/accounts/add")
async def handle_add(request):
    data = await request.json()
    token, role = data.get("token", "").strip(), data.get("role", "streamer")
    if not token: return web.json_response({"success": False, "error": "Token empty."})
    sess_id = request.get("session_id", "global")
    is_admin = (request.cookies.get("admin_token") == ADMIN_SECRET or request.headers.get("X-Admin-Token") == ADMIN_SECRET)
    valid, msg, u_info, req_join = await manager.verify_token_and_membership(token)
    if not valid: return web.json_response({"success": False, "error": msg, "requires_join": req_join})
    u_id, analytics = u_info.get("id"), u_info.get("analytics_token", "")
    av = u_info.get("avatar")
    av_url = f"https://cdn.discordapp.com/avatars/{u_id}/{av}.png?size=128" if av else f"https://cdn.discordapp.com/embed/avatars/{int(u_id or 0)%5}.png"
    acc = ManagedAccount(
        acc_id=str(uuid.uuid4())[:8], token=token, role=role, guild_id=manager.guild_id, channel_id=DEFAULT_CHANNEL_ID,
        owner_id="admin" if is_admin else sess_id, scope="community" if is_admin else "private",
        username=u_info.get("username", "User"), avatar_url=av_url, analytics_token=analytics
    )
    acc.user_id = u_id
    manager.accounts[acc.acc_id] = acc
    await manager.update_account_badge_from_discord(acc)
    await manager.save_account_to_db(acc)
    manager.ensure_background_tasks()
    await acc.start()
    asyncio.create_task(manager.balance_and_pair_streams())
    return web.json_response({"success": True, "username": acc.username})

@routes.post("/api/accounts/toggle")
async def handle_toggle(request):
    data, sess_id = await request.json(), request.get("session_id", "")
    is_admin = (request.cookies.get("admin_token") == ADMIN_SECRET or request.headers.get("X-Admin-Token") == ADMIN_SECRET)
    acc = manager.accounts.get(data.get("id"))
    if not acc: return web.json_response({"success": False, "error": "Not found."})
    if not is_admin and acc.owner_id != sess_id: return web.json_response({"success": False, "error": "Unauthorized!"})
    if acc.running:
        await acc.stop()
        return web.json_response({"success": True, "running": False})
    else:
        manager.ensure_background_tasks()
        await acc.start()
        asyncio.create_task(manager.balance_and_pair_streams())
        return web.json_response({"success": True, "running": True})

@routes.post("/api/accounts/remove")
async def handle_remove(request):
    data, sess_id = await request.json(), request.get("session_id", "")
    is_admin = (request.cookies.get("admin_token") == ADMIN_SECRET or request.headers.get("X-Admin-Token") == ADMIN_SECRET)
    acc = manager.accounts.get(data.get("id"))
    if not acc: return web.json_response({"success": False, "error": "Not found."})
    if not is_admin and acc.owner_id != sess_id: return web.json_response({"success": False, "error": "Unauthorized!"})
    await acc.stop()
    del manager.accounts[acc.acc_id]
    await manager.remove_account_from_db(acc.acc_id)
    return web.json_response({"success": True})

@routes.post("/api/start")
async def handle_start(request):
    if request.cookies.get("admin_token") != ADMIN_SECRET and request.headers.get("X-Admin-Token") != ADMIN_SECRET: return web.json_response({"success": False})
    await manager.start_all()
    return web.json_response({"success": True})

@routes.post("/api/stop")
async def handle_stop(request):
    if request.cookies.get("admin_token") != ADMIN_SECRET and request.headers.get("X-Admin-Token") != ADMIN_SECRET: return web.json_response({"success": False})
    await manager.stop_all()
    return web.json_response({"success": True})

app = web.Application(middlewares=[session_cookie_middleware])
app.add_routes(routes)
app.router.add_static("/public/", path="public", name="public")

async def main():
    await manager.resolve_guild()
    await manager.init_mongo()
    manager.ensure_background_tasks()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    print(f"🌐 Server Live on Port: {PORT}", flush=True)
    await site.start()
    while True: await asyncio.sleep(3600)

if __name__ == "__main__": asyncio.run(main())
