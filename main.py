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
import aiohttp
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient

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
    1: (1, "Tier 1: Newcomer (1 hour)"),
    2: (5, "Tier 2: Fledgling (5 hours)"),
    3: (20, "Tier 3: Breakout (20 hours)"),
    4: (75, "Tier 4: Standout (75 hours)"),
    5: (150, "Tier 5: Trendsetter (150 hours)"),
    6: (300, "Tier 6: Headliner (300 hours)"),
    7: (500, "Tier 7: Star (500 hours)"),
    8: (1000, "Tier 8: Sensation (1,000 hours)"),
    9: (2000, "Tier 9: Visionary (2,000 hours)"),
    10: (5000, "Tier 10: Phenomenon (5,000+ hours)"),
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
            raise ConnectionError(f"Handshake failed: {line.decode('utf-8', errors='ignore').strip()}")
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
                    except Exception:
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
            except Exception:
                pass

class RTCVoiceSession:
    def __init__(self, endpoint: str, server_id: str, channel_id: str, user_id: str, session_id: str, voice_token: str, is_stream: bool = False):
        self.endpoint = endpoint
        self.server_id = server_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.session_id = session_id
        self.voice_token = voice_token
        self.is_stream = is_stream
        self.ws = None
        self._task = None
        self._hb_task = None
        self.running = False

    def start(self):
        self.running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self.running = False
        if self._hb_task:
            self._hb_task.cancel()
        if self.ws:
            await self.ws.close()
        if self._task:
            self._task.cancel()

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
                    if self._hb_task:
                        self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._hb(interval))
                elif op == 2:
                    await self.ws.send_json({
                        "op": 1,
                        "d": {
                            "protocol": "udp",
                            "data": {"address": d.get("ip", "127.0.0.1"), "port": d.get("port", 1337), "mode": "aead_aes256_gcm_rtpsize"},
                            "codecs": [
                                {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120},
                                {"name": "H264", "type": "video", "priority": 1000, "payload_type": 101, "rtx_payload_type": 102, "encode": True, "decode": True}
                            ],
                            "rtc_connection_id": str(uuid.uuid4())
                        }
                    })
                    ssrc = d.get("ssrc", 1)
                    await self.ws.send_json({
                        "op": 12,
                        "d": {
                            "audio_ssrc": ssrc, "video_ssrc": ssrc + 1, "rtx_ssrc": ssrc + 2,
                            "streams": [{
                                "type": "video", "rid": "100", "ssrc": ssrc + 1, "active": True,
                                "quality": 100, "rtx_ssrc": ssrc + 2, "max_bitrate": 1500000,
                                "max_framerate": 30, "max_resolution": {"type": "fixed", "width": 1280, "height": 720}
                            }]
                        }
                    })
        except Exception as e:
            print(f"⚠️ RTCVoiceSession error: {e}", flush=True)

    async def _hb(self, interval: float):
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": -1}})
                await asyncio.sleep(interval)
        except Exception:
            pass

class ManagedAccount:
    def __init__(self, acc_id: str, token: str, role: str, guild_id: str, channel_id: str, owner_id: str = "global", scope: str = "community", username="Loading...", avatar_url=None, tier_num=0, raw_hours=None, total_hours=0.0, current_tier="No Badge (0h)", next_tier="Tier 1: Newcomer (1 hour)", remaining_hours=1.0, progress_percent=0.0):
        self.acc_id = acc_id
        self.token = token.strip()
        self.role = role
        self.guild_id = guild_id.strip()
        self.channel_id = channel_id.strip()
        self.owner_id = owner_id
        self.scope = scope
        self.username = username
        self.avatar_url = avatar_url or "https://cdn.discordapp.com/embed/avatars/0.png"
        
        # Real Discord Badge State (No fake local counter)
        self.tier_num = tier_num
        self.raw_hours = raw_hours
        self.total_hours = total_hours
        self.current_tier = current_tier
        self.next_tier = next_tier
        self.remaining_hours = remaining_hours
        self.progress_percent = progress_percent

        self.status = "Idle 🛑"
        self.is_connected = False
        self.connected_since = None
        self.user_id = None
        self.stream_key = None
        self.voice_session_id = None
        self.watching_streams = set()
        self.ws = None
        self.voice_rtc = None
        self.stream_rtc = None
        self.running = False
        self.task = None
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

    async def start(self):
        if self.running:
            return
        self.running = True
        self.status = "Connecting... ⏳"
        self.task = asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        if self.ws and not self.ws.closed:
            try:
                if self.role == "streamer" and self.stream_key:
                    await self.ws.send_json({"op": 19, "d": {"stream_key": self.stream_key}})
                await self.ws.send_json({"op": 4, "d": {"guild_id": self.guild_id, "channel_id": None, "self_mute": True, "self_deaf": True}})
            except Exception:
                pass
            await self.ws.close()
        if self.voice_rtc:
            await self.voice_rtc.stop()
        if self.stream_rtc:
            await self.stream_rtc.stop()
        self.is_connected = False
        self.connected_since = None
        self.status = "Idle 🛑"

    async def _loop(self):
        while self.running:
            try:
                self.status = "Connecting... ⏳"
                print(f"🔄 Connecting [{self.username}] ({self.role}) to Gateway...", flush=True)
                self.ws = AsyncWebSocket(GATEWAY_URL)
                await self.ws.connect()
                hb_t = None
                try:
                    while self.running:
                        msg = await self.ws.recv_json()
                        if not msg:
                            break
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
                    if hb_t:
                        hb_t.cancel()
                    if self.voice_rtc:
                        await self.voice_rtc.stop()
                    if self.stream_rtc:
                        await self.stream_rtc.stop()
                    if self.ws:
                        await self.ws.close()
            except Exception as e:
                if not self.running:
                    break
                self.status = f"Error: {e}"
                print(f"❌ [{self.username}] Gateway Error: {e}", flush=True)
                await asyncio.sleep(6)

    async def _hb(self, interval: float):
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 1, "d": None})
                await asyncio.sleep(interval)
        except Exception:
            pass

    async def _handle_dispatch(self, t: str, d: dict):
        if t == "READY":
            user = d.get("user") or {}
            self.user_id = user.get("id")
            self.username = user.get("username", "DiscordUser")
            av = user.get("avatar")
            if av:
                self.avatar_url = f"https://cdn.discordapp.com/avatars/{self.user_id}/{av}.png?size=128"
            else:
                self.avatar_url = f"https://cdn.discordapp.com/embed/avatars/{int(self.user_id or 0) % 5}.png"
            self.voice_session_id = d.get("session_id")
            self.is_connected = True
            self.connected_since = time.time()
            print(f"✅ [{self.username}] Authenticated on Gateway!", flush=True)

            # Join target voice room
            await self.ws.send_json({
                "op": 4,
                "d": {"guild_id": self.guild_id, "channel_id": self.channel_id, "self_mute": True, "self_deaf": True, "self_video": False}
            })

            if self.role == "streamer":
                await asyncio.sleep(1.5)
                await self.ws.send_json({
                    "op": 18,
                    "d": {"type": "guild" if self.guild_id else "call", "guild_id": self.guild_id, "channel_id": self.channel_id, "preferred_region": None}
                })
                self.stream_key = self.build_stream_key()
                await asyncio.sleep(0.5)
                await self.ws.send_json({"op": 22, "d": {"stream_key": self.stream_key, "paused": False}})
                self.status = "Live Streaming 📺"
                print(f"📺 [{self.username}] Broadcast live!", flush=True)
            else:
                self.status = "Watching VC 👀"
                print(f"👀 [{self.username}] Watcher is active! ({self.scope})", flush=True)

        elif t == "VOICE_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            v_token = d.get("token")
            srv_id = d.get("guild_id") or self.channel_id
            if endpoint and v_token and self.user_id and self.voice_session_id:
                if self.voice_rtc:
                    await self.voice_rtc.stop()
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
                if self.stream_rtc:
                    await self.stream_rtc.stop()
                self.stream_rtc = RTCVoiceSession(endpoint, self.guild_id or self.channel_id, self.channel_id, self.user_id, self.voice_session_id, s_token, True)
                self.stream_rtc.start()

    async def watch_target(self, target_stream_key: str):
        if self.ws and not self.ws.closed:
            await self.ws.send_json({"op": 20, "d": {"stream_key": target_stream_key}})
            self.watching_streams.add(target_stream_key)
            cnt = len(self.watching_streams)
            self.status = f"Watching {cnt} Stream(s) 👀🔥"
            print(f"👀 [{self.username}] Watching stream: {target_stream_key}", flush=True)

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
                            print(f"🎯 Resolved Guild: {self.guild_name} ({self.guild_id})", flush=True)
                            return
        except Exception:
            pass
        self.guild_id = DEFAULT_GUILD_ID
        print(f"ℹ️ Fixed Guild ID: {self.guild_id}", flush=True)

    # Real Badge Inspector from Discord Profile API v9
    async def update_account_badge_from_discord(self, acc: ManagedAccount):
        if not acc.user_id or not acc.token:
            return
        url = f"https://discord.com/api/v9/users/{acc.user_id}/profile"
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

                            # Check for official streaming progression badge IDs
                            if "streaming" in b_id or "stream" in b_id:
                                match = re.search(r"streaming(?:_tier)?_(\d+)", b_id, re.I)
                                if match:
                                    matched_tier = int(match.group(1))

                                # Extract raw hours if reported by Discord
                                hrs_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)", desc, re.I)
                                if hrs_match:
                                    try:
                                        raw_hrs = float(hrs_match.group(1))
                                    except Exception:
                                        pass
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
                        print(f"📊 [Discord Profile API v9] {acc.username}: Verified Tier {matched_tier} ({acc.total_hours} hrs)", flush=True)
        except Exception as e:
            print(f"⚠️ Could not sync Discord profile for {acc.username}: {e}", flush=True)

    # Background loop: Re-fetches real badge progress every 5 minutes (300 seconds)
    async def _badge_refresh_loop(self):
        try:
            while True:
                await asyncio.sleep(300)
                for acc in list(self.accounts.values()):
                    if acc.user_id and acc.token:
                        await self.update_account_badge_from_discord(acc)
                        await self.save_account_to_db(acc)
        except asyncio.CancelledError:
            pass

    async def verify_token_and_membership(self, token: str):
        headers = {"Authorization": token, "User-Agent": USER_AGENT}
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v10/users/@me", headers=headers, timeout=8) as r1:
                if r1.status == 401:
                    return False, "Invalid or expired Discord Token.", None, False
                if r1.status != 200:
                    return False, f"Discord API Error (HTTP {r1.status})", None, False
                user_info = await r1.json()

            target_gid = str(self.guild_id)
            async with session.get("https://discord.com/api/v10/users/@me/guilds", headers=headers, timeout=8) as r2:
                if r2.status == 200:
                    user_guilds = await r2.json()
                    member_guild_ids = [str(g.get("id")) for g in user_guilds if "id" in g]
                    if target_gid not in member_guild_ids:
                        return False, "This account is not in our server. You must join first!", user_info, True
                else:
                    return False, "Could not verify server membership.", user_info, False

        return True, "Valid", user_info, False

    async def init_mongo(self):
        if not MONGO_URI:
            print("⚠️ MONGO_URI not provided. Storing in-memory.", flush=True)
            return
        try:
            self.mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client["streamer_badge_db"]
            cursor = self.db.accounts.find({})
            async for doc in cursor:
                acc_id = doc["acc_id"]
                self.accounts[acc_id] = ManagedAccount(
                    acc_id=acc_id,
                    token=doc["token"],
                    role=doc["role"],
                    guild_id=self.guild_id,
                    channel_id=DEFAULT_CHANNEL_ID,
                    owner_id=doc.get("owner_id", "global"),
                    scope=doc.get("scope", "community" if doc.get("owner_id") == "admin" else "private"),
                    username=doc.get("username", "Saved Account"),
                    avatar_url=doc.get("avatar_url"),
                    tier_num=doc.get("tier_num", 0),
                    raw_hours=doc.get("raw_hours"),
                    total_hours=doc.get("total_hours", 0.0),
                    current_tier=doc.get("current_tier", "No Badge (0h)"),
                    next_tier=doc.get("next_tier", "Tier 1: Newcomer (1 hour)"),
                    remaining_hours=doc.get("remaining_hours", 1.0),
                    progress_percent=doc.get("progress_percent", 0.0)
                )
            print(f"✅ Restored {len(self.accounts)} accounts from MongoDB!", flush=True)
        except Exception as e:
            print(f"⚠️ MongoDB Connection Failed: {e}", flush=True)
            self.db = None

    async def save_account_to_db(self, acc: ManagedAccount):
        if self.db is not None:
            try:
                await self.db.accounts.update_one(
                    {"acc_id": acc.acc_id},
                    {"$set": {
                        "acc_id": acc.acc_id,
                        "token": acc.token,
                        "role": acc.role,
                        "guild_id": acc.guild_id,
                        "channel_id": acc.channel_id,
                        "owner_id": acc.owner_id,
                        "scope": acc.scope,
                        "username": acc.username,
                        "avatar_url": acc.avatar_url,
                        "tier_num": acc.tier_num,
                        "raw_hours": acc.raw_hours,
                        "total_hours": acc.total_hours,
                        "current_tier": acc.current_tier,
                        "next_tier": acc.next_tier,
                        "remaining_hours": acc.remaining_hours,
                        "progress_percent": acc.progress_percent
                    }},
                    upsert=True
                )
            except Exception:
                pass

    async def remove_account_from_db(self, acc_id: str):
        if self.db is not None:
            try:
                await self.db.accounts.delete_one({"acc_id": acc_id})
            except Exception:
                pass

    async def balance_and_pair_streams(self):
        await asyncio.sleep(3)
        active_streamers = [a for a in self.accounts.values() if a.role == "streamer" and a.is_connected and a.stream_key]
        active_watchers = [a for a in self.accounts.values() if a.role == "watcher" and a.is_connected]

        private_watchers = [w for w in active_watchers if w.scope == "private"]
        for pw in private_watchers:
            my_streamers = [s for s in active_streamers if s.owner_id == pw.owner_id]
            for s in my_streamers:
                s_key = s.stream_key or s.build_stream_key()
                if s_key not in pw.watching_streams:
                    await pw.watch_target(s_key)
                    await asyncio.sleep(5)

        comm_watchers = [w for w in active_watchers if w.scope == "community"]
        num_comm = len(comm_watchers)
        if num_comm > 0 and len(active_streamers) > 0:
            for idx, s in enumerate(active_streamers):
                assigned_watcher = comm_watchers[idx % num_comm]
                s_key = s.stream_key or s.build_stream_key()
                if s_key not in assigned_watcher.watching_streams:
                    await assigned_watcher.watch_target(s_key)
                    await asyncio.sleep(5)

    async def start_all(self):
        self.is_running = True
        self.ensure_background_tasks()
        print("🚀 Starting all accounts...", flush=True)
        for acc in self.accounts.values():
            if not acc.running:
                await acc.start()
                await asyncio.sleep(5)
        await self.send_webhook("🚀 System: All accounts initiated.")

    async def stop_all(self):
        self.is_running = False
        for acc in self.accounts.values():
            if acc.running:
                await acc.stop()
                await self.save_account_to_db(acc)
        print("🛑 All accounts stopped.", flush=True)
        await self.send_webhook("🛑 System: All accounts halted.")

    async def _periodic_rebalance_loop(self):
        try:
            while True:
                await self.balance_and_pair_streams()
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            pass

    async def _three_hour_cooldown_loop(self):
        try:
            while True:
                await asyncio.sleep(3 * 3600)
                has_active = any(a.is_connected for a in self.accounts.values())
                if not has_active:
                    continue

                print("☕ [Anti-Rate-Limit] 3-minute cooldown rest for all active accounts...", flush=True)
                for acc in self.accounts.values():
                    if acc.is_connected and acc.ws and not acc.ws.closed:
                        acc.cooldown_active = True
                        acc.status = "Cooldown Rest (3 mins) ☕"
                        acc.watching_streams.clear()
                        await acc.ws.send_json({"op": 4, "d": {"guild_id": acc.guild_id, "channel_id": None, "self_mute": True, "self_deaf": True}})

                await asyncio.sleep(180)
                print("🔄 [Anti-Rate-Limit] Resuming accounts after rest...", flush=True)
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
        except asyncio.CancelledError:
            pass

    # Real DB auto-save loop: Saves state without fake local counter ticks
    async def _auto_save_loop(self):
        try:
            while True:
                await asyncio.sleep(30)
                for acc in self.accounts.values():
                    if acc.running:
                        await self.save_account_to_db(acc)
        except asyncio.CancelledError:
            pass

    async def send_webhook(self, title: str):
        if not WEBHOOK_URL:
            return
        fields = []
        for acc in self.accounts.values():
            if acc.role == "streamer":
                fields.append({
                    "name": f"📺 {acc.username} ({acc.total_hours}h)",
                    "value": f"Current: {acc.current_tier}\nNext: {acc.next_tier}\nRemaining: {acc.remaining_hours}h",
                    "inline": True
                })
        if not fields:
            fields.append({"name": "Active Accounts", "value": "None currently active", "inline": False})

        payload = {
            "username": "ScreenShare Cloud Monitor",
            "avatar_url": "https://cdn.discordapp.com/emojis/1041155314777309244.webp",
            "embeds": [{
                "title": title, "color": 5793266, "fields": fields,
                "footer": {"text": "Discord Profile v9 Verified • Railway"}
            }]
        }
        try:
            req = urllib.request.Request(
                WEBHOOK_URL, data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}, method="POST"
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass

manager = CloudSystemManager()
routes = web.RouteTableDef()

@web.middleware
async def session_cookie_middleware(request, handler):
    sess_id = request.cookies.get("spoofer_session")
    is_new = False
    if not sess_id:
        sess_id = "sess_" + uuid.uuid4().hex
        is_new = True
    request["session_id"] = sess_id
    response = await handler(request)
    if is_new:
        response.set_cookie(
            "spoofer_session", sess_id,
            max_age=31536000, httponly=True, samesite="Lax"
        )
    return response

@routes.get("/")
async def handle_index(request):
    return web.FileResponse("public/index.html")

# Security fix: Strictly check password only (removed username check)
@routes.post("/api/admin/login")
async def handle_admin_login(request):
    data = await request.json()
    pwd = data.get("password", "").strip()
    if pwd == ADMIN_SECRET:
        resp = web.json_response({"success": True})
        resp.set_cookie("admin_token", ADMIN_SECRET, max_age=86400 * 7, httponly=True, samesite="Lax")
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
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    is_admin = (admin_cookie == ADMIN_SECRET or admin_header == ADMIN_SECRET)

    accs = []
    for acc in manager.accounts.values():
        is_owner = is_admin or (acc.owner_id == sess_id)

        accs.append({
            "id": acc.acc_id,
            "role": acc.role,
            "scope": acc.scope,
            "username": acc.username,
            "avatar": acc.avatar_url,
            "maskedToken": acc.mask_token(),
            "status": acc.status,
            "connected": acc.is_connected,
            "running": acc.running,
            "uptime": acc.get_uptime_string(),
            "tierNum": acc.tier_num,
            "totalHours": round(acc.total_hours, 2),
            "hasRawHours": acc.raw_hours is not None,
            "currentTier": acc.current_tier,
            "nextTier": acc.next_tier,
            "remainingHours": round(acc.remaining_hours, 2),
            "progressPercent": round(acc.progress_percent, 1),
            "isOwner": is_owner
        })

    # Sort streamers by real Discord hours descending
    accs.sort(key=lambda x: (0 if x["role"] == "streamer" else 1, -x["totalHours"]))

    return web.json_response({
        "isRunning": manager.is_running,
        "mongoConnected": manager.db is not None,
        "guildName": manager.guild_name,
        "guildId": manager.guild_id,
        "channelId": DEFAULT_CHANNEL_ID,
        "serverInvite": f"https://discord.gg/{SERVER_INVITE_CODE}",
        "isAdmin": is_admin,
        "accounts": accs
    })

@routes.post("/api/accounts/add")
async def handle_add(request):
    data = await request.json()
    token = data.get("token", "").strip()
    role = data.get("role", "streamer")
    sess_id = request.get("session_id", "global")
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    is_admin = (admin_cookie == ADMIN_SECRET or admin_header == ADMIN_SECRET)

    if not token:
        return web.json_response({"success": False, "error": "Token cannot be empty."})

    valid, msg, user_info, requires_join = await manager.verify_token_and_membership(token)
    if not valid:
        return web.json_response({
            "success": False,
            "error": msg,
            "requires_join": requires_join,
            "invite": f"https://discord.gg/{SERVER_INVITE_CODE}"
        })

    u_id = user_info.get("id")
    acc_id = str(uuid.uuid4())[:8]
    av = user_info.get("avatar")
    avatar_url = f"https://cdn.discordapp.com/avatars/{u_id}/{av}.png?size=128" if av else f"https://cdn.discordapp.com/embed/avatars/{int(u_id or 0) % 5}.png"

    scope = "community" if is_admin else "private"

    acc = ManagedAccount(
        acc_id=acc_id,
        token=token,
        role=role,
        guild_id=manager.guild_id,
        channel_id=DEFAULT_CHANNEL_ID,
        owner_id="admin" if is_admin else sess_id,
        scope=scope,
        username=user_info.get("username", "DiscordUser"),
        avatar_url=avatar_url
    )
    acc.user_id = u_id
    manager.accounts[acc_id] = acc

    # Fetch real Discord profile badge immediately upon adding
    await manager.update_account_badge_from_discord(acc)
    await manager.save_account_to_db(acc)

    manager.ensure_background_tasks()
    await acc.start()
    asyncio.create_task(manager.balance_and_pair_streams())

    return web.json_response({"success": True, "username": acc.username, "scope": scope})

@routes.post("/api/accounts/toggle")
async def handle_toggle(request):
    data = await request.json()
    acc_id = data.get("id")
    sess_id = request.get("session_id", "")
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    is_admin = (admin_cookie == ADMIN_SECRET or admin_header == ADMIN_SECRET)

    if acc_id not in manager.accounts:
        return web.json_response({"success": False, "error": "Account not found."})

    acc = manager.accounts[acc_id]
    if not is_admin and acc.owner_id != sess_id:
        return web.json_response({"success": False, "error": "Unauthorized: You can only control your own accounts!"})

    if acc.running:
        await acc.stop()
        await manager.save_account_to_db(acc)
        return web.json_response({"success": True, "running": False, "status": "Idle 🛑"})
    else:
        manager.ensure_background_tasks()
        await acc.start()
        await manager.save_account_to_db(acc)
        asyncio.create_task(manager.balance_and_pair_streams())
        return web.json_response({"success": True, "running": True, "status": "Connecting... ⏳"})

@routes.post("/api/accounts/remove")
async def handle_remove(request):
    data = await request.json()
    acc_id = data.get("id")
    sess_id = request.get("session_id", "")
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    is_admin = (admin_cookie == ADMIN_SECRET or admin_header == ADMIN_SECRET)

    if acc_id not in manager.accounts:
        return web.json_response({"success": False, "error": "Account not found."})

    acc = manager.accounts[acc_id]
    if not is_admin and acc.owner_id != sess_id:
        return web.json_response({"success": False, "error": "Unauthorized: You can only delete your own accounts!"})

    await acc.stop()
    del manager.accounts[acc_id]
    await manager.remove_account_from_db(acc_id)
    return web.json_response({"success": True})

@routes.post("/api/start")
async def handle_start(request):
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    if admin_cookie != ADMIN_SECRET and admin_header != ADMIN_SECRET:
        return web.json_response({"success": False, "error": "Admin privileges required!"})
    await manager.start_all()
    return web.json_response({"success": True})

@routes.post("/api/stop")
async def handle_stop(request):
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    if admin_cookie != ADMIN_SECRET and admin_header != ADMIN_SECRET:
        return web.json_response({"success": False, "error": "Admin privileges required!"})
    await manager.stop_all()
    return web.json_response({"success": True})

@routes.post("/api/webhook/test")
async def handle_webhook_test(request):
    admin_cookie = request.cookies.get("admin_token", "")
    admin_header = request.headers.get("X-Admin-Token", "")
    if admin_cookie != ADMIN_SECRET and admin_header != ADMIN_SECRET:
        return web.json_response({"success": False, "error": "Admin privileges required!"})
    await manager.send_webhook("🔔 Admin Test: Webhook verified successfully!")
    return web.json_response({"success": True})

app = web.Application(middlewares=[session_cookie_middleware])
app.add_routes(routes)
app.router.add_static("/public/", path="public", name="public")

async def main():
    print(f"🚀 Initializing Community Badge Booster Server...", flush=True)
    await manager.resolve_guild()
    await manager.init_mongo()
    manager.ensure_background_tasks()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    print(f"🌐 Server Live on Port: {PORT} (Target Guild: {manager.guild_id})", flush=True)
    await site.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
