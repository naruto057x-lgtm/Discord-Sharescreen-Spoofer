#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import base64
import json
import os
import re
import socket
import ssl
import struct
import subprocess
import time
import urllib.request
import uuid
import aiohttp
from aiohttp import web
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import davey
import nacl.secret
from motor.motor_asyncio import AsyncIOMotorClient

PORT = int(os.environ.get("PORT", 3000))
MONGO_URI = os.environ.get("MONGO_URI", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "18102010")

SERVER_INVITE_CODE = "rAx625W8Gn"
DEFAULT_GUILD_ID = os.environ.get("GUILD_ID", "1059780215720648725")
DEFAULT_CHANNEL_ID = "1569527599426707546"
H264_LOOP_PATH = "/app/stream_loop.h264"

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

TIER_NAME_TO_NUM = {
    "newcomer": 1,
    "fledgling": 2,
    "breakout": 3,
    "standout": 4,
    "trendsetter": 5,
    "headliner": 6,
    "superstar": 7,
    "star": 7,
    "sensation": 8,
    "visionary": 9,
    "phenomenon": 10,
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

# =====================================================================
# H.264 Annex-B Frame Parser & Pre-Baked Loader
# =====================================================================
def split_annexb_nals(data: bytes) -> list[bytes]:
    nals = []
    i = 0
    n = len(data)
    start = -1
    while i < n:
        if i + 3 < n and data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 0 and data[i + 3] == 1:
            if start != -1 and i > start:
                nals.append(data[start:i])
            start = i + 4
            i += 4
        elif i + 2 < n and data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 1:
            if start != -1 and i > start:
                nals.append(data[start:i])
            start = i + 3
            i += 3
        else:
            i += 1
    if start != -1 and n > start:
        nals.append(data[start:n])
    return nals

def group_nals_into_frames(nals: list[bytes]) -> list[bytes]:
    frames = []
    current = bytearray()
    for nal in nals:
        if not nal:
            continue
        nal_type = nal[0] & 0x1F
        current.extend(b"\x00\x00\x00\x01")
        current.extend(nal)
        if nal_type in (1, 5):
            frames.append(bytes(current))
            current.clear()
    if current:
        frames.append(bytes(current))
    return frames

def load_video_frames() -> list[bytes]:
    path = H264_LOOP_PATH if os.path.exists(H264_LOOP_PATH) else "stream_loop.h264"
    if not os.path.exists(path):
        try:
            print("🎞️ Generating pre-baked 5 FPS H.264 loop via FFmpeg...", flush=True)
            subprocess.run([
                "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=size=320x180:rate=5", "-t", "6",
                "-threads", "1", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-profile:v", "baseline", "-level", "3.1", "-pix_fmt", "yuv420p",
                "-b:v", "30k", "-maxrate", "40k", "-bufsize", "40k",
                "-g", "5", "-keyint_min", "5", "-sc_threshold", "0", "-bf", "0",
                "-x264-params", "slices=1:sliced-threads=0:repeat-headers=1", "-f", "h264", path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"⚠️ Could not generate H.264 file: {e}", flush=True)
            return []
    try:
        with open(path, "rb") as f:
            raw = f.read()
        frames = group_nals_into_frames(split_annexb_nals(raw))
        print(f"✅ Loaded {len(frames)} real FFmpeg H.264 frames ({len(raw)} bytes total)", flush=True)
        return frames
    except Exception as e:
        print(f"⚠️ Failed loading H.264 frames: {e}", flush=True)
        return []

H264_FRAMES: list[bytes] = []

# =====================================================================
# Async WebSocket (Text JSON + Binary DAVE + Explicit Close Code Errors)
# =====================================================================
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

    async def send_binary(self, opcode: int, data: bytes) -> None:
        await self._send_frame(0x2, bytes([opcode]) + data)

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

    async def recv_msg(self) -> tuple[int, object] | None:
        if not self.reader or self.closed:
            return None
        fragments = bytearray()
        message_opcode = None
        while True:
            first_two = await self.reader.readexactly(2)
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
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                reason = payload[2:].decode("utf-8", errors="ignore") if len(payload) > 2 else ""
                self.closed = True
                raise ConnectionResetError(f"WS Closed ({code}): {reason}")
            elif opcode == 0x9:
                await self._send_frame(0xA, payload)
                continue
            elif opcode == 0xA:
                continue
            elif opcode in (0x1, 0x2):
                message_opcode = opcode
                fragments.extend(payload)
            elif opcode == 0x0:
                fragments.extend(payload)

            if fin:
                if message_opcode == 0x1:
                    return 0x1, json.loads(fragments.decode("utf-8", errors="replace"))
                elif message_opcode == 0x2:
                    return 0x2, bytes(fragments)
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

# =====================================================================
# Real Voice / Stream RTC Session (DAVE v1 + H264 RTP + RTCP SR + PLI)
# =====================================================================
class RTCVoiceSession:
    def __init__(self, label: str, endpoint: str, server_id: str, channel_id: str, user_id: str, session_id: str, voice_token: str, rtc_connection_id: str, is_stream: bool = False, is_sender: bool = False):
        self.label = label
        self.endpoint = endpoint
        self.server_id = server_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.session_id = session_id
        self.voice_token = voice_token
        self.rtc_connection_id = rtc_connection_id
        self.is_stream = is_stream
        self.is_sender = is_sender

        self.ws: AsyncWebSocket | None = None
        self.udp_socket: socket.socket | None = None
        self.udp_ip: str = ""
        self.udp_port: int = 0

        self.ssrc: int = 1
        self.video_ssrc: int = 2
        self.rtx_ssrc: int = 3
        self.seq_ack: int = -1

        self.secret_key: bytes | None = None
        self.encryption_mode: str = ""
        self.aesgcm: AESGCM | None = None
        self.nacl_box: nacl.secret.Aead | None = None
        self.nonce_counter: int = 0

        self.dave_protocol_version: int = 0
        self.dave_session: davey.DaveSession | None = None

        self._task = None
        self._hb_task = None
        self._media_task = None
        self._logged_first_frame = False
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
            except Exception: pass
        if self.ws: await self.ws.close()
        if self._task: self._task.cancel()

    def _encrypt_packet(self, header: bytes, payload: bytes) -> bytes:
        self.nonce_counter = (self.nonce_counter + 1) & 0xFFFFFFFF
        nonce_4 = struct.pack(">I", self.nonce_counter)

        if self.encryption_mode == "aead_aes256_gcm_rtpsize" and self.aesgcm:
            nonce_12 = nonce_4 + (b"\x00" * 8)
            ct = self.aesgcm.encrypt(nonce_12, payload, header)
            return header + ct + nonce_4

        elif self.encryption_mode == "aead_xchacha20_poly1305_rtpsize" and self.nacl_box:
            nonce_24 = nonce_4 + (b"\x00" * 20)
            ct = self.nacl_box.encrypt(payload, aad=header, nonce=nonce_24).ciphertext
            return header + ct + nonce_4

        raise RuntimeError(f"Unsupported encryption mode: {self.encryption_mode}")

    def _send_udp(self, pkt: bytes):
        if not self.udp_socket or not self.udp_ip:
            return
        try:
            self.udp_socket.sendto(pkt, (self.udp_ip, self.udp_port))
        except (BlockingIOError, OSError):
            pass

    def _build_rtp_header(self, payload_type: int, seq: int, timestamp: int, ssrc: int, marker: bool = False) -> bytes:
        hdr = bytearray(12)
        hdr[0] = 0x80
        hdr[1] = (0x80 | payload_type) if marker else (payload_type & 0x7F)
        struct.pack_into(">H", hdr, 2, seq & 0xFFFF)
        struct.pack_into(">I", hdr, 4, timestamp & 0xFFFFFFFF)
        struct.pack_into(">I", hdr, 8, ssrc & 0xFFFFFFFF)
        return bytes(hdr)

    def _build_rtcp_sr(self, ssrc: int, rtp_ts: int, pkt_count: int, octet_count: int) -> bytes:
        now = time.time()
        ntp_sec = int(now) + 2208988800
        ntp_frac = int((now - int(now)) * (1 << 32))
        sr = bytearray(28)
        sr[0] = 0x80
        sr[1] = 200
        struct.pack_into(">H", sr, 2, 6)
        struct.pack_into(">I", sr, 4, ssrc & 0xFFFFFFFF)
        struct.pack_into(">I", sr, 8, ntp_sec & 0xFFFFFFFF)
        struct.pack_into(">I", sr, 12, ntp_frac & 0xFFFFFFFF)
        struct.pack_into(">I", sr, 16, rtp_ts & 0xFFFFFFFF)
        struct.pack_into(">I", sr, 20, pkt_count & 0xFFFFFFFF)
        struct.pack_into(">I", sr, 24, octet_count & 0xFFFFFFFF)
        return bytes(sr)

    def _drain_udp_and_check_feedback(self) -> bool:
        saw_feedback = False
        if not self.udp_socket:
            return False
        for _ in range(32):
            try:
                pkt = self.udp_socket.recv(4096)
                if len(pkt) >= 8:
                    pt = pkt[1]
                    if 205 <= pt <= 207:
                        saw_feedback = True
            except (BlockingIOError, OSError):
                break
        return saw_feedback

    async def _udp_media_loop(self):
        video_seq = 0
        video_ts = 0
        video_pkts_sent = 0
        video_octets_sent = 0

        audio_seq = 0
        audio_ts = 0
        audio_pkts_sent = 0
        audio_octets_sent = 0

        frame_idx = 0
        last_sr_time = 0.0
        last_audio_time = 0.0
        last_ka_time = 0.0

        try:
            while self.running and self.udp_socket and self.secret_key:
                now = time.monotonic()

                if self._drain_udp_and_check_feedback() and H264_FRAMES:
                    frame_idx = 0

                if self.is_sender and H264_FRAMES:
                    raw_frame = H264_FRAMES[frame_idx % len(H264_FRAMES)]
                    frame_idx += 1

                    can_send_video = True
                    frame_to_packetize = raw_frame
                    if self.dave_protocol_version > 0:
                        if self.dave_session and self.dave_session.can_encrypt:
                            try:
                                frame_to_packetize = self.dave_session.encrypt(
                                    davey.MediaType.video, davey.Codec.h264, raw_frame
                                )
                            except Exception as exc:
                                print(f"⚠️ [{self.label}] DAVE video encrypt err: {exc}", flush=True)
                                can_send_video = False
                        else:
                            can_send_video = False

                    if can_send_video:
                        nals = split_annexb_nals(frame_to_packetize)
                        total_nals = len(nals)
                        for n_idx, nal in enumerate(nals):
                            is_last_nal = (n_idx == total_nals - 1)
                            if len(nal) <= 1150:
                                hdr = self._build_rtp_header(101, video_seq, video_ts, self.video_ssrc, marker=is_last_nal)
                                video_seq = (video_seq + 1) & 0xFFFF
                                self._send_udp(self._encrypt_packet(hdr, nal))
                                video_pkts_sent += 1
                                video_octets_sent += len(nal)
                            else:
                                nal_hdr = nal[0]
                                fu_indicator = (nal_hdr & 0xE0) | 28
                                nal_type = nal_hdr & 0x1F
                                body = nal[1:]
                                chunk_size = 1140
                                chunks = [body[i:i + chunk_size] for i in range(0, len(body), chunk_size)]
                                for c_idx, chunk in enumerate(chunks):
                                    is_first = (c_idx == 0)
                                    is_last_chunk = (c_idx == len(chunks) - 1)
                                    fu_hdr = (0x80 if is_first else 0) | (0x40 if is_last_chunk else 0) | nal_type
                                    rtp_payload = bytes([fu_indicator, fu_hdr]) + chunk
                                    hdr = self._build_rtp_header(
                                        101, video_seq, video_ts, self.video_ssrc,
                                        marker=(is_last_nal and is_last_chunk)
                                    )
                                    video_seq = (video_seq + 1) & 0xFFFF
                                    self._send_udp(self._encrypt_packet(hdr, rtp_payload))
                                    video_pkts_sent += 1
                                    video_octets_sent += len(rtp_payload)
                        video_ts = (video_ts + 18000) & 0xFFFFFFFF

                        if not self._logged_first_frame:
                            self._logged_first_frame = True
                            print(f"🎬 [{self.label}] First real H.264 frame transmitted over UDP! (size={len(frame_to_packetize)}B, DAVE_v{self.dave_protocol_version})", flush=True)

                if now - last_audio_time >= 1.0:
                    last_audio_time = now
                    opus_silence = b"\xF8\xFF\xFE"
                    if self.dave_protocol_version > 0 and self.dave_session and self.dave_session.can_encrypt:
                        try:
                            opus_silence = self.dave_session.encrypt_opus(opus_silence)
                        except Exception:
                            pass
                    hdr = self._build_rtp_header(120, audio_seq, audio_ts, self.ssrc, marker=False)
                    audio_seq = (audio_seq + 1) & 0xFFFF
                    audio_ts = (audio_ts + 960) & 0xFFFFFFFF
                    self._send_udp(self._encrypt_packet(hdr, opus_silence))
                    audio_pkts_sent += 1
                    audio_octets_sent += len(opus_silence)

                if now - last_sr_time >= 1.0:
                    last_sr_time = now
                    if self.is_sender:
                        sr_vid = self._build_rtcp_sr(self.video_ssrc, video_ts, video_pkts_sent, video_octets_sent)
                        self._send_udp(self._encrypt_packet(sr_vid[:8], sr_vid[8:]))
                    sr_aud = self._build_rtcp_sr(self.ssrc, audio_ts, audio_pkts_sent, audio_octets_sent)
                    self._send_udp(self._encrypt_packet(sr_aud[:8], sr_aud[8:]))

                if now - last_ka_time >= 5.0:
                    last_ka_time = now
                    self._send_udp(struct.pack("<Q", int(time.time() * 1000)))

                await asyncio.sleep(0.2 if self.is_sender else 1.0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"⚠️ [{self.label}] Media loop error: {e}", flush=True)

    async def _send_dave_key_package(self):
        if self.ws and not self.ws.closed and self.dave_session:
            kp = self.dave_session.get_serialized_key_package()
            await self.ws.send_binary(26, kp)

    async def _handle_dave_binary(self, payload: bytes):
        if len(payload) < 3:
            return
        seq = struct.unpack_from(">H", payload, 0)[0]
        self.seq_ack = seq
        v_op = payload[2]
        body = payload[3:]

        if v_op == 25:
            if not self.dave_session:
                self.dave_session = davey.DaveSession(
                    max(1, self.dave_protocol_version), int(self.user_id), int(self.channel_id)
                )
            self.dave_session.set_external_sender(body)

        elif v_op == 27:
            if not self.dave_session or len(body) < 1:
                return
            op_type = body[0]
            proposals = body[1:]
            op_enum = davey.ProposalsOperationType.append if op_type == 0 else davey.ProposalsOperationType.revoke
            cw = self.dave_session.process_proposals(op_enum, proposals)
            if isinstance(cw, davey.CommitWelcome):
                out = cw.commit + (cw.welcome if cw.welcome else b"")
                await self.ws.send_binary(28, out)

        elif v_op == 29:
            if not self.dave_session or len(body) < 2:
                return
            tid = struct.unpack_from(">H", body, 0)[0]
            commit = body[2:]
            try:
                should_prep = self.dave_session.process_commit(commit)
                if should_prep:
                    self.dave_session.prepare_transition(tid, self.dave_session.protocol_version)
                    if tid == 0:
                        self.dave_session.execute_transition(0)
                    else:
                        await self.ws.send_json({"op": 23, "d": {"transition_id": tid}})
            except Exception as exc:
                print(f"⚠️ [{self.label}] DAVE Commit failed (tid={tid}): {exc}", flush=True)
                await self.ws.send_json({"op": 31, "d": {"transition_id": tid}})
                self.dave_session.reset()
                await self._send_dave_key_package()

        elif v_op == 30:
            if not self.dave_session or len(body) < 2:
                return
            tid = struct.unpack_from(">H", body, 0)[0]
            welcome = body[2:]
            try:
                self.dave_session.process_welcome(welcome)
                self.dave_session.prepare_transition(tid, self.dave_session.protocol_version)
                if tid == 0:
                    self.dave_session.execute_transition(0)
                else:
                    await self.ws.send_json({"op": 23, "d": {"transition_id": tid}})
                print(f"🛡️ [{self.label}] DAVE E2EE Group Established (can_encrypt={self.dave_session.can_encrypt})", flush=True)
            except Exception as exc:
                print(f"⚠️ [{self.label}] DAVE Welcome failed (tid={tid}): {exc}", flush=True)
                await self.ws.send_json({"op": 31, "d": {"transition_id": tid}})
                self.dave_session.reset()
                await self._send_dave_key_package()

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
                    "server_id": str(self.server_id),
                    "channel_id": str(self.channel_id),
                    "user_id": str(self.user_id),
                    "session_id": self.session_id,
                    "token": self.voice_token,
                    "video": True,
                    "streams": [{"type": "screen" if self.is_stream else "video", "rid": "100", "quality": 100}],
                    "max_dave_protocol_version": 1
                }
            })

            while self.running:
                res = await self.ws.recv_msg()
                if not res:
                    break
                ws_op, data = res

                if ws_op == 0x2:
                    await self._handle_dave_binary(data)
                    continue

                msg = data
                if "seq" in msg and msg["seq"] is not None:
                    self.seq_ack = msg["seq"]

                op = msg.get("op")
                d = msg.get("d") or {}

                if op == 8:
                    interval = d.get("heartbeat_interval", 15000) / 1000.0
                    if self._hb_task: self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._hb(interval))

                elif op == 2:
                    self.udp_ip = d.get("ip", "")
                    self.udp_port = int(d.get("port", 0))
                    self.ssrc = int(d.get("ssrc", 1))
                    streams = d.get("streams") or []
                    if streams:
                        self.video_ssrc = int(streams[0].get("ssrc", self.ssrc + 1))
                        self.rtx_ssrc = int(streams[0].get("rtx_ssrc", self.video_ssrc + 1))
                    else:
                        self.video_ssrc = self.ssrc + 1
                        self.rtx_ssrc = self.ssrc + 2

                    modes = d.get("modes") or ["aead_xchacha20_poly1305_rtpsize"]
                    if "aead_aes256_gcm_rtpsize" in modes:
                        chosen_mode = "aead_aes256_gcm_rtpsize"
                    elif "aead_xchacha20_poly1305_rtpsize" in modes:
                        chosen_mode = "aead_xchacha20_poly1305_rtpsize"
                    else:
                        chosen_mode = modes[0]

                    ext_ip, ext_port = "127.0.0.1", 1337
                    try:
                        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        self.udp_socket.setblocking(False)
                        disc_pkt = bytearray(74)
                        struct.pack_into(">H", disc_pkt, 0, 1)
                        struct.pack_into(">H", disc_pkt, 2, 70)
                        struct.pack_into(">I", disc_pkt, 4, self.ssrc)
                        self.udp_socket.sendto(disc_pkt, (self.udp_ip, self.udp_port))
                        loop = asyncio.get_running_loop()
                        resp, _ = await asyncio.wait_for(loop.sock_recv(self.udp_socket, 74), timeout=5.0)
                        ext_ip = resp[8:72].decode("utf-8").strip("\x00")
                        ext_port = struct.unpack_from(">H", resp, 72)[0]
                        print(f"🌐 [{self.label}] UDP Discovery OK: {ext_ip}:{ext_port}", flush=True)
                    except Exception as e:
                        print(f"⚠️ [{self.label}] UDP Discovery Failed: {e}", flush=True)

                    await self.ws.send_json({
                        "op": 1,
                        "d": {
                            "protocol": "udp",
                            "data": {"address": ext_ip, "port": ext_port, "mode": chosen_mode},
                            "codecs": [
                                {"name": "opus", "type": "audio", "priority": 1000, "payload_type": 120},
                                {"name": "H264", "type": "video", "priority": 1000, "payload_type": 101, "rtx_payload_type": 102, "encode": True, "decode": True}
                            ],
                            "rtc_connection_id": self.rtc_connection_id
                        }
                    })

                elif op == 4:
                    raw_key = d.get("secret_key") or []
                    self.encryption_mode = d.get("mode", "")
                    self.dave_protocol_version = int(d.get("dave_protocol_version", 0))
                    if len(raw_key) == 32:
                        self.secret_key = bytes(raw_key)
                        if self.encryption_mode == "aead_aes256_gcm_rtpsize":
                            self.aesgcm = AESGCM(self.secret_key)
                        elif self.encryption_mode == "aead_xchacha20_poly1305_rtpsize":
                            self.nacl_box = nacl.secret.Aead(self.secret_key)

                    print(f"🔒 [{self.label}] Ready! Cipher={self.encryption_mode} | VideoCodec={d.get('video_codec')} | DAVE_v{self.dave_protocol_version}", flush=True)

                    if self.dave_protocol_version > 0:
                        self.dave_session = davey.DaveSession(
                            self.dave_protocol_version, int(self.user_id), int(self.channel_id)
                        )
                        await self._send_dave_key_package()

                    await self.ws.send_json({
                        "op": 5,
                        "d": {"speaking": 2 if self.is_stream else 1, "delay": 0, "ssrc": self.ssrc}
                    })

                    await self.ws.send_json({
                        "op": 12,
                        "d": {
                            "audio_ssrc": self.ssrc,
                            "video_ssrc": self.video_ssrc,
                            "rtx_ssrc": self.rtx_ssrc,
                            "streams": [{
                                "type": "video",
                                "rid": "100",
                                "ssrc": self.video_ssrc,
                                "active": True if self.is_sender else False,
                                "quality": 100,
                                "rtx_ssrc": self.rtx_ssrc,
                                "max_bitrate": 500000,
                                "max_framerate": 5,
                                "max_resolution": {"type": "fixed", "width": 320, "height": 180}
                            }]
                        }
                    })

                    if not self._media_task:
                        self._media_task = asyncio.create_task(self._udp_media_loop())

                elif op == 21:
                    tid = int(d.get("transition_id", 0))
                    pver = int(d.get("protocol_version", 0))
                    if self.dave_session:
                        self.dave_session.prepare_transition(tid, pver)
                        if pver == 0:
                            self.dave_session.set_passthrough_mode(True, 120)
                    if tid == 0 and self.dave_session:
                        self.dave_session.execute_transition(0)
                    else:
                        await self.ws.send_json({"op": 23, "d": {"transition_id": tid}})

                elif op == 22:
                    tid = int(d.get("transition_id", 0))
                    if self.dave_session:
                        self.dave_session.execute_transition(tid)

                elif op == 24:
                    epoch = int(d.get("epoch", 0))
                    pver = int(d.get("protocol_version", 1))
                    self.dave_protocol_version = pver
                    if epoch == 1:
                        if self.dave_session:
                            self.dave_session.reinit(pver, int(self.user_id), int(self.channel_id))
                        else:
                            self.dave_session = davey.DaveSession(pver, int(self.user_id), int(self.channel_id))
                        await self._send_dave_key_package()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"❌ [{self.label}] RTCVoiceSession closed/error: {e}", flush=True)

    async def _hb(self, interval: float):
        try:
            while self.running and self.ws and not self.ws.closed:
                await self.ws.send_json({"op": 3, "d": {"t": int(time.time() * 1000), "seq_ack": self.seq_ack}})
                await asyncio.sleep(interval)
        except Exception:
            pass

# =====================================================================
# Managed Account & Main Gateway Logic
# =====================================================================
class ManagedAccount:
    def __init__(self, acc_id: str, token: str, role: str, guild_id: str, channel_id: str, owner_id: str = "global", scope: str = "community", username="Loading...", avatar_url=None, tier_num=0, raw_hours=None, total_hours=0.0, current_tier="No Badge (0h)", next_tier="Tier 1: Newcomer (1 hour)", remaining_hours=1.0, progress_percent=0.0, analytics_token="", installation_id="", was_running=False):
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
        self.installation_id = installation_id
        self.was_running = was_running

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
        self.voice_rtc_id = str(uuid.uuid4())
        self.stream_rtc_id = str(uuid.uuid4())
        self.client_session_id = str(uuid.uuid4())

        self.stream_meta: dict[str, tuple[str, str]] = {}
        self.watching_streams: set[str] = set()
        self.ws: AsyncWebSocket | None = None
        self.voice_rtc: RTCVoiceSession | None = None
        self.stream_rtcs: dict[str, RTCVoiceSession] = {}

        self.running = False
        self.task = None
        self.telemetry_task = None
        self.cooldown_active = False
        self._logged_badge_debug = False

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

    async def ensure_telemetry_credentials(self):
        headers = {
            "Authorization": self.token,
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties(),
            "X-Discord-Locale": "en-US"
        }
        if self.installation_id:
            headers["X-Installation-ID"] = self.installation_id

        async with aiohttp.ClientSession() as session:
            if not self.installation_id:
                for surface in (8, 2):
                    try:
                        async with session.get(f"https://discord.com/api/v9/apex/experiments?surface={surface}", headers=headers, timeout=8) as r_apex:
                            if r_apex.status == 200:
                                apex_data = await r_apex.json()
                                inst = apex_data.get("installation")
                                if inst:
                                    self.installation_id = inst
                                    headers["X-Installation-ID"] = inst
                                    print(f"🪪 [{self.username}] Obtained X-Installation-ID (surface={surface})", flush=True)
                                    break
                    except Exception:
                        pass

            if not self.analytics_token or not self.user_id:
                try:
                    async with session.get("https://discord.com/api/v9/users/@me?with_analytics_token=true", headers=headers, timeout=8) as r_me:
                        if r_me.status == 200:
                            me_data = await r_me.json()
                            if not self.user_id and me_data.get("id"):
                                self.user_id = str(me_data["id"])
                            tok = me_data.get("analytics_token") or ""
                            if tok and not self.analytics_token:
                                self.analytics_token = tok
                                print(f"🔑 [{self.username}] Fetched analytics_token from v9", flush=True)
                except Exception as e:
                    print(f"⚠️ [{self.username}] v9 @me lookup error: {e}", flush=True)

    async def _send_telemetry(self, events):
        if not self.is_connected:
            return
        if not self.analytics_token:
            await self.ensure_telemetry_credentials()
        if not self.analytics_token:
            self.telemetry_status = "No Analytics Token ⚠️"
            return

        payload_events = [{
            "type": evt,
            "properties": {
                "client_track_timestamp": int(time.time() * 1000),
                "client_heartbeat_session_id": self.client_session_id,
                "guild_id": self.guild_id,
                "channel_id": self.channel_id,
                "media_session_id": self.voice_session_id,
                "rtc_connection_id": self.stream_rtc_id,
                "duration": 60000,
                "duration_tracked_ms": 60000,
                "is_video_blank": False,
                "connection_type": "webrtc"
            }
        } for evt in events]
        payload = {"token": self.analytics_token, "events": payload_events}
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties()
        }
        if self.installation_id:
            headers["X-Installation-ID"] = self.installation_id
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://discord.com/api/v9/science", json=payload, headers=headers, timeout=8) as r:
                    if r.status in (200, 204):
                        self.telemetry_status = "Tracking Active 🟢"
                    else:
                        self.telemetry_status = f"Err: {r.status} 🔴"
                        print(f"⚠️ [{self.username}] /science returned HTTP {r.status}", flush=True)
        except Exception as e:
            self.telemetry_status = "Network Err 🟡"
            print(f"⚠️ [{self.username}] /science network error: {e}", flush=True)

    async def _telemetry_loop(self):
        try:
            while self.running and (not self.is_connected or not self.stream_key):
                await asyncio.sleep(1)
            if not self.running:
                return
            await self.ensure_telemetry_credentials()
            if self.role == "streamer":
                await self._send_telemetry(["media_session_joined", "video_stream_started"])
                print(f"📡 [Science] Telemetry status for {self.username}: {self.telemetry_status}", flush=True)
            while self.running:
                await asyncio.sleep(60)
                if self.role == "streamer" and self.is_connected and not self.cooldown_active:
                    await self._send_telemetry(["video_stream_heartbeat"])
        except asyncio.CancelledError:
            pass

    async def start(self):
        if self.running: return
        self.running = True
        self.was_running = True
        self.status = "Connecting... ⏳"
        self.telemetry_status = "Initializing..."
        self.voice_rtc_id = str(uuid.uuid4())
        self.stream_rtc_id = str(uuid.uuid4())
        self.client_session_id = str(uuid.uuid4())
        self.task = asyncio.create_task(self._loop())
        if self.role == "streamer":
            self.telemetry_task = asyncio.create_task(self._telemetry_loop())

    async def _stop_all_stream_rtcs(self):
        for rtc in list(self.stream_rtcs.values()):
            await rtc.stop()
        self.stream_rtcs.clear()
        self.watching_streams.clear()

    async def stop(self):
        self.running = False
        self.was_running = False
        if self.role == "streamer":
            await self._send_telemetry(["video_stream_ended"])
        if self.task: self.task.cancel()
        if self.telemetry_task: self.telemetry_task.cancel()
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
            self.voice_rtc = None
        await self._stop_all_stream_rtcs()
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
                        res = await self.ws.recv_msg()
                        if not res: break
                        ws_op, msg = res
                        if ws_op != 0x1: continue
                        op, t, d = msg.get("op"), msg.get("t"), msg.get("d") or {}
                        if op == 10:
                            interval = d.get("heartbeat_interval", 41250) / 1000.0
                            hb_t = asyncio.create_task(self._hb(interval))
                            ident_d = {
                                "token": self.token, "capabilities": 16381,
                                "properties": json.loads(base64.b64decode(make_super_properties())),
                                "presence": {"status": "online", "since": 0, "activities": [], "afk": False}
                            }
                            if self.installation_id:
                                ident_d["installation_id"] = self.installation_id
                            await self.ws.send_json({"op": 2, "d": ident_d})
                        elif op == 1:
                            await self.ws.send_json({"op": 1, "d": None})
                        elif op == 0:
                            await self._handle_dispatch(t, d)
                finally:
                    if hb_t: hb_t.cancel()
                    if self.voice_rtc:
                        await self.voice_rtc.stop()
                        self.voice_rtc = None
                    await self._stop_all_stream_rtcs()
                    if self.ws: await self.ws.close()
            except Exception as e:
                if not self.running: break
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
            self.username = user.get("username", "User")
            av = user.get("avatar")
            self.avatar_url = f"https://cdn.discordapp.com/avatars/{self.user_id}/{av}.png?size=128" if av else f"https://cdn.discordapp.com/embed/avatars/{int(self.user_id or 0) % 5}.png"
            self.voice_session_id = d.get("session_id")
            apex = d.get("apex_experiments") or {}
            if apex.get("installation"):
                self.installation_id = apex["installation"]
            self.is_connected = True
            self.connected_since = time.time()
            print(f"✅ [{self.username}] Gateway READY (uid={self.user_id})", flush=True)

            asyncio.create_task(manager.update_account_badge_from_discord(self))

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

        elif t == "VOICE_STATE_UPDATE":
            uid = str(d.get("user_id", ""))
            ch_id = str(d.get("channel_id") or "")
            is_streaming = bool(d.get("self_stream", False))

            if uid == str(self.user_id):
                self.voice_session_id = d.get("session_id") or self.voice_session_id
            elif self.role == "watcher" and self.is_connected and self.scope == "community":
                s_key = f"guild:{self.guild_id}:{self.channel_id}:{uid}"
                if ch_id == self.channel_id and is_streaming:
                    if s_key not in self.watching_streams:
                        asyncio.create_task(self._safe_delayed_watch(s_key))
                elif s_key in self.watching_streams:
                    self.watching_streams.discard(s_key)
                    old_rtc = self.stream_rtcs.pop(s_key, None)
                    if old_rtc:
                        await old_rtc.stop()
                    self.status = f"Watching {len(self.watching_streams)} Stream(s) 👀"

        elif t == "VOICE_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            v_token = d.get("token")
            srv_id = d.get("guild_id") or self.channel_id
            if endpoint and v_token and self.user_id and self.voice_session_id:
                if self.voice_rtc:
                    await self.voice_rtc.stop()
                self.voice_rtc = RTCVoiceSession(
                    f"Voice-{self.username}", endpoint, str(srv_id), str(self.channel_id),
                    str(self.user_id), self.voice_session_id, v_token, self.voice_rtc_id,
                    is_stream=False, is_sender=False
                )
                self.voice_rtc.start()

        elif t == "STREAM_CREATE":
            s_key = d.get("stream_key")
            if s_key:
                rtc_srv = str(d.get("rtc_server_id") or self.guild_id or self.channel_id)
                rtc_ch = str(d.get("rtc_channel_id") or self.channel_id)
                self.stream_meta[s_key] = (rtc_srv, rtc_ch)
                if self.role == "streamer":
                    self.stream_key = s_key
                    await self.ws.send_json({"op": 22, "d": {"stream_key": self.stream_key, "paused": False}})

        elif t == "STREAM_DELETE":
            s_key = d.get("stream_key")
            if self.role == "watcher" and s_key:
                self.watching_streams.discard(s_key)
                old_rtc = self.stream_rtcs.pop(s_key, None)
                if old_rtc:
                    await old_rtc.stop()
                self.status = f"Watching {len(self.watching_streams)} Stream(s) 👀"

        elif t == "STREAM_SERVER_UPDATE":
            endpoint = d.get("endpoint")
            s_token = d.get("token")
            s_key = d.get("stream_key") or self.stream_key
            if endpoint and s_token and self.user_id and self.voice_session_id and s_key:
                rtc_srv, rtc_ch = self.stream_meta.get(s_key, (str(self.guild_id or self.channel_id), str(self.channel_id)))
                old_rtc = self.stream_rtcs.pop(s_key, None)
                if old_rtc:
                    await old_rtc.stop()
                rtc = RTCVoiceSession(
                    f"Stream-{self.username}", endpoint, rtc_srv, rtc_ch,
                    str(self.user_id), self.voice_session_id, s_token,
                    self.stream_rtc_id if self.role == "streamer" else str(uuid.uuid4()),
                    is_stream=True, is_sender=(self.role == "streamer")
                )
                self.stream_rtcs[s_key] = rtc
                rtc.start()

    async def _safe_delayed_watch(self, target_stream_key: str):
        await asyncio.sleep(4)
        if self.running and self.is_connected and target_stream_key not in self.watching_streams:
            await self.watch_target(target_stream_key)

    async def watch_target(self, target_stream_key: str):
        if self.ws and not self.ws.closed:
            await self.ws.send_json({"op": 20, "d": {"stream_key": target_stream_key}})
            self.watching_streams.add(target_stream_key)
            self.status = f"Watching {len(self.watching_streams)} Stream(s) 👀🔥"

# =====================================================================
# Cloud Manager & HTTP Server
# =====================================================================
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
                            print(f"🎯 Guild: {self.guild_name} ({self.guild_id})", flush=True)
                            return
        except Exception as e:
            print(f"ℹ️ Invite lookup fallback ({e})", flush=True)
        self.guild_id = DEFAULT_GUILD_ID
        print(f"🎯 Using Fixed Guild ID: {self.guild_id}", flush=True)

    @staticmethod
    def _extract_badge_objects(payload, parent_key: str = "") -> list[dict]:
        found = []
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    item_copy = dict(item)
                    if parent_key and "_parent_key" not in item_copy:
                        item_copy["_parent_key"] = parent_key
                    found.append(item_copy)
                elif isinstance(item, list):
                    found.extend(CloudSystemManager._extract_badge_objects(item, parent_key))
        elif isinstance(payload, dict):
            # If this dict itself looks like a badge/progress entry, include it
            if any(k in payload for k in ("id", "badge_id", "tier", "level", "description", "progress", "current_value", "hours")):
                item_copy = dict(payload)
                if parent_key and "_parent_key" not in item_copy:
                    item_copy["_parent_key"] = parent_key
                found.append(item_copy)
            # Recurse into child dicts/lists to catch dict-keyed badge catalogs
            for k, v in payload.items():
                if isinstance(v, (dict, list)):
                    found.extend(CloudSystemManager._extract_badge_objects(v, str(k)))
        return found

    @staticmethod
    def _parse_streaming_badge(badge_objs: list[dict]) -> tuple[int, float | None]:
        matched_tier = 0
        raw_hrs = None
        valid_hour_targets = {1, 5, 20, 75, 150, 300, 500, 1000, 2000, 5000}

        for b in badge_objs:
            b_id = str(b.get("id") or b.get("badge_id") or b.get("key") or b.get("_parent_key") or "").lower()
            desc = str(b.get("description") or b.get("name") or b.get("title") or "")
            desc_l = desc.lower()

            is_streaming = (
                "stream" in b_id
                or "stream" in desc_l
                or "broadcast" in desc_l
                or "بث" in desc_l
                or any(name in b_id or name in desc_l for name in TIER_NAME_TO_NUM if name != "star")
                or ("star" in b_id and "hour" in desc_l)
            )
            if not is_streaming:
                continue

            for tk in ("tier", "level", "stage", "current_tier"):
                tv = b.get(tk)
                if isinstance(tv, (int, float)) and tv > 0:
                    matched_tier = max(matched_tier, int(tv))

            id_m = re.search(r"(?:tier|level|stage|streaming)_?(\d+)", b_id, re.I)
            if id_m:
                matched_tier = max(matched_tier, int(id_m.group(1)))

            desc_m = re.search(r"(?:tier|level|stage|المستوى)\s*(\d+)", desc_l, re.I)
            if desc_m:
                matched_tier = max(matched_tier, int(desc_m.group(1)))

            for name, t_num in TIER_NAME_TO_NUM.items():
                if name == "star" and "superstar" in (b_id + " " + desc_l):
                    continue
                if re.search(rf"\b{name}\b", b_id.replace("_", " ") + " " + desc_l):
                    matched_tier = max(matched_tier, t_num)

            # Extract explicit hours/seconds/minutes if Discord provides exact progress
            for hk in ("hours", "current_hours", "total_hours"):
                hv = b.get(hk)
                if isinstance(hv, (int, float)):
                    raw_hrs = float(hv)
            if raw_hrs is None:
                for sk in ("seconds", "total_seconds", "duration_seconds"):
                    sv = b.get(sk)
                    if isinstance(sv, (int, float)) and sv > 0:
                        raw_hrs = float(sv) / 3600.0
            if raw_hrs is None:
                for mk in ("minutes", "total_minutes"):
                    mv = b.get(mk)
                    if isinstance(mv, (int, float)) and mv > 0:
                        raw_hrs = float(mv) / 60.0
            if raw_hrs is None:
                prog = b.get("progress")
                if isinstance(prog, dict):
                    cur_v = None
                    tgt_v = None
                    for pk in ("current", "value", "current_value", "hours"):
                        if isinstance(prog.get(pk), (int, float)):
                            cur_v = float(prog[pk])
                            break
                    for gk in ("target", "goal", "max", "next_milestone"):
                        if isinstance(prog.get(gk), (int, float)):
                            tgt_v = float(prog[gk])
                            break
                    if cur_v is not None:
                        if tgt_v and int(tgt_v) in valid_hour_targets:
                            raw_hrs = cur_v
                        elif tgt_v and int(tgt_v / 3600) in valid_hour_targets:
                            raw_hrs = cur_v / 3600.0
                        elif tgt_v and int(tgt_v / 60) in valid_hour_targets:
                            raw_hrs = cur_v / 60.0
                elif isinstance(prog, (int, float)):
                    unit = str(b.get("unit") or "").lower()
                    tgt = b.get("target") or b.get("goal")
                    if "sec" in unit or (isinstance(tgt, (int, float)) and int(tgt / 3600) in valid_hour_targets):
                        raw_hrs = float(prog) / 3600.0
                    elif "min" in unit or (isinstance(tgt, (int, float)) and int(tgt / 60) in valid_hour_targets):
                        raw_hrs = float(prog) / 60.0
                    elif "hour" in unit or "hr" in unit or (isinstance(tgt, (int, float)) and int(tgt) in valid_hour_targets):
                        raw_hrs = float(prog)

            if raw_hrs is None:
                hrs_m = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|ساعة|ساعات)", desc_l)
                if hrs_m:
                    try:
                        raw_hrs = float(hrs_m.group(1))
                    except Exception:
                        pass

            if matched_tier == 0 and raw_hrs is not None:
                for t_num in sorted(STREAM_BADGE_MILESTONES.keys(), reverse=True):
                    if raw_hrs >= STREAM_BADGE_MILESTONES[t_num][0]:
                        matched_tier = t_num
                        break

        return min(matched_tier, 10), raw_hrs

    async def update_account_badge_from_discord(self, acc: ManagedAccount):
        if not acc.token:
            return
        await acc.ensure_telemetry_credentials()

        headers = {
            "Authorization": acc.token,
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties(),
            "X-Discord-Locale": "en-US"
        }
        if acc.installation_id:
            headers["X-Installation-ID"] = acc.installation_id

        all_badge_objs: list[dict] = []
        badges_status = None
        profile_status = None
        raw_badges_preview = ""

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get("https://discord.com/api/v9/users/%40me/badges", headers=headers, timeout=8) as r_b:
                    badges_status = r_b.status
                    if r_b.status == 200:
                        b_data = await r_b.json()
                        raw_badges_preview = json.dumps(b_data, ensure_ascii=False)[:400]
                        all_badge_objs.extend(self._extract_badge_objects(b_data))
            except Exception as e:
                badges_status = f"err:{e}"

            if acc.user_id:
                try:
                    p_url = f"https://discord.com/api/v9/users/{acc.user_id}/profile?with_mutual_guilds=false"
                    async with session.get(p_url, headers=headers, timeout=8) as r_p:
                        profile_status = r_p.status
                        if r_p.status == 200:
                            p_data = await r_p.json()
                            all_badge_objs.extend(self._extract_badge_objects(p_data.get("badges", [])))
                            if isinstance(p_data.get("user_profile"), dict):
                                all_badge_objs.extend(self._extract_badge_objects(p_data["user_profile"].get("badges", [])))
                except Exception as e:
                    profile_status = f"err:{e}"

        if acc.user_id and not acc._logged_badge_debug:
            acc._logged_badge_debug = True
            summary = [
                {k: b.get(k) for k in ("_parent_key", "id", "badge_id", "description", "name", "tier", "progress", "hours") if b.get(k) is not None}
                for b in all_badge_objs
            ]
            print(f"🔍 [Badge Inspect] @{acc.username} | /%40me/badges={badges_status} {raw_badges_preview} | /profile={profile_status} | parsed={json.dumps(summary, ensure_ascii=False)[:700]}", flush=True)

        matched_tier, raw_hrs = self._parse_streaming_badge(all_badge_objs)
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

        await self.save_account_to_db(acc)

    async def _badge_refresh_loop(self):
        try:
            while True:
                await asyncio.sleep(300)
                for acc in list(self.accounts.values()):
                    if acc.token:
                        await self.update_account_badge_from_discord(acc)
        except asyncio.CancelledError:
            pass

    async def verify_token_and_membership(self, token: str):
        headers = {
            "Authorization": token,
            "User-Agent": USER_AGENT,
            "X-Super-Properties": make_super_properties(),
            "X-Discord-Locale": "en-US"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v9/users/@me?with_analytics_token=true", headers=headers, timeout=8) as r1:
                if r1.status == 401: return False, "Invalid Discord Token.", None, False
                if r1.status != 200: return False, f"API Error ({r1.status})", None, False
                user_info = await r1.json()

            async with session.get("https://discord.com/api/v9/users/@me/guilds", headers=headers, timeout=8) as r2:
                if r2.status == 200:
                    user_guilds = await r2.json()
                    if str(self.guild_id) not in [str(g.get("id")) for g in user_guilds if "id" in g]:
                        return False, "You must join our server first!", user_info, True
                else:
                    return False, "Could not verify server membership.", user_info, False
        return True, "Valid", user_info, False

    async def init_mongo(self):
        if not MONGO_URI:
            print("ℹ️ MONGO_URI not set — running in-memory mode.", flush=True)
            return
        try:
            self.mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            await self.mongo_client.admin.command("ping")
            self.db = self.mongo_client["streamer_badge_db"]
            async for doc in self.db.accounts.find({}):
                acc_id = doc["acc_id"]
                acc = ManagedAccount(
                    acc_id=acc_id, token=doc["token"], role=doc["role"],
                    guild_id=self.guild_id, channel_id=DEFAULT_CHANNEL_ID,
                    owner_id=doc.get("owner_id", "global"), scope=doc.get("scope", "private"),
                    username=doc.get("username", "Saved Account"), avatar_url=doc.get("avatar_url"),
                    analytics_token=doc.get("analytics_token", ""),
                    installation_id=doc.get("installation_id", ""),
                    tier_num=doc.get("tier_num", 0),
                    raw_hours=doc.get("raw_hours"),
                    total_hours=doc.get("total_hours", 0.0),
                    current_tier=doc.get("current_tier", "No Badge (0h)"),
                    next_tier=doc.get("next_tier", "Tier 1: Newcomer (1 hour)"),
                    remaining_hours=doc.get("remaining_hours", 1.0),
                    progress_percent=doc.get("progress_percent", 0.0),
                    was_running=doc.get("was_running", False)
                )
                acc.user_id = doc.get("user_id")
                self.accounts[acc_id] = acc
            print(f"✅ MongoDB connected! Restored {len(self.accounts)} account(s).", flush=True)
        except Exception as e:
            print(f"❌ MongoDB Connection Failed: {e}", flush=True)
            self.db = None

    async def auto_resume_accounts(self):
        for acc in list(self.accounts.values()):
            asyncio.create_task(self.update_account_badge_from_discord(acc))

        to_resume = [a for a in self.accounts.values() if a.was_running]
        if not to_resume:
            return
        self.is_running = True
        print(f"🔄 Auto-resuming {len(to_resume)} previously running account(s)...", flush=True)
        for acc in to_resume:
            await acc.start()
            await asyncio.sleep(4)
        asyncio.create_task(self.balance_and_pair_streams())

    async def save_account_to_db(self, acc: ManagedAccount):
        if self.db is not None:
            try:
                await self.db.accounts.update_one({"acc_id": acc.acc_id}, {"$set": {
                    "acc_id": acc.acc_id, "user_id": acc.user_id, "token": acc.token, "role": acc.role,
                    "guild_id": acc.guild_id, "channel_id": acc.channel_id, "owner_id": acc.owner_id,
                    "scope": acc.scope, "username": acc.username, "avatar_url": acc.avatar_url,
                    "analytics_token": acc.analytics_token, "installation_id": acc.installation_id,
                    "tier_num": acc.tier_num, "raw_hours": acc.raw_hours,
                    "total_hours": acc.total_hours, "current_tier": acc.current_tier,
                    "next_tier": acc.next_tier, "remaining_hours": acc.remaining_hours,
                    "progress_percent": acc.progress_percent, "was_running": acc.was_running
                }}, upsert=True)
            except Exception:
                pass

    async def remove_account_from_db(self, acc_id: str):
        if self.db is not None:
            try: await self.db.accounts.delete_one({"acc_id": acc_id})
            except Exception: pass

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
                await self.save_account_to_db(acc)
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
        except asyncio.CancelledError:
            pass

    async def _three_hour_cooldown_loop(self):
        try:
            while True:
                await asyncio.sleep(3 * 3600)
                if not any(a.is_connected for a in self.accounts.values()): continue
                for acc in self.accounts.values():
                    if acc.is_connected and acc.ws and not acc.ws.closed:
                        acc.cooldown_active = True
                        acc.status = "Cooldown Rest (3 mins) ☕"
                        await acc._stop_all_stream_rtcs()
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
        except asyncio.CancelledError:
            pass

    async def _auto_save_loop(self):
        try:
            while True:
                await asyncio.sleep(60)
                for acc in self.accounts.values():
                    if acc.running: await self.save_account_to_db(acc)
        except asyncio.CancelledError:
            pass

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
    manager.ensure_background_tasks()
    await acc.start()
    await manager.save_account_to_db(acc)
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
        await manager.save_account_to_db(acc)
        return web.json_response({"success": True, "running": False})
    else:
        manager.ensure_background_tasks()
        await acc.start()
        await manager.save_account_to_db(acc)
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
    global H264_FRAMES
    H264_FRAMES = load_video_frames()
    await manager.resolve_guild()
    await manager.init_mongo()
    manager.ensure_background_tasks()
    asyncio.create_task(manager.auto_resume_accounts())
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    print(f"🌐 Live on {PORT}", flush=True)
    await site.start()
    while True: await asyncio.sleep(3600)

if __name__ == "__main__": asyncio.run(main())
