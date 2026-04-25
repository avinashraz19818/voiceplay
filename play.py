"""
Voice Chat Audio Player Bot — Simple single user
"""

import asyncio
import json
import logging
import os
import re
import random
import subprocess
import sys
import threading
import time
from typing import Dict, Optional

import ntgcalls
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait
from pyrogram.raw import functions, types as raw_types
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler,
    CommandHandler, ContextTypes, MessageHandler, filters,
)

from config import TOKEN, APIID, APIHASH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()],
)
log = logging.getLogger("VCBot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

SESSION_DIR      = "sessions"
DATA_FILE        = "bot_data.json"
AUDIO_DIR        = "audio_files"
MONITOR_INTERVAL = 15

# ── Playlist / PCM constants ───────────────────────────────────────────────────
SAMPLE_RATE  = 48000
CHANNELS     = 2
BYTES_SEC    = SAMPLE_RATE * CHANNELS * 2   # s16le = 2 bytes/sample
PCM_CHUNK    = BYTES_SEC                    # 1-second chunks

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR,   exist_ok=True)

# ── Playlist streaming (embedded, no external file needed) ─────────────────────
def _pcm_stream_file(path: str, out):
    proc = subprocess.Popen(
        ["ffmpeg", "-i", path, "-f", "s16le", "-acodec", "pcm_s16le",
         "-ar", str(SAMPLE_RATE), "-ac", str(CHANNELS), "pipe:1", "-loglevel", "quiet"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    try:
        while True:
            chunk = proc.stdout.read(PCM_CHUNK)
            if not chunk:
                break
            out.write(chunk)
            out.flush()
    finally:
        try: proc.kill()
        except Exception: pass
        proc.wait()

def _pcm_silence(seconds: float, out):
    total = int(BYTES_SEC * seconds)
    silence = bytes(PCM_CHUNK)
    written = 0
    while written < total:
        n = min(PCM_CHUNK, total - written)
        out.write(silence[:n])
        out.flush()
        written += n

def playlist_loop(files: list, delay: float, break_t: float, out):
    while True:
        valid = [f for f in files if os.path.isfile(f)]
        if not valid:
            _pcm_silence(5, out)
            continue
        for i, f in enumerate(valid):
            _pcm_stream_file(f, out)
            if i < len(valid) - 1 and delay > 0:
                _pcm_silence(delay, out)
        if break_t > 0:
            _pcm_silence(break_t, out)

def build_combined_pcm(files: list, delay_sec: float, break_sec: float, out_path: str):
    """Pre-render full playlist + delays + break into one regular PCM file on disk."""
    valid = [f for f in files if os.path.isfile(f)]
    with open(out_path, "wb") as out:
        for i, f in enumerate(valid):
            proc = subprocess.Popen(
                ["ffmpeg", "-i", f, "-f", "s16le", "-acodec", "pcm_s16le",
                 "-ar", str(SAMPLE_RATE), "-ac", str(CHANNELS), "pipe:1", "-loglevel", "quiet"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            )
            try:
                while True:
                    chunk = proc.stdout.read(PCM_CHUNK)
                    if not chunk: break
                    out.write(chunk)
            finally:
                try: proc.kill()
                except Exception: pass
                proc.wait()
            if i < len(valid) - 1 and delay_sec > 0:
                out.write(bytes(int(BYTES_SEC * delay_sec)))
        if break_sec > 0:
            out.write(bytes(int(BYTES_SEC * break_sec)))

S_PHONE = 1; S_CODE = 2; S_PASS = 3
S_ADD_AUDIO = 4; S_SET_DELAY = 5; S_SET_BREAK = 6; S_ADD_CHANNEL = 7

H = "HTML"

def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"phone": None, "channels": [], "playlist": [], "delay": 30, "break_time": 300}

def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Account",  callback_data="m:account"),
         InlineKeyboardButton("📢 Channels", callback_data="m:channels")],
        [InlineKeyboardButton("🎵 Playlist", callback_data="m:playlist"),
         InlineKeyboardButton("⚙️ Settings", callback_data="m:settings")],
        [InlineKeyboardButton("📊 Status",   callback_data="m:status")],
    ])

def kb_account(logged_in):
    btns = []
    if logged_in:
        btns.append([InlineKeyboardButton("🚪 Logout", callback_data="a:logout")])
    else:
        btns.append([InlineKeyboardButton("🔑 Login",  callback_data="a:login")])
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="m:main")])
    return InlineKeyboardMarkup(btns)

def kb_channels(channels):
    btns = []
    for i, ch in enumerate(channels):
        name = ch.replace("https://t.me/", "@")
        btns.append([InlineKeyboardButton(f"❌ {name}", callback_data=f"ch:remove:{i}")])
    btns.append([InlineKeyboardButton("➕ Add Channel", callback_data="ch:add")])
    btns.append([InlineKeyboardButton("🔙 Back",        callback_data="m:main")])
    return InlineKeyboardMarkup(btns)

def kb_playlist(playlist):
    btns = []
    for i, item in enumerate(playlist):
        name = item.get("name", f"Track {i+1}")
        btns.append([InlineKeyboardButton(f"❌ {i+1}. {name}", callback_data=f"pl:remove:{i}")])
    btns.append([InlineKeyboardButton("➕ Add Audio", callback_data="pl:add"),
                 InlineKeyboardButton("🗑 Clear All",  callback_data="pl:clear")])
    btns.append([InlineKeyboardButton("⏱ Set Delay",  callback_data="pl:delay"),
                 InlineKeyboardButton("☕ Set Break",  callback_data="pl:break")])
    btns.append([InlineKeyboardButton("🔙 Back", callback_data="m:main")])
    return InlineKeyboardMarkup(btns)

class VCSession:
    def __init__(self):
        self.pyro = None; self.nt = None
        self.active_chat_id = None; self.active_call = None; self.active_link = None
        self.monitor_tasks: Dict[str, asyncio.Task] = {}
        self.keepalive_task = None
        self.combined_pcm = None
        self.loop = None

    def _on_stream_end(self, *args):
        try:
            chat_id = args[0] if args else self.active_chat_id
            if chat_id != self.active_chat_id: return
            if not self.combined_pcm or not os.path.isfile(self.combined_pcm): return
            if not self.nt or not self.loop: return
            log.info(f"Stream ended in {chat_id} — looping playlist")
            media = ntgcalls.MediaDescription(
                microphone=ntgcalls.AudioDescription(
                    ntgcalls.MediaSource.FILE, SAMPLE_RATE, CHANNELS, self.combined_pcm
                )
            )
            asyncio.run_coroutine_threadsafe(
                self.nt.set_stream_sources(chat_id, ntgcalls.StreamMode.CAPTURE, media),
                self.loop,
            )
        except Exception as e:
            log.warning(f"_on_stream_end: {e}")

    def get_nt(self):
        if self.nt is None:
            self.nt = ntgcalls.NTgCalls()
            self.nt.on_connection_change(lambda cid, info: log.info(f"NTG {cid}: {getattr(info,'state',info)}"))
            try: self.nt.on_stream_end(self._on_stream_end)
            except Exception as e: log.warning(f"on_stream_end not available: {e}")
        return self.nt

    async def safe_name(self):
        try:
            if self.pyro and self.pyro.me:
                return self.pyro.me.first_name or "?"
        except Exception:
            pass
        return "?"

    def is_in_vc(self):
        return self.active_chat_id is not None

sess = VCSession()

def make_pyro(phone):
    return Client(phone, api_id=APIID, api_hash=APIHASH, workdir=SESSION_DIR)

async def check_vc(client, chat_link):
    try:
        m = re.search(r"t\.me/(\w+)", chat_link)
        if not m: return None, None
        chat = await client.get_chat(m.group(1))
        if chat.type not in (ChatType.SUPERGROUP, ChatType.CHANNEL, ChatType.GROUP):
            return None, None
        if chat.type in (ChatType.SUPERGROUP, ChatType.CHANNEL):
            full = await client.invoke(functions.channels.GetFullChannel(channel=await client.resolve_peer(chat.id)))
        else:
            full = await client.invoke(functions.messages.GetFullChat(chat_id=chat.id))
        call_inp = getattr(full.full_chat, "call", None)
        if not call_inp: return chat, None
        try:
            res = await client.invoke(functions.phone.GetGroupCall(call=call_inp, limit=0))
            if getattr(res.call, "rtmp_stream", False):
                log.info(f"RTMP in {chat_link} — skipping")
                return chat, None
        except Exception:
            pass
        return chat, call_inp
    except ValueError as e:
        if "Peer id invalid" not in str(e): log.warning(f"check_vc: {e}")
        return None, None
    except Exception as e:
        log.warning(f"check_vc [{chat_link}]: {e}")
        return None, None

def extract_transport(updates):
    try:
        for upd in getattr(updates, "updates", []):
            if hasattr(upd, "params") and hasattr(upd.params, "data"):
                return upd.params.data
        if hasattr(updates, "params") and hasattr(updates.params, "data"):
            return updates.params.data
    except Exception:
        pass
    return None

async def vc_join(chat, call_inp, data):
    files = [p["path"] for p in data.get("playlist", []) if os.path.isfile(p["path"])]
    if not files:
        log.warning("No audio files — skipping join")
        return False
    nt = sess.get_nt()
    sess.loop = asyncio.get_event_loop()
    chat_id = chat.id
    delay = data.get("delay", 30)
    break_t = data.get("break_time", 300)
    try:
        # Pre-build full playlist as a single PCM file on disk (regular file works reliably)
        combined_pcm = f"/tmp/vcbot_playlist_{abs(chat_id)}.pcm"
        log.info(f"Building combined PCM ({len(files)} track(s), delay={delay}s, break={break_t}s)...")
        await sess.loop.run_in_executor(
            None, build_combined_pcm, files, delay, break_t, combined_pcm
        )
        size_mb = os.path.getsize(combined_pcm) / (1024 * 1024)
        dur_s = os.path.getsize(combined_pcm) / BYTES_SEC
        log.info(f"PCM ready: {size_mb:.1f} MB, ~{dur_s:.0f}s")

        params_json = await nt.create_call(chat_id)
        result = None
        for _ in range(3):
            try:
                result = await sess.pyro.invoke(
                    functions.phone.JoinGroupCall(
                        call=call_inp, join_as=raw_types.InputPeerSelf(),
                        muted=False, video_stopped=True,
                        params=raw_types.DataJSON(data=params_json),
                    )
                )
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
        if result is None: return False
        transport = extract_transport(result)
        if not transport:
            log.error("No transport received")
            return False
        await nt.connect(chat_id, transport, False)

        sess.combined_pcm = combined_pcm
        media = ntgcalls.MediaDescription(
            microphone=ntgcalls.AudioDescription(
                ntgcalls.MediaSource.FILE, SAMPLE_RATE, CHANNELS, combined_pcm
            )
        )
        await nt.set_stream_sources(chat_id, ntgcalls.StreamMode.CAPTURE, media)
        log.info("Audio: streaming PCM file (will loop on stream end)")

        await nt.unmute(chat_id)
        sess.active_chat_id = chat_id
        sess.active_call = call_inp
        log.info(f"Joined VC in {chat.title}!")
        return True
    except Exception as e:
        log.error(f"vc_join error: {e}")
        try: await nt.stop(chat_id)
        except Exception: pass
        return False

async def vc_leave():
    if sess.active_chat_id is None: return
    chat_id = sess.active_chat_id
    if sess.nt:
        try: await sess.nt.stop(chat_id)
        except Exception: pass
    if sess.pyro and sess.active_call:
        try: await sess.pyro.invoke(functions.phone.LeaveGroupCall(call=sess.active_call))
        except Exception: pass
    if sess.combined_pcm and os.path.exists(sess.combined_pcm):
        try: os.remove(sess.combined_pcm)
        except Exception: pass
    sess.combined_pcm = None
    sess.active_chat_id = None; sess.active_call = None; sess.active_link = None
    log.info("Left VC")

async def keepalive_loop():
    while True:
        await asyncio.sleep(20)
        if not sess.pyro: continue
        try:
            if not sess.pyro.is_connected: await sess.pyro.start()
            else: await sess.pyro.invoke(functions.Ping(ping_id=random.randint(1, 999999)))
        except Exception: pass

def start_keepalive():
    if sess.keepalive_task and not sess.keepalive_task.done(): return
    sess.keepalive_task = asyncio.create_task(keepalive_loop())

async def monitor_channel(chat_link):
    log.info(f"Monitoring: {chat_link}")
    was_active = False
    while True:
        try:
            if not sess.pyro or not sess.pyro.is_connected:
                await asyncio.sleep(MONITOR_INTERVAL); continue
            data = load_data()
            chat, call = await check_vc(sess.pyro, chat_link)
            if chat and call and not was_active:
                log.info(f"Live detected: {chat_link}")
                ok = await vc_join(chat, call, data)
                if ok: sess.active_link = chat_link; was_active = True
            elif not call and was_active:
                log.info(f"Live ended: {chat_link}")
                await vc_leave(); was_active = False
        except asyncio.CancelledError:
            await vc_leave(); return
        except Exception as e:
            log.warning(f"Monitor [{chat_link}]: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)

def start_monitors(channels):
    for link in channels:
        if link not in sess.monitor_tasks or sess.monitor_tasks[link].done():
            sess.monitor_tasks[link] = asyncio.create_task(monitor_channel(link))

def stop_all_monitors():
    for t in sess.monitor_tasks.values(): t.cancel()
    sess.monitor_tasks.clear()

async def start_pyro_session(phone):
    sf = os.path.join(SESSION_DIR, f"{phone}.session")
    if not os.path.exists(sf): return False
    c = make_pyro(phone)
    try:
        await c.start()
        if c.me is None: await c.get_me()
        sess.pyro = c; sess.get_nt(); start_keepalive()
        start_monitors(load_data().get("channels", []))
        log.info(f"Pyrogram ready: {phone}")
        return True
    except Exception as e:
        log.warning(f"Pyrogram start failed: {e}")
        return False

def main_text(data):
    phone = data.get("phone") or "Not logged in"
    in_vc = "🟢 <b>In Live</b>" if sess.is_in_vc() else "🔴 Idle"
    return (
        "<blockquote>🎵 <b>VOICE CHAT BOT</b></blockquote>\n\n"
        f"👤 Account : <code>{phone}</code>\n"
        f"📡 Status  : {in_vc}"
    )

def playlist_text(data):
    pl = data.get("playlist", [])
    delay = data.get("delay", 30); break_ = data.get("break_time", 300)
    lines = "\n".join(f"  <b>{i+1}.</b> {p['name']}" for i, p in enumerate(pl)) if pl else "<i>No audio yet. Use ➕ Add Audio.</i>"
    return (
        "<blockquote>🎵 <b>PLAYLIST</b></blockquote>\n\n"
        f"{lines}\n\n"
        f"⏱ Delay between tracks : <b>{delay}s</b>\n"
        f"☕ Break after full loop : <b>{break_}s</b>"
    )

def channels_text(data):
    chs = data.get("channels", [])
    if not chs:
        return "<blockquote>📢 <b>CHANNELS</b></blockquote>\n\n<i>No channels yet. Use ➕ Add Channel.</i>"
    lines = "\n".join(f"  • <code>{c}</code>" for c in chs)
    return f"<blockquote>📢 <b>CHANNELS</b></blockquote>\n\n{lines}"

def status_text(data):
    phone = data.get("phone") or "—"
    link = sess.active_link or "—"
    pl = data.get("playlist", []); chs = data.get("channels", [])
    return (
        "<blockquote>📊 <b>STATUS</b></blockquote>\n\n"
        f"👤 Account  : <code>{phone}</code>\n"
        f"🎙 In VC    : {'<b>Yes</b> — ' + link if sess.is_in_vc() else 'No'}\n"
        f"🎵 Playlist : <b>{len(pl)}</b> track(s)\n"
        f"📢 Channels : <b>{len(chs)}</b> monitored"
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(main_text(load_data()), parse_mode=H, reply_markup=kb_main())

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; cb = q.data
    await q.answer()
    data = load_data()

    if cb == "m:main":
        await q.edit_message_text(main_text(data), parse_mode=H, reply_markup=kb_main())
    elif cb == "m:account":
        logged = bool(data.get("phone")) and sess.pyro is not None
        txt = ("<blockquote>👤 <b>ACCOUNT</b></blockquote>\n\n" +
               (f"✅ Logged in as <code>{data.get('phone','')}</code>" if logged
                else "❌ Not logged in.\n\nClick <b>Login</b> to connect."))
        await q.edit_message_text(txt, parse_mode=H, reply_markup=kb_account(logged))
    elif cb == "m:channels":
        await q.edit_message_text(channels_text(data), parse_mode=H, reply_markup=kb_channels(data.get("channels", [])))
    elif cb == "m:playlist":
        await q.edit_message_text(playlist_text(data), parse_mode=H, reply_markup=kb_playlist(data.get("playlist", [])))
    elif cb == "m:settings":
        await q.edit_message_text(
            f"<blockquote>⚙️ <b>SETTINGS</b></blockquote>\n\n"
            f"⏱ Delay : <b>{data.get('delay',30)}s</b>\n"
            f"☕ Break : <b>{data.get('break_time',300)}s</b>\n\n"
            "<i>Change from Playlist menu.</i>",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="m:main")]]))
    elif cb == "m:status":
        try:
            await q.edit_message_text(status_text(data), parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Refresh", callback_data="m:status"),
                    InlineKeyboardButton("🔙 Back", callback_data="m:main"),
                ]]))
        except Exception:
            pass
    elif cb == "a:login":
        await q.edit_message_text(
            "<blockquote>🔑 <b>LOGIN</b></blockquote>\n\nSend your phone number:\n<i>Example: +919876543210</i>",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="a:cancel")]]))
        ctx.user_data["state"] = S_PHONE
    elif cb == "a:logout":
        phone = data.get("phone")
        if phone:
            stop_all_monitors()
            if sess.is_in_vc(): await vc_leave()
            if sess.keepalive_task: sess.keepalive_task.cancel()
            if sess.pyro:
                try: await sess.pyro.stop()
                except Exception: pass
                sess.pyro = None
            data["phone"] = None; save_data(data)
        await q.edit_message_text("✅ <b>Logged out.</b>", parse_mode=H, reply_markup=kb_account(False))
    elif cb == "a:cancel":
        ctx.user_data.pop("state", None)
        await q.edit_message_text(main_text(load_data()), parse_mode=H, reply_markup=kb_main())
    elif cb == "ch:add":
        ctx.user_data["state"] = S_ADD_CHANNEL
        await q.edit_message_text(
            "<blockquote>📢 <b>ADD CHANNEL</b></blockquote>\n\nSend the link:\n<i>https://t.me/channelname</i>",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m:channels")]]))
    elif cb.startswith("ch:remove:"):
        idx = int(cb.split(":")[2]); chs = data.get("channels", [])
        if 0 <= idx < len(chs):
            removed = chs.pop(idx); data["channels"] = chs; save_data(data)
            t = sess.monitor_tasks.pop(removed, None)
            if t: t.cancel()
        data = load_data()
        await q.edit_message_text(channels_text(data), parse_mode=H, reply_markup=kb_channels(data.get("channels", [])))
    elif cb == "pl:add":
        ctx.user_data["state"] = S_ADD_AUDIO
        await q.edit_message_text(
            "<blockquote>🎵 <b>ADD AUDIO</b></blockquote>\n\nUpload MP3 / audio / voice file:",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m:playlist")]]))
    elif cb == "pl:clear":
        data["playlist"] = []; save_data(data)
        await q.edit_message_text(playlist_text(data), parse_mode=H, reply_markup=kb_playlist([]))
    elif cb.startswith("pl:remove:"):
        idx = int(cb.split(":")[2]); pl = data.get("playlist", [])
        if 0 <= idx < len(pl): pl.pop(idx); data["playlist"] = pl; save_data(data)
        data = load_data()
        await q.edit_message_text(playlist_text(data), parse_mode=H, reply_markup=kb_playlist(data.get("playlist", [])))
    elif cb == "pl:delay":
        ctx.user_data["state"] = S_SET_DELAY
        await q.edit_message_text(
            f"<blockquote>⏱ <b>SET DELAY</b></blockquote>\n\nCurrent: <b>{data.get('delay',30)}s</b>\n\nSend seconds:",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m:playlist")]]))
    elif cb == "pl:break":
        ctx.user_data["state"] = S_SET_BREAK
        await q.edit_message_text(
            f"<blockquote>☕ <b>SET BREAK</b></blockquote>\n\nCurrent: <b>{data.get('break_time',300)}s</b>\n\nSend seconds:",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="m:playlist")]]))

async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state = ctx.user_data.get("state")
    msg = update.message; data = load_data()

    if state == S_PHONE:
        phone = msg.text.strip(); c = make_pyro(phone)
        await c.connect()
        try:
            sent = await c.send_code(phone)
            ctx.user_data.update({"tmp_phone": phone, "tmp_client": c, "tmp_hash": sent.phone_code_hash, "state": S_CODE})
            await msg.reply_text("🔢 <b>OTP sent!</b> Enter the code:", parse_mode=H)
        except Exception as e:
            await c.disconnect(); ctx.user_data.pop("state", None)
            await msg.reply_text(f"❌ Error: <code>{e}</code>", parse_mode=H)
    elif state == S_CODE:
        c = ctx.user_data.get("tmp_client")
        if not c: ctx.user_data.pop("state", None); return
        try:
            await c.sign_in(ctx.user_data["tmp_phone"], ctx.user_data["tmp_hash"], msg.text.strip())
            await finalize_login(msg, ctx, c)
        except Exception as e:
            if any(k in str(e).lower() for k in ("2fa", "password", "session_password")):
                ctx.user_data["state"] = S_PASS
                await msg.reply_text("🔑 Enter your <b>2FA password</b>:", parse_mode=H)
            else:
                await c.disconnect(); ctx.user_data.pop("state", None)
                await msg.reply_text(f"❌ Login failed: <code>{e}</code>", parse_mode=H)
    elif state == S_PASS:
        c = ctx.user_data.get("tmp_client")
        if not c: ctx.user_data.pop("state", None); return
        try:
            await c.check_password(msg.text.strip()); await finalize_login(msg, ctx, c)
        except Exception as e:
            await c.disconnect(); ctx.user_data.pop("state", None)
            await msg.reply_text(f"❌ Password error: <code>{e}</code>", parse_mode=H)
    elif state == S_ADD_CHANNEL:
        link = msg.text.strip()
        if not link.startswith("https://t.me/"):
            await msg.reply_text("❌ Must start with <code>https://t.me/</code>", parse_mode=H); return
        chs = data.setdefault("channels", [])
        if link not in chs:
            chs.append(link); save_data(data)
            if sess.pyro: start_monitors([link])
        ctx.user_data.pop("state", None)
        await msg.reply_text(f"✅ Monitoring:\n<code>{link}</code>", parse_mode=H,
                             reply_markup=kb_channels(data.get("channels", [])))
    elif state == S_ADD_AUDIO:
        doc = msg.document or msg.audio or msg.voice
        if doc:
            fname = getattr(doc, "file_name", None) or f"audio_{int(time.time())}.mp3"
            spath = os.path.join(AUDIO_DIR, fname)
            try:
                tf = await ctx.bot.get_file(doc.file_id); await tf.download_to_drive(spath)
                data.setdefault("playlist", []).append({"path": spath, "name": fname}); save_data(data)
                ctx.user_data.pop("state", None)
                await msg.reply_text(f"✅ <b>Added:</b> <code>{fname}</code>", parse_mode=H,
                                     reply_markup=kb_playlist(data.get("playlist", [])))
            except Exception as e:
                await msg.reply_text(f"❌ Upload failed: <code>{e}</code>", parse_mode=H)
        elif msg.text:
            path = msg.text.strip()
            if not os.path.isfile(path):
                await msg.reply_text(f"❌ Not found: <code>{path}</code>", parse_mode=H); return
            name = os.path.basename(path)
            data.setdefault("playlist", []).append({"path": path, "name": name}); save_data(data)
            ctx.user_data.pop("state", None)
            await msg.reply_text(f"✅ <b>Added:</b> <code>{name}</code>", parse_mode=H,
                                 reply_markup=kb_playlist(data.get("playlist", [])))
    elif state == S_SET_DELAY:
        try:
            val = int(msg.text.strip()); data["delay"] = val; save_data(data)
            ctx.user_data.pop("state", None)
            await msg.reply_text(f"✅ <b>Delay set to {val}s</b>", parse_mode=H,
                                 reply_markup=kb_playlist(data.get("playlist", [])))
        except ValueError:
            await msg.reply_text("❌ Send a number.")
    elif state == S_SET_BREAK:
        try:
            val = int(msg.text.strip()); data["break_time"] = val; save_data(data)
            ctx.user_data.pop("state", None)
            await msg.reply_text(f"✅ <b>Break set to {val}s</b>", parse_mode=H,
                                 reply_markup=kb_playlist(data.get("playlist", [])))
        except ValueError:
            await msg.reply_text("❌ Send a number.")

async def finalize_login(msg, ctx, c):
    me = await c.get_me()
    phone = c.me.phone_number if c.me else ctx.user_data.get("tmp_phone", "?")
    phone = f"+{phone}" if not str(phone).startswith("+") else phone
    for k in ("state", "tmp_client", "tmp_phone", "tmp_hash"): ctx.user_data.pop(k, None)
    sess.pyro = c; sess.get_nt(); start_keepalive()
    data = load_data(); data["phone"] = phone; save_data(data)
    start_monitors(data.get("channels", []))
    name = me.first_name if me else phone
    await msg.reply_text(f"✅ <b>Logged in as {name}</b>\n<code>{phone}</code>",
                         parse_mode=H, reply_markup=kb_main())

def _exc_handler(loop, context):
    exc = context.get("exception")
    msg = str(exc) if exc else context.get("message", "")
    if any(k in msg for k in ("Peer id invalid", "ID not found", "not modified")):
        return
    loop.default_exception_handler(context)

async def main():
    asyncio.get_event_loop().set_exception_handler(_exc_handler)
    os.makedirs(SESSION_DIR, exist_ok=True)
    os.makedirs(AUDIO_DIR,   exist_ok=True)
    data = load_data()
    phone = data.get("phone")
    if phone:
        await start_pyro_session(phone)
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, on_message))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=False)
    log.info("Bot started.")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        log.info("Shutting down...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
