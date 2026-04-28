"""
VC Streaming Platform — Advanced Admin + Multi-Client Bot
"""

import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import ntgcalls
from pyrogram import Client as PyroClient
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait
from pyrogram.raw import functions, types as raw_types
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, ApplicationBuilder, CallbackQueryHandler,
    CommandHandler, ContextTypes, MessageHandler, filters,
)

from config import TOKEN as ADMIN_TOKEN, APIID, APIHASH, ADMIN_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler("platform.log"), logging.StreamHandler()],
)
log = logging.getLogger("VCPlatform")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

H = "HTML"
DATA_FILE   = "platform_data.json"
SESSION_DIR = "sessions"
AUDIO_DIR   = "audio_files"
SAMPLE_RATE = 48000
CHANNELS    = 2
BYTES_SEC   = SAMPLE_RATE * CHANNELS * 2
PCM_CHUNK   = BYTES_SEC
MONITOR_INT = 15

os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# PREMIUM EMOJIS
# ─────────────────────────────────────────────────────────────

def e(emoji_id: str, fallback: str) -> str:
    if not emoji_id:
        return fallback
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


# Real verified Telegram premium animated emoji IDs
EM = {
    "crown":   e("5039727497143387500", "👑"),
    "star":    e("5042176294222037888", "⭐"),
    "fire":    e("5389038097860144794", "🔥"),
    "diamond": e("5042050649248760772", "💎"),
    "check":   e("5039844895779455925", "✅"),
    "cross":   e("5040042498634810056", "❌"),
    "music":   e("5463107823946717464", "🎵"),
    "mic":     e("6086916415980312572", "🎙"),
    "phone":   e("5407025283456835913", "📱"),
    "channel": e("5041888071851705019", "📣"),
    "cal":     e("5413879192267805083", "🗓"),
    "clock":   e("6285240160120477644", "⏰"),
    "warn":    e("5039665997506675838", "⚠️"),
    "green":   e("5039928501612839813", "🟢"),
    "red":     e("5042042652019655612", "🔴"),
    "yellow":  e("5339082633160703625", "🟡"),
    "lock":    e("5305609152704297298", "🔒"),
    "rocket":  e("5389057356493511934", "🚀"),
    "trophy":  e("5188344996356448758", "🏆"),
    "bell":    e("5042111805288089118", "🔔"),
    "shield":  e("5042328396193864923", "🛡"),
    "gift":    e("5039778134807806727", "🎁"),
    "chart":   e("5042290883949495533", "📊"),
    "zap":     e("5042334757040423886", "⚡"),
    "person":  e("6165860934242798778", "👤"),
    "bot":     e("5372981976804366741", "🤖"),
    "list":    e("5039600026809009149", "📌"),
    "settings":e("5339068773301240682", "⚙️"),
    "link":    e("5042101437237036298", "🔗"),
    "stop":    e("4956442665320186933", "⏸"),
    "play":    e("4956583802240500602", "▶️"),
    "refresh": e("5041837837914211014", "🔄"),
    "live":    e("5256134032852278918", "📡"),
    "sparkle": e("5040016479722931047", "✨"),
    "star2":   e("5042200814190330758", "💫"),
    "chat":    e("5040036030414062506", "💬"),
    "trash":   e("5039614900280754969", "🗑"),
    "money":   e("5039789890133296083", "💰"),
    "idea":    e("5039660273953853888", "💡"),
}


# ─────────────────────────────────────────────────────────────
# SUBSCRIPTION PLANS
# ─────────────────────────────────────────────────────────────

PLANS = {
    "basic": {
        "name": "Basic",
        "emoji": EM["star"],
        "price": "₹2,000",
        "channels": 1,
        "audio": 2,
        "desc": "1 Account • 1 Channel • 2 Audio Files",
    },
    "pro": {
        "name": "Pro",
        "emoji": EM["diamond"],
        "price": "Custom",
        "channels": 3,
        "audio": None,
        "desc": "1 Account • 3 Channels • Unlimited Audio",
    },
}


def plan_limits(plan: str):
    return PLANS.get(plan, PLANS["basic"])


# ─────────────────────────────────────────────────────────────
# STATES
# ─────────────────────────────────────────────────────────────

SA_USERID   = 10
SA_TOKEN    = 11
SA_PLAN     = 12
SA_DAYS     = 13
SA_EXT_UID  = 14
SA_EXT_DAYS = 15

SC_PHONE    = 20
SC_OTP      = 21
SC_PASS     = 22
SC_CHANNEL  = 23
SC_AUDIO    = 24
SC_DELAY    = 25
SC_BREAK    = 26


# ─────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────

def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"users": {}}


def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_user(uid: str) -> dict:
    return load_data()["users"].get(uid, {})


def save_user(uid: str, udata: dict):
    data = load_data()
    data["users"][uid] = udata
    save_data(data)


def generate_unique_key(owner_id: str) -> str:
    """If owner already has a bot, generate owner_2, owner_3, etc."""
    data = load_data()
    users = data.get("users", {})
    if owner_id not in users:
        return owner_id
    n = 2
    while f"{owner_id}_{n}" in users:
        n += 1
    return f"{owner_id}_{n}"


def get_owner_id(uid: str) -> str:
    """Get actual Telegram user ID from storage key."""
    udata = get_user(uid)
    return str(udata.get("owner_id", uid.split("_")[0]))


def get_client_display(uid: str) -> str:
    """Returns 'Name (ID)' for a client, or just 'ID' if name not saved yet."""
    udata = get_user(uid)
    owner_id = udata.get("owner_id", uid.split("_")[0])
    name = udata.get("client_name", "")
    return f"<b>{name}</b> (<code>{owner_id}</code>)" if name else f"<code>{owner_id}</code>"


def days_left(udata: dict) -> int:
    rem = udata.get("subscribed_until", 0) - time.time()
    return max(0, int(rem / 86400))


def is_active(udata: dict) -> bool:
    return time.time() < udata.get("subscribed_until", 0)


def fmt_date(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%d %b %Y")


def fmt_datetime(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%d %b %Y, %I:%M %p")


# ─────────────────────────────────────────────────────────────
# PCM HELPER
# ─────────────────────────────────────────────────────────────

def build_combined_pcm(files, delay_sec, break_sec, out_path):
    valid = [f for f in files if os.path.isfile(f)]
    with open(out_path, "wb") as out:
        for i, f in enumerate(valid):
            proc = subprocess.Popen(
                ["ffmpeg", "-i", f, "-f", "s16le", "-acodec", "pcm_s16le",
                 "-ar", str(SAMPLE_RATE), "-ac", str(CHANNELS), "pipe:1", "-loglevel", "quiet"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
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


# ─────────────────────────────────────────────────────────────
# VC SESSION
# ─────────────────────────────────────────────────────────────

class VCSession:
    def __init__(self, uid: str):
        self.uid = uid
        self.pyro: Optional[PyroClient] = None
        self.nt = None
        self.active_chat_id = None
        self.active_call = None
        self.active_link: Optional[str] = None
        self.monitor_tasks: Dict[str, asyncio.Task] = {}
        self.keepalive_task = None
        self.combined_pcm: Optional[str] = None
        self.loop = None

    def is_in_vc(self) -> bool:
        return self.active_chat_id is not None

    def get_nt(self):
        if self.nt is None:
            self.nt = ntgcalls.NTgCalls()
            self.nt.on_connection_change(
                lambda cid, info: log.info(f"[{self.uid}] NTG: {getattr(info,'state',info)}"))
            try: self.nt.on_stream_end(self._on_stream_end)
            except Exception: pass
        return self.nt

    def _on_stream_end(self, *args):
        try:
            chat_id = args[0] if args else self.active_chat_id
            if chat_id != self.active_chat_id or not self.combined_pcm: return
            if not self.nt or not self.loop: return
            media = ntgcalls.MediaDescription(
                microphone=ntgcalls.AudioDescription(
                    ntgcalls.MediaSource.FILE, SAMPLE_RATE, CHANNELS, self.combined_pcm))
            async def _restart():
                for method in ("change_stream", "set_stream_sources", "edit_call"):
                    fn = getattr(self.nt, method, None)
                    if not fn: continue
                    try:
                        if method == "set_stream_sources":
                            await fn(chat_id, ntgcalls.StreamMode.CAPTURE, media)
                        else:
                            await fn(chat_id, media)
                        return
                    except Exception: pass
            asyncio.run_coroutine_threadsafe(_restart(), self.loop)
        except Exception: pass


# ─────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────

vc_sessions: Dict[str, VCSession] = {}
client_bots: Dict[str, Application] = {}
bot_info_cache: Dict[str, dict] = {}   # uid -> {"username": ..., "first_name": ...}
admin_app_ref: Optional[Application] = None


def get_vc_sess(uid: str) -> VCSession:
    if uid not in vc_sessions:
        vc_sessions[uid] = VCSession(uid)
    return vc_sessions[uid]


# ─────────────────────────────────────────────────────────────
# VC CORE
# ─────────────────────────────────────────────────────────────

async def check_vc(client, chat_link: str):
    try:
        link = chat_link.strip()
        m = re.search(r"t\.me/(\w+)", link)
        target = link if ("/+" in link or "/joinchat/" in link) else (m.group(1) if m else None)
        if not target: return None, None, True
        chat = await client.get_chat(target)
        if chat.type not in (ChatType.SUPERGROUP, ChatType.CHANNEL, ChatType.GROUP):
            return None, None, True
        if chat.type in (ChatType.SUPERGROUP, ChatType.CHANNEL):
            full = await client.invoke(
                functions.channels.GetFullChannel(channel=await client.resolve_peer(chat.id)))
        else:
            full = await client.invoke(functions.messages.GetFullChat(chat_id=chat.id))
        call_inp = getattr(full.full_chat, "call", None)
        return chat, call_inp, True
    except FloodWait as fw:
        await asyncio.sleep(min(fw.value, 30)); return None, None, False
    except Exception as e:
        log.warning(f"check_vc [{chat_link}]: {e}"); return None, None, True


def extract_transport(updates):
    try:
        for upd in getattr(updates, "updates", []):
            if hasattr(upd, "params") and hasattr(upd.params, "data"):
                return upd.params.data
        if hasattr(updates, "params") and hasattr(updates.params, "data"):
            return updates.params.data
    except Exception: pass
    return None


async def vc_join(sess: VCSession, chat, call_inp, uid: str) -> bool:
    udata = get_user(uid)
    files = [p["path"] for p in udata.get("playlist", []) if os.path.isfile(p["path"])]
    if not files: return False
    nt = sess.get_nt()
    sess.loop = asyncio.get_event_loop()
    chat_id = chat.id
    try:
        pcm_path = f"/tmp/vcbot_{uid}_{abs(chat_id)}.pcm"
        await sess.loop.run_in_executor(
            None, build_combined_pcm, files,
            udata.get("delay", 30), udata.get("break_time", 300), pcm_path)
        sess.combined_pcm = pcm_path
        media = ntgcalls.MediaDescription(
            microphone=ntgcalls.AudioDescription(
                ntgcalls.MediaSource.FILE, SAMPLE_RATE, CHANNELS, pcm_path))
        try:
            params_json = await nt.create_call(chat_id, media); need_set = False
        except TypeError:
            params_json = await nt.create_call(chat_id); need_set = True
        result = None
        for _ in range(3):
            try:
                result = await sess.pyro.invoke(
                    functions.phone.JoinGroupCall(
                        call=call_inp, join_as=raw_types.InputPeerSelf(),
                        muted=False, video_stopped=True,
                        params=raw_types.DataJSON(data=params_json)))
                break
            except FloodWait as fw:
                await asyncio.sleep(fw.value + 2)
        if not result: return False
        transport = extract_transport(result)
        if not transport: return False
        await nt.connect(chat_id, transport, False)
        if need_set:
            await nt.set_stream_sources(chat_id, ntgcalls.StreamMode.CAPTURE, media)
        await nt.unmute(chat_id)
        sess.active_chat_id = chat_id
        sess.active_call = call_inp
        return True
    except Exception as e:
        log.error(f"[{uid}] vc_join: {e}")
        try: await nt.stop(chat_id)
        except Exception: pass
        return False


async def vc_leave(sess: VCSession):
    if not sess.active_chat_id: return
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
    sess.active_chat_id = None
    sess.active_call = None
    sess.active_link = None


async def create_live_in(sess: VCSession, link: str) -> str:
    if not sess.pyro: return f"{EM['cross']} Account not logged in"
    chat, _, _ = await check_vc(sess.pyro, link)
    if not chat: return f"{EM['cross']} Channel resolve nahi hua"
    try:
        peer = await sess.pyro.resolve_peer(chat.id)
        await sess.pyro.invoke(
            functions.phone.CreateGroupCall(peer=peer, random_id=random.randint(1, 2**31 - 1)))
        return f"{EM['check']} Live started in <b>{chat.title}</b>"
    except Exception as e:
        msg = str(e)
        if "ALREADY" in msg: return f"{EM['zap']} Live already running in <b>{chat.title}</b>"
        if "ADMIN_REQUIRED" in msg: return f"{EM['cross']} Account ko Manage Calls admin permission chahiye"
        return f"{EM['cross']} Failed: <code>{msg}</code>"


async def end_live_in(sess: VCSession, link: str) -> str:
    if not sess.pyro: return f"{EM['cross']} Account not logged in"
    chat, call_inp, _ = await check_vc(sess.pyro, link)
    if not chat: return f"{EM['cross']} Channel resolve nahi hua"
    if not call_inp: return f"{EM['warn']} Koi live nahi chal raha"
    try:
        if sess.active_chat_id == chat.id: await vc_leave(sess)
        await sess.pyro.invoke(functions.phone.DiscardGroupCall(call=call_inp))
        return f"{EM['check']} Live stopped in <b>{chat.title}</b>"
    except Exception as e:
        return f"{EM['cross']} Failed: <code>{str(e)}</code>"


async def keepalive_loop(sess: VCSession):
    while True:
        await asyncio.sleep(20)
        if not sess.pyro: continue
        try:
            if not sess.pyro.is_connected: await sess.pyro.start()
            else: await sess.pyro.invoke(functions.Ping(ping_id=random.randint(1, 999999)))
        except Exception: pass


async def monitor_channel(sess: VCSession, uid: str, link: str):
    was_active = False
    interval = MONITOR_INT
    while True:
        try:
            if not sess.pyro: await asyncio.sleep(interval); continue
            udata = get_user(uid)
            if not is_active(udata):
                await vc_leave(sess); return
            chat, call, fatal = await check_vc(sess.pyro, link)
            if not fatal: interval = 60; await asyncio.sleep(interval); continue
            interval = MONITOR_INT
            if chat and call and not was_active:
                ok = await vc_join(sess, chat, call, uid)
                if ok: sess.active_link = link; was_active = True
            elif not call and was_active:
                await vc_leave(sess); was_active = False
        except asyncio.CancelledError:
            await vc_leave(sess); return
        except Exception as e:
            log.warning(f"[{uid}] Monitor [{link}]: {e}")
        await asyncio.sleep(interval)


def start_monitors(sess: VCSession, uid: str, channels: list):
    for link in channels:
        if link not in sess.monitor_tasks or sess.monitor_tasks[link].done():
            sess.monitor_tasks[link] = asyncio.create_task(monitor_channel(sess, uid, link))


def stop_monitors(sess: VCSession):
    for t in sess.monitor_tasks.values(): t.cancel()
    sess.monitor_tasks.clear()


async def start_pyro_session(sess: VCSession, uid: str) -> bool:
    udata = get_user(uid)
    phone = udata.get("account_phone")
    if not phone: return False
    sf = os.path.join(SESSION_DIR, f"{phone}.session")
    if not os.path.exists(sf): return False
    c = PyroClient(phone, api_id=APIID, api_hash=APIHASH, workdir=SESSION_DIR)
    try:
        await c.start()
        if c.me is None: await c.get_me()
        sess.pyro = c; sess.get_nt()
        sess.keepalive_task = asyncio.create_task(keepalive_loop(sess))
        start_monitors(sess, uid, udata.get("channels", []))
        return True
    except Exception as e:
        log.warning(f"[{uid}] Pyro start: {e}"); return False


async def stop_client_bot(uid: str):
    sess = vc_sessions.get(uid)
    if sess:
        stop_monitors(sess)
        if sess.is_in_vc(): await vc_leave(sess)
        if sess.keepalive_task: sess.keepalive_task.cancel()
        if sess.pyro:
            try: await sess.pyro.stop()
            except Exception: pass
            sess.pyro = None
    app = client_bots.pop(uid, None)
    if app:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception: pass
    bot_info_cache.pop(uid, None)
    log.info(f"[{uid}] Bot stopped")


# ─────────────────────────────────────────────────────────────
# STATUS HELPERS
# ─────────────────────────────────────────────────────────────

def user_status_icon(uid: str) -> str:
    udata = get_user(uid)
    if not is_active(udata): return EM["cross"]
    sess = vc_sessions.get(uid)
    if sess and sess.is_in_vc(): return EM["live"]
    if sess and sess.pyro: return EM["green"]
    return EM["yellow"]


def bot_display_name(uid: str) -> str:
    info = bot_info_cache.get(uid, {})
    uname = info.get("username", "")
    fname = info.get("first_name", f"User {uid}")
    return f"@{uname}" if uname else fname


# ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
#   ADMIN BOT — TEXTS & KEYBOARDS
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────

def adm_home_text() -> str:
    data = load_data()
    users = data.get("users", {})
    total  = len(users)
    active = sum(1 for u in users.values() if is_active(u))
    online = sum(1 for uid in users if vc_sessions.get(uid) and vc_sessions[uid].pyro)
    live   = sum(1 for uid in users if vc_sessions.get(uid) and vc_sessions[uid].is_in_vc())
    return (
        f"{EM['crown']} <b>ADMIN DASHBOARD</b>\n"
        f"{'─'*28}\n"
        f"{EM['chart']} Total Clients   : <b>{total}</b>\n"
        f"{EM['check']} Active Sub       : <b>{active}</b>\n"
        f"{EM['green']} Online Bots      : <b>{online}</b>\n"
        f"{EM['live']}  In Live VC       : <b>{live}</b>\n"
        f"{'─'*28}"
    )


def kb_adm_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 All Bots",          callback_data="adm:allbots"),
         InlineKeyboardButton("📋 Manage Subs",       callback_data="adm:subs")],
        [InlineKeyboardButton("➕ Add Client",        callback_data="adm:add"),
         InlineKeyboardButton("📊 Full Stats",        callback_data="adm:stats")],
        [InlineKeyboardButton("🚀 Start All",         callback_data="adm:startall"),
         InlineKeyboardButton("🛑 Stop All",          callback_data="adm:stopall")],
        [InlineKeyboardButton("🔃 Restart Platform",  callback_data="adm:restart")],
    ])


def adm_allbots_text() -> str:
    data = load_data()
    users = data.get("users", {})
    if not users:
        return f"{EM['bot']} <b>ALL BOTS</b>\n\n<i>Koi client nahi.</i>"
    lines = [f"{EM['bot']} <b>ALL BOTS ({len(users)})</b>\n{'─'*28}"]
    for uid, udata in users.items():
        info    = bot_info_cache.get(uid, {})
        uname   = f"@{info['username']}" if info.get("username") else "—"
        fname   = info.get("first_name", f"User {uid}")
        plan    = udata.get("plan", "basic")
        pl_info = PLANS.get(plan, PLANS["basic"])
        sess    = vc_sessions.get(uid)
        status  = user_status_icon(uid)
        dl      = days_left(udata)
        in_vc   = f" {EM['live']} <i>Live</i>" if (sess and sess.is_in_vc()) else ""
        lines.append(
            f"\n{status} <b>{fname}</b> {uname}\n"
            f"   {EM['person']} ID: <code>{uid}</code>{in_vc}\n"
            f"   {pl_info['emoji']} {pl_info['name']} • {EM['cal']} {dl} din left"
        )
    return "\n".join(lines)


def kb_adm_allbots() -> InlineKeyboardMarkup:
    data = load_data()
    users = data.get("users", {})
    rows = []
    for uid, udata in users.items():
        info   = bot_info_cache.get(uid, {})
        buname = f"@{info['username']}" if info.get("username") else f"bot:{uid}"
        cname  = udata.get("client_name", "")
        owner  = udata.get("owner_id", uid.split("_")[0])
        label  = f"{cname} ({owner})" if cname else owner
        sess   = vc_sessions.get(uid)
        online = "🟢" if (sess and sess.pyro) else "🔴"
        running = bool(sess and sess.pyro)
        rows.append([
            InlineKeyboardButton(f"{online} {label} • {buname}", callback_data=f"adm:user:{uid}"),
        ])
        rows.append([
            InlineKeyboardButton("▶️ Start" if not running else "🛑 Stop",
                callback_data=f"adm:startbot:{uid}" if not running else f"adm:stopbot:{uid}"),
            InlineKeyboardButton("👁 View",  callback_data=f"adm:user:{uid}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="adm:home")])
    return InlineKeyboardMarkup(rows)


def adm_subs_text() -> str:
    data = load_data()
    users = data.get("users", {})
    if not users:
        return f"{EM['shield']} <b>MANAGE SUBSCRIPTIONS</b>\n\n<i>Koi client nahi.</i>"
    lines = [f"{EM['shield']} <b>MANAGE SUBSCRIPTIONS</b>\n{'─'*28}"]
    for uid, udata in users.items():
        active = is_active(udata)
        dl     = days_left(udata)
        until  = udata.get("subscribed_until", 0)
        plan   = udata.get("plan", "basic")
        pl_inf = PLANS.get(plan, PLANS["basic"])
        cname  = udata.get("client_name", "")
        owner  = udata.get("owner_id", uid.split("_")[0])
        name   = f"{cname} ({owner})" if cname else owner
        icon   = EM["check"] if active else EM["cross"]
        warn   = f" {EM['bell']} <b>Expiring soon!</b>" if (0 < dl <= 3) else ""
        lines.append(
            f"\n{icon} <b>{name}</b>{warn}\n"
            f"   {pl_inf['emoji']} Plan: <b>{pl_inf['name']}</b>\n"
            f"   {EM['cal']} Expires: <b>{fmt_date(until) if until else '—'}</b> "
            f"({EM['clock']} <b>{dl} din</b>)"
        )
    return "\n".join(lines)


def kb_adm_subs() -> InlineKeyboardMarkup:
    data = load_data()
    users = data.get("users", {})
    rows = []
    for uid, udata in users.items():
        dl    = days_left(udata)
        info  = bot_info_cache.get(uid, {})
        name  = info.get("first_name", f"User {uid}")
        icon  = "✅" if is_active(udata) else "❌"
        warn  = " ⚠️" if (0 < dl <= 3) else ""
        rows.append([
            InlineKeyboardButton(f"{icon} {name}{warn}", callback_data=f"adm:user:{uid}"),
            InlineKeyboardButton("➕ Extend",            callback_data=f"adm:extend:{uid}"),
            InlineKeyboardButton("🗑 Remove",            callback_data=f"adm:remove:{uid}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="adm:home")])
    return InlineKeyboardMarkup(rows)


def adm_user_detail_text(uid: str) -> str:
    udata = get_user(uid)
    sess  = vc_sessions.get(uid)
    info  = bot_info_cache.get(uid, {})
    uname = f"@{info['username']}" if info.get("username") else "—"
    cname = udata.get("client_name", "")
    fname = cname if cname else f"User {get_owner_id(uid)}"
    plan  = udata.get("plan", "basic")
    pl_inf = PLANS.get(plan, PLANS["basic"])
    active = is_active(udata)
    dl     = days_left(udata)
    until  = udata.get("subscribed_until", 0)
    added  = udata.get("added_on", 0)
    phone  = udata.get("account_phone", "—")
    chs    = udata.get("channels", [])
    pl_lst = udata.get("playlist", [])
    in_vc  = sess.is_in_vc() if sess else False
    link   = (sess.active_link if sess else None) or "—"
    online = bool(sess and sess.pyro)

    ch_lines = ""
    if chs:
        ch_lines = "\n" + "\n".join(
            f"   {'🟢' if (sess and uid in str(sess.monitor_tasks)) else '🔴'} <code>{c}</code>"
            for c in chs)

    owner_id  = udata.get("owner_id", uid)
    is_extra  = "_" in uid
    name_id   = f"<b>{cname}</b> (<code>{owner_id}</code>)" if cname else f"<code>{owner_id}</code>"
    uid_line  = (
        f"{EM['person']} User     : {name_id}\n"
        f"{EM['list']}  Bot Key  : <code>{uid}</code> 🔢\n"
    ) if is_extra else (
        f"{EM['person']} User     : {name_id}\n"
    )

    return (
        f"{EM['crown']} <b>{fname}</b> • {uname}\n"
        f"{'─'*28}\n"
        f"{uid_line}"
        f"{EM['bot']}  Bot      : {bot_display_name(uid)}\n"
        f"{EM['phone']} Account  : <code>{phone}</code>\n"
        f"{EM['live']}  In VC    : {'🟢 ' + link if in_vc else '🔴 No'}\n"
        f"{EM['green']} Online   : {'✅ Yes' if online else '❌ No'}\n"
        f"{'─'*28}\n"
        f"{pl_inf['emoji']} Plan     : <b>{pl_inf['name']}</b> ({pl_inf['price']})\n"
        f"{EM['channel']} Channels : <b>{len(chs)}</b> / {pl_inf['channels']}{ch_lines}\n"
        f"{EM['music']} Playlist : <b>{len(pl_lst)}</b> track(s)"
        + (f" / {pl_inf['audio']}" if pl_inf['audio'] else " / ∞") + "\n"
        f"{EM['cal']} Sub Until: <b>{fmt_date(until) if until else '—'}</b>\n"
        f"{EM['clock']} Remaining: <b>{dl} din</b>\n"
        f"{EM['settings']} Added On : {fmt_date(added) if added else '—'}"
    )


def kb_adm_user(uid: str) -> InlineKeyboardMarkup:
    sess    = vc_sessions.get(uid)
    running = bool(sess and sess.pyro)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Extend Sub",  callback_data=f"adm:extend:{uid}"),
         InlineKeyboardButton("🔄 Change Plan", callback_data=f"adm:chplan:{uid}")],
        [InlineKeyboardButton("🛑 Stop Bot" if running else "▶️ Start Bot",
                               callback_data=f"adm:stopbot:{uid}" if running else f"adm:startbot:{uid}"),
         InlineKeyboardButton("🗑 Remove",      callback_data=f"adm:remove:{uid}")],
        [InlineKeyboardButton("📊 View Details", callback_data=f"adm:user:{uid}"),
         InlineKeyboardButton("🔄 Restart Mon", callback_data=f"adm:restmon:{uid}")],
        [InlineKeyboardButton("🎮 Control Bot",  callback_data=f"adm:ctrl:{uid}")],
        [InlineKeyboardButton("🔙 Back",         callback_data="adm:subs")],
    ])


def adm_stats_text() -> str:
    data = load_data()
    users = data.get("users", {})
    total  = len(users)
    active = sum(1 for u in users.values() if is_active(u))
    expired = total - active
    online = sum(1 for uid in users if vc_sessions.get(uid) and vc_sessions[uid].pyro)
    live   = sum(1 for uid in users if vc_sessions.get(uid) and vc_sessions[uid].is_in_vc())
    basic_cnt = sum(1 for u in users.values() if u.get("plan","basic") == "basic")
    pro_cnt   = sum(1 for u in users.values() if u.get("plan","basic") == "pro")
    warn_cnt  = sum(1 for u in users.values() if 0 < days_left(u) <= 3)

    lines = [
        f"{EM['chart']} <b>PLATFORM FULL STATS</b>\n{'─'*28}",
        f"{EM['person']} Total Clients : <b>{total}</b>",
        f"{EM['check']} Active Sub    : <b>{active}</b>",
        f"{EM['cross']} Expired       : <b>{expired}</b>",
        f"{EM['green']} Online Bots   : <b>{online}</b>",
        f"{EM['live']}  In Live VC    : <b>{live}</b>",
        f"{EM['bell']} Expiring Soon : <b>{warn_cnt}</b>",
        f"\n{EM['star']} Basic Plan    : <b>{basic_cnt}</b>",
        f"{EM['diamond']} Pro Plan      : <b>{pro_cnt}</b>",
        f"\n{'─'*28}\n{EM['clock']} Updated: {fmt_datetime(time.time())}",
    ]
    return "\n".join(lines)


def welcome_message_for_client(uid: str, udata: dict) -> str:
    plan     = udata.get("plan", "basic")
    pl_inf   = PLANS.get(plan, PLANS["basic"])
    until    = udata.get("subscribed_until", 0)
    added    = udata.get("added_on", time.time())
    owner_id = udata.get("owner_id", uid.split("_")[0])
    cname    = udata.get("client_name", "")
    name_line = f"<b>{cname}</b> (<code>{owner_id}</code>)" if cname else f"<code>{owner_id}</code>"

    return (
        f"{EM['fire']} <b>Welcome to VC Streaming!</b>\n"
        f"{'─'*30}\n\n"
        f"{EM['person']} <b>User      :</b> {name_line}\n"
        f"{pl_inf['emoji']} <b>Plan      :</b> <b>{pl_inf['name']}</b> ({pl_inf['price']})\n"
        f"{EM['cal']} <b>Start Date:</b> {fmt_date(added)}\n"
        f"{EM['cal']} <b>End Date  :</b> <b>{fmt_date(until)}</b>\n"
        f"{EM['clock']} <b>Duration  :</b> {days_left(udata)} din\n\n"
        f"{'─'*30}\n"
        f"{EM['shield']} <i>Koi problem? Bot mein Contact Admin button se reach karo.</i>"
    )


# ─────────────────────────────────────────────────────────────
# ADMIN HANDLERS
# ─────────────────────────────────────────────────────────────

async def adm_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else 0
    if uid != ADMIN_ID:
        await update.message.reply_text(f"{EM['lock']} Unauthorized.", parse_mode=H); return
    await update.message.reply_text(adm_home_text(), parse_mode=H, reply_markup=kb_adm_home())


async def adm_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.from_user: return
    await q.answer()
    if q.from_user.id != ADMIN_ID:
        await q.edit_message_text(f"{EM['lock']} Unauthorized."); return
    cb = q.data

    if cb == "adm:home":
        ctx.user_data.clear()
        await q.edit_message_text(adm_home_text(), parse_mode=H, reply_markup=kb_adm_home())

    elif cb == "adm:allbots":
        await q.edit_message_text(
            adm_allbots_text(), parse_mode=H, reply_markup=kb_adm_allbots())

    elif cb == "adm:subs":
        await q.edit_message_text(
            adm_subs_text(), parse_mode=H, reply_markup=kb_adm_subs())

    elif cb == "adm:stats":
        await q.edit_message_text(
            adm_stats_text(), parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="adm:stats")],
                [InlineKeyboardButton("🔙 Back",    callback_data="adm:home")],
            ]))

    elif cb == "adm:add":
        ctx.user_data["state"] = SA_USERID
        await q.edit_message_text(
            f"{EM['crown']} <b>ADD NEW CLIENT</b>\n{'─'*25}\n\n"
            f"{EM['settings']} <b>Step 1 of 4</b>\n\n"
            f"{EM['person']} Client ka <b>Telegram User ID</b> bhejo:\n"
            f"<i>Example: 987654321</i>\n\n"
            f"{EM['zap']} <i>ID pane ke liye: @userinfobot pe message karo</i>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adm:home")]]))

    elif cb.startswith("adm:user:"):
        uid = cb.split(":")[2]
        if not get_user(uid):
            await q.edit_message_text("❌ User not found.", parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm:subs")]])); return
        await q.edit_message_text(
            adm_user_detail_text(uid), parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb.startswith("adm:chplan:"):
        uid = cb.split(":")[2]
        udata = get_user(uid)
        current = udata.get("plan", "basic")
        new_plan = "pro" if current == "basic" else "basic"
        udata["plan"] = new_plan
        save_user(uid, udata)
        await q.edit_message_text(
            f"{EM['check']} Plan changed to <b>{PLANS[new_plan]['name']}</b>",
            parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb.startswith("adm:extend:"):
        uid = cb.split(":")[2]
        ctx.user_data["state"] = SA_EXT_DAYS
        ctx.user_data["ext_uid"] = uid
        udata = get_user(uid)
        await q.edit_message_text(
            f"{EM['gift']} <b>EXTEND SUBSCRIPTION</b>\n{'─'*25}\n\n"
            f"{EM['person']} Client: {get_client_display(uid)}\n"
            f"{EM['clock']} Current: <b>{days_left(udata)} din bache</b>\n"
            f"{EM['cal']} Expires: <b>{fmt_date(udata.get('subscribed_until',0))}</b>\n\n"
            f"Kitne <b>din aur</b> add karne hain?",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("30 Din", callback_data=f"adm:extdays:{uid}:30"),
                 InlineKeyboardButton("60 Din", callback_data=f"adm:extdays:{uid}:60"),
                 InlineKeyboardButton("90 Din", callback_data=f"adm:extdays:{uid}:90")],
                [InlineKeyboardButton("❌ Cancel", callback_data=f"adm:user:{uid}")],
            ]))

    elif cb.startswith("adm:extdays:"):
        parts = cb.split(":")
        uid  = parts[2]
        days = int(parts[3])
        udata = get_user(uid)
        was_expired = not is_active(udata)
        current_until = max(udata.get("subscribed_until", time.time()), time.time())
        new_until = current_until + (days * 86400)
        udata["subscribed_until"] = new_until
        udata.setdefault("reminders_sent", []).clear()
        save_user(uid, udata)
        ctx.user_data.clear()
        # Auto-restart bot if it was stopped due to expiry
        auto_started = False
        if uid not in client_bots:
            await q.edit_message_text(f"{EM['rocket']} Subscription renewed — bot restart ho raha hai...", parse_mode=H)
            auto_started = await launch_client_bot(uid)
        restart_txt = f"\n{EM['rocket']} Bot auto-started!" if auto_started else (f"\n{EM['refresh']} Bot already running." if uid in client_bots else "")
        # Notify client about renewal
        cl_app = client_bots.get(uid)
        if cl_app:
            try:
                await cl_app.bot.send_message(
                    int(uid),
                    f"{EM['gift']} <b>Subscription Renewed!</b>\n\n"
                    f"{EM['check']} +{days} din add ho gaye\n"
                    f"{EM['cal']} New Expiry: <b>{fmt_date(new_until)}</b>\n\n"
                    f"{EM['rocket']} Bot active hai, enjoy!",
                    parse_mode=H)
            except Exception: pass
        await q.edit_message_text(
            f"{EM['check']} <b>Subscription Extended!</b>\n\n"
            f"{EM['person']} User: <code>{uid}</code>\n"
            f"{EM['gift']} Added: <b>+{days} din</b>\n"
            f"{EM['cal']} New Expiry: <b>{fmt_date(new_until)}</b>"
            f"{restart_txt}",
            parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb.startswith("adm:remove:"):
        uid = cb.split(":")[2]
        udata = get_user(uid)
        info = bot_info_cache.get(uid, {})
        name = info.get("first_name", f"User {uid}")
        await q.edit_message_text(
            f"{EM['warn']} <b>Confirm Remove?</b>\n\n{EM['person']} {name} <code>[{uid}]</code>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Haan, Hatao", callback_data=f"adm:confirm_rm:{uid}")],
                [InlineKeyboardButton("❌ Cancel",      callback_data=f"adm:user:{uid}")],
            ]))

    elif cb.startswith("adm:confirm_rm:"):
        uid = cb.split(":")[2]
        await stop_client_bot(uid)
        data = load_data(); data["users"].pop(uid, None); save_data(data)
        await q.edit_message_text(
            f"{EM['check']} Client <code>{uid}</code> remove ho gaya.",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm:subs")]]))

    elif cb.startswith("adm:startbot:"):
        uid = cb.split(":")[2]
        await q.edit_message_text(f"{EM['rocket']} Starting bot...", parse_mode=H)
        await launch_client_bot(uid)
        await q.edit_message_text(adm_user_detail_text(uid), parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb.startswith("adm:stopbot:"):
        uid = cb.split(":")[2]
        await q.edit_message_text(f"{EM['clock']} Stopping...", parse_mode=H)
        await stop_client_bot(uid)
        await q.edit_message_text(adm_user_detail_text(uid), parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb.startswith("adm:restmon:"):
        uid = cb.split(":")[2]
        sess = get_vc_sess(uid)
        stop_monitors(sess)
        await asyncio.sleep(0.5)
        udata = get_user(uid)
        start_monitors(sess, uid, udata.get("channels", []))
        running = sum(1 for t in sess.monitor_tasks.values() if not t.done())
        await q.edit_message_text(
            f"{EM['refresh']} Monitors restarted ({running} running)",
            parse_mode=H, reply_markup=kb_adm_user(uid))

    elif cb == "adm:startall":
        data = load_data(); started = 0
        await q.edit_message_text(f"{EM['rocket']} Starting all bots...", parse_mode=H)
        for uid, udata in data.get("users", {}).items():
            if is_active(udata) and uid not in client_bots:
                await launch_client_bot(uid); started += 1
        await q.edit_message_text(
            f"{EM['check']} Start All — <b>{started}</b> bots started",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm:home")]]))

    elif cb == "adm:stopall":
        uids = list(client_bots.keys())
        await q.edit_message_text(f"{EM['clock']} Stopping all...", parse_mode=H)
        for uid in uids: await stop_client_bot(uid)
        await q.edit_message_text(
            f"{EM['check']} All stopped — <b>{len(uids)}</b> bots",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm:home")]]))

    elif cb == "adm:restart":
        await q.edit_message_text(f"{EM['refresh']} <b>Platform restarting...</b>", parse_mode=H)
        await asyncio.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    # ── ADMIN CONTROL BOT PANEL ───────────────────────────
    elif cb.startswith("adm:ctrl:"):
        uid   = cb.split(":")[2]
        udata = get_user(uid)
        if not udata:
            await q.edit_message_text("❌ User not found.", parse_mode=H); return
        sess   = get_vc_sess(uid)
        plan   = udata.get("plan", "basic")
        pl_inf = PLANS.get(plan, PLANS["basic"])
        phone  = udata.get("account_phone", "—")
        chs    = udata.get("channels", [])
        pl_lst = udata.get("playlist", [])
        in_vc  = sess.is_in_vc()
        link   = sess.active_link or "—"
        info   = bot_info_cache.get(uid, {})
        fname  = info.get("first_name", f"User {uid}")
        await q.edit_message_text(
            f"{EM['crown']} <b>ADMIN CONTROL</b> — {fname}\n"
            f"{'─'*28}\n"
            f"{EM['phone']} Account : <code>{phone}</code>\n"
            f"{EM['live']}  In VC   : {'🟢 ' + link if in_vc else '🔴 No'}\n"
            f"{EM['channel']} Channels: <b>{len(chs)}</b>/{pl_inf['channels']}\n"
            f"{EM['music']} Playlist: <b>{len(pl_lst)}</b> track(s)\n"
            f"{'─'*28}\n"
            f"<i>Admin ke taur pe client ka bot control karo</i>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Channels",    callback_data=f"adm:ctrl_ch:{uid}"),
                 InlineKeyboardButton("🎵 Playlist",    callback_data=f"adm:ctrl_pl:{uid}")],
                [InlineKeyboardButton("🎙 Live Control", callback_data=f"adm:ctrl_live:{uid}"),
                 InlineKeyboardButton("🔄 Restart Mon", callback_data=f"adm:ctrl_restmon:{uid}")],
                [InlineKeyboardButton("🛑 Force Leave VC", callback_data=f"adm:ctrl_fleave:{uid}"),
                 InlineKeyboardButton("📊 Refresh",     callback_data=f"adm:ctrl:{uid}")],
                [InlineKeyboardButton("🔙 Client",       callback_data=f"adm:user:{uid}")],
            ]))

    elif cb.startswith("adm:ctrl_ch:"):
        uid   = cb.split(":")[2]
        udata = get_user(uid)
        chs   = udata.get("channels", [])
        plan  = udata.get("plan", "basic")
        limit = PLANS[plan]["channels"]
        ch_text = "\n".join(f"{EM['live']}  <code>{c}</code>" for c in chs) if chs else "<i>Koi channel nahi.</i>"
        rows = []
        for i, ch in enumerate(chs):
            name_ch = ch.replace("https://t.me/", "@")
            rows.append([InlineKeyboardButton(f"❌ {name_ch}", callback_data=f"adm:ctrl_rmch:{uid}:{i}")])
        rows.append([InlineKeyboardButton("➕ Add Channel", callback_data=f"adm:ctrl_addch:{uid}")])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")])
        await q.edit_message_text(
            f"{EM['channel']} <b>CHANNELS</b> — {bot_display_name(uid)}\n"
            f"({len(chs)}/{limit})\n\n{ch_text}",
            parse_mode=H, reply_markup=InlineKeyboardMarkup(rows))

    elif cb.startswith("adm:ctrl_addch:"):
        uid = cb.split(":")[2]
        ctx.user_data["state"] = "ctrl_addch"
        ctx.user_data["ctrl_uid"] = uid
        await q.edit_message_text(
            f"{EM['channel']} Channel link bhejo:\n<i>https://t.me/channelname</i>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"adm:ctrl_ch:{uid}")]]))

    elif cb.startswith("adm:ctrl_rmch:"):
        parts = cb.split(":")
        uid = parts[2]; idx = int(parts[3])
        udata = get_user(uid)
        chs = udata.get("channels", [])
        if 0 <= idx < len(chs):
            removed = chs.pop(idx); udata["channels"] = chs; save_user(uid, udata)
            sess = vc_sessions.get(uid)
            if sess:
                t = sess.monitor_tasks.pop(removed, None)
                if t: t.cancel()
        await q.edit_message_text(f"{EM['check']} Channel removed.", parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl_ch:{uid}")]]))

    elif cb.startswith("adm:ctrl_pl:"):
        uid   = cb.split(":")[2]
        udata = get_user(uid)
        pl    = udata.get("playlist", [])
        plan  = udata.get("plan", "basic")
        limit = PLANS[plan]["audio"]
        pl_text = "\n".join(f"  {i+1}. {p['name']}" for i, p in enumerate(pl)) if pl else "<i>Koi track nahi.</i>"
        lim_txt = f"/{limit}" if limit else "/∞"
        rows = []
        for i, p in enumerate(pl):
            rows.append([InlineKeyboardButton(f"❌ {i+1}. {p['name']}", callback_data=f"adm:ctrl_rmpl:{uid}:{i}")])
        rows.append([InlineKeyboardButton("➕ Add Audio",  callback_data=f"adm:ctrl_addpl:{uid}"),
                     InlineKeyboardButton("🗑 Clear All",  callback_data=f"adm:ctrl_clrpl:{uid}")])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")])
        await q.edit_message_text(
            f"{EM['music']} <b>PLAYLIST</b> — {bot_display_name(uid)}\n"
            f"({len(pl)}{lim_txt})\n\n{pl_text}\n\n"
            f"⏱ Delay: <b>{udata.get('delay',30)}s</b>  ☕ Break: <b>{udata.get('break_time',300)}s</b>",
            parse_mode=H, reply_markup=InlineKeyboardMarkup(rows))

    elif cb.startswith("adm:ctrl_addpl:"):
        uid = cb.split(":")[2]
        ctx.user_data["state"] = "ctrl_addpl"
        ctx.user_data["ctrl_uid"] = uid
        await q.edit_message_text(
            f"{EM['music']} Audio file upload karo (MP3/voice):",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"adm:ctrl_pl:{uid}")]]))

    elif cb.startswith("adm:ctrl_clrpl:"):
        uid = cb.split(":")[2]
        udata = get_user(uid); udata["playlist"] = []; save_user(uid, udata)
        await q.edit_message_text(f"{EM['check']} Playlist cleared.",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl_pl:{uid}")]]))

    elif cb.startswith("adm:ctrl_rmpl:"):
        parts = cb.split(":"); uid = parts[2]; idx = int(parts[3])
        udata = get_user(uid); pl = udata.get("playlist", [])
        if 0 <= idx < len(pl): pl.pop(idx)
        udata["playlist"] = pl; save_user(uid, udata)
        await q.edit_message_text(f"{EM['check']} Track removed.",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl_pl:{uid}")]]))

    elif cb.startswith("adm:ctrl_live:"):
        uid   = cb.split(":")[2]
        udata = get_user(uid)
        chs   = udata.get("channels", [])
        if not chs:
            await q.edit_message_text(f"{EM['warn']} Koi channel nahi.",
                parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")]])); return
        rows = []
        for i, ch in enumerate(chs):
            name_ch = ch.replace("https://t.me/", "@")
            if len(name_ch) > 20: name_ch = name_ch[:20] + "…"
            rows.append([
                InlineKeyboardButton("▶️ Start", callback_data=f"adm:ctrl_lstart:{uid}:{i}"),
                InlineKeyboardButton(name_ch,     callback_data=f"adm:ctrl:{uid}"),
                InlineKeyboardButton("⏹ Stop",  callback_data=f"adm:ctrl_lstop:{uid}:{i}"),
            ])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")])
        await q.edit_message_text(
            f"{EM['mic']} <b>LIVE CONTROL</b> — {bot_display_name(uid)}\n\nStart/Stop karo:",
            parse_mode=H, reply_markup=InlineKeyboardMarkup(rows))

    elif cb.startswith("adm:ctrl_lstart:") or cb.startswith("adm:ctrl_lstop:"):
        parts  = cb.split(":")
        action = parts[1]   # ctrl_lstart or ctrl_lstop
        uid    = parts[2]; idx = int(parts[3])
        udata  = get_user(uid); chs = udata.get("channels", [])
        sess   = get_vc_sess(uid)
        if 0 <= idx < len(chs):
            link = chs[idx]
            await q.edit_message_text(f"{EM['clock']} Processing...", parse_mode=H)
            if action == "ctrl_lstart":
                result = await create_live_in(sess, link)
            else:
                result = await end_live_in(sess, link)
            # Rebuild live control keyboard
            chs2 = get_user(uid).get("channels", [])
            rows = []
            for i, ch in enumerate(chs2):
                n = ch.replace("https://t.me/", "@")
                if len(n) > 20: n = n[:20] + "…"
                rows.append([
                    InlineKeyboardButton("▶️ Start", callback_data=f"adm:ctrl_lstart:{uid}:{i}"),
                    InlineKeyboardButton(n,           callback_data=f"adm:ctrl:{uid}"),
                    InlineKeyboardButton("⏹ Stop",  callback_data=f"adm:ctrl_lstop:{uid}:{i}"),
                ])
            rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")])
            await q.edit_message_text(result, parse_mode=H, reply_markup=InlineKeyboardMarkup(rows))

    elif cb.startswith("adm:ctrl_restmon:"):
        uid  = cb.split(":")[2]
        sess = get_vc_sess(uid)
        stop_monitors(sess)
        await asyncio.sleep(0.5)
        udata = get_user(uid)
        start_monitors(sess, uid, udata.get("channels", []))
        running = sum(1 for t in sess.monitor_tasks.values() if not t.done())
        await q.edit_message_text(
            f"{EM['refresh']} Monitors restarted ({running} running)",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")]]))

    elif cb.startswith("adm:ctrl_fleave:"):
        uid  = cb.split(":")[2]
        sess = get_vc_sess(uid)
        if not sess.is_in_vc():
            await q.edit_message_text(f"{EM['warn']} Bot VC mein nahi hai.",
                parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")]])); return
        link = sess.active_link or "?"
        await vc_leave(sess)
        await q.edit_message_text(
            f"{EM['check']} Force left VC: <code>{link}</code>",
            parse_mode=H, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:ctrl:{uid}")]]))

    # Plan selection during add-user flow
    elif cb.startswith("adm:selectplan:"):
        plan = cb.split(":")[2]
        ctx.user_data["new_plan"] = plan
        ctx.user_data["state"] = SA_DAYS
        pl_inf = PLANS[plan]
        await q.edit_message_text(
            f"{EM['crown']} <b>ADD NEW CLIENT</b>\n{'─'*25}\n\n"
            f"{EM['settings']} <b>Step 4 of 4</b>\n\n"
            f"{pl_inf['emoji']} Plan: <b>{pl_inf['name']}</b> — {pl_inf['desc']}\n\n"
            f"{EM['cal']} <b>Kitne din</b> ka subscription dena hai?\n\n"
            f"{EM['zap']} Quick select:\n"
            f"<i>Ya koi bhi custom number type karo — jaise <b>45</b>, <b>120</b>, <b>365</b></i>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("30 Din",  callback_data=f"adm:quickdays:30"),
                 InlineKeyboardButton("60 Din",  callback_data=f"adm:quickdays:60"),
                 InlineKeyboardButton("90 Din",  callback_data=f"adm:quickdays:90")],
                [InlineKeyboardButton("180 Din", callback_data=f"adm:quickdays:180"),
                 InlineKeyboardButton("365 Din", callback_data=f"adm:quickdays:365")],
                [InlineKeyboardButton("❌ Cancel", callback_data="adm:home")],
            ]))

    elif cb.startswith("adm:quickdays:"):
        days = int(cb.split(":")[2])
        await finalize_add_user(q, ctx, days)


async def finalize_add_user(q_or_msg, ctx, days: int):
    owner_id = ctx.user_data.get("new_uid")
    tok      = ctx.user_data.get("new_token")
    plan     = ctx.user_data.get("new_plan", "basic")
    if not owner_id or not tok: ctx.user_data.clear(); return

    # Generate unique storage key (allows multiple bots per owner)
    uid   = generate_unique_key(owner_id)
    until = time.time() + (days * 86400)
    udata = {
        "owner_id": owner_id,          # actual Telegram user ID
        "bot_token": tok, "plan": plan,
        "subscribed_until": until, "added_on": time.time(),
        "account_phone": None, "channels": [],
        "playlist": [], "delay": 30, "break_time": 300,
        "reminders_sent": [],
    }
    save_user(uid, udata)
    ctx.user_data.clear()

    edit_fn = q_or_msg.edit_message_text if hasattr(q_or_msg, "edit_message_text") else q_or_msg.reply_text
    await edit_fn(f"{EM['rocket']} Bot shuru ho raha hai...", parse_mode=H)
    ok = await launch_client_bot(uid)

    welcome = welcome_message_for_client(uid, get_user(uid))
    pl_inf  = PLANS.get(plan, PLANS["basic"])
    is_extra = "_" in uid  # 2nd, 3rd bot for same owner
    extra_note = f"\n{EM['star']} <b>Bot #{uid.split('_')[-1]}</b> for this client" if is_extra else ""

    await edit_fn(
        f"{EM['check']} <b>CLIENT ADDED SUCCESSFULLY!</b>\n{'─'*30}\n\n"
        f"{EM['person']} Owner ID : <code>{owner_id}</code>\n"
        f"{EM['list']}  Bot Key  : <code>{uid}</code>{extra_note}\n"
        f"{EM['bot']}  Bot      : {bot_display_name(uid)}\n"
        f"{pl_inf['emoji']} Plan     : <b>{pl_inf['name']}</b>\n"
        f"{EM['cal']} Expires  : <b>{fmt_date(until)}</b> ({days} din)\n"
        f"{EM['rocket']} Status   : {'✅ Running' if ok else '⚠️ No session (client login karega)'}\n\n"
        f"{EM['gift']} <b>Forward this to client:</b>",
        parse_mode=H,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📊 View Client", callback_data=f"adm:user:{uid}"),
                                            InlineKeyboardButton("🔙 Panel",       callback_data="adm:home")]]))

    # Send welcome message separately (admin can forward to client)
    try:
        send_fn = q_or_msg.message.reply_text if hasattr(q_or_msg, "message") else q_or_msg.reply_text
        await send_fn(welcome, parse_mode=H)
    except Exception: pass


async def adm_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.user_data: return
    msg = update.message
    if not msg or not msg.chat or msg.chat.type != "private": return
    if (update.effective_user.id if update.effective_user else 0) != ADMIN_ID: return
    state = ctx.user_data.get("state")

    if state == SA_USERID:
        try:
            owner_id = str(int((msg.text or "").strip()))
            # Count existing bots for this owner
            data = load_data()
            existing = [k for k, v in data["users"].items()
                        if str(v.get("owner_id", k.split("_")[0])) == owner_id]
            existing_txt = ""
            if existing:
                existing_txt = (
                    f"\n\n{EM['bell']} Is user ke already <b>{len(existing)}</b> bot(s) hain:\n"
                    + "\n".join(f"  {EM['green']} <code>{k}</code> — {bot_info_cache.get(k,{}).get('username','?')}" for k in existing)
                    + f"\n\n{EM['zap']} Naya bot add ho jayega as: <code>{generate_unique_key(owner_id)}</code>"
                )
            ctx.user_data["new_uid"] = owner_id
            ctx.user_data["state"] = SA_TOKEN
            await msg.reply_text(
                f"{EM['crown']} <b>ADD NEW CLIENT</b>\n{'─'*25}\n\n"
                f"{EM['settings']} <b>Step 2 of 4</b>\n\n"
                f"{EM['person']} User ID: <code>{owner_id}</code> {EM['check']}"
                f"{existing_txt}\n\n"
                f"{EM['bot']}  Client ka <b>Bot Token</b> bhejo:\n"
                f"<i>(@BotFather se mila token)</i>",
                parse_mode=H)
        except ValueError:
            await msg.reply_text(f"{EM['cross']} Valid User ID (number) bhejo.")

    elif state == SA_TOKEN:
        token = (msg.text or "").strip()
        if ":" not in token or len(token) < 20:
            await msg.reply_text(f"{EM['cross']} Valid bot token chahiye. Format: 12345:ABC..."); return
        ctx.user_data["new_token"] = token
        ctx.user_data["state"] = SA_PLAN
        await msg.reply_text(
            f"{EM['crown']} <b>ADD NEW CLIENT</b>\n{'─'*25}\n\n"
            f"{EM['settings']} <b>Step 3 of 4</b>\n\n"
            f"{EM['bot']}  Token saved {EM['check']}\n\n"
            f"{EM['trophy']} <b>Kaunsa subscription plan dena hai?</b>\n\n"
            f"{EM['star']} <b>Basic — ₹2,000/month</b>\n"
            f"  • 1 Account • 1 Channel • 2 Audio\n\n"
            f"{EM['diamond']} <b>Pro — Custom Pricing</b>\n"
            f"  • 1 Account • 3 Channels • Unlimited Audio",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"⭐ Basic — ₹2,000", callback_data="adm:selectplan:basic")],
                [InlineKeyboardButton(f"💎 Pro — Custom",   callback_data="adm:selectplan:pro")],
                [InlineKeyboardButton("❌ Cancel",           callback_data="adm:home")],
            ]))

    elif state == SA_DAYS:
        try:
            days = int((msg.text or "").strip())
            await finalize_add_user(msg, ctx, days)
        except ValueError:
            await msg.reply_text(f"{EM['cross']} Number bhejo (days).")

    elif state == SA_EXT_DAYS:
        try:
            days = int((msg.text or "").strip())
            uid  = ctx.user_data.get("ext_uid")
            udata = get_user(uid)
            current_until = max(udata.get("subscribed_until", time.time()), time.time())
            new_until = current_until + (days * 86400)
            udata["subscribed_until"] = new_until
            udata.setdefault("reminders_sent", []).clear()
            save_user(uid, udata)
            ctx.user_data.clear()
            # Auto-restart if bot was stopped
            auto_started = False
            if uid not in client_bots:
                auto_started = await launch_client_bot(uid)
            # Notify client
            cl_app = client_bots.get(uid)
            if cl_app:
                try:
                    await cl_app.bot.send_message(
                        int(uid),
                        f"{EM['gift']} <b>Subscription Renewed!</b>\n\n"
                        f"{EM['check']} +{days} din add ho gaye\n"
                        f"{EM['cal']} New Expiry: <b>{fmt_date(new_until)}</b>\n\n"
                        f"{EM['rocket']} Bot active hai, enjoy!",
                        parse_mode=H)
                except Exception: pass
            restart_txt = f"\n{EM['rocket']} Bot auto-started!" if auto_started else ""
            await msg.reply_text(
                f"{EM['check']} Subscription extended!\n"
                f"New expiry: <b>{fmt_date(new_until)}</b>{restart_txt}",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"adm:user:{uid}")]]))
        except ValueError:
            await msg.reply_text(f"{EM['cross']} Number bhejo.")

    elif state == "ctrl_addch":
        uid  = ctx.user_data.get("ctrl_uid")
        link = (msg.text or "").strip()
        if not link.startswith("https://t.me/"):
            await msg.reply_text(f"{EM['cross']} <code>https://t.me/</code> se shuru karo.", parse_mode=H); return
        udata = get_user(uid)
        plan  = udata.get("plan", "basic")
        limit = PLANS[plan]["channels"]
        chs   = udata.setdefault("channels", [])
        if len(chs) >= limit:
            await msg.reply_text(
                f"{EM['lock']} Channel limit ({limit}) reached for {PLANS[plan]['name']} plan.",
                parse_mode=H); return
        if link not in chs:
            chs.append(link); save_user(uid, udata)
            sess = vc_sessions.get(uid)
            if sess and sess.pyro: start_monitors(sess, uid, [link])
        ctx.user_data.clear()
        await msg.reply_text(
            f"{EM['check']} Channel added for <code>{uid}</code>:\n<code>{link}</code>",
            parse_mode=H,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Channels", callback_data=f"adm:ctrl_ch:{uid}")]]))

    elif state == "ctrl_addpl":
        uid = ctx.user_data.get("ctrl_uid")
        doc = msg.document or msg.audio or msg.voice
        udata = get_user(uid)
        plan  = udata.get("plan", "basic")
        limit = PLANS[plan]["audio"]
        pl    = udata.get("playlist", [])
        if limit and len(pl) >= limit:
            await msg.reply_text(
                f"{EM['lock']} Audio limit ({limit}) reached for {PLANS[plan]['name']} plan.",
                parse_mode=H); return
        if doc:
            fname = getattr(doc, "file_name", None) or f"audio_{int(time.time())}.mp3"
            spath = os.path.join(AUDIO_DIR, f"{uid}_{fname}")
            try:
                tf = await ctx.bot.get_file(doc.file_id)
                await tf.download_to_drive(spath)
                udata.setdefault("playlist", []).append({"path": spath, "name": fname})
                save_user(uid, udata)
                ctx.user_data.clear()
                await msg.reply_text(
                    f"{EM['check']} Audio added for <code>{uid}</code>: <code>{fname}</code>",
                    parse_mode=H,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Playlist", callback_data=f"adm:ctrl_pl:{uid}")]]))
            except Exception as ex:
                await msg.reply_text(f"{EM['cross']} Upload failed: <code>{ex}</code>", parse_mode=H)
        elif msg.text:
            path = (msg.text or "").strip()
            if not os.path.isfile(path):
                await msg.reply_text(f"{EM['cross']} File not found: <code>{path}</code>", parse_mode=H); return
            name = os.path.basename(path)
            udata.setdefault("playlist", []).append({"path": path, "name": name})
            save_user(uid, udata)
            ctx.user_data.clear()
            await msg.reply_text(
                f"{EM['check']} Added path for <code>{uid}</code>: <code>{name}</code>",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Playlist", callback_data=f"adm:ctrl_pl:{uid}")]]))


# ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
#   CLIENT BOT
# ══════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────

def cl_home_text(uid: str) -> str:
    udata = get_user(uid)
    sess  = get_vc_sess(uid)
    plan  = udata.get("plan", "basic")
    pl_inf = PLANS.get(plan, PLANS["basic"])
    dl    = days_left(udata)
    phone = udata.get("account_phone", "—")
    in_vc = sess.is_in_vc()
    link  = sess.active_link or "—"
    warn  = f"\n{EM['bell']} <b>Subscription {dl} din mein expire hogi!</b>" if 0 < dl <= 3 else ""
    return (
        f"{EM['mic']} <b>VC STREAMING BOT</b>\n"
        f"{'─'*26}\n"
        f"{EM['phone']} Account  : <code>{phone}</code>\n"
        f"{EM['live']}  Status   : {'🟢 Live — ' + link if in_vc else '🔴 Idle'}\n"
        f"{pl_inf['emoji']} Plan     : <b>{pl_inf['name']}</b>\n"
        f"{EM['clock']} Sub Left : <b>{dl} din</b>"
        f"{warn}"
    )


def kb_cl_home(uid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Account",     callback_data=f"c:account:{uid}"),
         InlineKeyboardButton("📢 Channels",    callback_data=f"c:channels:{uid}")],
        [InlineKeyboardButton("🎵 Playlist",    callback_data=f"c:playlist:{uid}"),
         InlineKeyboardButton("🎙 Live Ctrl",   callback_data=f"c:live:{uid}")],
        [InlineKeyboardButton("🔄 Restart Mon", callback_data=f"c:restmon:{uid}"),
         InlineKeyboardButton("📊 Status",      callback_data=f"c:status:{uid}")],
        [InlineKeyboardButton("📞 Contact Admin", callback_data=f"c:contact:{uid}")],
    ])


def kb_cl_account(uid: str) -> InlineKeyboardMarkup:
    udata = get_user(uid); sess = vc_sessions.get(uid)
    logged = bool(udata.get("account_phone") and sess and sess.pyro)
    rows = []
    if logged:
        rows.append([InlineKeyboardButton("🚪 Logout", callback_data=f"c:logout:{uid}")])
    else:
        rows.append([InlineKeyboardButton("🔑 Login",  callback_data=f"c:login:{uid}")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")])
    return InlineKeyboardMarkup(rows)


def kb_cl_channels(uid: str) -> InlineKeyboardMarkup:
    udata = get_user(uid)
    chs   = udata.get("channels", [])
    plan  = udata.get("plan", "basic")
    limit = PLANS[plan]["channels"]
    rows  = []
    for i, ch in enumerate(chs):
        name = ch.replace("https://t.me/", "@")
        rows.append([InlineKeyboardButton(f"❌ {name}", callback_data=f"c:rmch:{uid}:{i}")])
    if len(chs) < limit:
        rows.append([InlineKeyboardButton("➕ Add Channel", callback_data=f"c:addch:{uid}")])
    else:
        rows.append([InlineKeyboardButton(f"🔒 Limit: {limit} (Plan upgrade karo)", callback_data=f"c:home:{uid}")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")])
    return InlineKeyboardMarkup(rows)


def kb_cl_playlist(uid: str) -> InlineKeyboardMarkup:
    udata = get_user(uid)
    pl    = udata.get("playlist", [])
    plan  = udata.get("plan", "basic")
    limit = PLANS[plan]["audio"]
    rows  = []
    for i, p in enumerate(pl):
        rows.append([InlineKeyboardButton(f"❌ {i+1}. {p['name']}", callback_data=f"c:rmpl:{uid}:{i}")])
    can_add = (limit is None) or (len(pl) < limit)
    if can_add:
        rows.append([InlineKeyboardButton("➕ Add Audio", callback_data=f"c:addpl:{uid}"),
                     InlineKeyboardButton("🗑 Clear All", callback_data=f"c:clrpl:{uid}")])
    else:
        rows.append([InlineKeyboardButton(f"🔒 Max {limit} audio (Plan upgrade karo)", callback_data=f"c:home:{uid}")])
    rows.append([InlineKeyboardButton("⏱ Delay",  callback_data=f"c:delay:{uid}"),
                 InlineKeyboardButton("☕ Break",  callback_data=f"c:break:{uid}")])
    rows.append([InlineKeyboardButton("🔙 Back",   callback_data=f"c:home:{uid}")])
    return InlineKeyboardMarkup(rows)


def kb_cl_live(uid: str) -> InlineKeyboardMarkup:
    udata = get_user(uid)
    chs   = udata.get("channels", [])
    rows  = []
    for i, ch in enumerate(chs):
        name = ch.replace("https://t.me/", "@")
        if len(name) > 20: name = name[:20] + "…"
        rows.append([
            InlineKeyboardButton("▶️ Start", callback_data=f"c:lstart:{uid}:{i}"),
            InlineKeyboardButton(name,        callback_data=f"c:home:{uid}"),
            InlineKeyboardButton("⏹ Stop",  callback_data=f"c:lstop:{uid}:{i}"),
        ])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")])
    return InlineKeyboardMarkup(rows)


def make_client_handlers(uid: str):

    def _is_allowed(user_id: int) -> bool:
        owner = get_owner_id(uid)   # actual Telegram user ID (supports multi-bot)
        return str(user_id) == owner or user_id == ADMIN_ID

    def _is_admin(user_id: int) -> bool:
        owner = get_owner_id(uid)
        return user_id == ADMIN_ID and str(user_id) != owner

    def _cl_home_text_for(requester_id: int) -> str:
        base = cl_home_text(uid)
        if _is_admin(requester_id):
            info  = bot_info_cache.get(uid, {})
            fname = info.get("first_name", f"User {uid}")
            return (
                f"{EM['crown']} <b>ADMIN MODE</b> — controlling <b>{fname}</b>\n"
                f"{'─'*28}\n" + base
            )
        return base

    def _kb_home_for(requester_id: int) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton("👤 Account",     callback_data=f"c:account:{uid}"),
             InlineKeyboardButton("📢 Channels",    callback_data=f"c:channels:{uid}")],
            [InlineKeyboardButton("🎵 Playlist",    callback_data=f"c:playlist:{uid}"),
             InlineKeyboardButton("🎙 Live Ctrl",   callback_data=f"c:live:{uid}")],
            [InlineKeyboardButton("🔄 Restart Mon", callback_data=f"c:restmon:{uid}"),
             InlineKeyboardButton("📊 Status",      callback_data=f"c:status:{uid}")],
        ]
        if not _is_admin(requester_id):
            rows.append([InlineKeyboardButton("📞 Contact Admin", callback_data=f"c:contact:{uid}")])
        return InlineKeyboardMarkup(rows)

    async def cl_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or not _is_allowed(user.id): return
        udata = get_user(uid)
        # Save client's Telegram name whenever they /start
        if not _is_admin(user.id):
            cname = " ".join(filter(None, [user.first_name, user.last_name]))
            if cname and udata.get("client_name") != cname:
                udata["client_name"] = cname
                save_user(uid, udata)
        if not is_active(udata) and not _is_admin(user.id):
            await update.message.reply_text(
                f"{EM['warn']} <b>SUBSCRIPTION EXPIRED</b>\n\n"
                f"Aapki subscription khatam ho gayi hai.\n"
                f"{EM['bell']} Admin se renew karwao.",
                parse_mode=H); return
        await update.message.reply_text(
            _cl_home_text_for(user.id), parse_mode=H,
            reply_markup=_kb_home_for(user.id))

    async def cl_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        if not q or not q.from_user: return
        await q.answer()
        if not _is_allowed(q.from_user.id): return
        udata = get_user(uid)
        if not is_active(udata) and not _is_admin(q.from_user.id):
            await q.edit_message_text(
                f"{EM['warn']} <b>Subscription expired.</b>\nAdmin se contact karo.",
                parse_mode=H); return
        cb = q.data
        sess = get_vc_sess(uid)
        requester = q.from_user.id

        if cb == f"c:home:{uid}":
            ctx.user_data.clear()
            await q.edit_message_text(
                _cl_home_text_for(requester), parse_mode=H,
                reply_markup=_kb_home_for(requester))

        elif cb == f"c:account:{uid}":
            udata = get_user(uid)
            phone = udata.get("account_phone", "—")
            logged = bool(udata.get("account_phone") and sess.pyro)
            await q.edit_message_text(
                f"{EM['person']} <b>ACCOUNT</b>\n{'─'*20}\n\n"
                + (f"{EM['check']} Logged in: <code>{phone}</code>" if logged
                   else f"{EM['cross']} Not logged in.\n\nLogin karo account se."),
                parse_mode=H, reply_markup=kb_cl_account(uid))

        elif cb == f"c:login:{uid}":
            ctx.user_data["state"] = SC_PHONE
            await q.edit_message_text(
                f"{EM['phone']} <b>LOGIN</b>\n\nPhone number bhejo:\n<i>+919876543210</i>",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"c:account:{uid}")]]))

        elif cb == f"c:logout:{uid}":
            stop_monitors(sess)
            if sess.is_in_vc(): await vc_leave(sess)
            if sess.keepalive_task: sess.keepalive_task.cancel()
            if sess.pyro:
                try: await sess.pyro.stop()
                except Exception: pass
                sess.pyro = None
            udata = get_user(uid); udata["account_phone"] = None; save_user(uid, udata)
            await q.edit_message_text(
                f"{EM['check']} <b>Logged out.</b>", parse_mode=H, reply_markup=kb_cl_account(uid))

        elif cb == f"c:channels:{uid}":
            udata = get_user(uid)
            chs   = udata.get("channels", [])
            plan  = udata.get("plan", "basic")
            limit = PLANS[plan]["channels"]
            ch_text = "\n".join(f"{EM['live']}  <code>{c}</code>" for c in chs) if chs else "<i>Koi channel nahi.</i>"
            await q.edit_message_text(
                f"{EM['channel']} <b>CHANNELS</b> ({len(chs)}/{limit})\n{'─'*20}\n\n{ch_text}",
                parse_mode=H, reply_markup=kb_cl_channels(uid))

        elif cb == f"c:addch:{uid}":
            ctx.user_data["state"] = SC_CHANNEL
            await q.edit_message_text(
                f"{EM['channel']} <b>ADD CHANNEL</b>\n\nLink bhejo:\n<i>https://t.me/yourchannel</i>",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"c:channels:{uid}")]]))

        elif cb.startswith(f"c:rmch:{uid}:"):
            idx   = int(cb.split(":")[-1])
            udata = get_user(uid)
            chs   = udata.get("channels", [])
            if 0 <= idx < len(chs):
                removed = chs.pop(idx); udata["channels"] = chs; save_user(uid, udata)
                t = sess.monitor_tasks.pop(removed, None)
                if t: t.cancel()
            await q.edit_message_text(f"{EM['check']} Channel removed.", parse_mode=H, reply_markup=kb_cl_channels(uid))

        elif cb == f"c:playlist:{uid}":
            udata  = get_user(uid)
            pl     = udata.get("playlist", [])
            plan   = udata.get("plan", "basic")
            limit  = PLANS[plan]["audio"]
            pl_txt = "\n".join(f"  {i+1}. {p['name']}" for i, p in enumerate(pl)) if pl else "<i>Koi track nahi.</i>"
            lim_txt = f"/{limit}" if limit else "/∞"
            await q.edit_message_text(
                f"{EM['music']} <b>PLAYLIST</b> ({len(pl)}{lim_txt})\n{'─'*20}\n\n{pl_txt}\n\n"
                f"{EM['clock']} Delay: <b>{udata.get('delay',30)}s</b>   {EM['stop']} Break: <b>{udata.get('break_time',300)}s</b>",
                parse_mode=H, reply_markup=kb_cl_playlist(uid))

        elif cb == f"c:addpl:{uid}":
            ctx.user_data["state"] = SC_AUDIO
            await q.edit_message_text(
                f"{EM['music']} <b>ADD AUDIO</b>\n\nMP3/audio/voice file upload karo:",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"c:playlist:{uid}")]]))

        elif cb == f"c:clrpl:{uid}":
            udata = get_user(uid); udata["playlist"] = []; save_user(uid, udata)
            await q.edit_message_text(f"{EM['check']} Playlist cleared.", parse_mode=H, reply_markup=kb_cl_playlist(uid))

        elif cb.startswith(f"c:rmpl:{uid}:"):
            idx = int(cb.split(":")[-1])
            udata = get_user(uid); pl = udata.get("playlist", [])
            if 0 <= idx < len(pl): pl.pop(idx)
            udata["playlist"] = pl; save_user(uid, udata)
            await q.edit_message_text(f"{EM['check']} Track removed.", parse_mode=H, reply_markup=kb_cl_playlist(uid))

        elif cb == f"c:delay:{uid}":
            ctx.user_data["state"] = SC_DELAY
            udata = get_user(uid)
            await q.edit_message_text(
                f"{EM['clock']} <b>SET DELAY</b>\n\nCurrent: <b>{udata.get('delay',30)}s</b>\n\nSeconds bhejo:",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"c:playlist:{uid}")]]))

        elif cb == f"c:break:{uid}":
            ctx.user_data["state"] = SC_BREAK
            udata = get_user(uid)
            await q.edit_message_text(
                f"{EM['stop']} <b>SET BREAK</b>\n\nCurrent: <b>{udata.get('break_time',300)}s</b>\n\nSeconds bhejo:",
                parse_mode=H,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"c:playlist:{uid}")]]))

        elif cb == f"c:live:{uid}":
            udata = get_user(uid)
            chs   = udata.get("channels", [])
            if not chs:
                await q.edit_message_text(
                    f"{EM['warn']} Pehle channel add karo.", parse_mode=H,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")]])); return
            await q.edit_message_text(
                f"{EM['mic']} <b>LIVE CONTROL</b>\n\nChannel ka Start/Stop karo:",
                parse_mode=H, reply_markup=kb_cl_live(uid))

        elif cb.startswith(f"c:lstart:{uid}:") or cb.startswith(f"c:lstop:{uid}:"):
            action = "start" if "lstart" in cb else "stop"
            idx    = int(cb.split(":")[-1])
            udata  = get_user(uid); chs = udata.get("channels", [])
            if 0 <= idx < len(chs):
                link = chs[idx]
                await q.edit_message_text(f"{EM['clock']} Processing...", parse_mode=H)
                result = await (create_live_in(sess, link) if action == "start" else end_live_in(sess, link))
                await q.edit_message_text(result, parse_mode=H, reply_markup=kb_cl_live(uid))

        elif cb == f"c:restmon:{uid}":
            stop_monitors(sess)
            await asyncio.sleep(0.5)
            udata = get_user(uid)
            start_monitors(sess, uid, udata.get("channels", []))
            running = sum(1 for t in sess.monitor_tasks.values() if not t.done())
            await q.edit_message_text(
                f"{EM['refresh']} Monitors restarted ({running} running)",
                parse_mode=H, reply_markup=_kb_home_for(requester))

        elif cb == f"c:status:{uid}":
            try:
                await q.edit_message_text(
                    _cl_home_text_for(requester), parse_mode=H,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Refresh", callback_data=f"c:status:{uid}")],
                        [InlineKeyboardButton("🔙 Back",    callback_data=f"c:home:{uid}")],
                    ]))
            except Exception: pass

        elif cb == f"c:contact:{uid}":
            try:
                udata = get_user(uid)
                user  = q.from_user
                admin_info = await q.get_bot().get_chat(ADMIN_ID)
                admin_uname = f"@{admin_info.username}" if admin_info.username else "Admin"
                await q.edit_message_text(
                    f"{EM['phone']} <b>CONTACT ADMIN</b>\n\n"
                    f"Admin se direct baat karo:\n{admin_uname}\n\n"
                    f"<i>Apna User ID share karo: <code>{uid}</code></i>",
                    parse_mode=H,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💬 Admin se baat karo", url=f"https://t.me/{admin_info.username}" if admin_info.username else f"tg://user?id={ADMIN_ID}")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")],
                    ]))
                # Notify admin
                if admin_app_ref:
                    try:
                        await admin_app_ref.bot.send_message(
                            ADMIN_ID,
                            f"{EM['bell']} <b>Client Contact Request</b>\n\n"
                            f"{EM['person']} User ID: <code>{uid}</code>\n"
                            f"{EM['phone']} Account: <code>{udata.get('account_phone','—')}</code>\n"
                            f"{EM['zap']} @{user.username or 'no_username'} | {user.full_name}",
                            parse_mode=H)
                    except Exception: pass
            except Exception:
                await q.edit_message_text(
                    f"{EM['phone']} <b>Contact Admin</b>\n\nAdmin ka User ID: <code>{ADMIN_ID}</code>",
                    parse_mode=H,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"c:home:{uid}")]]))

    async def cl_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not ctx.user_data: return
        msg = update.message
        if not msg or not msg.chat or msg.chat.type != "private": return
        caller_id = update.effective_user.id if update.effective_user else 0
        if not _is_allowed(caller_id): return
        udata = get_user(uid)
        if not is_active(udata) and not _is_admin(caller_id):
            await msg.reply_text(f"{EM['warn']} Subscription expired. Admin se contact karo."); return

        state = ctx.user_data.get("state")
        sess  = get_vc_sess(uid)

        if state == SC_PHONE:
            phone = (msg.text or "").strip()
            c = PyroClient(phone, api_id=APIID, api_hash=APIHASH, workdir=SESSION_DIR)
            await c.connect()
            try:
                sent = await c.send_code(phone)
                ctx.user_data.update({"tmp_phone": phone, "tmp_client": c,
                                       "tmp_hash": sent.phone_code_hash, "state": SC_OTP})
                await msg.reply_text(f"{EM['zap']} OTP bheja <code>{phone}</code> par!\n\nCode enter karo:", parse_mode=H)
            except Exception as e:
                await c.disconnect(); ctx.user_data.pop("state", None)
                await msg.reply_text(f"{EM['cross']} Error: <code>{e}</code>", parse_mode=H)

        elif state == SC_OTP:
            c = ctx.user_data.get("tmp_client")
            if not c: ctx.user_data.pop("state", None); return
            try:
                await c.sign_in(ctx.user_data["tmp_phone"], ctx.user_data["tmp_hash"], (msg.text or "").strip())
                await cl_finalize_login(msg, ctx, c, uid, sess)
            except Exception as e:
                if any(k in str(e).lower() for k in ("2fa", "password", "session_password")):
                    ctx.user_data["state"] = SC_PASS
                    await msg.reply_text(f"{EM['lock']} <b>2FA Password</b> enter karo:", parse_mode=H)
                else:
                    await c.disconnect(); ctx.user_data.pop("state", None)
                    await msg.reply_text(f"{EM['cross']} Login failed: <code>{e}</code>", parse_mode=H)

        elif state == SC_PASS:
            c = ctx.user_data.get("tmp_client")
            if not c: ctx.user_data.pop("state", None); return
            try:
                await c.check_password((msg.text or "").strip())
                await cl_finalize_login(msg, ctx, c, uid, sess)
            except Exception as e:
                await c.disconnect(); ctx.user_data.pop("state", None)
                await msg.reply_text(f"{EM['cross']} Password error: <code>{e}</code>", parse_mode=H)

        elif state == SC_CHANNEL:
            link = (msg.text or "").strip()
            if not link.startswith("https://t.me/"):
                await msg.reply_text(f"{EM['cross']} <code>https://t.me/</code> se shuru karo.", parse_mode=H); return
            udata = get_user(uid)
            plan  = udata.get("plan", "basic")
            limit = PLANS[plan]["channels"]
            chs   = udata.setdefault("channels", [])
            if len(chs) >= limit:
                await msg.reply_text(
                    f"{EM['lock']} <b>Channel limit reached!</b>\n"
                    f"Aapke {PLANS[plan]['name']} plan mein sirf <b>{limit}</b> channel allowed hai.\n"
                    f"Upgrade ke liye admin se contact karo.", parse_mode=H); return
            if link not in chs:
                chs.append(link); save_user(uid, udata)
                if sess.pyro: start_monitors(sess, uid, [link])
            ctx.user_data.pop("state", None)
            await msg.reply_text(
                f"{EM['check']} Channel added:\n<code>{link}</code>",
                parse_mode=H, reply_markup=kb_cl_channels(uid))

        elif state == SC_AUDIO:
            doc = msg.document or msg.audio or msg.voice
            udata = get_user(uid)
            plan  = udata.get("plan", "basic")
            limit = PLANS[plan]["audio"]
            pl    = udata.get("playlist", [])
            if limit and len(pl) >= limit:
                await msg.reply_text(
                    f"{EM['lock']} <b>Audio limit reached!</b>\n"
                    f"Aapke {PLANS[plan]['name']} plan mein sirf <b>{limit}</b> audio allowed hai.", parse_mode=H); return
            if doc:
                fname = getattr(doc, "file_name", None) or f"audio_{int(time.time())}.mp3"
                spath = os.path.join(AUDIO_DIR, f"{uid}_{fname}")
                try:
                    tf = await ctx.bot.get_file(doc.file_id)
                    await tf.download_to_drive(spath)
                    udata.setdefault("playlist", []).append({"path": spath, "name": fname})
                    save_user(uid, udata)
                    ctx.user_data.pop("state", None)
                    await msg.reply_text(
                        f"{EM['check']} Added: <code>{fname}</code>",
                        parse_mode=H, reply_markup=kb_cl_playlist(uid))
                except Exception as ex:
                    await msg.reply_text(f"{EM['cross']} Upload failed: <code>{ex}</code>", parse_mode=H)
            elif msg.text:
                path = (msg.text or "").strip()
                if not os.path.isfile(path):
                    await msg.reply_text(f"{EM['cross']} File not found: <code>{path}</code>", parse_mode=H); return
                name = os.path.basename(path)
                udata.setdefault("playlist", []).append({"path": path, "name": name})
                save_user(uid, udata)
                ctx.user_data.pop("state", None)
                await msg.reply_text(
                    f"{EM['check']} Added: <code>{name}</code>",
                    parse_mode=H, reply_markup=kb_cl_playlist(uid))

        elif state == SC_DELAY:
            try:
                val = int((msg.text or "").strip())
                udata = get_user(uid); udata["delay"] = val; save_user(uid, udata)
                ctx.user_data.pop("state", None)
                await msg.reply_text(f"{EM['check']} Delay: <b>{val}s</b>", parse_mode=H, reply_markup=kb_cl_playlist(uid))
            except ValueError:
                await msg.reply_text(f"{EM['cross']} Number bhejo.")

        elif state == SC_BREAK:
            try:
                val = int((msg.text or "").strip())
                udata = get_user(uid); udata["break_time"] = val; save_user(uid, udata)
                ctx.user_data.pop("state", None)
                await msg.reply_text(f"{EM['check']} Break: <b>{val}s</b>", parse_mode=H, reply_markup=kb_cl_playlist(uid))
            except ValueError:
                await msg.reply_text(f"{EM['cross']} Number bhejo.")

    return cl_start, cl_callback, cl_message


async def cl_finalize_login(msg, ctx, c: PyroClient, uid: str, sess: VCSession):
    me = await c.get_me()
    phone = c.me.phone_number if c.me else ctx.user_data.get("tmp_phone", "?")
    phone = f"+{phone}" if not str(phone).startswith("+") else phone
    for k in ("state", "tmp_client", "tmp_phone", "tmp_hash"): ctx.user_data.pop(k, None)
    udata = get_user(uid); udata["account_phone"] = phone; save_user(uid, udata)
    sess.pyro = c; sess.get_nt()
    sess.keepalive_task = asyncio.create_task(keepalive_loop(sess))
    start_monitors(sess, uid, udata.get("channels", []))
    name = me.first_name if me else phone
    await msg.reply_text(
        f"{EM['check']} <b>Login ho gaya!</b>\n{name} — <code>{phone}</code>",
        parse_mode=H, reply_markup=kb_cl_home(uid))


# ─────────────────────────────────────────────────────────────
# CLIENT BOT LAUNCH
# ─────────────────────────────────────────────────────────────

async def launch_client_bot(uid: str) -> bool:
    udata = get_user(uid)
    token = udata.get("bot_token")
    if not token: return False
    if uid in client_bots: return True
    try:
        app = ApplicationBuilder().token(token).build()
        cl_start, cl_callback, cl_message = make_client_handlers(uid)
        app.add_handler(CommandHandler("start", cl_start))
        app.add_handler(CallbackQueryHandler(cl_callback))
        app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, cl_message))
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        client_bots[uid] = app
        # Cache bot info
        try:
            me = await app.bot.get_me()
            bot_info_cache[uid] = {"username": me.username, "first_name": me.first_name}
        except Exception: pass
        # Auto-start Pyrogram
        sess = get_vc_sess(uid)
        await start_pyro_session(sess, uid)
        log.info(f"[{uid}] Client bot launched: @{bot_info_cache.get(uid,{}).get('username','?')}")
        return True
    except Exception as e:
        log.error(f"[{uid}] Launch failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# SUBSCRIPTION WATCHER & REMINDERS
# ─────────────────────────────────────────────────────────────

REMINDER_MSGS = [
    "{warn} <b>Subscription Reminder!</b>\n\nAapki subscription <b>{days} din</b> mein expire hogi.\n{cal} Expiry: <b>{date}</b>\n\n{bell} Admin se renew karwao warna bot band ho jayega.",
    "{fire} <b>Hey! Subscription expire hone wali hai!</b>\n\n{clock} Sirf <b>{days} din</b> bache hain!\n{cal} End Date: <b>{date}</b>\n\nAbhi renew karo {bell}",
    "{warn} <b>Important Notice</b>\n\nAapka VC Bot subscription <b>{days} din</b> mein khatam ho raha hai.\n{cal} Date: <b>{date}</b>\n\nContact Admin for renewal.",
]

EXPIRY_MSGS = [
    "{cross} <b>Subscription Expired</b>\n\nAapki subscription khatam ho gayi hai. Bot band ho gaya hai.\n\n{bell} Admin se contact karo renewal ke liye.",
    "{warn} <b>Bot Stopped!</b>\n\nAapki subscription expire ho gayi. Bot band ho gaya.\n\nRenew karo {rocket}",
]


async def send_reminder(uid: str, days: int, until: float, app: Application):
    tmpl = random.choice(REMINDER_MSGS)
    text = tmpl.format(
        warn=EM["warn"], fire=EM["fire"], clock=EM["clock"],
        bell=EM["bell"], cal=EM["cal"], days=days, date=fmt_date(until))
    try:
        await app.bot.send_message(int(uid), text, parse_mode=H)
        log.info(f"[{uid}] Reminder sent ({days} days left)")
    except Exception as e:
        log.warning(f"[{uid}] Reminder failed: {e}")


async def sub_watcher():
    while True:
        await asyncio.sleep(random.randint(4800, 7200))  # 80-120 min random
        data = load_data()
        for uid, udata in data.get("users", {}).items():
            dl    = days_left(udata)
            until = udata.get("subscribed_until", 0)
            app   = client_bots.get(uid)
            reminders_sent = udata.get("reminders_sent", [])

            # Expired — stop bot + notify
            if not is_active(udata):
                if uid in client_bots:
                    # Send expiry message
                    try:
                        if app:
                            text = random.choice(EXPIRY_MSGS).format(
                                cross=EM["cross"], warn=EM["warn"], bell=EM["bell"], rocket=EM["rocket"])
                            await app.bot.send_message(int(uid), text, parse_mode=H)
                    except Exception: pass
                    await stop_client_bot(uid)
                    # Notify admin
                    if admin_app_ref:
                        try:
                            info = bot_info_cache.get(uid, {})
                            await admin_app_ref.bot.send_message(
                                ADMIN_ID,
                                f"{EM['warn']} <b>Client Expired & Stopped</b>\n\n"
                                f"{EM['person']} ID: <code>{uid}</code>\n"
                                f"{EM['bot']}  Bot: @{info.get('username','?')}",
                                parse_mode=H)
                        except Exception: pass
                continue

            # Expiring soon (1-3 days) — send random reminders
            if 0 < dl <= 3:
                day_key = str(dl)
                # Random chance (60%) to send reminder if not already sent for this day
                if day_key not in reminders_sent and random.random() < 0.60:
                    if app:
                        await send_reminder(uid, dl, until, app)
                        udata["reminders_sent"] = reminders_sent + [day_key]
                        save_user(uid, udata)
                elif day_key in reminders_sent and random.random() < 0.30:
                    # Even after sending, 30% chance for extra reminder
                    if app:
                        await send_reminder(uid, dl, until, app)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def _exc_handler(loop, context):
    exc = context.get("exception")
    msg_str = str(exc) if exc else context.get("message", "")
    if any(k in msg_str for k in ("Peer id invalid", "ID not found", "not modified", "Message is not modified")):
        return
    loop.default_exception_handler(context)


async def main():
    global admin_app_ref
    asyncio.get_event_loop().set_exception_handler(_exc_handler)

    admin_app = ApplicationBuilder().token(ADMIN_TOKEN).build()
    admin_app_ref = admin_app
    admin_app.add_handler(CommandHandler("start", adm_start))
    admin_app.add_handler(CallbackQueryHandler(adm_callback))
    admin_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, adm_message))
    await admin_app.initialize()
    await admin_app.start()
    await admin_app.updater.start_polling(drop_pending_updates=True)
    log.info("Admin bot started")

    data = load_data()
    for uid, udata in data.get("users", {}).items():
        if is_active(udata):
            await launch_client_bot(uid)

    asyncio.create_task(sub_watcher())
    log.info("Platform fully running.")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        log.info("Shutting down...")
        for uid in list(client_bots.keys()):
            await stop_client_bot(uid)
        await admin_app.updater.stop()
        await admin_app.stop()
        await admin_app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
