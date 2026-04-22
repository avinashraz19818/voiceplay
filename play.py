"""
Advanced Voice Chat Audio Auto-Player Bot
==========================================

Features:
- Single owner (admin) — sirf aap hi bot chala sakte ho
- Ek user account (apna khud ka account) jo channel/group me admin hota hai
- Bot se multiple audio files (mp3 / voice / audio) set kar sakte ho
- Jaise hi channel me Live (Voice Chat) start hoga, user account auto join karega
- Saare set audios ek ke baad ek loop me play honge
  (audio1 -> gap -> audio2 -> gap -> audio3 -> BREAK -> repeat)
- Gap (audios ke beech) aur Break (cycle ke baad) bot se badal sakte ho
- Persistent storage (restart ke baad bhi data safe)
- Encrypted session storage

Run:
    python bot.py

Pehli baar run karoge to /login bhejke apna user account add karo.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
    CallbackQueryHandler,
)

from pyrogram import Client, errors as pyro_errors
from pyrogram.raw import functions, types as raw_types

from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream, AudioQuality
from pytgcalls.exceptions import NoActiveGroupCall

from cryptography.fernet import Fernet

from config import (
    TOKEN, APIID, APIHASH, OWNER_ID,
    SESSION_DIR, AUDIO_DIR, DATA_FILE, ENCRYPTION_KEY_FILE,
    DEFAULT_GAP_SECONDS, DEFAULT_BREAK_SECONDS, LIVESTREAM_CHECK_INTERVAL,
)

# ─────────────────────────── Setup ───────────────────────────
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(AUDIO_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("VCBot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pytgcalls").setLevel(logging.WARNING)


# ─────────────────────── Encryption Manager ───────────────────────
class EncryptionManager:
    def __init__(self):
        if os.path.exists(ENCRYPTION_KEY_FILE):
            with open(ENCRYPTION_KEY_FILE, "rb") as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(ENCRYPTION_KEY_FILE, "wb") as f:
                f.write(self.key)
        self.cipher = Fernet(self.key)

    def encrypt(self, data: str) -> str:
        return self.cipher.encrypt(data.encode()).decode()

    def decrypt(self, data: str) -> str:
        return self.cipher.decrypt(data.encode()).decode()


encryption = EncryptionManager()


# ─────────────────────── Data Classes ───────────────────────
@dataclass
class AudioItem:
    file_id: str            # Telegram file_id (bot side)
    file_path: str          # local path on disk
    title: str
    duration: int = 0


@dataclass
class ChannelEntry:
    chat_id: int
    title: str = ""
    username: Optional[str] = None
    last_call_id: Optional[int] = None       # last seen group_call id
    playing: bool = False


@dataclass
class BotData:
    user_session: Optional[str] = None       # encrypted pyrogram session string
    user_phone: Optional[str] = None
    user_name: Optional[str] = None
    user_id: Optional[int] = None
    audios: List[Dict] = field(default_factory=list)
    channels: List[Dict] = field(default_factory=list)
    gap_seconds: int = DEFAULT_GAP_SECONDS
    break_seconds: int = DEFAULT_BREAK_SECONDS
    enabled: bool = True


# ─────────────────────── Persistent Store ───────────────────────
class Store:
    def __init__(self):
        self.data = self._load()

    def _load(self) -> BotData:
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                return BotData(**raw)
            except Exception as e:
                log.error(f"Data load failed, starting fresh: {e}")
        return BotData()

    def save(self):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(asdict(self.data), f, indent=2, ensure_ascii=False)

    # Audios -------------------------------------------------
    def add_audio(self, item: AudioItem):
        self.data.audios.append(asdict(item))
        self.save()

    def remove_audio(self, index: int) -> Optional[Dict]:
        if 0 <= index < len(self.data.audios):
            removed = self.data.audios.pop(index)
            self.save()
            try:
                if os.path.exists(removed["file_path"]):
                    os.remove(removed["file_path"])
            except Exception:
                pass
            return removed
        return None

    def clear_audios(self):
        for a in self.data.audios:
            try:
                if os.path.exists(a["file_path"]):
                    os.remove(a["file_path"])
            except Exception:
                pass
        self.data.audios = []
        self.save()

    def get_audios(self) -> List[AudioItem]:
        return [AudioItem(**a) for a in self.data.audios]

    # Channels -----------------------------------------------
    def add_channel(self, ch: ChannelEntry):
        for c in self.data.channels:
            if c["chat_id"] == ch.chat_id:
                return
        self.data.channels.append(asdict(ch))
        self.save()

    def remove_channel(self, chat_id: int):
        self.data.channels = [c for c in self.data.channels if c["chat_id"] != chat_id]
        self.save()

    def get_channels(self) -> List[ChannelEntry]:
        return [ChannelEntry(**c) for c in self.data.channels]

    def update_channel(self, chat_id: int, **kwargs):
        for c in self.data.channels:
            if c["chat_id"] == chat_id:
                c.update(kwargs)
                self.save()
                return


store = Store()


# ─────────────────────── User Account / VC Engine ───────────────────────
class VCEngine:
    """Manages the single user account, voice chat join, and looped audio playback."""

    def __init__(self):
        self.user: Optional[Client] = None
        self.calls: Optional[PyTgCalls] = None
        self.play_tasks: Dict[int, asyncio.Task] = {}    # chat_id -> task
        self.monitor_task: Optional[asyncio.Task] = None
        self._stop_flags: Dict[int, bool] = {}

    # -------- session lifecycle --------
    async def start_from_saved(self) -> bool:
        if not store.data.user_session:
            log.info("No saved user session — owner needs to /login")
            return False
        try:
            session_str = encryption.decrypt(store.data.user_session)
            self.user = Client(
                name="vc_user",
                api_id=APIID,
                api_hash=APIHASH,
                session_string=session_str,
                in_memory=True,
            )
            await self.user.start()
            me = await self.user.get_me()
            store.data.user_id = me.id
            store.data.user_name = me.first_name or me.username or ""
            store.save()

            self.calls = PyTgCalls(self.user)
            await self.calls.start()
            log.info(f"User account started: {store.data.user_name} ({me.id})")

            self.start_monitor()
            return True
        except Exception as e:
            log.error(f"Failed to start saved session: {e}")
            return False

    async def login_with_session_string(self, session_str: str) -> str:
        """Owner provides a Pyrogram v2 session string."""
        if self.user:
            try:
                await self.stop_all()
                await self.user.stop()
            except Exception:
                pass
            self.user = None

        client = Client(
            name="vc_user",
            api_id=APIID,
            api_hash=APIHASH,
            session_string=session_str,
            in_memory=True,
        )
        await client.start()
        me = await client.get_me()

        store.data.user_session = encryption.encrypt(session_str)
        store.data.user_id = me.id
        store.data.user_name = me.first_name or me.username or ""
        store.data.user_phone = me.phone_number or ""
        store.save()

        self.user = client
        self.calls = PyTgCalls(self.user)
        await self.calls.start()
        self.start_monitor()
        return f"{me.first_name} (@{me.username})" if me.username else me.first_name

    async def logout(self):
        await self.stop_all()
        if self.user:
            try:
                await self.user.log_out()
            except Exception:
                try:
                    await self.user.stop()
                except Exception:
                    pass
        self.user = None
        self.calls = None
        store.data.user_session = None
        store.data.user_id = None
        store.data.user_name = None
        store.data.user_phone = None
        store.save()

    # -------- channel monitoring --------
    def start_monitor(self):
        if self.monitor_task and not self.monitor_task.done():
            return
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self):
        log.info("Live-stream monitor started")
        while True:
            try:
                await asyncio.sleep(LIVESTREAM_CHECK_INTERVAL)
                if not store.data.enabled or not self.user:
                    continue
                for ch in store.get_channels():
                    try:
                        await self._check_channel(ch)
                    except Exception as e:
                        log.warning(f"Monitor error for {ch.chat_id}: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Monitor loop error: {e}")

    async def _check_channel(self, ch: ChannelEntry):
        try:
            peer = await self.user.resolve_peer(ch.chat_id)
        except Exception:
            return

        # Get group call info
        try:
            if isinstance(peer, raw_types.InputPeerChannel):
                full = await self.user.invoke(
                    functions.channels.GetFullChannel(channel=raw_types.InputChannel(
                        channel_id=peer.channel_id, access_hash=peer.access_hash
                    ))
                )
                call = getattr(full.full_chat, "call", None)
            else:
                full = await self.user.invoke(
                    functions.messages.GetFullChat(chat_id=peer.chat_id)
                )
                call = getattr(full.full_chat, "call", None)
        except Exception as e:
            log.debug(f"GetFull failed for {ch.chat_id}: {e}")
            return

        if call is None:
            # No active VC
            if ch.playing:
                await self.stop_play(ch.chat_id)
                store.update_channel(ch.chat_id, playing=False, last_call_id=None)
            return

        # Active VC exists
        if not ch.playing or ch.last_call_id != call.id:
            log.info(f"📡 Live detected in {ch.title or ch.chat_id} — joining & playing")
            store.update_channel(ch.chat_id, playing=True, last_call_id=call.id)
            await self.start_play(ch.chat_id)

    # -------- playback control --------
    async def start_play(self, chat_id: int):
        if chat_id in self.play_tasks and not self.play_tasks[chat_id].done():
            return
        self._stop_flags[chat_id] = False
        self.play_tasks[chat_id] = asyncio.create_task(self._play_loop(chat_id))

    async def stop_play(self, chat_id: int):
        self._stop_flags[chat_id] = True
        task = self.play_tasks.get(chat_id)
        if task:
            task.cancel()
            try:
                await task
            except Exception:
                pass
            self.play_tasks.pop(chat_id, None)
        try:
            if self.calls:
                await self.calls.leave_call(chat_id)
        except Exception:
            pass

    async def stop_all(self):
        for cid in list(self.play_tasks.keys()):
            await self.stop_play(cid)

    async def _play_loop(self, chat_id: int):
        """Plays all set audios in sequence with gaps and a break, looping forever."""
        try:
            while not self._stop_flags.get(chat_id):
                audios = store.get_audios()
                if not audios:
                    log.info(f"No audios set — waiting (chat {chat_id})")
                    await asyncio.sleep(15)
                    continue

                gap = max(0, store.data.gap_seconds)
                brk = max(0, store.data.break_seconds)

                for idx, audio in enumerate(audios, start=1):
                    if self._stop_flags.get(chat_id):
                        return
                    if not os.path.exists(audio.file_path):
                        log.warning(f"File missing: {audio.file_path}")
                        continue
                    try:
                        log.info(f"▶ Playing [{idx}/{len(audios)}] {audio.title} in {chat_id}")
                        await self.calls.play(
                            chat_id,
                            MediaStream(
                                audio.file_path,
                                audio_flags=MediaStream.Flags.REQUIRED,
                                video_flags=MediaStream.Flags.IGNORE,
                                audio_parameters=AudioQuality.HIGH,
                            ),
                        )
                        # Wait for the audio to finish (duration + small buffer).
                        duration = audio.duration if audio.duration > 0 else 60
                        waited = 0
                        while waited < duration + 2:
                            if self._stop_flags.get(chat_id):
                                return
                            await asyncio.sleep(1)
                            waited += 1

                        # gap between audios
                        if idx < len(audios) and gap > 0:
                            log.info(f"⏸ Gap {gap}s before next audio")
                            for _ in range(gap):
                                if self._stop_flags.get(chat_id):
                                    return
                                await asyncio.sleep(1)
                    except NoActiveGroupCall:
                        log.info(f"VC ended in {chat_id} — stopping loop")
                        store.update_channel(chat_id, playing=False, last_call_id=None)
                        return
                    except Exception as e:
                        log.error(f"Play error in {chat_id}: {e}")
                        await asyncio.sleep(3)

                # Break after one full cycle
                if brk > 0 and not self._stop_flags.get(chat_id):
                    log.info(f"☕ Break for {brk}s before next cycle")
                    for _ in range(brk):
                        if self._stop_flags.get(chat_id):
                            return
                        await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                if self.calls:
                    await self.calls.leave_call(chat_id)
            except Exception:
                pass


vc = VCEngine()


# ─────────────────────── Telegram Bot UI ───────────────────────
def is_owner(update: Update) -> bool:
    user = update.effective_user
    return user is not None and user.id == OWNER_ID


def main_menu_kb() -> InlineKeyboardMarkup:
    enabled = store.data.enabled
    rows = [
        [InlineKeyboardButton("🎵 Audios", callback_data="m:audios"),
         InlineKeyboardButton("📡 Channels", callback_data="m:channels")],
        [InlineKeyboardButton("⏱ Timings", callback_data="m:timings"),
         InlineKeyboardButton("👤 Account", callback_data="m:account")],
        [InlineKeyboardButton(
            "🟢 Auto-Play: ON" if enabled else "🔴 Auto-Play: OFF",
            callback_data="m:toggle"
        )],
        [InlineKeyboardButton("📊 Status", callback_data="m:status")],
    ]
    return InlineKeyboardMarkup(rows)


WELCOME = (
    "👋 *Welcome to VC Audio Auto-Player*\n\n"
    "Aap apna user account add karke channels me set karenge.\n"
    "Phir bot me audios upload kar do — jaise hi channel me Live shuru hoga,\n"
    "aapka user account auto join hokar saari audios *loop* me play karega.\n\n"
    "Use the buttons below:"
)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        await update.message.reply_text("⛔ Yeh private bot hai.")
        return
    await update.message.reply_text(WELCOME, parse_mode="Markdown", reply_markup=main_menu_kb())


# ── /login conversation: owner sends pyrogram session string ──
LOGIN_WAIT = 1


async def cmd_login(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return ConversationHandler.END
    await update.message.reply_text(
        "🔐 *Account add karne ke liye Pyrogram v2 SESSION STRING bhejo.*\n\n"
        "Generate karne ka tarika:\n"
        "1. https://t.me/SmartUtilBot ya kisi `Pyrogram String Session Generator` bot par jao,\n"
        "   ya is script se khud generate karo:\n\n"
        "```\nfrom pyrogram import Client\n"
        f"app = Client('me', api_id={APIID}, api_hash='YOUR_HASH', in_memory=True)\n"
        "with app:\n    print(app.export_session_string())\n```\n\n"
        "2. String yahan paste kar do (kisi ko share mat karna).\n\n"
        "Cancel karne ke liye /cancel",
        parse_mode="Markdown",
    )
    return LOGIN_WAIT


async def login_receive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return ConversationHandler.END
    session_str = (update.message.text or "").strip()
    try:
        await update.message.delete()        # security: delete msg
    except Exception:
        pass
    msg = await update.message.chat.send_message("⏳ Logging in...")
    try:
        name = await vc.login_with_session_string(session_str)
        await msg.edit_text(f"✅ Logged in as *{name}*", parse_mode="Markdown",
                            reply_markup=main_menu_kb())
    except Exception as e:
        await msg.edit_text(f"❌ Login failed: `{e}`", parse_mode="Markdown")
    return ConversationHandler.END


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.", reply_markup=main_menu_kb())
    return ConversationHandler.END


# ── Audio upload handler ──
async def on_audio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    msg = update.message
    audio = msg.audio or msg.voice or msg.document
    if not audio:
        return
    # voice / audio / mp3 doc
    if msg.document and not (msg.document.mime_type or "").startswith("audio"):
        return

    file = await ctx.bot.get_file(audio.file_id)
    title = (
        getattr(msg.audio, "title", None)
        or getattr(msg.audio, "file_name", None)
        or getattr(msg.document, "file_name", None)
        or f"audio_{int(time.time())}"
    )
    safe_name = "".join(c for c in title if c.isalnum() or c in "._-")[:60] or f"a_{int(time.time())}"
    ext = ".ogg" if msg.voice else ".mp3"
    if not safe_name.lower().endswith((".mp3", ".ogg", ".m4a", ".wav")):
        safe_name += ext
    path = os.path.join(AUDIO_DIR, f"{int(time.time()*1000)}_{safe_name}")
    await file.download_to_drive(path)

    duration = getattr(audio, "duration", 0) or 0
    item = AudioItem(file_id=audio.file_id, file_path=path,
                     title=title, duration=duration)
    store.add_audio(item)
    await msg.reply_text(
        f"✅ Audio added: *{title}*\nTotal saved: *{len(store.data.audios)}*",
        parse_mode="Markdown",
        reply_markup=audios_kb(),
    )


# ── Inline keyboards ──
def audios_kb() -> InlineKeyboardMarkup:
    rows = []
    for i, a in enumerate(store.data.audios):
        rows.append([
            InlineKeyboardButton(f"🎵 {i+1}. {a['title'][:30]}", callback_data=f"noop"),
            InlineKeyboardButton("🗑", callback_data=f"a:del:{i}"),
        ])
    rows.append([
        InlineKeyboardButton("🧹 Clear All", callback_data="a:clear"),
        InlineKeyboardButton("⬅️ Back", callback_data="m:back"),
    ])
    return InlineKeyboardMarkup(rows)


def channels_kb() -> InlineKeyboardMarkup:
    rows = []
    for c in store.get_channels():
        flag = "🟢" if c.playing else "⚪"
        rows.append([
            InlineKeyboardButton(f"{flag} {c.title or c.chat_id}", callback_data="noop"),
            InlineKeyboardButton("❌", callback_data=f"c:del:{c.chat_id}"),
        ])
    rows.append([InlineKeyboardButton("➕ Add Channel", callback_data="c:add")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="m:back")])
    return InlineKeyboardMarkup(rows)


def timings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⏱ Gap: {store.data.gap_seconds}s", callback_data="t:gap"),
         InlineKeyboardButton(f"☕ Break: {store.data.break_seconds}s", callback_data="t:brk")],
        [InlineKeyboardButton("⬅️ Back", callback_data="m:back")],
    ])


def account_kb() -> InlineKeyboardMarkup:
    if store.data.user_id:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"👤 {store.data.user_name}", callback_data="noop")],
            [InlineKeyboardButton("🚪 Logout", callback_data="acc:logout")],
            [InlineKeyboardButton("⬅️ Back", callback_data="m:back")],
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 Login (send /login)", callback_data="noop")],
        [InlineKeyboardButton("⬅️ Back", callback_data="m:back")],
    ])


# ── Conversation: ask channel / set times ──
ASK_CHANNEL, ASK_GAP, ASK_BREAK = range(10, 13)


async def callback_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    if q.from_user.id != OWNER_ID:
        await q.answer("⛔", show_alert=True); return
    await q.answer()
    data = q.data or ""

    if data == "noop":
        return

    # main menu
    if data == "m:back":
        await q.edit_message_text(WELCOME, parse_mode="Markdown", reply_markup=main_menu_kb()); return
    if data == "m:audios":
        text = "🎵 *Audios*\n\nNaya audio add karne ke liye sirf bot me .mp3 ya voice bhej do."
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=audios_kb()); return
    if data == "m:channels":
        await q.edit_message_text("📡 *Channels*", parse_mode="Markdown", reply_markup=channels_kb()); return
    if data == "m:timings":
        await q.edit_message_text("⏱ *Timings*", parse_mode="Markdown", reply_markup=timings_kb()); return
    if data == "m:account":
        await q.edit_message_text("👤 *Account*", parse_mode="Markdown", reply_markup=account_kb()); return
    if data == "m:toggle":
        store.data.enabled = not store.data.enabled
        store.save()
        if not store.data.enabled:
            await vc.stop_all()
        await q.edit_message_text(WELCOME, parse_mode="Markdown", reply_markup=main_menu_kb()); return
    if data == "m:status":
        await q.edit_message_text(build_status_text(), parse_mode="Markdown",
                                  reply_markup=main_menu_kb()); return

    # audios
    if data.startswith("a:del:"):
        idx = int(data.split(":")[2])
        store.remove_audio(idx)
        await q.edit_message_reply_markup(reply_markup=audios_kb()); return
    if data == "a:clear":
        store.clear_audios()
        await q.edit_message_reply_markup(reply_markup=audios_kb()); return

    # channels
    if data == "c:add":
        await q.edit_message_text(
            "📡 Channel add karne ke liye uska *@username* ya *invite link* ya *-100xxxx id* bhejo.\n"
            "Cancel: /cancel",
            parse_mode="Markdown",
        )
        ctx.user_data["next"] = ASK_CHANNEL
        return
    if data.startswith("c:del:"):
        cid = int(data.split(":")[2])
        await vc.stop_play(cid)
        store.remove_channel(cid)
        await q.edit_message_reply_markup(reply_markup=channels_kb()); return

    # timings
    if data == "t:gap":
        await q.edit_message_text("⏱ Naya *gap* seconds me bhejo (audios ke beech).",
                                  parse_mode="Markdown")
        ctx.user_data["next"] = ASK_GAP; return
    if data == "t:brk":
        await q.edit_message_text("☕ Naya *break* seconds me bhejo (cycle ke baad).",
                                  parse_mode="Markdown")
        ctx.user_data["next"] = ASK_BREAK; return

    # account
    if data == "acc:logout":
        await vc.logout()
        await q.edit_message_text("🚪 Logged out.", reply_markup=main_menu_kb()); return


async def text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    nxt = ctx.user_data.pop("next", None)
    if nxt is None:
        return
    txt = (update.message.text or "").strip()

    if nxt == ASK_GAP:
        try:
            store.data.gap_seconds = max(0, int(txt))
            store.save()
            await update.message.reply_text(f"✅ Gap set to {store.data.gap_seconds}s",
                                            reply_markup=timings_kb())
        except ValueError:
            await update.message.reply_text("❌ Number bhejo.")
        return

    if nxt == ASK_BREAK:
        try:
            store.data.break_seconds = max(0, int(txt))
            store.save()
            await update.message.reply_text(f"✅ Break set to {store.data.break_seconds}s",
                                            reply_markup=timings_kb())
        except ValueError:
            await update.message.reply_text("❌ Number bhejo.")
        return

    if nxt == ASK_CHANNEL:
        if not vc.user:
            await update.message.reply_text("❌ Pehle /login karke account add karo.")
            return
        try:
            chat = await vc.user.get_chat(txt)
            entry = ChannelEntry(
                chat_id=chat.id,
                title=chat.title or chat.first_name or str(chat.id),
                username=chat.username,
            )
            store.add_channel(entry)
            await update.message.reply_text(
                f"✅ Channel added: *{entry.title}*\n"
                f"⚠️ Make sure aapka user account is channel me admin/member ho.",
                parse_mode="Markdown",
                reply_markup=channels_kb(),
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Add failed: `{e}`", parse_mode="Markdown")
        return


def build_status_text() -> str:
    lines = ["📊 *Status*\n"]
    lines.append(f"• Auto-Play: {'🟢 ON' if store.data.enabled else '🔴 OFF'}")
    lines.append(f"• User: {store.data.user_name or '— not logged in —'}")
    lines.append(f"• Audios saved: {len(store.data.audios)}")
    lines.append(f"• Channels: {len(store.data.channels)}")
    lines.append(f"• Gap: {store.data.gap_seconds}s | Break: {store.data.break_seconds}s")
    if store.data.channels:
        lines.append("\n*Channels:*")
        for c in store.get_channels():
            flag = "🟢 playing" if c.playing else "⚪ idle"
            lines.append(f"  - {c.title}: {flag}")
    return "\n".join(lines)


# ─────────────────────── Main ───────────────────────
async def post_init(app: Application):
    await vc.start_from_saved()
    log.info("Bot ready ✅")


def main():
    if not TOKEN or "YAHAN" in TOKEN:
        raise SystemExit("❌ config.py me BOT_TOKEN set karo")
    if not OWNER_ID:
        raise SystemExit("❌ config.py me OWNER_ID set karo")
    if not APIID or not APIHASH or "YAHAN" in str(APIHASH):
        raise SystemExit("❌ config.py me API_ID / API_HASH set karo")

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", lambda u, c:
                                   u.message.reply_text(build_status_text(),
                                                        parse_mode="Markdown",
                                                        reply_markup=main_menu_kb())
                                   if is_owner(u) else None))

    login_conv = ConversationHandler(
        entry_points=[CommandHandler("login", cmd_login)],
        states={LOGIN_WAIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )
    app.add_handler(login_conv)

    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE | filters.Document.AUDIO, on_audio))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    log.info("Starting bot polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
