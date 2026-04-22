"""
Advanced Voice Chat Auto-Joiner Bot - FULLY WORKING
Fixed login issues, proper conversation handling
"""

import asyncio
import json
import logging
import os
import random
import re
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

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
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.raw import functions, types
import aiofiles

from config import *

# ── Configuration ───────────────────────────────────────────────────────────
BOT_TOKEN = TOKEN
API_ID = APIID
API_HASH = APIHASH

# ── Conversation States ─────────────────────────────────────────────────
PHONE_STATE, CODE_STATE, PASSWORD_STATE, WAITING_FOR_CODE = range(4)
ADD_CHANNEL_STATE = 10
ADD_AUDIO_STATE = 11
EDIT_DELAY_STATE = 12
EDIT_BREAK_STATE = 13

# ── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("VCBot")

# ── Enums ─────────────────────────────────────────────────────────────
class AccountStatus(Enum):
    IDLE = "💤 Idle"
    IN_VC = "🎙 In Voice Chat"
    PLAYING = "🎵 Playing Audio"
    OFFLINE = "⚫ Offline"

# ── Data Classes ─────────────────────────────────────────────────────────
@dataclass
class AudioTrack:
    id: str
    filename: str
    original_name: str
    duration: float
    file_size: int
    added_at: float
    play_count: int = 0

@dataclass
class Account:
    phone: str
    name: str = ""
    user_id: int = 0
    status: AccountStatus = AccountStatus.IDLE
    current_chat: Optional[str] = None

@dataclass
class MonitoredChat:
    link: str
    is_active: bool = False
    last_active: float = 0

# ── State Manager ──────────────────────────────────────────────────────────
class StateManager:
    def __init__(self):
        self.data_file = DATA_FILE
        self.data = self.load()
    
    def load(self) -> dict:
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {
            "account": None,
            "channels": [],
            "audio_tracks": [],
            "settings": {
                "delay_between_tracks": DEFAULT_GAP_SECONDS,
                "break_after_playlist": DEFAULT_BREAK_SECONDS,
                "loop_playlist": True
            }
        }
    
    def save(self):
        with open(self.data_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)
    
    def get_account(self):
        return self.data.get("account")
    
    def set_account(self, account_data):
        self.data["account"] = account_data
        self.save()
    
    def remove_account(self):
        self.data["account"] = None
        self.save()
    
    def get_channels(self):
        return self.data.get("channels", [])
    
    def add_channel(self, link):
        if link not in self.data["channels"]:
            self.data["channels"].append(link)
            self.save()
    
    def remove_channel(self, link):
        if link in self.data["channels"]:
            self.data["channels"].remove(link)
            self.save()
    
    def get_audio_tracks(self):
        return self.data.get("audio_tracks", [])
    
    def add_audio_track(self, track):
        self.data["audio_tracks"].append(track)
        self.save()
    
    def remove_audio_track(self, track_id):
        self.data["audio_tracks"] = [t for t in self.data["audio_tracks"] if t["id"] != track_id]
        self.save()
    
    def clear_playlist(self):
        # Delete files
        for track in self.data["audio_tracks"]:
            filepath = os.path.join(AUDIO_DIR, track['filename'])
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass
        self.data["audio_tracks"] = []
        self.save()
    
    def get_settings(self):
        return self.data.get("settings", {})
    
    def update_settings(self, settings):
        self.data["settings"].update(settings)
        self.save()

state_manager = StateManager()

# ── Account Manager ────────────────────────────────────────────────────────
class AccountManager:
    def __init__(self):
        self.client: Optional[Client] = None
        self.account: Optional[Account] = None
        self.vc_state: Optional[dict] = None
        self.monitor_tasks: Dict[str, asyncio.Task] = {}
        self.monitored_chats: Dict[str, MonitoredChat] = {}
        self.keepalive_task: Optional[asyncio.Task] = None
        self.playback_task: Optional[asyncio.Task] = None
    
    def set_client(self, client: Client, account_info: dict):
        self.client = client
        self.account = Account(
            phone=account_info["phone"],
            name=account_info["name"],
            user_id=account_info["user_id"]
        )
        state_manager.set_account(account_info)
        log.info(f"Account set: {account_info['name']}")
    
    async def remove_account(self):
        if self.keepalive_task:
            self.keepalive_task.cancel()
        
        if self.playback_task:
            self.playback_task.cancel()
        
        for task in self.monitor_tasks.values():
            task.cancel()
        self.monitor_tasks.clear()
        
        if self.client:
            try:
                await self.client.stop()
            except:
                pass
            self.client = None
        
        self.account = None
        self.vc_state = None
        state_manager.remove_account()
        log.info("Account removed")
    
    async def join_voice_chat(self, chat_link: str) -> bool:
        if not self.client:
            log.error("No client available")
            return False
        
        try:
            # Extract username from link
            match = re.search(r"t\.me/([a-zA-Z0-9_]+)", chat_link)
            if not match:
                log.error(f"Cannot parse link: {chat_link}")
                return False
            
            username = match.group(1)
            log.info(f"Joining voice chat in: {username}")
            
            # Get chat
            chat = await self.client.get_chat(username)
            
            # Get full chat info
            if chat.type in (ChatType.SUPERGROUP, ChatType.CHANNEL):
                full = await self.client.invoke(
                    functions.channels.GetFullChannel(
                        channel=await self.client.resolve_peer(chat.id)
                    )
                )
            else:
                full = await self.client.invoke(
                    functions.messages.GetFullChat(chat_id=chat.id)
                )
            
            # Check for voice chat
            if not (hasattr(full.full_chat, "call") and full.full_chat.call):
                log.info(f"No active voice chat in {chat_link}")
                return False
            
            call = full.full_chat.call
            ssrc = random.randint(1_000_000_000, 2_147_483_647)
            
            # Join the voice chat
            await self.client.invoke(
                functions.phone.JoinGroupCall(
                    call=call,
                    join_as=types.InputPeerSelf(),
                    muted=False,
                    video_stopped=True,
                    params=types.DataJSON(
                        data=json.dumps({
                            "ssrc": ssrc,
                            "source": ssrc,
                            "source_groups": [],
                            "protocol": "0.1.0"
                        })
                    ),
                )
            )
            
            self.vc_state = {
                "chat_id": chat.id,
                "chat_link": chat_link,
                "call_id": call.id,
                "ssrc": ssrc
            }
            
            if self.account:
                self.account.status = AccountStatus.IN_VC
                self.account.current_chat = chat_link
            
            log.info(f"Successfully joined voice chat in {chat_link}")
            
            # Start playing playlist
            await self.start_playlist()
            
            return True
            
        except Exception as e:
            log.error(f"Join error: {e}")
            return False
    
    async def start_playlist(self):
        """Start playing the playlist"""
        if self.playback_task:
            self.playback_task.cancel()
        
        tracks = state_manager.get_audio_tracks()
        if tracks and self.vc_state:
            self.playback_task = asyncio.create_task(self._play_loop())
            log.info("Playback started")
    
    async def stop_playlist(self):
        """Stop playing the playlist"""
        if self.playback_task:
            self.playback_task.cancel()
            self.playback_task = None
        log.info("Playback stopped")
    
    async def _play_loop(self):
        """Simple playback loop"""
        if not self.vc_state:
            return
        
        settings = state_manager.get_settings()
        
        while True:
            tracks = state_manager.get_audio_tracks()
            if not tracks:
                await asyncio.sleep(5)
                continue
            
            if not settings.get("loop_playlist", True):
                break
            
            for idx, track_data in enumerate(tracks):
                if not self.vc_state:
                    return
                
                track = AudioTrack(**track_data)
                log.info(f"Playing: {track.original_name} ({idx+1}/{len(tracks)})")
                
                if self.account:
                    self.account.status = AccountStatus.PLAYING
                
                # Send voice message
                audio_path = os.path.join(AUDIO_DIR, track.filename)
                if os.path.exists(audio_path):
                    try:
                        with open(audio_path, 'rb') as f:
                            await self.client.send_voice(
                                chat_id=self.vc_state["chat_id"],
                                voice=f.read(),
                                duration=int(track.duration)
                            )
                        
                        # Update play count
                        track.play_count += 1
                        for t in state_manager.data["audio_tracks"]:
                            if t["id"] == track.id:
                                t["play_count"] = track.play_count
                                break
                        state_manager.save()
                        
                        # Wait for track duration + delay
                        delay = settings.get("delay_between_tracks", DEFAULT_GAP_SECONDS)
                        await asyncio.sleep(track.duration + delay)
                        
                    except Exception as e:
                        log.error(f"Play error: {e}")
                        await asyncio.sleep(5)
            
            # Break after full playlist
            if self.vc_state:
                if self.account:
                    self.account.status = AccountStatus.IN_VC
                break_duration = settings.get("break_after_playlist", DEFAULT_BREAK_SECONDS)
                log.info(f"Playlist completed. Taking {break_duration}s break")
                await asyncio.sleep(break_duration)
    
    async def leave_voice_chat(self):
        if self.vc_state:
            await self.stop_playlist()
            self.vc_state = None
            if self.account:
                self.account.status = AccountStatus.IDLE
                self.account.current_chat = None
            log.info("Left voice chat")
    
    async def detect_voice_chat(self, chat_link: str) -> bool:
        if not self.client:
            return False
        
        try:
            match = re.search(r"t\.me/([a-zA-Z0-9_]+)", chat_link)
            if not match:
                return False
            
            username = match.group(1)
            chat = await self.client.get_chat(username)
            
            if chat.type in (ChatType.SUPERGROUP, ChatType.CHANNEL):
                full = await self.client.invoke(
                    functions.channels.GetFullChannel(
                        channel=await self.client.resolve_peer(chat.id)
                    )
                )
            else:
                full = await self.client.invoke(
                    functions.messages.GetFullChat(chat_id=chat.id)
                )
            
            return hasattr(full.full_chat, "call") and full.full_chat.call is not None
        except Exception as e:
            log.error(f"Detect error: {e}")
            return False
    
    def start_monitoring(self, link: str):
        if link in self.monitor_tasks:
            return
        
        self.monitored_chats[link] = MonitoredChat(link=link)
        self.monitor_tasks[link] = asyncio.create_task(self._monitor_loop(link))
        log.info(f"Started monitoring: {link}")
    
    def stop_monitoring(self, link: str):
        if link in self.monitor_tasks:
            self.monitor_tasks[link].cancel()
            del self.monitor_tasks[link]
        if link in self.monitored_chats:
            del self.monitored_chats[link]
        state_manager.remove_channel(link)
        log.info(f"Stopped monitoring: {link}")
    
    async def _monitor_loop(self, link: str):
        was_active = False
        while True:
            try:
                if self.client:
                    active = await self.detect_voice_chat(link)
                    
                    if active and not was_active:
                        log.info(f"🎤 Voice chat detected in {link}!")
                        await self.join_voice_chat(link)
                        if link in self.monitored_chats:
                            self.monitored_chats[link].is_active = True
                            self.monitored_chats[link].last_active = time.time()
                        was_active = True
                    elif not active and was_active:
                        log.info(f"🔇 Voice chat ended in {link}")
                        if link in self.monitored_chats:
                            self.monitored_chats[link].is_active = False
                        was_active = False
                
                await asyncio.sleep(MONITOR_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Monitor error for {link}: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
    
    def get_monitored_chats(self):
        return list(self.monitored_chats.values())

account_manager = AccountManager()

# ── Audio File Manager ────────────────────────────────────────────────────
class AudioFileManager:
    @staticmethod
    async def save_audio_file(file, filename: str):
        os.makedirs(AUDIO_DIR, exist_ok=True)
        
        timestamp = int(time.time())
        ext = os.path.splitext(filename)[1] or ".ogg"
        safe_name = f"{timestamp}_{hashlib.md5(filename.encode()).hexdigest()[:8]}{ext}"
        filepath = os.path.join(AUDIO_DIR, safe_name)
        
        try:
            await file.download_to_drive(filepath)
            duration = AudioFileManager.get_duration(filepath)
            file_size = os.path.getsize(filepath)
            return safe_name, duration, file_size
        except Exception as e:
            log.error(f"Save error: {e}")
            return None, 0, 0
    
    @staticmethod
    def get_duration(filepath: str) -> float:
        # Estimate duration: assume 32KB per second for voice quality
        file_size = os.path.getsize(filepath)
        return max(5.0, file_size / 32000)

# ── Bot UI ─────────────────────────────────────────────────────────────
class BotUI:
    @staticmethod
    def main_menu(has_account=False):
        keyboard = []
        if has_account:
            keyboard = [
                [InlineKeyboardButton("🔗 Monitored Channels", callback_data="menu_channels")],
                [InlineKeyboardButton("🎵 Audio Playlist", callback_data="menu_playlist")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
                [InlineKeyboardButton("📊 Dashboard", callback_data="dashboard")],
                [InlineKeyboardButton("🔌 Leave Voice Chat", callback_data="leave_vc")],
                [InlineKeyboardButton("🗑️ Remove Account", callback_data="remove_account")],
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("📱 Add Account", callback_data="add_account")],
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
            ]
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def channels_menu():
        keyboard = [[InlineKeyboardButton("➕ Add Channel", callback_data="add_channel")]]
        for chat in account_manager.get_monitored_chats():
            status = "🟢" if chat.is_active else "⚪"
            display_name = chat.link[:35] if len(chat.link) <= 35 else chat.link[:32] + "..."
            keyboard.append([InlineKeyboardButton(f"{status} {display_name}", callback_data=f"channel_{chat.link}")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def playlist_menu():
        tracks = state_manager.get_audio_tracks()
        keyboard = [[InlineKeyboardButton("➕ Add Audio", callback_data="add_audio")]]
        for idx, track in enumerate(tracks):
            name = track['original_name'][:25] if len(track['original_name']) <= 25 else track['original_name'][:22] + "..."
            keyboard.append([InlineKeyboardButton(f"{idx+1}. {name}", callback_data=f"track_{track['id']}")])
        if tracks:
            keyboard.extend([
                [InlineKeyboardButton("▶️ Start Playback", callback_data="start_playback")],
                [InlineKeyboardButton("⏹️ Stop Playback", callback_data="stop_playback")],
                [InlineKeyboardButton("🗑️ Clear All", callback_data="clear_playlist")],
            ])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        return InlineKeyboardMarkup(keyboard)
    
    @staticmethod
    def settings_menu():
        settings = state_manager.get_settings()
        keyboard = [
            [InlineKeyboardButton(f"⏱️ Track Delay: {settings.get('delay_between_tracks', 3)}s", callback_data="edit_delay")],
            [InlineKeyboardButton(f"⏸️ Break: {settings.get('break_after_playlist', 30)}s", callback_data="edit_break")],
            [InlineKeyboardButton(f"🔄 Loop: {'✅ ON' if settings.get('loop_playlist', True) else '❌ OFF'}", callback_data="toggle_loop")],
            [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
        ]
        return InlineKeyboardMarkup(keyboard)

# ── Bot Handlers ───────────────────────────────────────────────────────────
class BotHandlers:
    def __init__(self):
        self.login_data: Dict[int, dict] = {}
    
    async def start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        has_account = account_manager.account is not None
        await update.message.reply_text(
            "🎙️ *Voice Chat Auto-Joiner Bot*\n\n"
            "I help you auto-join voice chats and play audio playlists!\n\n"
            "✨ *Features:*\n"
            "• Auto-join voice chats\n"
            "• Upload multiple audio files\n"
            "• Loop playback with delays\n"
            "• Professional dashboard\n\n"
            "Click the button below to get started!",
            parse_mode="Markdown",
            reply_markup=BotUI.main_menu(has_account)
        )
    
    async def callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        
        log.info(f"Callback: {data}")
        
        if data == "main_menu":
            has_account = account_manager.account is not None
            await query.edit_message_text(
                "🎙️ *Main Menu*",
                parse_mode="Markdown",
                reply_markup=BotUI.main_menu(has_account)
            )
        
        elif data == "menu_channels":
            await query.edit_message_text(
                "🔗 *Monitored Channels*\n\nChannels where I'll auto-join voice chats:",
                parse_mode="Markdown",
                reply_markup=BotUI.channels_menu()
            )
        
        elif data == "menu_playlist":
            tracks = state_manager.get_audio_tracks()
            text = f"🎵 *Audio Playlist*\n\nTotal tracks: {len(tracks)}" if tracks else "🎵 *Audio Playlist*\n\nNo tracks yet. Add some audio files!"
            await query.edit_message_text(
                text,
                parse_mode="Markdown",
                reply_markup=BotUI.playlist_menu()
            )
        
        elif data == "menu_settings":
            await query.edit_message_text(
                "⚙️ *Playlist Settings*",
                parse_mode="Markdown",
                reply_markup=BotUI.settings_menu()
            )
        
        elif data == "dashboard":
            await self.show_dashboard(query)
        
        elif data == "refresh":
            await self.show_dashboard(query)
        
        elif data == "add_account":
            await query.edit_message_text(
                "📱 *Add Account*\n\n"
                "Please send your phone number in international format.\n"
                "Example: `+919876543210`\n\n"
                "⚠️ Make sure to include country code!\n\n"
                "Type /cancel to cancel.",
                parse_mode="Markdown"
            )
        
        elif data == "remove_account":
            await account_manager.remove_account()
            await query.edit_message_text(
                "✅ Account removed successfully!",
                reply_markup=BotUI.main_menu(False)
            )
        
        elif data == "leave_vc":
            await account_manager.leave_voice_chat()
            await query.edit_message_text(
                "✅ Left voice chat!",
                reply_markup=BotUI.main_menu(True)
            )
        
        elif data == "add_channel":
            await query.edit_message_text(
                "🔗 *Add Channel*\n\n"
                "Send the channel link to monitor.\n"
                "Example: `https://t.me/yourchannel`\n\n"
                "⚠️ Your account must be an admin in this channel!\n\n"
                "Type /cancel to cancel.",
                parse_mode="Markdown"
            )
        
        elif data == "add_audio":
            await query.edit_message_text(
                "🎵 *Add Audio*\n\n"
                "Send me an audio file (MP3, OGG, or voice message).\n\n"
                "You can send multiple files.\n"
                "Type /done when finished.\n"
                "Type /cancel to cancel.",
                parse_mode="Markdown"
            )
        
        elif data == "start_playback":
            if account_manager.vc_state:
                await account_manager.start_playlist()
                await query.edit_message_text(
                    "▶️ Playback started!",
                    reply_markup=BotUI.playlist_menu()
                )
            else:
                await query.edit_message_text(
                    "❌ Not in a voice chat! Wait for auto-join or join manually.",
                    reply_markup=BotUI.playlist_menu()
                )
        
        elif data == "stop_playback":
            await account_manager.stop_playlist()
            await query.edit_message_text(
                "⏹️ Playback stopped!",
                reply_markup=BotUI.playlist_menu()
            )
        
        elif data == "clear_playlist":
            state_manager.clear_playlist()
            await query.edit_message_text(
                "🗑️ Playlist cleared!",
                reply_markup=BotUI.playlist_menu()
            )
        
        elif data == "edit_delay":
            await query.edit_message_text(
                f"⏱️ *Edit Track Delay*\n\n"
                f"Current delay: `{state_manager.get_settings().get('delay_between_tracks', 3)} seconds`\n\n"
                f"Send new delay (1-300 seconds):\n\n"
                f"Type /cancel to cancel.",
                parse_mode="Markdown"
            )
        
        elif data == "edit_break":
            await query.edit_message_text(
                f"⏸️ *Edit Playlist Break*\n\n"
                f"Current break: `{state_manager.get_settings().get('break_after_playlist', 30)} seconds`\n\n"
                f"Send new break (0-600 seconds):\n\n"
                f"Type /cancel to cancel.",
                parse_mode="Markdown"
            )
        
        elif data == "toggle_loop":
            settings = state_manager.get_settings()
            settings['loop_playlist'] = not settings.get('loop_playlist', True)
            state_manager.update_settings(settings)
            await query.edit_message_text(
                "⚙️ Settings updated!",
                reply_markup=BotUI.settings_menu()
            )
        
        elif data.startswith("channel_"):
            link = data.replace("channel_", "")
            chat = account_manager.monitored_chats.get(link)
            if chat:
                keyboard = [
                    [InlineKeyboardButton("⛔ Stop Monitoring", callback_data=f"stop_{link}")],
                    [InlineKeyboardButton("🔙 Back", callback_data="menu_channels")],
                ]
                status = "🟢 Active" if chat.is_active else "⚪ Inactive"
                await query.edit_message_text(
                    f"*Channel Details*\n\n"
                    f"🔗 `{link}`\n"
                    f"📊 Status: {status}\n"
                    f"⏱️ Last Active: {datetime.fromtimestamp(chat.last_active).strftime('%Y-%m-%d %H:%M:%S') if chat.last_active else 'Never'}",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        
        elif data.startswith("stop_"):
            link = data.replace("stop_", "")
            account_manager.stop_monitoring(link)
            await query.edit_message_text(
                "✅ Stopped monitoring!",
                reply_markup=BotUI.channels_menu()
            )
        
        elif data.startswith("track_"):
            track_id = data.replace("track_", "")
            track = next((t for t in state_manager.get_audio_tracks() if t['id'] == track_id), None)
            if track:
                keyboard = [
                    [InlineKeyboardButton("🗑️ Remove", callback_data=f"delete_{track_id}")],
                    [InlineKeyboardButton("🔙 Back", callback_data="menu_playlist")],
                ]
                await query.edit_message_text(
                    f"*Track Details*\n\n"
                    f"🎵 Name: `{track['original_name']}`\n"
                    f"⏱️ Duration: `{int(track.get('duration', 0))} seconds`\n"
                    f"📊 Plays: `{track.get('play_count', 0)}`\n"
                    f"📅 Added: {datetime.fromtimestamp(track['added_at']).strftime('%Y-%m-%d %H:%M:%S')}",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        
        elif data.startswith("delete_"):
            track_id = data.replace("delete_", "")
            track = next((t for t in state_manager.get_audio_tracks() if t['id'] == track_id), None)
            if track:
                filepath = os.path.join(AUDIO_DIR, track['filename'])
                if os.path.exists(filepath):
                    os.remove(filepath)
                state_manager.remove_audio_track(track_id)
            await query.edit_message_text(
                "✅ Track removed!",
                reply_markup=BotUI.playlist_menu()
            )
    
    async def show_dashboard(self, query):
        lines = ["📊 *Dashboard*\n" + "="*20 + "\n"]
        
        if account_manager.account:
            lines.append(f"*📱 Account:* {account_manager.account.name}")
            lines.append(f"*📊 Status:* {account_manager.account.status.value}")
            if account_manager.account.current_chat:
                lines.append(f"*🎯 In:* {account_manager.account.current_chat[:40]}")
        else:
            lines.append("*📱 Account:* ❌ Not configured")
        
        lines.append(f"\n*🔗 Monitored Channels:* {len(account_manager.get_monitored_chats())}")
        lines.append(f"*🎵 Playlist Tracks:* {len(state_manager.get_audio_tracks())}")
        
        settings = state_manager.get_settings()
        lines.append(f"\n*⚙️ Settings:*")
        lines.append(f"  • Delay between tracks: `{settings.get('delay_between_tracks', 3)}s`")
        lines.append(f"  • Break after playlist: `{settings.get('break_after_playlist', 30)}s`")
        lines.append(f"  • Loop playlist: `{'ON' if settings.get('loop_playlist', True) else 'OFF'}`")
        
        if account_manager.vc_state:
            lines.append(f"\n*🎙️ Currently in voice chat!*")
        
        await query.edit_message_text(
            "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh", callback_data="dashboard")],
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ])
        )
    
    # ── Login Handlers ─────────────────────────────────────────────
    async def login_phone(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        phone = update.message.text.strip()
        user_id = update.effective_user.id
        
        log.info(f"Login attempt for phone: {phone}")
        
        if not phone.startswith('+'):
            await update.message.reply_text(
                "❌ Please include country code!\n"
                "Example: `+919876543210`\n\n"
                "Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return PHONE_STATE
        
        try:
            # Create client
            client = Client(
                name=phone,
                api_id=API_ID,
                api_hash=API_HASH,
                workdir=SESSION_DIR,
                in_memory=False
            )
            
            await client.connect()
            
            # Send code
            sent_code = await client.send_code(phone)
            
            # Store session data
            self.login_data[user_id] = {
                "phone": phone,
                "client": client,
                "phone_code_hash": sent_code.phone_code_hash
            }
            
            await update.message.reply_text(
                f"✅ Code sent to {phone}\n\n"
                f"Please enter the verification code you received:\n"
                f"(Format: 12345)\n\n"
                f"Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return CODE_STATE
            
        except Exception as e:
            log.error(f"Login error: {e}")
            await update.message.reply_text(
                f"❌ Error: {str(e)[:200]}\n\n"
                f"Please check your phone number and try again.\n"
                f"Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return PHONE_STATE
    
    async def login_code(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        code = update.message.text.strip()
        user_id = update.effective_user.id
        
        log.info(f"Code received for user {user_id}")
        
        login_data = self.login_data.get(user_id)
        if not login_data:
            await update.message.reply_text(
                "❌ Session expired. Please start over with /start",
                parse_mode="Markdown"
            )
            return ConversationHandler.END
        
        client = login_data["client"]
        phone = login_data["phone"]
        
        try:
            # Sign in
            await client.sign_in(
                phone_number=phone,
                code=code,
                phone_code_hash=login_data["phone_code_hash"]
            )
            
            # Get user info
            me = await client.get_me()
            
            # Set account
            account_manager.set_client(client, {
                "phone": phone,
                "name": me.first_name or me.username or phone,
                "user_id": me.id
            })
            
            # Start monitoring saved channels
            for link in state_manager.get_channels():
                account_manager.start_monitoring(link)
            
            # Cleanup
            del self.login_data[user_id]
            
            await update.message.reply_text(
                f"✅ *Login Successful!*\n\n"
                f"👤 Name: {me.first_name or me.username}\n"
                f"📞 Phone: {phone}\n"
                f"🆔 ID: {me.id}\n\n"
                f"Now you can add channels to monitor and upload audio files!",
                parse_mode="Markdown",
                reply_markup=BotUI.main_menu(True)
            )
            return ConversationHandler.END
            
        except Exception as e:
            error_msg = str(e).lower()
            
            if "password" in error_msg or "two-step" in error_msg:
                await update.message.reply_text(
                    "🔐 *Two-Factor Authentication Enabled*\n\n"
                    "Please enter your cloud password:\n\n"
                    "Send /cancel to cancel.",
                    parse_mode="Markdown"
                )
                return PASSWORD_STATE
            else:
                await client.disconnect()
                del self.login_data[user_id]
                await update.message.reply_text(
                    f"❌ Error: {str(e)[:200]}\n\n"
                    f"Please try again with /start",
                    parse_mode="Markdown"
                )
                return ConversationHandler.END
    
    async def login_password(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        password = update.message.text.strip()
        user_id = update.effective_user.id
        
        log.info(f"Password received for user {user_id}")
        
        login_data = self.login_data.get(user_id)
        if not login_data:
            await update.message.reply_text("Session expired. Please use /start")
            return ConversationHandler.END
        
        client = login_data["client"]
        phone = login_data["phone"]
        
        try:
            await client.check_password(password)
            me = await client.get_me()
            
            account_manager.set_client(client, {
                "phone": phone,
                "name": me.first_name or me.username or phone,
                "user_id": me.id
            })
            
            for link in state_manager.get_channels():
                account_manager.start_monitoring(link)
            
            del self.login_data[user_id]
            
            await update.message.reply_text(
                f"✅ *Login Successful!*\n\n"
                f"👤 Name: {me.first_name or me.username}\n"
                f"📞 Phone: {phone}\n\n"
                f"Now you can add channels to monitor!",
                parse_mode="Markdown",
                reply_markup=BotUI.main_menu(True)
            )
            
        except Exception as e:
            await client.disconnect()
            del self.login_data[user_id]
            await update.message.reply_text(
                f"❌ Wrong password: {str(e)[:100]}\n\nPlease try again with /start",
                parse_mode="Markdown"
            )
        
        return ConversationHandler.END
    
    # ── Channel Handler ─────────────────────────────────────────────────────
    async def add_channel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        link = update.message.text.strip()
        
        log.info(f"Adding channel: {link}")
        
        if not link.startswith("https://t.me/"):
            await update.message.reply_text(
                "❌ Invalid link. Must start with `https://t.me/`\n\n"
                "Example: `https://t.me/yourchannel`\n\n"
                "Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return ADD_CHANNEL_STATE
        
        if link in account_manager.monitored_chats:
            await update.message.reply_text("ℹ️ Already monitoring this channel.")
            return ConversationHandler.END
        
        account_manager.start_monitoring(link)
        state_manager.add_channel(link)
        
        await update.message.reply_text(
            f"✅ *Now monitoring* {link}\n\n"
            f"I'll automatically join when a voice chat starts!",
            parse_mode="Markdown",
            reply_markup=BotUI.main_menu(True)
        )
        return ConversationHandler.END
    
    # ── Audio Handler ──────────────────────────────────────────────────────
    async def add_audio(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        message = update.message
        
        # Check for /done command
        if message.text and message.text.lower() == '/done':
            await message.reply_text(
                "✅ Audio upload finished!",
                reply_markup=BotUI.main_menu(True)
            )
            return ConversationHandler.END
        
        # Check for audio
        audio = None
        filename = None
        
        if message.audio:
            audio = message.audio
            filename = audio.file_name or "audio.mp3"
            log.info(f"Received audio: {filename}")
        elif message.voice:
            audio = message.voice
            filename = f"voice_{int(time.time())}.ogg"
            log.info("Received voice message")
        else:
            await message.reply_text(
                "❌ Please send an audio file (MP3, OGG) or voice message.\n\n"
                "Send /done when finished or /cancel to cancel.",
                parse_mode="Markdown"
            )
            return ADD_AUDIO_STATE
        
        # Download and save
        file = await message.bot.get_file(audio.file_id)
        safe_name, duration, file_size = await AudioFileManager.save_audio_file(file, filename)
        
        if safe_name:
            track = {
                "id": hashlib.md5(f"{safe_name}_{time.time()}".encode()).hexdigest()[:16],
                "filename": safe_name,
                "original_name": filename,
                "duration": max(5.0, duration),
                "file_size": file_size,
                "added_at": time.time(),
                "play_count": 0
            }
            state_manager.add_audio_track(track)
            
            await message.reply_text(
                f"✅ *Added to playlist:*\n"
                f"🎵 `{filename}`\n"
                f"⏱️ Duration: `{int(duration)} seconds`\n\n"
                f"Send more files, /done to finish, or /cancel to cancel.",
                parse_mode="Markdown"
            )
        else:
            await message.reply_text(
                "❌ Failed to save audio. Please try again.",
                parse_mode="Markdown"
            )
        
        return ADD_AUDIO_STATE
    
    # ── Settings Handlers ──────────────────────────────────────────────────
    async def edit_delay(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            value = int(update.message.text.strip())
            if 1 <= value <= 300:
                settings = state_manager.get_settings()
                settings['delay_between_tracks'] = value
                state_manager.update_settings(settings)
                await update.message.reply_text(
                    f"✅ Delay set to {value} seconds!",
                    reply_markup=BotUI.main_menu(True)
                )
            else:
                await update.message.reply_text(
                    "❌ Value must be between 1 and 300 seconds.\n\n"
                    "Send /cancel to cancel.",
                    parse_mode="Markdown"
                )
                return EDIT_DELAY_STATE
        except ValueError:
            await update.message.reply_text(
                "❌ Please send a valid number.\n\n"
                "Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return EDIT_DELAY_STATE
        return ConversationHandler.END
    
    async def edit_break(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        try:
            value = int(update.message.text.strip())
            if 0 <= value <= 600:
                settings = state_manager.get_settings()
                settings['break_after_playlist'] = value
                state_manager.update_settings(settings)
                await update.message.reply_text(
                    f"✅ Break set to {value} seconds!",
                    reply_markup=BotUI.main_menu(True)
                )
            else:
                await update.message.reply_text(
                    "❌ Value must be between 0 and 600 seconds.\n\n"
                    "Send /cancel to cancel.",
                    parse_mode="Markdown"
                )
                return EDIT_BREAK_STATE
        except ValueError:
            await update.message.reply_text(
                "❌ Please send a valid number.\n\n"
                "Send /cancel to cancel.",
                parse_mode="Markdown"
            )
            return EDIT_BREAK_STATE
        return ConversationHandler.END
    
    async def cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        # Cleanup login session
        if user_id in self.login_data:
            try:
                await self.login_data[user_id]["client"].disconnect()
            except:
                pass
            del self.login_data[user_id]
        
        await update.message.reply_text(
            "❌ Operation cancelled.",
            reply_markup=BotUI.main_menu(account_manager.account is not None)
        )
        return ConversationHandler.END

# ── Main ──────────────────────────────────────────────────────────────
async def load_saved_session():
    """Load saved session on startup"""
    account_data = state_manager.get_account()
    if not account_data:
        log.info("No saved account found")
        return
    
    phone = account_data["phone"]
    log.info(f"Loading saved session for {phone}")
    
    try:
        client = Client(
            name=phone,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir=SESSION_DIR,
            in_memory=False
        )
        
        await client.connect()
        
        # Check if session is valid
        try:
            me = await client.get_me()
            account_manager.set_client(client, {
                "phone": phone,
                "name": me.first_name or me.username or phone,
                "user_id": me.id
            })
            
            # Start monitoring channels
            for link in state_manager.get_channels():
                account_manager.start_monitoring(link)
            
            log.info(f"✅ Loaded session for {me.first_name or me.username}")
        except Exception as e:
            log.warning(f"Session expired for {phone}: {e}")
            state_manager.remove_account()
            
    except Exception as e:
        log.warning(f"Could not load session: {e}")
        state_manager.remove_account()

async def post_init(app: Application):
    """Initialize after bot starts"""
    os.makedirs(SESSION_DIR, exist_ok=True)
    os.makedirs(AUDIO_DIR, exist_ok=True)
    await load_saved_session()
    log.info("Bot is ready!")

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    handlers = BotHandlers()
    
    # Command handlers
    app.add_handler(CommandHandler("start", handlers.start))
    app.add_handler(CommandHandler("cancel", handlers.cancel))
    app.add_handler(CommandHandler("done", handlers.add_audio))
    
    # Login conversation - FIXED: No lambda, clean pattern matching
    login_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handlers.callback, pattern="^add_account$")],
        states={
            PHONE_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.login_phone)],
            CODE_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.login_code)],
            PASSWORD_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.login_password)],
        },
        fallbacks=[CommandHandler("cancel", handlers.cancel)],
        allow_reentry=True,
    )
    app.add_handler(login_conv)
    
    # Add channel conversation
    channel_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handlers.callback, pattern="^add_channel$")],
        states={ADD_CHANNEL_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.add_channel)]},
        fallbacks=[CommandHandler("cancel", handlers.cancel)],
        allow_reentry=True,
    )
    app.add_handler(channel_conv)
    
    # Add audio conversation
    audio_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handlers.callback, pattern="^add_audio$")],
        states={ADD_AUDIO_STATE: [MessageHandler(filters.AUDIO | filters.VOICE, handlers.add_audio)]},
        fallbacks=[CommandHandler("cancel", handlers.cancel), CommandHandler("done", handlers.add_audio)],
        allow_reentry=True,
    )
    app.add_handler(audio_conv)
    
    # Edit delay conversation
    delay_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handlers.callback, pattern="^edit_delay$")],
        states={EDIT_DELAY_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.edit_delay)]},
        fallbacks=[CommandHandler("cancel", handlers.cancel)],
        allow_reentry=True,
    )
    app.add_handler(delay_conv)
    
    # Edit break conversation
    break_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handlers.callback, pattern="^edit_break$")],
        states={EDIT_BREAK_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handlers.edit_break)]},
        fallbacks=[CommandHandler("cancel", handlers.cancel)],
        allow_reentry=True,
    )
    app.add_handler(break_conv)
    
    # IMPORTANT: Generic callback handler MUST be LAST
    app.add_handler(CallbackQueryHandler(handlers.callback))
    
    log.info("🚀 Bot started successfully!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
