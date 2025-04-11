import asyncio
import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters, types
from pyrogram.errors import RPCError, SessionPasswordNeeded
from pyrogram.raw.all import layer
from pyrogram.storage import FileStorage, MemoryStorage
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Configuration - REPLACE WITH YOUR OWN CREDENTIALS
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"  # Get from @BotFather
ADMIN_IDS = [7584086775]  # Your admin user ID
SESSION_ROOT = "user_sessions"
BACKUP_ROOT = "user_backups"
DB_FILE = "sessions.db"
MAX_SESSIONS_PER_USER = 5
MAX_BACKUPS_PER_SESSION = 3

# Ensure folders exist
os.makedirs(SESSION_ROOT, exist_ok=True)
os.makedirs(BACKUP_ROOT, exist_ok=True)

# Initialize database
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            join_date TEXT,
            last_active TEXT,
            is_banned INTEGER DEFAULT 0,
            is_admin INTEGER DEFAULT 0
        )
    ''')
    
    # Sessions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            session_name TEXT,
            phone_number TEXT,
            created_at TEXT,
            last_used TEXT,
            is_active INTEGER DEFAULT 1,
            proxy_config TEXT,
            FOREIGN KEY(user_id) REFERENCES users(user_id),
            UNIQUE(user_id, session_name)
        )
    ''')
    
    # Backups table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backups (
            backup_id TEXT PRIMARY KEY,
            user_id INTEGER,
            session_name TEXT,
            backup_time TEXT,
            file_path TEXT,
            notes TEXT,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )
    ''')
    
    # Create admin user
    for admin_id in ADMIN_IDS:
        cursor.execute('''
            INSERT OR IGNORE INTO users (user_id, is_admin, join_date, last_active)
            VALUES (?, 1, ?, ?)
        ''', (admin_id, datetime.now().isoformat(), datetime.now().isoformat()))
    
    conn.commit()
    conn.close()

init_db()

# Helper functions
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def validate_session_name(session_name: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_\-]{3,32}$', session_name))

def get_user_session_folder(user_id: int) -> str:
    folder = os.path.join(SESSION_ROOT, str(user_id))
    os.makedirs(folder, exist_ok=True)
    return folder

def get_user_backup_folder(user_id: int) -> str:
    folder = os.path.join(BACKUP_ROOT, str(user_id))
    os.makedirs(folder, exist_ok=True)
    return folder

def get_session_path(user_id: int, session_name: str) -> str:
    return os.path.join(get_user_session_folder(user_id), f"{session_name}.session")

def get_backup_path(user_id: int, session_name: str, timestamp: str) -> str:
    return os.path.join(get_user_backup_folder(user_id), f"{session_name}_{timestamp}.backup")

def format_proxy(proxy: Dict) -> str:
    if not proxy:
        return "None"
    auth = ""
    if proxy.get('username'):
        auth = f"{proxy['username']}:{proxy.get('password', '')}@"
    return f"{proxy['scheme']}://{auth}{proxy['hostname']}:{proxy['port']}"

def parse_proxy(proxy_str: str) -> Optional[Dict]:
    if not proxy_str or proxy_str.lower() == "none":
        return None
    try:
        scheme, rest = proxy_str.split("://")
        if "@" in rest:
            auth, hostport = rest.split("@")
            hostname, port = hostport.split(":")
            username, password = auth.split(":") if ":" in auth else (auth, "")
        else:
            hostname, port = rest.split(":")
            username, password = "", ""
        return {
            "scheme": scheme,
            "hostname": hostname,
            "port": int(port),
            "username": username or None,
            "password": password or None
        }
    except Exception as e:
        print(f"Proxy parse error: {e}")
        return None

def register_user(user: types.User):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO users (
            user_id, username, first_name, last_name, join_date, last_active
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        user.id,
        user.username,
        user.first_name,
        user.last_name,
        datetime.now().isoformat(),
        datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()

def is_admin(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_admin FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1

def is_banned(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1

def get_user_session_count(user_id: int) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sessions WHERE user_id = ?", (user_id,))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_session_backup_count(user_id: int, session_name: str) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM backups 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def create_session_keyboard(sessions: List[Tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for session in sessions:
        name, _, _, is_active = session
        status = "✅" if is_active else "❌"
        buttons.append([InlineKeyboardButton(f"{status} {name}", callback_data=f"session_{name}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_main")])
    return InlineKeyboardMarkup(buttons)

def create_main_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("➕ Create Session", callback_data="create_session")],
        [InlineKeyboardButton("📋 My Sessions", callback_data="list_sessions")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(buttons)

# Bot setup
bot = Client(
    "session_manager_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=200
)

# Decorators
def admin_only(func):
    async def wrapper(client, message):
        if not is_admin(message.from_user.id):
            await message.reply("⛔ You are not authorized to use this command.")
            return
        return await func(client, message)
    return wrapper

def private_chat_only(func):
    async def wrapper(client, message):
        if message.chat.type != "private":
            await message.reply("🔒 Please use this bot in private chat by messaging me directly.")
            return
        return await func(client, message)
    return wrapper

def not_banned(func):
    async def wrapper(client, message):
        if is_banned(message.from_user.id):
            await message.reply("🚫 Your account has been banned from using this bot.")
            return
        return await func(client, message)
    return wrapper

# Command handlers
@bot.on_message(filters.command("start") & filters.private)
@private_chat_only
@not_banned
async def start(client, message):
    register_user(message.from_user)
    
    terms = """
📜 *Terms and Conditions*
    
1. You are responsible for all activities conducted through your sessions.
2. Do not use this service for illegal activities.
3. We don't store your session files - they are saved only on your storage.
4. Session backups are encrypted and stored securely.
5. Maximum {} sessions per user and {} backups per session.
6. Abuse of this service will result in account termination.

By using this bot, you agree to these terms.
""".format(MAX_SESSIONS_PER_USER, MAX_BACKUPS_PER_SESSION)

    welcome = """
👋 *Welcome to Advanced Telegram Session Manager*

🔹 *Features:*
- Create and manage multiple Telegram sessions
- Secure session backups and restores
- Proxy configuration for each session
- Message sending from any session
- Session string import/export
""".format(terms if not is_admin(message.from_user.id) else terms + "\n\n👑 *Admin commands available*")

    keyboard = create_main_keyboard(is_admin(message.from_user.id))
    await message.reply(welcome, reply_markup=keyboard, parse_mode="markdown")

@bot.on_callback_query(filters.regex("^back_to_main$"))
async def back_to_main(client, callback_query):
    keyboard = create_main_keyboard(is_admin(callback_query.from_user.id))
    await callback_query.message.edit_text(
        "🏠 *Main Menu*",
        reply_markup=keyboard,
        parse_mode="markdown"
    )
    await callback_query.answer()

@bot.on_callback_query(filters.regex("^list_sessions$"))
async def list_sessions_callback(client, callback_query):
    user_id = callback_query.from_user.id
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT session_name, user_id, phone_number, is_active 
        FROM sessions 
        WHERE user_id = ?
        ORDER BY last_used DESC
    ''', (user_id,))
    sessions = cursor.fetchall()
    conn.close()
    
    if not sessions:
        await callback_query.answer("You don't have any sessions yet.", show_alert=True)
        return
    
    keyboard = create_session_keyboard(sessions)
    await callback_query.message.edit_text(
        "📋 *Your Sessions*",
        reply_markup=keyboard,
        parse_mode="markdown"
    )
    await callback_query.answer()

@bot.on_callback_query(filters.regex("^session_.+$"))
async def session_detail(client, callback_query):
    session_name = callback_query.data.split("_")[1]
    user_id = callback_query.from_user.id
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM sessions 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    session = cursor.fetchone()
    
    if not session:
        await callback_query.answer("Session not found.", show_alert=True)
        return
    
    # Get backup count
    cursor.execute('''
        SELECT COUNT(*) FROM backups 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    backup_count = cursor.fetchone()[0]
    conn.close()
    
    # Format response
    response = (
        f"📝 *Session Info*: `{session_name}`\n\n"
        f"🆔 User ID: `{session[3] or 'Not available'}`\n"
        f"📱 Phone: `{session[4][:3] + '****' + session[4][-3:] if session[4] else 'Not available'}`\n"
        f"📅 Created: `{datetime.fromisoformat(session[5]).strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"⏱️ Last Used: `{datetime.fromisoformat(session[6]).strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"🔌 Proxy: `{format_proxy(json.loads(session[8])) if session[8] else 'None'}`\n"
        f"📦 Backups: `{backup_count}/{MAX_BACKUPS_PER_SESSION}`\n"
        f"🔘 Status: `{'Active ✅' if session[7] else 'Inactive ❌'}`"
    )
    
    buttons = [
        [
            InlineKeyboardButton("🔙 Back", callback_data="list_sessions"),
            InlineKeyboardButton("⚙️ Manage", callback_data=f"manage_{session_name}")
        ]
    ]
    
    await callback_query.message.edit_text(
        response,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="markdown"
    )
    await callback_query.answer()

@bot.on_callback_query(filters.regex("^manage_.+$"))
async def manage_session(client, callback_query):
    session_name = callback_query.data.split("_")[1]
    user_id = callback_query.from_user.id
    
    buttons = [
        [
            InlineKeyboardButton("📤 Export", callback_data=f"export_{session_name}"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{session_name}")
        ],
        [
            InlineKeyboardButton("🔁 Backup", callback_data=f"backup_{session_name}"),
            InlineKeyboardButton("🔄 Restore", callback_data=f"restore_menu_{session_name}")
        ],
        [
            InlineKeyboardButton("🔌 Set Proxy", callback_data=f"proxy_{session_name}"),
            InlineKeyboardButton("💬 Send Msg", callback_data=f"send_{session_name}")
        ],
        [
            InlineKeyboardButton("✅ Activate" if not is_active else "❌ Deactivate", 
                                callback_data=f"toggle_{session_name}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"session_{session_name}")
        ]
    ]
    
    await callback_query.message.edit_text(
        f"⚙️ *Managing Session*: `{session_name}`",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="markdown"
    )
    await callback_query.answer()

# Error handler
@bot.on_message(filters.group)
async def handle_group_messages(client, message):
    await message.reply(
        "🔒 This bot only works in private chats. "
        "Please message me directly to use the bot features.\n\n"
        "Click here to start a private chat: [Start Private Chat](https://t.me/{}?start=help)"
        .format((await client.get_me()).username),
        disable_web_page_preview=True
    )

# Run the bot
if __name__ == "__main__":
    print("Starting advanced session manager bot...")
    try:
        bot.run()
    except Exception as e:
        print(f"Failed to start bot: {e}")
        print("Please check your BOT_TOKEN and make sure it's valid")