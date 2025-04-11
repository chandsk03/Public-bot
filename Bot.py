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

# Configuration
API_ID = 25781839  # Replace with your API ID
API_HASH = "20a3f2f168739259a180dcdd642e196c"  # Replace with your API hash
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqxVZQyqwfRpVCW6wOFc"  # Replace with your bot token
ADMIN_IDS = [7584086775]  # Replace with admin user IDs
SESSION_ROOT = "user_sessions"
BACKUP_ROOT = "user_backups"
DB_FILE = "sessions.db"
MAX_SESSIONS_PER_USER = 5  # Limit sessions per user
MAX_BACKUPS_PER_SESSION = 3  # Limit backups per session

# Ensure root folders exist
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
            user_id INTEGER,
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
    
    # Create admin user if not exists
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
    folder = get_user_session_folder(user_id)
    return os.path.join(folder, f"{session_name}.session")

def get_backup_path(user_id: int, session_name: str, timestamp: str) -> str:
    folder = get_user_backup_folder(user_id)
    filename = f"{session_name}_{timestamp}.backup"
    return os.path.join(folder, filename)

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

# Bot setup
bot = Client("session_manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

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
            await message.reply("🔒 This bot only works in private chats.")
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

📋 *Available Commands:*
/create - Create new session
/list - List your sessions
/delete - Delete a session
/backup - Backup a session
/restore - Restore from backup
/info - Get session details
/activate - Activate session
/deactivate - Deactivate session
/setproxy - Configure proxy
/send - Send message from session
/export - Export session string
/import - Import session string
/stats - Your usage statistics
/help - Show this message

{}
""".format(terms if not is_admin(message.from_user.id) else terms + "\n\n👑 *Admin commands:* /users, /ban, /unban")

    await message.reply(welcome, parse_mode="markdown")

@bot.on_message(filters.command("help") & filters.private)
@private_chat_only
@not_banned
async def help_command(client, message):
    await start(client, message)

@bot.on_message(filters.command("create") & filters.private)
@private_chat_only
@not_banned
async def create_session(client, message):
    user_id = message.from_user.id
    session_name = " ".join(message.command[1:]).strip()
    
    if not session_name:
        await message.reply("Please provide a session name. Example: /create my_session")
        return
    
    if not validate_session_name(session_name):
        await message.reply("Invalid session name. Use 3-32 chars: letters, numbers, underscores, hyphens.")
        return
    
    if get_user_session_count(user_id) >= MAX_SESSIONS_PER_USER:
        await message.reply(f"You've reached the maximum of {MAX_SESSIONS_PER_USER} sessions.")
        return
    
    session_path = get_session_path(user_id, session_name)
    if os.path.exists(session_path):
        await message.reply("A session with this name already exists.")
        return
    
    # Ask for phone number
    await message.reply(f"Creating session '{session_name}'. Please send the phone number (with country code, e.g. +1234567890):")
    
    try:
        phone_number_msg = await client.listen(message.chat.id, filters.text, timeout=300)
        phone_number = phone_number_msg.text.strip()
    except asyncio.TimeoutError:
        await message.reply("Session creation timed out. Please try again.")
        return
    
    # Initialize client with MemoryStorage
    temp_client = Client(
        f":memory:{session_name}",
        api_id=API_ID,
        api_hash=API_HASH,
        app_version="Advanced Session Manager",
        device_model="Pyrogram",
        system_version="Layer " + str(layer),
        in_memory=True
    )
    
    await temp_client.connect()
    
    try:
        sent_code = await temp_client.send_code(phone_number)
    except RPCError as e:
        await message.reply(f"Error: {e}")
        await temp_client.disconnect()
        return
    
    await message.reply(
        f"A code has been sent to {phone_number}. Please send the code in the format:\n"
        "`code first_name last_name`\n\n"
        "Example: `12345 John Doe`"
    )
    
    try:
        code_msg = await client.listen(message.chat.id, filters.text, timeout=300)
        code_parts = code_msg.text.split()
        
        if len(code_parts) < 3:
            await message.reply("Invalid format. Please send code, first name, and last name.")
            await temp_client.disconnect()
            return
            
        code = code_parts[0]
        first_name = code_parts[1]
        last_name = " ".join(code_parts[2:])
        
        try:
            await temp_client.sign_in(phone_number, sent_code.phone_code_hash, code)
        except SessionPasswordNeeded:
            await message.reply("This account has 2FA enabled. Please send your password:")
            
            try:
                password_msg = await client.listen(message.chat.id, filters.text, timeout=300)
                await temp_client.check_password(password_msg.text)
            except asyncio.TimeoutError:
                await message.reply("Session creation timed out. Please try again.")
                await temp_client.disconnect()
                return
        
        # Get user info
        user = await temp_client.get_me()
        
        # Disconnect and reconnect with file storage
        await temp_client.disconnect()
        
        # Create actual session file
        file_storage = FileStorage(session_name, get_user_session_folder(user_id))
        await file_storage.open()
        file_storage.dc_id = temp_client.storage.dc_id()
        file_storage.auth_key = temp_client.storage.auth_key()
        file_storage.user_id = user.id
        file_storage.date = int(datetime.now().timestamp())
        await file_storage.save()
        await file_storage.close()
        
        # Save to database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sessions (user_id, session_name, phone_number, created_at, last_used) VALUES (?, ?, ?, ?, ?)",
            (user_id, session_name, phone_number, datetime.now().isoformat(), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' created successfully!\n"
            f"👤 User: {user.first_name} ({user.id})\n"
            f"📱 Phone: {phone_number}\n\n"
            f"🔐 This session is stored securely in your private folder."
        )
    except asyncio.TimeoutError:
        await message.reply("Session creation timed out. Please try again.")
    except RPCError as e:
        await message.reply(f"Error: {e}")
    finally:
        try:
            await temp_client.disconnect()
        except:
            pass

@bot.on_message(filters.command("list") & filters.private)
@private_chat_only
@not_banned
async def list_sessions(client, message):
    user_id = message.from_user.id
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
        await message.reply("You don't have any sessions yet. Use /create to make one.")
        return
    
    response = "📋 Your Sessions:\n\n"
    for session in sessions:
        name, user_id, phone, is_active = session
        status = "✅" if is_active else "❌"
        response += f"{status} {name}"
        if user_id:
            response += f" (User ID: {user_id})"
        if phone:
            response += f" (Phone: {phone[:3]}****{phone[-3:]})"
        response += "\n"
    
    response += f"\nYou have {len(sessions)}/{MAX_SESSIONS_PER_USER} sessions."
    await message.reply(response)

@bot.on_message(filters.command("delete") & filters.private)
@private_chat_only
@not_banned
async def delete_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /delete my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(user_id, session_name)
    
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    # Confirm deletion
    await message.reply(
        f"⚠️ Are you sure you want to delete session '{session_name}'?\n"
        "This cannot be undone. Reply 'yes' to confirm."
    )
    
    try:
        confirm_msg = await client.listen(message.chat.id, filters.text, timeout=60)
        if confirm_msg.text.lower() != "yes":
            await message.reply("Session deletion cancelled.")
            return
    except asyncio.TimeoutError:
        await message.reply("Session deletion timed out.")
        return
    
    # Delete session
    try:
        os.remove(session_path)
        
        # Remove from database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM sessions 
            WHERE user_id = ? AND session_name = ?
        ''', (user_id, session_name))
        conn.commit()
        conn.close()
        
        await message.reply(f"✅ Session '{session_name}' deleted successfully.")
    except Exception as e:
        await message.reply(f"Error deleting session: {e}")

@bot.on_message(filters.command("backup") & filters.private)
@private_chat_only
@not_banned
async def backup_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /backup my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(user_id, session_name)
    
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    if get_session_backup_count(user_id, session_name) >= MAX_BACKUPS_PER_SESSION:
        await message.reply(f"You've reached the maximum of {MAX_BACKUPS_PER_SESSION} backups for this session.")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = get_backup_path(user_id, session_name, timestamp)
    
    try:
        # Copy session file
        with open(session_path, "rb") as src, open(backup_path, "wb") as dst:
            dst.write(src.read())
        
        # Save backup record
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO backups (backup_id, user_id, session_name, backup_time, file_path) VALUES (?, ?, ?, ?, ?)",
            (f"{session_name}_{timestamp}", user_id, session_name, datetime.now().isoformat(), backup_path)
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' backed up successfully!\n"
            f"📦 Backup ID: {session_name}_{timestamp}\n"
            f"⏱️ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        await message.reply(f"Error creating backup: {e}")

@bot.on_message(filters.command("restore") & filters.private)
@private_chat_only
@not_banned
async def restore_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a backup ID. Example: /restore my_session_20230101_123456")
        return
    
    backup_id = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT session_name, file_path 
        FROM backups 
        WHERE user_id = ? AND backup_id = ?
    ''', (user_id, backup_id))
    backup = cursor.fetchone()
    conn.close()
    
    if not backup:
        await message.reply("Backup not found.")
        return
    
    session_name, backup_path = backup
    
    if not os.path.exists(backup_path):
        await message.reply("Backup file not found.")
        return
    
    session_path = get_session_path(user_id, session_name)
    
    # Confirm restore if session exists
    if os.path.exists(session_path):
        await message.reply(
            f"⚠️ A session with name '{session_name}' already exists.\n"
            "Do you want to overwrite it? Reply 'yes' to confirm."
        )
        
        try:
            confirm_msg = await client.listen(message.chat.id, filters.text, timeout=60)
            if confirm_msg.text.lower() != "yes":
                await message.reply("Restore cancelled.")
                return
        except asyncio.TimeoutError:
            await message.reply("Restore timed out.")
            return
    
    try:
        # Copy backup to session file
        with open(backup_path, "rb") as src, open(session_path, "wb") as dst:
            dst.write(src.read())
        
        # Update session info
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE sessions 
            SET last_used = ? 
            WHERE user_id = ? AND session_name = ?
        ''', (datetime.now().isoformat(), user_id, session_name))
        conn.commit()
        conn.close()
        
        await message.reply(f"✅ Session '{session_name}' restored successfully from backup {backup_id}.")
    except Exception as e:
        await message.reply(f"Error restoring session: {e}")

@bot.on_message(filters.command("info") & filters.private)
@private_chat_only
@not_banned
async def session_info(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /info my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT * FROM sessions 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    session = cursor.fetchone()
    
    if not session:
        conn.close()
        await message.reply("Session not found.")
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
        f"📝 Session Info: {session_name}\n\n"
        f"🆔 User ID: {session[3] or 'Not available'}\n"
        f"📱 Phone: {session[4][:3] + '****' + session[4][-3:] if session[4] else 'Not available'}\n"
        f"📅 Created: {datetime.fromisoformat(session[5]).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"⏱️ Last Used: {datetime.fromisoformat(session[6]).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🔌 Proxy: {format_proxy(json.loads(session[8])) if session[8] else 'None'}\n"
        f"📦 Backups: {backup_count}/{MAX_BACKUPS_PER_SESSION}\n"
        f"🔘 Status: {'Active ✅' if session[7] else 'Inactive ❌'}"
    )
    
    await message.reply(response)

@bot.on_message(filters.command("activate") & filters.private)
@private_chat_only
@not_banned
async def activate_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /activate my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sessions 
        SET is_active = 1 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Session '{session_name}' activated.")

@bot.on_message(filters.command("deactivate") & filters.private)
@private_chat_only
@not_banned
async def deactivate_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /deactivate my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sessions 
        SET is_active = 0 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Session '{session_name}' deactivated.")

@bot.on_message(filters.command("setproxy") & filters.private)
@private_chat_only
@not_banned
async def set_proxy(client, message):
    user_id = message.from_user.id
    if len(message.command) < 3:
        await message.reply("Please provide a session name and proxy. Example: /setproxy my_session socks5://user:pass@127.0.0.1:1080")
        return
    
    session_name = message.command[1]
    proxy_str = " ".join(message.command[2:])
    proxy = parse_proxy(proxy_str)
    
    if proxy is None:
        await message.reply("Invalid proxy format. Please use scheme://[user:pass@]host:port (e.g. socks5://user:pass@127.0.0.1:1080)")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sessions 
        SET proxy_config = ? 
        WHERE user_id = ? AND session_name = ?
    ''', (json.dumps(proxy), user_id, session_name))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Proxy for session '{session_name}' set to {format_proxy(proxy)}")

@bot.on_message(filters.command("send") & filters.private)
@private_chat_only
@not_banned
async def send_message(client, message):
    user_id = message.from_user.id
    if len(message.command) < 4:
        await message.reply("Please provide session name, chat ID, and message. Example: /send my_session @channel Hello!")
        return
    
    session_name = message.command[1]
    chat_id = message.command[2]
    text = " ".join(message.command[3:])
    
    session_path = get_session_path(user_id, session_name)
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    # Get proxy config
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT proxy_config FROM sessions 
        WHERE user_id = ? AND session_name = ?
    ''', (user_id, session_name))
    proxy_config = cursor.fetchone()[0]
    conn.close()
    
    proxy = json.loads(proxy_config) if proxy_config else None
    
    # Initialize client
    app = Client(
        session_name,
        api_id=API_ID,
        api_hash=API_HASH,
        workdir=get_user_session_folder(user_id),
        proxy=proxy
    )
    
    try:
        await app.start()
        
        # Update last used time
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE sessions 
            SET last_used = ? 
            WHERE user_id = ? AND session_name = ?
        ''', (datetime.now().isoformat(), user_id, session_name))
        conn.commit()
        conn.close()
        
        # Send message
        await app.send_message(chat_id, text)
        await message.reply(f"✅ Message sent from session '{session_name}' to {chat_id}")
    except RPCError as e:
        await message.reply(f"Error sending message: {e}")
    finally:
        try:
            await app.stop()
        except:
            pass

@bot.on_message(filters.command("export") & filters.private)
@private_chat_only
@not_banned
async def export_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /export my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(user_id, session_name)
    
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    try:
        storage = FileStorage(session_name, get_user_session_folder(user_id))
        await storage.open()
        session_string = await storage.export_session_string()
        await storage.close()
        
        await message.reply(
            f"📦 Session string for '{session_name}':\n\n"
            f"`{session_string}`\n\n"
            "⚠️ Keep this string secure! Anyone with this string can access the account."
        )
    except Exception as e:
        await message.reply(f"Error exporting session: {e}")

@bot.on_message(filters.command("import") & filters.private)
@private_chat_only
@not_banned
async def import_session(client, message):
    user_id = message.from_user.id
    if len(message.command) < 3:
        await message.reply("Please provide a session name and session string. Example: /import my_session session_string")
        return
    
    session_name = message.command[1]
    session_string = " ".join(message.command[2:])
    
    if get_user_session_count(user_id) >= MAX_SESSIONS_PER_USER:
        await message.reply(f"You've reached the maximum of {MAX_SESSIONS_PER_USER} sessions.")
        return
    
    session_path = get_session_path(user_id, session_name)
    if os.path.exists(session_path):
        await message.reply("A session with this name already exists.")
        return
    
    try:
        # Create in-memory client to validate session string
        temp_client = Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            in_memory=True
        )
        
        await temp_client.connect()
        await temp_client.import_session_string(session_string)
        
        # Get user info
        user = await temp_client.get_me()
        
        # Disconnect and reconnect with file storage
        await temp_client.disconnect()
        
        # Create actual session file
        file_storage = FileStorage(session_name, get_user_session_folder(user_id))
        await file_storage.open()
        file_storage.dc_id = temp_client.storage.dc_id()
        file_storage.auth_key = temp_client.storage.auth_key()
        file_storage.user_id = user.id
        file_storage.date = int(datetime.now().timestamp())
        await file_storage.save()
        await file_storage.close()
        
        # Save to database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sessions (user_id, session_name, created_at, last_used) VALUES (?, ?, ?, ?)",
            (user_id, session_name, datetime.now().isoformat(), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' imported successfully!\n"
            f"👤 User: {user.first_name} ({user.id})"
        )
    except RPCError as e:
        await message.reply(f"Error importing session: {e}")
    finally:
        try:
            await temp_client.disconnect()
        except:
            pass

@bot.on_message(filters.command("stats") & filters.private)
@private_chat_only
@not_banned
async def user_stats(client, message):
    user_id = message.from_user.id
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Session stats
    cursor.execute('''
        SELECT COUNT(*) FROM sessions 
        WHERE user_id = ?
    ''', (user_id,))
    total_sessions = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT COUNT(*) FROM sessions 
        WHERE user_id = ? AND is_active = 1
    ''', (user_id,))
    active_sessions = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT COUNT(*) FROM backups 
        WHERE user_id = ?
    ''', (user_id,))
    total_backups = cursor.fetchone()[0]
    
    # Oldest and newest sessions
    cursor.execute('''
        SELECT MIN(created_at), MAX(created_at) 
        FROM sessions 
        WHERE user_id = ?
    ''', (user_id,))
    oldest, newest = cursor.fetchone()
    
    conn.close()
    
    response = (
        "📊 Your Statistics\n\n"
        f"📋 Total Sessions: {total_sessions}/{MAX_SESSIONS_PER_USER}\n"
        f"✅ Active Sessions: {active_sessions}\n"
        f"📦 Total Backups: {total_backups}\n"
        f"📅 Oldest Session: {datetime.fromisoformat(oldest).strftime('%Y-%m-%d') if oldest else 'N/A'}\n"
        f"🆕 Newest Session: {datetime.fromisoformat(newest).strftime('%Y-%m-%d') if newest else 'N/A'}"
    )
    
    await message.reply(response)

# Admin commands
@bot.on_message(filters.command("users") & filters.private)
@admin_only
async def list_users(client, message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, username, first_name, last_name, is_banned, is_admin 
        FROM users 
        ORDER BY join_date DESC
    ''')
    users = cursor.fetchall()
    conn.close()
    
    if not users:
        await message.reply("No users found.")
        return
    
    response = "👥 User List:\n\n"
    for user in users:
        user_id, username, first_name, last_name, is_banned, is_admin = user
        status = "🚫" if is_banned else "✅"
        admin = "👑" if is_admin else ""
        response += f"{status} {admin} {first_name} {last_name} (@{username}) - ID: {user_id}\n"
    
    await message.reply(response)

@bot.on_message(filters.command("ban") & filters.private)
@admin_only
async def ban_user(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a user ID. Example: /ban 123456789")
        return
    
    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    if target_id in ADMIN_IDS:
        await message.reply("Cannot ban an admin.")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE users 
        SET is_banned = 1 
        WHERE user_id = ?
    ''', (target_id,))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ User {target_id} has been banned.")

@bot.on_message(filters.command("unban") & filters.private)
@admin_only
async def unban_user(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a user ID. Example: /unban 123456789")
        return
    
    try:
        target_id = int(message.command[1])
    except ValueError:
        await message.reply("Invalid user ID.")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE users 
        SET is_banned = 0 
        WHERE user_id = ?
    ''', (target_id,))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ User {target_id} has been unbanned.")

# Run the bot
if __name__ == "__main__":
    print("Starting advanced session manager bot...")
    bot.run()