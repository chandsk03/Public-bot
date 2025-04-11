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
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"  # Replace with your bot token
ADMIN_IDS = [7584086775]  # Replace with your admin user IDs
SESSION_FOLDER = "sessions"
BACKUP_FOLDER = "backups"
DB_FILE = "sessions.db"

# Ensure folders exist
os.makedirs(SESSION_FOLDER, exist_ok=True)
os.makedirs(BACKUP_FOLDER, exist_ok=True)

# Initialize database
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_name TEXT PRIMARY KEY,
            user_id INTEGER,
            phone_number TEXT,
            created_at TEXT,
            last_used TEXT,
            is_active INTEGER DEFAULT 1,
            proxy_config TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backups (
            backup_id TEXT PRIMARY KEY,
            session_name TEXT,
            backup_time TEXT,
            file_path TEXT,
            notes TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# Helper functions
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def validate_session_name(session_name: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_\-]+$', session_name))

def get_session_path(session_name: str) -> str:
    return os.path.join(SESSION_FOLDER, f"{session_name}.session")

def get_backup_path(session_name: str, timestamp: str) -> str:
    filename = f"{session_name}_{timestamp}.backup"
    return os.path.join(BACKUP_FOLDER, filename)

def format_proxy(proxy: Dict) -> str:
    if not proxy:
        return "None"
    return f"{proxy['scheme']}://{proxy['hostname']}:{proxy['port']}"

def parse_proxy(proxy_str: str) -> Optional[Dict]:
    if not proxy_str or proxy_str.lower() == "none":
        return None
    
    try:
        scheme, rest = proxy_str.split("://")
        hostname, port = rest.split(":")
        return {
            "scheme": scheme,
            "hostname": hostname,
            "port": int(port),
            "username": None,
            "password": None
        }
    except:
        return None

# Bot setup
bot = Client("session_manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Admin decorator
def admin_only(func):
    async def wrapper(client, message):
        if message.from_user.id not in ADMIN_IDS:
            await message.reply("⛔ You are not authorized to use this command.")
            return
        return await func(client, message)
    return wrapper

# Command handlers
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply(
        "👋 Welcome to the Advanced Telegram Session Manager Bot!\n\n"
        "Available commands:\n"
        "/list - List all sessions\n"
        "/create - Create a new session\n"
        "/delete - Delete a session\n"
        "/backup - Backup a session\n"
        "/restore - Restore a session from backup\n"
        "/info - Get session info\n"
        "/activate - Activate a session\n"
        "/deactivate - Deactivate a session\n"
        "/setproxy - Set proxy for a session\n"
        "/send - Send a message from a session\n"
        "/export - Export session string\n"
        "/import - Import session string\n"
        "/stats - Get bot statistics"
    )

@bot.on_message(filters.command("list") & filters.private)
@admin_only
async def list_sessions(client, message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT session_name, user_id, phone_number, is_active FROM sessions ORDER BY last_used DESC")
    sessions = cursor.fetchall()
    conn.close()
    
    if not sessions:
        await message.reply("No sessions found.")
        return
    
    response = "📋 Active Sessions:\n\n"
    for session in sessions:
        name, user_id, phone, is_active = session
        status = "✅" if is_active else "❌"
        response += f"{status} {name}"
        if user_id:
            response += f" (User ID: {user_id})"
        if phone:
            response += f" (Phone: {phone})"
        response += "\n"
    
    await message.reply(response)

@bot.on_message(filters.command("create") & filters.private)
@admin_only
async def create_session(client, message):
    session_name = " ".join(message.command[1:]).strip()
    
    if not session_name:
        await message.reply("Please provide a session name. Example: /create my_session")
        return
    
    if not validate_session_name(session_name):
        await message.reply("Invalid session name. Only letters, numbers, underscores and hyphens are allowed.")
        return
    
    session_path = get_session_path(session_name)
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
    
    # Initialize client with MemoryStorage to avoid file creation before successful login
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
        file_storage = FileStorage(session_name, SESSION_FOLDER)
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
            "INSERT INTO sessions (session_name, user_id, phone_number, created_at, last_used) VALUES (?, ?, ?, ?, ?)",
            (session_name, user.id, phone_number, datetime.now().isoformat(), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' created successfully!\n"
            f"User: {user.first_name} ({user.id})\n"
            f"Phone: {phone_number}"
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

@bot.on_message(filters.command("delete") & filters.private)
@admin_only
async def delete_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /delete my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(session_name)
    
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
        cursor.execute("DELETE FROM sessions WHERE session_name = ?", (session_name,))
        conn.commit()
        conn.close()
        
        await message.reply(f"✅ Session '{session_name}' deleted successfully.")
    except Exception as e:
        await message.reply(f"Error deleting session: {e}")

@bot.on_message(filters.command("backup") & filters.private)
@admin_only
async def backup_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /backup my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(session_name)
    
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = get_backup_path(session_name, timestamp)
    
    try:
        # Copy session file
        with open(session_path, "rb") as src, open(backup_path, "wb") as dst:
            dst.write(src.read())
        
        # Save backup record
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO backups (backup_id, session_name, backup_time, file_path) VALUES (?, ?, ?, ?)",
            (f"{session_name}_{timestamp}", session_name, datetime.now().isoformat(), backup_path)
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' backed up successfully!\n"
            f"Backup ID: {session_name}_{timestamp}\n"
            f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    except Exception as e:
        await message.reply(f"Error creating backup: {e}")

@bot.on_message(filters.command("restore") & filters.private)
@admin_only
async def restore_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a backup ID. Example: /restore my_session_20230101_123456")
        return
    
    backup_id = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT session_name, file_path FROM backups WHERE backup_id = ?", (backup_id,))
    backup = cursor.fetchone()
    conn.close()
    
    if not backup:
        await message.reply("Backup not found.")
        return
    
    session_name, backup_path = backup
    
    if not os.path.exists(backup_path):
        await message.reply("Backup file not found.")
        return
    
    session_path = get_session_path(session_name)
    
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
        cursor.execute(
            "UPDATE sessions SET last_used = ? WHERE session_name = ?",
            (datetime.now().isoformat(), session_name)
        )
        conn.commit()
        conn.close()
        
        await message.reply(f"✅ Session '{session_name}' restored successfully from backup {backup_id}.")
    except Exception as e:
        await message.reply(f"Error restoring session: {e}")

@bot.on_message(filters.command("info") & filters.private)
@admin_only
async def session_info(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /info my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sessions WHERE session_name = ?", (session_name,))
    session = cursor.fetchone()
    conn.close()
    
    if not session:
        await message.reply("Session not found.")
        return
    
    # Get backup count
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM backups WHERE session_name = ?", (session_name,))
    backup_count = cursor.fetchone()[0]
    conn.close()
    
    # Format response
    response = (
        f"📝 Session Info: {session_name}\n\n"
        f"🆔 User ID: {session[1] or 'Not available'}\n"
        f"📱 Phone: {session[2] or 'Not available'}\n"
        f"📅 Created: {datetime.fromisoformat(session[3]).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"⏱️ Last Used: {datetime.fromisoformat(session[4]).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🔌 Proxy: {format_proxy(json.loads(session[6])) if session[6] else 'None'}\n"
        f"📦 Backups: {backup_count}\n"
        f"🔘 Status: {'Active ✅' if session[5] else 'Inactive ❌'}"
    )
    
    await message.reply(response)

@bot.on_message(filters.command("activate") & filters.private)
@admin_only
async def activate_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /activate my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE sessions SET is_active = 1 WHERE session_name = ?", (session_name,))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Session '{session_name}' activated.")

@bot.on_message(filters.command("deactivate") & filters.private)
@admin_only
async def deactivate_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /deactivate my_session")
        return
    
    session_name = message.command[1]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE sessions SET is_active = 0 WHERE session_name = ?", (session_name,))
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Session '{session_name}' deactivated.")

@bot.on_message(filters.command("setproxy") & filters.private)
@admin_only
async def set_proxy(client, message):
    if len(message.command) < 3:
        await message.reply("Please provide a session name and proxy. Example: /setproxy my_session socks5://127.0.0.1:1080")
        return
    
    session_name = message.command[1]
    proxy_str = " ".join(message.command[2:])
    proxy = parse_proxy(proxy_str)
    
    if proxy is None:
        await message.reply("Invalid proxy format. Please use scheme://host:port (e.g. socks5://127.0.0.1:1080)")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE sessions SET proxy_config = ? WHERE session_name = ?",
        (json.dumps(proxy), session_name)
    conn.commit()
    conn.close()
    
    await message.reply(f"✅ Proxy for session '{session_name}' set to {format_proxy(proxy)}")

@bot.on_message(filters.command("send") & filters.private)
@admin_only
async def send_message(client, message):
    if len(message.command) < 4:
        await message.reply("Please provide session name, chat ID, and message. Example: /send my_session @channel Hello!")
        return
    
    session_name = message.command[1]
    chat_id = message.command[2]
    text = " ".join(message.command[3:])
    
    session_path = get_session_path(session_name)
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    # Get proxy config
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT proxy_config FROM sessions WHERE session_name = ?", (session_name,))
    proxy_config = cursor.fetchone()[0]
    conn.close()
    
    proxy = json.loads(proxy_config) if proxy_config else None
    
    # Initialize client
    app = Client(
        session_name,
        api_id=API_ID,
        api_hash=API_HASH,
        workdir=SESSION_FOLDER,
        proxy=proxy
    )
    
    try:
        await app.start()
        
        # Update last used time
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE sessions SET last_used = ? WHERE session_name = ?",
            (datetime.now().isoformat(), session_name)
        )
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
@admin_only
async def export_session(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a session name. Example: /export my_session")
        return
    
    session_name = message.command[1]
    session_path = get_session_path(session_name)
    
    if not os.path.exists(session_path):
        await message.reply("Session not found.")
        return
    
    try:
        storage = FileStorage(session_name, SESSION_FOLDER)
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
@admin_only
async def import_session(client, message):
    if len(message.command) < 3:
        await message.reply("Please provide a session name and session string. Example: /import my_session session_string")
        return
    
    session_name = message.command[1]
    session_string = " ".join(message.command[2:])
    
    session_path = get_session_path(session_name)
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
        file_storage = FileStorage(session_name, SESSION_FOLDER)
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
            "INSERT INTO sessions (session_name, user_id, created_at, last_used) VALUES (?, ?, ?, ?)",
            (session_name, user.id, datetime.now().isoformat(), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        
        await message.reply(
            f"✅ Session '{session_name}' imported successfully!\n"
            f"User: {user.first_name} ({user.id})"
        )
    except RPCError as e:
        await message.reply(f"Error importing session: {e}")
    finally:
        try:
            await temp_client.disconnect()
        except:
            pass

@bot.on_message(filters.command("stats") & filters.private)
@admin_only
async def bot_stats(client, message):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Session stats
    cursor.execute("SELECT COUNT(*) FROM sessions")
    total_sessions = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM sessions WHERE is_active = 1")
    active_sessions = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM backups")
    total_backups = cursor.fetchone()[0]
    
    # Oldest and newest sessions
    cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM sessions")
    oldest, newest = cursor.fetchone()
    
    conn.close()
    
    response = (
        "📊 Bot Statistics\n\n"
        f"📋 Total Sessions: {total_sessions}\n"
        f"✅ Active Sessions: {active_sessions}\n"
        f"📦 Total Backups: {total_backups}\n"
        f"📅 Oldest Session: {datetime.fromisoformat(oldest).strftime('%Y-%m-%d') if oldest else 'N/A'}\n"
        f"🆕 Newest Session: {datetime.fromisoformat(newest).strftime('%Y-%m-%d') if newest else 'N/A'}"
    )
    
    await message.reply(response)

# Run the bot
if __name__ == "__main__":
    print("Starting session manager bot...")
    bot.run()