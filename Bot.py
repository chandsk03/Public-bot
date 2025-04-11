import os
import asyncio
import datetime
import sqlite3
import aiosqlite
import logging
from typing import Union, List, Dict, Any
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)
from pyrogram.errors import (
    FloodWait,
    PeerIdInvalid,
    ChannelInvalid,
    ChatAdminRequired,
    UserNotParticipant
)

# Configuration
class Config:
    API_ID = 25781839
    API_HASH = "20a3f2f168739259a180dcdd642e196c"
    BOT_TOKEN = "7585970885:AAGgo0Wc1GXEWd6XB_cuQgtp1-q61WAxnvw"
    ADMIN_IDS = [7584086775]
    DB_NAME = "bot_database.db"
    MEDIA_DIR = "media"
    PROXY_FILE = "proxy.txt"
    LOG_FILE = "bot.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 5

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ensure directories exist
os.makedirs(Config.MEDIA_DIR, exist_ok=True)

# Proxy Configuration
def load_proxy_config():
    if os.path.exists(Config.PROXY_FILE):
        with open(Config.PROXY_FILE, 'r') as f:
            proxy_data = f.read().strip()
            if proxy_data:
                return {
                    "scheme": "socks5",
                    "hostname": proxy_data.split(':')[0],
                    "port": int(proxy_data.split(':')[1]),
                    "username": proxy_data.split(':')[2] if len(proxy_data.split(':')) > 2 else None,
                    "password": proxy_data.split(':')[3] if len(proxy_data.split(':')) > 3 else None
                }
    return None

# Initialize Database (async version)
async def init_db():
    async with aiosqlite.connect(Config.DB_NAME) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP,
            language_code TEXT DEFAULT 'en'
        )
        """)
        
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            account_id INTEGER,
            account_username TEXT,
            auth_date TIMESTAMP,
            access_token TEXT,
            UNIQUE(user_id, account_id)
        )
        """)
        
        await db.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            target TEXT,
            target_type TEXT CHECK(target_type IN ('user', 'group', 'channel')),
            text TEXT,
            media_path TEXT,
            media_type TEXT CHECK(media_type IN (NULL, 'photo', 'video', 'document', 'audio')),
            parse_mode TEXT DEFAULT 'markdown',
            scheduled_time TIMESTAMP,
            status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'sent', 'failed')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 0
        )
        """)
        
        await db.execute("""
        CREATE TABLE IF NOT EXISTS group_participation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            group_link TEXT,
            group_title TEXT,
            group_id INTEGER,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'joined' CHECK(status IN ('joined', 'left', 'banned', 'kicked'))
        )
        """)
        
        await db.execute("""
        CREATE TABLE IF NOT EXISTS message_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            target TEXT,
            message_id INTEGER,
            sent_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT CHECK(status IN ('sent', 'deleted', 'edited'))
        )
        """)
        
        await db.commit()

# Database Helper Class (Async)
class AsyncDatabase:
    @staticmethod
    async def execute(query: str, params: tuple = (), fetch_one: bool = False, fetch_all: bool = False):
        async with aiosqlite.connect(Config.DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query, params)
            await db.commit()
            
            if fetch_one:
                result = await cursor.fetchone()
                return dict(result) if result else None
            elif fetch_all:
                result = await cursor.fetchall()
                return [dict(row) for row in result]
            return cursor.lastrowid
    
    @staticmethod
    async def user_exists(telegram_id: int) -> bool:
        result = await AsyncDatabase.execute(
            "SELECT 1 FROM users WHERE telegram_id = ?",
            (telegram_id,),
            fetch_one=True
        )
        return bool(result)
    
    @staticmethod
    async def create_user(user_data: Dict[str, Any]) -> int:
        user_id = await AsyncDatabase.execute(
            """INSERT INTO users (telegram_id, username, first_name, last_name, last_active, language_code)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)""",
            (user_data['id'], user_data.get('username'), 
             user_data.get('first_name'), user_data.get('last_name'),
             user_data.get('language_code', 'en'))
        )
        return user_id
    
    @staticmethod
    async def update_user_activity(telegram_id: int):
        await AsyncDatabase.execute(
            "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE telegram_id = ?",
            (telegram_id,)
        )
    
    @staticmethod
    async def schedule_message(data: Dict[str, Any]) -> int:
        message_id = await AsyncDatabase.execute(
            """INSERT INTO scheduled_messages 
            (user_id, target, target_type, text, media_path, media_type, parse_mode, scheduled_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (data['user_id'], data['target'], data['target_type'], data['text'],
             data.get('media_path'), data.get('media_type'), data.get('parse_mode', 'markdown'), 
             data['scheduled_time'])
        )
        return message_id
    
    @staticmethod
    async def get_pending_messages() -> List[Dict[str, Any]]:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return await AsyncDatabase.execute(
            """SELECT id, user_id, target, target_type, text, media_path, media_type, parse_mode, retry_count
            FROM scheduled_messages
            WHERE status = 'pending' AND scheduled_time <= ?""",
            (now,),
            fetch_all=True
        )
    
    @staticmethod
    async def update_message_status(message_id: int, status: str):
        await AsyncDatabase.execute(
            "UPDATE scheduled_messages SET status = ? WHERE id = ?",
            (status, message_id)
        )
    
    @staticmethod
    async def increment_retry_count(message_id: int):
        await AsyncDatabase.execute(
            "UPDATE scheduled_messages SET retry_count = retry_count + 1 WHERE id = ?",
            (message_id,)
        )
    
    @staticmethod
    async def add_group_participation(data: Dict[str, Any]):
        await AsyncDatabase.execute(
            """INSERT INTO group_participation 
            (user_id, group_link, group_title, group_id, status)
            VALUES (?, ?, ?, ?, ?)""",
            (data['user_id'], data['group_link'], data['group_title'], 
             data.get('group_id'), data.get('status', 'joined'))
        )
    
    @staticmethod
    async def get_user_scheduled_messages(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        return await AsyncDatabase.execute(
            """SELECT id, target, target_type, scheduled_time, status 
            FROM scheduled_messages 
            WHERE user_id = ? 
            ORDER BY scheduled_time DESC 
            LIMIT ?""",
            (user_id, limit),
            fetch_all=True
        )
    
    @staticmethod
    async def log_message_history(data: Dict[str, Any]):
        await AsyncDatabase.execute(
            """INSERT INTO message_history 
            (user_id, target, message_id, status)
            VALUES (?, ?, ?, ?)""",
            (data['user_id'], data['target'], data['message_id'], data['status'])
        )

# Initialize Pyrogram Client with Proxy
proxy_config = load_proxy_config()
app = Client(
    name="advanced_bot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    proxy=proxy_config,
    workers=100,
    workdir=os.getcwd(),
    parse_mode=enums.ParseMode.MARKDOWN
)

# Helper Functions
async def parse_schedule_time(time_str: str) -> Union[datetime.datetime, None]:
    try:
        # Try full datetime format
        return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        try:
            # Try time-only format (assume today)
            now = datetime.datetime.now()
            time_part = datetime.datetime.strptime(time_str, "%H:%M").time()
            return datetime.datetime.combine(now.date(), time_part)
        except ValueError:
            return None

async def save_media(message: Message) -> Union[tuple, None]:
    if not message.media:
        return None
    
    media_type = None
    file_ext = ""
    
    if message.photo:
        media_type = "photo"
        file_ext = ".jpg"
    elif message.video:
        media_type = "video"
        file_ext = ".mp4"
    elif message.document:
        media_type = "document"
        file_ext = os.path.splitext(message.document.file_name or "")[1] or ".bin"
    elif message.audio:
        media_type = "audio"
        file_ext = os.path.splitext(message.audio.file_name or "")[1] or ".mp3"
    
    if not media_type:
        return None
    
    file_id = message.media_group_id or message.id
    file_name = f"{file_id}{file_ext}"
    file_path = os.path.join(Config.MEDIA_DIR, file_name)
    
    try:
        await app.download_media(message, file_name=file_path)
        return file_name, media_type
    except Exception as e:
        logger.error(f"Failed to save media: {str(e)}")
        return None

async def send_message_with_retry(target: str, text: str, media_path: str = None, 
                                media_type: str = None, parse_mode: str = "markdown"):
    retries = 0
    last_error = None
    
    while retries < Config.MAX_RETRIES:
        try:
            if media_path:
                full_path = os.path.join(Config.MEDIA_DIR, media_path)
                
                if media_type == "photo":
                    return await app.send_photo(
                        chat_id=target,
                        photo=full_path,
                        caption=text,
                        parse_mode=parse_mode
                    )
                elif media_type == "video":
                    return await app.send_video(
                        chat_id=target,
                        video=full_path,
                        caption=text,
                        parse_mode=parse_mode
                    )
                elif media_type == "document":
                    return await app.send_document(
                        chat_id=target,
                        document=full_path,
                        caption=text,
                        parse_mode=parse_mode
                    )
                elif media_type == "audio":
                    return await app.send_audio(
                        chat_id=target,
                        audio=full_path,
                        caption=text,
                        parse_mode=parse_mode
                    )
            else:
                return await app.send_message(
                    chat_id=target,
                    text=text,
                    parse_mode=parse_mode
                )
                
        except FloodWait as e:
            wait_time = e.value
            logger.warning(f"Flood wait for {wait_time} seconds")
            await asyncio.sleep(wait_time)
        except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired, UserNotParticipant) as e:
            logger.error(f"Invalid target or permissions: {str(e)}")
            last_error = str(e)
            break  # No point retrying these errors
        except Exception as e:
            logger.error(f"Attempt {retries + 1} failed: {str(e)}")
            last_error = str(e)
            retries += 1
            await asyncio.sleep(Config.RETRY_DELAY)
    
    raise Exception(f"Failed after {retries} retries. Last error: {last_error}")

# Background Tasks
async def scheduled_messages_task():
    while True:
        try:
            messages = await AsyncDatabase.get_pending_messages()
            
            for msg in messages:
                try:
                    result = await send_message_with_retry(
                        target=msg['target'],
                        text=msg['text'],
                        media_path=msg['media_path'],
                        media_type=msg['media_type'],
                        parse_mode=msg['parse_mode']
                    )
                    
                    # Log successful message
                    await AsyncDatabase.log_message_history({
                        'user_id': msg['user_id'],
                        'target': msg['target'],
                        'message_id': result.id,
                        'status': 'sent'
                    })
                    
                    await AsyncDatabase.update_message_status(msg['id'], 'sent')
                    logger.info(f"Successfully sent message {msg['id']} to {msg['target']}")
                    
                except Exception as e:
                    logger.error(f"Failed to send message {msg['id']}: {str(e)}")
                    await AsyncDatabase.increment_retry_count(msg['id'])
                    
                    if msg['retry_count'] >= Config.MAX_RETRIES:
                        await AsyncDatabase.update_message_status(msg['id'], 'failed')
                        logger.error(f"Message {msg['id']} marked as failed after max retries")
                    
        except Exception as e:
            logger.error(f"Error in scheduled messages loop: {str(e)}")
        
        await asyncio.sleep(60)  # Check every minute

# Command Handlers
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    user = message.from_user
    await AsyncDatabase.update_user_activity(user.id)
    
    if not await AsyncDatabase.user_exists(user.id):
        await AsyncDatabase.create_user({
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'language_code': user.language_code
        })
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
        [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
        [InlineKeyboardButton("➕ Join Group", callback_data="join_group")],
        [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
    ])
    
    await message.reply_text(
        "🤖 **Advanced Bot System**\n\n"
        "Welcome! I can help you:\n"
        "- Schedule messages with text/media\n"
        "- Auto-join groups/channels\n"
        "- Manage your communications\n\n"
        "Select an option below to get started:",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex("^help$"))
async def help_callback(client: Client, callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "📚 **Help Guide**\n\n"
        "🔹 **Schedule Messages**:\n"
        "Reply to a message or type:\n"
        "`/schedule YYYY-MM-DD HH:MM @username`\n"
        "or\n"
        "`/schedule HH:MM @username` (for today)\n\n"
        "🔹 **Join Groups**:\n"
        "Send me any Telegram group invite link\n\n"
        "🔹 **View Scheduled**:\n"
        "Check your pending messages\n\n"
        "You can attach photos, videos, or documents when scheduling!",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")]
        ])
    )

@app.on_message(filters.command("schedule"))
async def schedule_command(client: Client, message: Message):
    user = message.from_user
    await AsyncDatabase.update_user_activity(user.id)
    
    if not message.reply_to_message and not (message.text or message.caption):
        await message.reply_text("Please reply to a message or include text to schedule.")
        return
    
    args = message.text.split()[1:] if message.text else message.caption.split()[1:]
    
    if len(args) < 2:
        await message.reply_text(
            "Invalid format. Use:\n"
            "`/schedule 2023-12-31 23:59 @username`\n"
            "or\n"
            "`/schedule 23:59 @username` (for today)\n\n"
            "You can also attach media files."
        )
        return
    
    time_str = f"{args[0]} {args[1]}" if len(args) > 2 else args[0]
    target = args[-1]
    
    scheduled_time = await parse_schedule_time(time_str)
    if not scheduled_time:
        await message.reply_text(
            "Invalid time format. Please use:\n"
            "- YYYY-MM-DD HH:MM for specific dates\n"
            "- HH:MM for today's time"
        )
        return
    
    target_type = "user" if target.startswith("@") else "group" if "+" in target else "channel"
    
    content_msg = message.reply_to_message if message.reply_to_message else message
    text = content_msg.text or content_msg.caption or ""
    media_info = await save_media(content_msg) if content_msg.media else (None, None)
    
    message_id = await AsyncDatabase.schedule_message({
        'user_id': user.id,
        'target': target,
        'target_type': target_type,
        'text': text,
        'media_path': media_info[0] if media_info else None,
        'media_type': media_info[1] if media_info else None,
        'scheduled_time': scheduled_time.strftime("%Y-%m-%d %H:%M:%S"),
        'parse_mode': "markdown"
    })
    
    reply_text = f"""
✅ **Message Scheduled Successfully**

📅 **When**: {scheduled_time.strftime('%Y-%m-%d %H:%M')}
📩 **To**: {target}
📝 **Content**: {text[:50] + '...' if len(text) > 50 else text}
"""
    
    if media_info:
        reply_text += f"📎 **Media**: {media_info[1].capitalize()}\n"
    
    reply_text += f"\nID: `{message_id}`"
    
    await message.reply_text(
        reply_text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Delete Schedule", callback_data=f"delete_{message_id}")]
        ])
    )

@app.on_callback_query(filters.regex(r"^delete_(\d+)$"))
async def delete_schedule_callback(client: Client, callback: CallbackQuery):
    message_id = int(callback.data.split("_")[1])
    await AsyncDatabase.execute(
        "DELETE FROM scheduled_messages WHERE id = ?",
        (message_id,)
    )
    await callback.answer("Schedule deleted successfully!")
    await callback.message.edit_text("🗑 Schedule has been deleted.")

@app.on_message(filters.regex(r"https?://t\.me/") | filters.regex(r"t\.me/"))
async def handle_invite_links(client: Client, message: Message):
    user = message.from_user
    await AsyncDatabase.update_user_activity(user.id)
    
    link = message.text.strip()
    try:
        chat = await client.join_chat(link)
        
        await AsyncDatabase.add_group_participation({
            'user_id': user.id,
            'group_link': link,
            'group_title': chat.title,
            'group_id': chat.id
        })
        
        await message.reply_text(
            f"✅ Successfully joined **{chat.title}**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_main")]
            ])
        )
    except Exception as e:
        logger.error(f"Failed to join {link}: {str(e)}")
        await message.reply_text(
            f"❌ Failed to join: {str(e)}\n\n"
            "Possible reasons:\n"
            "- Invalid invite link\n"
            "- Link expired\n"
            "- I don't have permission to join\n"
            "- The group is private and requires admin approval"
        )

# Admin Commands
@app.on_message(filters.user(Config.ADMIN_IDS) & filters.command("stats"))
async def admin_stats_command(client: Client, message: Message):
    users = await AsyncDatabase.execute(
        "SELECT COUNT(*) as count FROM users",
        fetch_one=True
    )
    messages = await AsyncDatabase.execute(
        "SELECT COUNT(*) as count FROM scheduled_messages",
        fetch_one=True
    )
    groups = await AsyncDatabase.execute(
        "SELECT COUNT(*) as count FROM group_participation",
        fetch_one=True
    )
    
    stats_text = f"""
📊 **Bot Statistics**

👥 Users: {users['count']}
📨 Scheduled Messages: {messages['count']}
👥 Groups/Channels: {groups['count']}
"""
    await message.reply_text(stats_text)

# Main Function
async def main():
    await init_db()
    await app.start()
    logger.info("Bot started successfully!")
    
    # Start background tasks
    asyncio.create_task(scheduled_messages_task())
    
    # Keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        logger.info("Bot shutdown complete")