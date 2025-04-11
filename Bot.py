import os
import asyncio
import datetime
import logging
import aiosqlite
import signal
import sys
from typing import Dict, List, Optional, Union
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
    UserNotParticipant,
    RPCError
)

# Enhanced Configuration
class Config:
    API_ID = 25781839
    API_HASH = "20a3f2f168739259a180dcdd642e196c"
    BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
    ADMIN_IDS = [7584086775]
    DB_NAME = "bot_database.db"
    MEDIA_DIR = "media"
    LOG_FILE = "bot.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEZONE = "UTC"
    MAX_MESSAGE_LENGTH = 4096
    SCHEDULE_CHECK_INTERVAL = 60  # seconds
    USER_ACTIVITY_TIMEOUT = 300  # seconds

# Advanced Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(Config.LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ensure directories exist
os.makedirs(Config.MEDIA_DIR, exist_ok=True)

# Database Manager
class DatabaseManager:
    def __init__(self):
        self.db_path = Config.DB_NAME
        self.conn = None
        self.lock = asyncio.Lock()
    
    async def connect(self):
        try:
            self.conn = await aiosqlite.connect(self.db_path)
            self.conn.row_factory = aiosqlite.Row
            await self._initialize_database()
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    async def _initialize_database(self):
        try:
            await self.conn.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA foreign_keys=ON;
                
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP,
                    language_code TEXT DEFAULT 'en',
                    is_admin BOOLEAN DEFAULT FALSE
                );
                
                CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    target TEXT NOT NULL,
                    target_type TEXT NOT NULL CHECK(target_type IN ('user', 'group', 'channel')),
                    text TEXT,
                    media_path TEXT,
                    media_type TEXT CHECK(media_type IN (NULL, 'photo', 'video', 'document', 'audio')),
                    parse_mode TEXT DEFAULT 'markdown',
                    scheduled_time TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'sent', 'failed')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0
                );
                
                CREATE TABLE IF NOT EXISTS group_participation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    group_link TEXT NOT NULL,
                    group_title TEXT,
                    group_id INTEGER,
                    join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'joined' CHECK(status IN ('joined', 'left', 'banned', 'kicked'))
                );
                
                CREATE INDEX IF NOT EXISTS idx_scheduled_status ON scheduled_messages(status);
                CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_messages(scheduled_time);
                CREATE INDEX IF NOT EXISTS idx_user_active ON users(last_active);
            """)
            await self.conn.commit()
            logger.info("Database tables initialized")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    async def close(self):
        try:
            if self.conn:
                await self.conn.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")

    async def create_or_update_user(self, user_data: Dict) -> bool:
        try:
            async with self.lock:
                is_admin = 1 if user_data['id'] in Config.ADMIN_IDS else 0
                await self.conn.execute(
                    """INSERT INTO users 
                    (telegram_id, username, first_name, last_name, language_code, last_active, is_admin)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    language_code = excluded.language_code,
                    last_active = CURRENT_TIMESTAMP""",
                    (user_data['id'], user_data.get('username'), 
                     user_data.get('first_name'), user_data.get('last_name'),
                     user_data.get('language_code', 'en'), is_admin)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error creating/updating user: {e}")
            return False

    async def schedule_message(self, data: Dict) -> Optional[int]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """INSERT INTO scheduled_messages 
                    (user_id, target, target_type, text, media_path, media_type, parse_mode, scheduled_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (data['user_id'], data['target'], data['target_type'], data['text'],
                     data.get('media_path'), data.get('media_type'), 
                     data.get('parse_mode', 'markdown'), data['scheduled_time'])
                )
                await self.conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"Error scheduling message: {e}")
            return None

    async def get_pending_messages(self) -> List[Dict]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """SELECT * FROM scheduled_messages
                    WHERE status = 'pending' AND scheduled_time <= datetime('now')
                    ORDER BY scheduled_time ASC
                    LIMIT 100"""
                )
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting pending messages: {e}")
            return []

    async def update_message_status(self, message_id: int, status: str) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    "UPDATE scheduled_messages SET status = ? WHERE id = ?",
                    (status, message_id)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating message status: {e}")
            return False

# Bot Utilities
class BotUtils:
    @staticmethod
    async def parse_schedule_time(time_str: str) -> Optional[datetime.datetime]:
        try:
            return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                now = datetime.datetime.now()
                time_part = datetime.datetime.strptime(time_str, "%H:%M").time()
                return datetime.datetime.combine(now.date(), time_part)
            except ValueError:
                return None

    @staticmethod
    async def save_media(client: Client, message: Message) -> Optional[tuple]:
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
        
        file_id = message.media_group_id or message.id
        file_name = f"{file_id}{file_ext}"
        file_path = os.path.join(Config.MEDIA_DIR, file_name)
        
        try:
            await client.download_media(message, file_name=file_path)
            return file_name, media_type
        except Exception as e:
            logger.error(f"Failed to save media: {e}")
            return None

    @staticmethod
    async def send_message_with_retry(
        client: Client,
        target: str,
        text: str,
        media_path: Optional[str] = None,
        media_type: Optional[str] = None,
        parse_mode: str = "markdown"
    ) -> bool:
        retries = 0
        while retries < Config.MAX_RETRIES:
            try:
                if media_path:
                    full_path = os.path.join(Config.MEDIA_DIR, media_path)
                    
                    if media_type == "photo":
                        await client.send_photo(
                            chat_id=target,
                            photo=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "video":
                        await client.send_video(
                            chat_id=target,
                            video=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "document":
                        await client.send_document(
                            chat_id=target,
                            document=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "audio":
                        await client.send_audio(
                            chat_id=target,
                            audio=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                else:
                    await client.send_message(
                        chat_id=target,
                        text=text,
                        parse_mode=parse_mode
                    )
                return True
            except FloodWait as e:
                logger.warning(f"Flood wait for {e.value} seconds")
                await asyncio.sleep(e.value)
            except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired, UserNotParticipant) as e:
                logger.error(f"Invalid target or permissions: {e}")
                break
            except Exception as e:
                logger.error(f"Attempt {retries + 1} failed: {e}")
                retries += 1
                await asyncio.sleep(Config.RETRY_DELAY)
        return False

# Initialize Pyrogram Client
app = Client(
    name="advanced_bot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workers=100,
    workdir=os.getcwd(),
    parse_mode=enums.ParseMode.MARKDOWN
)

# Initialize Database
db = DatabaseManager()

# Signal Handler for Graceful Shutdown
def handle_signal(signum, frame):
    logger.info(f"Received signal {signum}, initiating shutdown...")
    asyncio.create_task(shutdown())

async def shutdown():
    try:
        await db.close()
        if app.is_initialized:
            await app.stop()
        logger.info("Bot shutdown completed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        os._exit(0)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Command Handlers
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    try:
        user = message.from_user
        logger.info(f"Start command from {user.id} (@{user.username})")
        
        if not await db.create_or_update_user({
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'language_code': user.language_code
        }):
            await message.reply_text("⚠️ Database error. Please try again later.")
            return
        
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
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await message.reply_text("⚠️ An error occurred. Please try again later.")

@app.on_message(filters.command("schedule"))
async def schedule_command(client: Client, message: Message):
    try:
        user = message.from_user
        
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
        
        scheduled_time = await BotUtils.parse_schedule_time(time_str)
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
        media_info = await BotUtils.save_media(client, content_msg) if content_msg.media else (None, None)
        
        message_id = await db.schedule_message({
            'user_id': user.id,
            'target': target,
            'target_type': target_type,
            'text': text,
            'media_path': media_info[0] if media_info else None,
            'media_type': media_info[1] if media_info else None,
            'scheduled_time': scheduled_time.strftime("%Y-%m-%d %H:%M:%S"),
            'parse_mode': "markdown"
        })
        
        if not message_id:
            await message.reply_text("⚠️ Failed to schedule message. Please try again.")
            return
        
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
    except Exception as e:
        logger.error(f"Error in schedule command: {e}")
        await message.reply_text("⚠️ Failed to schedule message. Please try again.")

# Background Task
async def scheduled_messages_task():
    await db.connect()
    
    while True:
        try:
            messages = await db.get_pending_messages()
            
            for msg in messages:
                success = await BotUtils.send_message_with_retry(
                    client=app,
                    target=msg['target'],
                    text=msg['text'],
                    media_path=msg['media_path'],
                    media_type=msg['media_type'],
                    parse_mode=msg['parse_mode']
                )
                
                if success:
                    await db.update_message_status(msg['id'], 'sent')
                    logger.info(f"Successfully sent message {msg['id']} to {msg['target']}")
                else:
                    await db.update_message_status(msg['id'], 'failed')
                    logger.error(f"Failed to send message {msg['id']} after retries")
            
            await asyncio.sleep(Config.SCHEDULE_CHECK_INTERVAL)
            
        except Exception as e:
            logger.error(f"Error in scheduled messages task: {e}")
            await asyncio.sleep(Config.SCHEDULE_CHECK_INTERVAL)

# Main Application
async def main():
    try:
        # Initialize database
        if not await db.connect():
            logger.error("Failed to connect to database")
            return
        
        # Start the client
        await app.start()
        me = await app.get_me()
        logger.info(f"Bot started as @{me.username} (ID: {me.id})")
        
        # Notify admin
        for admin_id in Config.ADMIN_IDS:
            try:
                await app.send_message(admin_id, "🤖 Bot started successfully!")
            except Exception as e:
                logger.error(f"Couldn't notify admin {admin_id}: {e}")
        
        # Start background tasks
        asyncio.create_task(scheduled_messages_task())
        
        # Keep running
        while True:
            await asyncio.sleep(3600)
            
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")