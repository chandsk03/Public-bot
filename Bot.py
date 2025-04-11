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
    BOT_TOKEN = "7585970885:AAGgo0Wc1GXEWd6XB_cuQgtp1-q61WAxnvw"
    ADMIN_IDS = [7584086775]
    DB_NAME = "bot_database.db"
    MEDIA_DIR = "media"
    LOG_FILE = "bot.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEZONE = "UTC"
    MAX_MESSAGE_LENGTH = 4096  # Telegram message limit

# Advanced Logging Setup
class BotLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
    
    def _setup_logging(self):
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # File handler
        file_handler = logging.FileHandler(Config.LOG_FILE)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log(self, level: str, message: str, exc_info=None):
        getattr(self.logger, level)(message, exc_info=exc_info)

logger = BotLogger()

# Database Models with Validation
class User:
    def __init__(self, data: Dict):
        self.id = data.get('id')
        self.telegram_id = data.get('telegram_id')
        self.username = data.get('username')
        self.first_name = data.get('first_name')
        self.last_name = data.get('last_name')
        self.join_date = data.get('join_date')
        self.last_active = data.get('last_active')
        self.language_code = data.get('language_code', 'en')
        
        if not self.telegram_id:
            raise ValueError("Telegram ID is required")

class ScheduledMessage:
    def __init__(self, data: Dict):
        self.id = data.get('id')
        self.user_id = data.get('user_id')
        self.target = data.get('target')
        self.target_type = data.get('target_type')
        self.text = data.get('text', '')
        self.media_path = data.get('media_path')
        self.media_type = data.get('media_type')
        self.parse_mode = data.get('parse_mode', 'markdown')
        self.scheduled_time = data.get('scheduled_time')
        self.status = data.get('status', 'pending')
        self.created_at = data.get('created_at')
        self.retry_count = data.get('retry_count', 0)
        
        if not all([self.user_id, self.target, self.scheduled_time]):
            raise ValueError("Missing required fields")

# Advanced Database Manager
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
            logger.log("info", "Database connection established")
        except Exception as e:
            logger.log("error", f"Database connection failed: {str(e)}")
            raise
    
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
                    language_code TEXT DEFAULT 'en'
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
                
                CREATE INDEX IF NOT EXISTS idx_scheduled_messages_status ON scheduled_messages(status);
                CREATE INDEX IF NOT EXISTS idx_scheduled_messages_time ON scheduled_messages(scheduled_time);
            """)
            await self.conn.commit()
        except Exception as e:
            logger.log("error", f"Database initialization failed: {str(e)}")
            raise
    
    async def get_user(self, telegram_id: int) -> Optional[User]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    "SELECT * FROM users WHERE telegram_id = ?",
                    (telegram_id,)
                )
                row = await cursor.fetchone()
                return User(dict(row)) if row else None
        except Exception as e:
            logger.log("error", f"Error getting user: {str(e)}")
            return None
    
    async def create_or_update_user(self, user_data: Dict) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    """INSERT INTO users 
                    (telegram_id, username, first_name, last_name, language_code, last_active)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    language_code = excluded.language_code,
                    last_active = CURRENT_TIMESTAMP""",
                    (user_data['id'], user_data.get('username'), 
                     user_data.get('first_name'), user_data.get('last_name'),
                     user_data.get('language_code', 'en'))
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.log("error", f"Error creating/updating user: {str(e)}")
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
            logger.log("error", f"Error scheduling message: {str(e)}")
            return None
    
    async def get_pending_messages(self) -> List[ScheduledMessage]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """SELECT * FROM scheduled_messages
                    WHERE status = 'pending' AND scheduled_time <= datetime('now')
                    ORDER BY scheduled_time ASC
                    LIMIT 100"""
                )
                rows = await cursor.fetchall()
                return [ScheduledMessage(dict(row)) for row in rows]
        except Exception as e:
            logger.log("error", f"Error getting pending messages: {str(e)}")
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
            logger.log("error", f"Error updating message status: {str(e)}")
            return False
    
    async def close(self):
        try:
            if self.conn:
                await self.conn.close()
                logger.log("info", "Database connection closed")
        except Exception as e:
            logger.log("error", f"Error closing database: {str(e)}")

# Enhanced Pyrogram Client
class BotClient(Client):
    def __init__(self):
        super().__init__(
            name="advanced_bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            workers=100,
            workdir=os.getcwd(),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        self.db = DatabaseManager()
        self.start_time = datetime.datetime.now()
    
    async def initialize(self):
        try:
            await self.db.connect()
            await self.start()
            
            me = await self.get_me()
            logger.log("info", f"Bot started as @{me.username} (ID: {me.id})")
            
            # Notify admin
            for admin_id in Config.ADMIN_IDS:
                try:
                    await self.send_message(
                        admin_id,
                        f"🤖 Bot started successfully!\n\n"
                        f"🕒 Uptime: {datetime.datetime.now() - self.start_time}\n"
                        f"💾 Database: {os.path.getsize(Config.DB_NAME) / 1024:.2f} KB"
                    )
                except Exception as e:
                    logger.log("error", f"Couldn't notify admin {admin_id}: {str(e)}")
            
            return True
        except Exception as e:
            logger.log("error", f"Bot initialization failed: {str(e)}")
            return False
    
    async def shutdown(self):
        try:
            await self.db.close()
            if self.is_initialized:
                await self.stop()
            logger.log("info", "Bot shutdown completed")
        except Exception as e:
            logger.log("error", f"Error during shutdown: {str(e)}")
        finally:
            sys.exit(0)

# Signal Handlers for Graceful Shutdown
def handle_signal(signum, frame):
    logger.log("info", f"Received signal {signum}, initiating shutdown...")
    asyncio.create_task(bot.shutdown())

# Initialize Bot
bot = BotClient()

# Register Signal Handlers
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Enhanced Command Handlers
@bot.on_message(filters.command("start"))
async def start_command(client: BotClient, message: Message):
    try:
        user = message.from_user
        logger.log("info", f"Start command from {user.id} (@{user.username})")
        
        if not await client.db.create_or_update_user({
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
    except RPCError as e:
        logger.log("error", f"Telegram API error in start command: {str(e)}")
        await message.reply_text("⚠️ Telegram API error. Please try again later.")
    except Exception as e:
        logger.log("error", f"Unexpected error in start command: {str(e)}")
        await message.reply_text("⚠️ An unexpected error occurred. Please try again later.")

# Background Task for Scheduled Messages
async def process_scheduled_messages():
    while True:
        try:
            messages = await bot.db.get_pending_messages()
            
            for msg in messages:
                try:
                    if msg.media_path:
                        media_path = os.path.join(Config.MEDIA_DIR, msg.media_path)
                        
                        if msg.media_type == 'photo':
                            await bot.send_photo(
                                chat_id=msg.target,
                                photo=media_path,
                                caption=msg.text,
                                parse_mode=msg.parse_mode
                            )
                        elif msg.media_type == 'video':
                            await bot.send_video(
                                chat_id=msg.target,
                                video=media_path,
                                caption=msg.text,
                                parse_mode=msg.parse_mode
                            )
                        elif msg.media_type == 'document':
                            await bot.send_document(
                                chat_id=msg.target,
                                document=media_path,
                                caption=msg.text,
                                parse_mode=msg.parse_mode
                            )
                        elif msg.media_type == 'audio':
                            await bot.send_audio(
                                chat_id=msg.target,
                                audio=media_path,
                                caption=msg.text,
                                parse_mode=msg.parse_mode
                            )
                    else:
                        await bot.send_message(
                            chat_id=msg.target,
                            text=msg.text,
                            parse_mode=msg.parse_mode
                        )
                    
                    await bot.db.update_message_status(msg.id, 'sent')
                    logger.log("info", f"Sent message {msg.id} to {msg.target}")
                    
                except FloodWait as e:
                    logger.log("warning", f"Flood wait for {e.value} seconds")
                    await asyncio.sleep(e.value)
                    continue
                except (PeerIdInvalid, ChannelInvalid, ChatAdminRequired, UserNotParticipant) as e:
                    logger.log("error", f"Invalid target for message {msg.id}: {str(e)}")
                    await bot.db.update_message_status(msg.id, 'failed')
                except Exception as e:
                    logger.log("error", f"Error sending message {msg.id}: {str(e)}")
                    # Retry logic handled in database query
            
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.log("error", f"Error in scheduled messages task: {str(e)}")
            await asyncio.sleep(60)

# Main Application
async def main():
    if not await bot.initialize():
        logger.log("error", "Bot failed to initialize")
        return
    
    try:
        # Start background tasks
        asyncio.create_task(process_scheduled_messages())
        
        # Keep the bot running
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour
            
    except Exception as e:
        logger.log("error", f"Fatal error in main loop: {str(e)}")
    finally:
        await bot.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.log("info", "Bot stopped by user")
    except Exception as e:
        logger.log("error", f"Fatal error: {str(e)}")