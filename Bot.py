import os
import asyncio
import datetime
import logging
import aiosqlite
import signal
import sys
from typing import Dict, List, Optional, Union
import pytz
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
    API_ID = 25781839  # Replace with your API ID
    API_HASH = "20a3f2f168739259a180dcdd642e196c"  # Replace with your API Hash
    BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"  # Replace with your Bot Token
    ADMIN_IDS = [7584086775]  # Replace with your admin Telegram IDs
    DB_NAME = "bot_database.db"
    MEDIA_DIR = "media"
    LOG_FILE = "bot.log"
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    TIMEZONE = "UTC"
    MAX_MESSAGE_LENGTH = 4096
    SCHEDULE_CHECK_INTERVAL = 60  # seconds
    USER_ACTIVITY_TIMEOUT = 300  # seconds
    MAX_MEDIA_SIZE = 50 * 1024 * 1024  # 50MB
    CLEANUP_INTERVAL = 24 * 3600  # 1 day
    SPAM_MIN_INTERVAL = 30  # Minimum seconds between spam messages
    MAX_SPAM_DURATION = 24 * 3600  # Max spam duration (1 day)

# Advanced Logging Setup
logging.basicConfig(
    level=logging.DEBUG,
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
                    is_admin BOOLEAN DEFAULT FALSE,
                    is_banned BOOLEAN DEFAULT FALSE
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
                    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'sent', 'failed', 'cancelled')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS spam_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    target TEXT NOT NULL,
                    target_type TEXT NOT NULL CHECK(target_type IN ('user', 'group', 'channel')),
                    text TEXT,
                    media_path TEXT,
                    media_type TEXT CHECK(media_type IN (NULL, 'photo', 'video', 'document', 'audio')),
                    parse_mode TEXT DEFAULT 'markdown',
                    interval INTEGER NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'active' CHECK(status IN ('active', 'stopped', 'completed')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

                CREATE TABLE IF NOT EXISTS bot_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    command TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_scheduled_status ON scheduled_messages(status);
                CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_messages(scheduled_time);
                CREATE INDEX IF NOT EXISTS idx_spam_status ON spam_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_user_active ON users(last_active);
                CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON bot_usage(timestamp);
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

    async def ban_user(self, telegram_id: int) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    "UPDATE users SET is_banned = 1 WHERE telegram_id = ?",
                    (telegram_id,)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error banning user {telegram_id}: {e}")
            return False

    async def unban_user(self, telegram_id: int) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    "UPDATE users SET is_banned = 0 WHERE telegram_id = ?",
                    (telegram_id,)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error unbanning user {telegram_id}: {e}")
            return False

    async def is_user_banned(self, telegram_id: int) -> bool:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    "SELECT is_banned FROM users WHERE telegram_id = ?",
                    (telegram_id,)
                )
                row = await cursor.fetchone()
                return row and row['is_banned']
        except Exception as e:
            logger.error(f"Error checking ban status for {telegram_id}: {e}")
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

    async def create_spam_task(self, data: Dict) -> Optional[int]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """INSERT INTO spam_tasks 
                    (user_id, target, target_type, text, media_path, media_type, parse_mode, interval, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (data['user_id'], data['target'], data['target_type'], data['text'],
                     data.get('media_path'), data.get('media_type'), 
                     data.get('parse_mode', 'markdown'), data['interval'], data['end_time'])
                )
                await self.conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"Error creating spam task: {e}")
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

    async def get_active_spam_tasks(self) -> List[Dict]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """SELECT * FROM spam_tasks
                    WHERE status = 'active' AND end_time > datetime('now')
                    ORDER BY created_at ASC"""
                )
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting active spam tasks: {e}")
            return []

    async def stop_spam_task(self, task_id: int) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    "UPDATE spam_tasks SET status = 'stopped' WHERE id = ?",
                    (task_id,)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error stopping spam task {task_id}: {e}")
            return False

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

    async def get_user_scheduled_messages(self, user_id: int) -> List[Dict]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """SELECT * FROM scheduled_messages
                    WHERE user_id = ? AND status = 'pending'
                    ORDER BY scheduled_time ASC""",
                    (user_id,)
                )
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user scheduled messages: {e}")
            return []

    async def get_user_spam_tasks(self, user_id: int) -> List[Dict]:
        try:
            async with self.lock:
                cursor = await self.conn.execute(
                    """SELECT * FROM spam_tasks
                    WHERE user_id = ? AND status = 'active'
                    ORDER BY created_at ASC""",
                    (user_id,)
                )
                return [dict(row) for row in await cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user spam tasks: {e}")
            return []

    async def cancel_scheduled_message(self, message_id: int) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    "UPDATE scheduled_messages SET status = 'cancelled' WHERE id = ?",
                    (message_id,)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error cancelling scheduled message: {e}")
            return False

    async def log_usage(self, user_id: int, command: str, details: Optional[str] = None) -> bool:
        try:
            async with self.lock:
                await self.conn.execute(
                    """INSERT INTO bot_usage (user_id, command, details)
                    VALUES (?, ?, ?)""",
                    (user_id, command, details)
                )
                await self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error logging usage: {e}")
            return False

    async def get_usage_stats(self, start_date: str, end_date: str) -> Dict:
        try:
            async with self.lock:
                # Total users
                cursor = await self.conn.execute("SELECT COUNT(*) as count FROM users")
                total_users = (await cursor.fetchone())['count']

                # Active users (last 24 hours)
                cursor = await self.conn.execute(
                    """SELECT COUNT(DISTINCT telegram_id) as count FROM users
                    WHERE last_active > datetime('now', '-24 hours')"""
                )
                active_users = (await cursor.fetchone())['count']

                # Command usage
                cursor = await self.conn.execute(
                    """SELECT command, COUNT(*) as count FROM bot_usage
                    WHERE timestamp BETWEEN ? AND ?
                    GROUP BY command""",
                    (start_date, end_date)
                )
                command_usage = {row['command']: row['count'] for row in await cursor.fetchall()}

                # Spam tasks
                cursor = await self.conn.execute(
                    """SELECT COUNT(*) as count FROM spam_tasks
                    WHERE created_at BETWEEN ? AND ?""",
                    (start_date, end_date)
                )
                spam_tasks = (await cursor.fetchone())['count']

                return {
                    'total_users': total_users,
                    'active_users': active_users,
                    'command_usage': command_usage,
                    'spam_tasks': spam_tasks
                }
        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {}

# Bot Utilities
class BotUtils:
    @staticmethod
    def get_timezone():
        return pytz.timezone(Config.TIMEZONE)

    @staticmethod
    async def parse_schedule_time(time_str: str) -> Optional[datetime.datetime]:
        try:
            dt = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M")
            return BotUtils.get_timezone().localize(dt)
        except ValueError:
            try:
                now = datetime.datetime.now(BotUtils.get_timezone())
                time_part = datetime.datetime.strptime(time_str, "%H:%M").time()
                dt = datetime.datetime.combine(now.date(), time_part)
                return BotUtils.get_timezone().localize(dt)
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

        stat = os.statvfs(Config.MEDIA_DIR)
        free_space = stat.f_bavail * stat.f_frsize
        if free_space < Config.MAX_MEDIA_SIZE:
            logger.error("Insufficient disk space for media")
            return None

        try:
            await client.download_media(message, file_name=file_path)
            file_size = os.path.getsize(file_path)
            if file_size > Config.MAX_MEDIA_SIZE:
                os.remove(file_path)
                logger.error(f"Media file too large: {file_size} bytes")
                return None
            return file_name, media_type
        except Exception as e:
            logger.error(f"Failed to save media: {e}")
            return None

    @staticmethod
    async def clean_old_media():
        while True:
            try:
                now = datetime.datetime.now(BotUtils.get_timezone())
                cutoff = now - datetime.timedelta(days=7)
                for file_name in os.listdir(Config.MEDIA_DIR):
                    file_path = os.path.join(Config.MEDIA_DIR, file_name)
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path), tz=BotUtils.get_timezone())
                    if mtime < cutoff:
                        os.remove(file_path)
                        logger.debug(f"Removed old media: {file_name}")
                await asyncio.sleep(Config.CLEANUP_INTERVAL)
            except Exception as e:
                logger.error(f"Error cleaning media: {e}")
                await asyncio.sleep(Config.CLEANUP_INTERVAL)

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
                return False
            except Exception as e:
                logger.error(f"Attempt {retries + 1} failed: {e}")
                retries += 1
                await asyncio.sleep(Config.RETRY_DELAY)
        return False

    @staticmethod
    async def validate_target(client: Client, target: str, target_type: str) -> bool:
        try:
            if target_type == "user" and target.startswith("@"):
                chat = await client.get_chat(target)
                return chat.type == enums.ChatType.PRIVATE
            elif target_type in ("group", "channel"):
                chat = await client.get_chat(target)
                return chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL)
            return False
        except Exception as e:
            logger.error(f"Error validating target {target}: {e}")
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
        sys.exit(0)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Command Handlers
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    try:
        user = message.from_user
        logger.debug(f"Start command from {user.id} (@{user.username})")

        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

        if not await db.create_or_update_user({
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'language_code': user.language_code
        }):
            await message.reply_text("⚠️ Database error. Please try again later.")
            return

        await db.log_usage(user.id, "/start")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
            [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
            [InlineKeyboardButton("📩 Start Spamming", callback_data="spam")],
            [InlineKeyboardButton("➕ Join Group", callback_data="join_group")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])

        await message.reply_text(
            "🤖 **Advanced Bot System**\n\n"
            "Welcome! I can help you:\n"
            "- Schedule one-time messages with text/media\n"
            "- Send repeated messages (spam) on a timer\n"
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

        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

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
        if not await BotUtils.validate_target(client, target, target_type):
            await message.reply_text("Invalid target. Please provide a valid username, group link, or channel.")
            return

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

        await db.log_usage(user.id, "/schedule", f"Scheduled message ID: {message_id}")

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
                [InlineKeyboardButton("🗑 Cancel Schedule", callback_data=f"delete_schedule_{message_id}")]
            ])
        )
    except Exception as e:
        logger.error(f"Error in schedule command: {e}")
        await message.reply_text("⚠️ Failed to schedule message. Please try again.")

@app.on_message(filters.command("spam"))
async def spam_command(client: Client, message: Message):
    try:
        user = message.from_user

        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

        if not message.reply_to_message and not (message.text or message.caption):
            await message.reply_text("Please reply to a message or include text to spam.")
            return

        args = message.text.split()[1:] if message.text else message.caption.split()[1:]

        if len(args) < 3:
            await message.reply_text(
                "Invalid format. Use:\n"
                "`/spam interval_seconds duration_hours @username`\n"
                "Example: `/spam 60 24 @username` (every 60s for 24h)\n\n"
                "You can also attach media files."
            )
            return

        try:
            interval = int(args[0])
            duration = int(args[1])
            target = args[2]
        except ValueError:
            await message.reply_text("Interval and duration must be numbers.")
            return

        if interval < Config.SPAM_MIN_INTERVAL:
            await message.reply_text(f"Interval must be at least {Config.SPAM_MIN_INTERVAL} seconds.")
            return

        if duration * 3600 > Config.MAX_SPAM_DURATION:
            await message.reply_text(f"Duration cannot exceed {Config.MAX_SPAM_DURATION // 3600} hours.")
            return

        target_type = "user" if target.startswith("@") else "group" if "+" in target else "channel"
        if not await BotUtils.validate_target(client, target, target_type):
            await message.reply_text("Invalid target. Please provide a valid username, group link, or channel.")
            return

        content_msg = message.reply_to_message if message.reply_to_message else message
        text = content_msg.text or content_msg.caption or ""
        media_info = await BotUtils.save_media(client, content_msg) if content_msg.media else (None, None)

        end_time = datetime.datetime.now(BotUtils.get_timezone()) + datetime.timedelta(hours=duration)

        task_id = await db.create_spam_task({
            'user_id': user.id,
            'target': target,
            'target_type': target_type,
            'text': text,
            'media_path': media_info[0] if media_info else None,
            'media_type': media_info[1] if media_info else None,
            'interval': interval,
            'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S"),
            'parse_mode': "markdown"
        })

        if not task_id:
            await message.reply_text("⚠️ Failed to start spam task. Please try again.")
            return

        await db.log_usage(user.id, "/spam", f"Spam task ID: {task_id}")

        reply_text = f"""
✅ **Spam Task Started Successfully**

📩 **To**: {target}
📝 **Content**: {text[:50] + '...' if len(text) > 50 else text}
🔄 **Interval**: Every {interval} seconds
⏰ **Until**: {end_time.strftime('%Y-%m-%d %H:%M')}
"""
        if media_info:
            reply_text += f"📎 **Media**: {media_info[1].capitalize()}\n"

        reply_text += f"\nID: `{task_id}`"

        await message.reply_text(
            reply_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛑 Stop Spam", callback_data=f"stop_spam_{task_id}")]
            ])
        )
    except Exception as e:
        logger.error(f"Error in spam command: {e}")
        await message.reply_text("⚠️ Failed to start spam task. Please try again.")

@app.on_message(filters.command("join"))
async def join_command(client: Client, message: Message):
    try:
        user = message.from_user

        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

        args = message.text.split()[1:]

        if not args:
            await message.reply_text("Please provide a group or channel link.\nExample: `/join https://t.me/group_link`")
            return

        link = args[0]
        try:
            chat = await client.join_chat(link)
            await db.conn.execute(
                """INSERT INTO group_participation (user_id, group_link, group_title, group_id, status)
                VALUES (?, ?, ?, ?, 'joined')""",
                (user.id, link, chat.title, chat.id)
            )
            await db.conn.commit()
            await db.log_usage(user.id, "/join", f"Joined group: {chat.title}")
            await message.reply_text(f"✅ Successfully joined {chat.title}!")
        except Exception as e:
            logger.error(f"Error joining chat {link}: {e}")
            await message.reply_text("⚠️ Failed to join the group/channel. Please check the link and try again.")
    except Exception as e:
        logger.error(f"Error in join command: {e}")
        await message.reply_text("⚠️ An error occurred. Please try again later.")

# Admin Commands
@app.on_message(filters.command("ban") & filters.user(Config.ADMIN_IDS))
async def ban_command(client: Client, message: Message):
    try:
        args = message.text.split()[1:]
        if not args:
            await message.reply_text("Usage: `/ban <user_id>`")
            return

        try:
            user_id = int(args[0])
        except ValueError:
            await message.reply_text("Please provide a valid user ID.")
            return

        if user_id in Config.ADMIN_IDS:
            await message.reply_text("Cannot ban an admin!")
            return

        if await db.ban_user(user_id):
            await message.reply_text(f"✅ User {user_id} has been banned.")
            await db.log_usage(message.from_user.id, "/ban", f"Banned user: {user_id}")
        else:
            await message.reply_text("⚠️ Failed to ban user.")
    except Exception as e:
        logger.error(f"Error in ban command: {e}")
        await message.reply_text("⚠️ An error occurred. Please try again later.")

@app.on_message(filters.command("unban") & filters.user(Config.ADMIN_IDS))
async def unban_command(client: Client, message: Message):
    try:
        args = message.text.split()[1:]
        if not args:
            await message.reply_text("Usage: `/unban <user_id>`")
            return

        try:
            user_id = int(args[0])
        except ValueError:
            await message.reply_text("Please provide a valid user ID.")
            return

        if await db.unban_user(user_id):
            await message.reply_text(f"✅ User {user_id} has been unbanned.")
            await db.log_usage(message.from_user.id, "/unban", f"Unbanned user: {user_id}")
        else:
            await message.reply_text("⚠️ Failed to unban user.")
    except Exception as e:
        logger.error(f"Error in unban command: {e}")
        await message.reply_text("⚠️ An error occurred. Please try again later.")

@app.on_message(filters.command("broadcast") & filters.user(Config.ADMIN_IDS))
async def broadcast_command(client: Client, message: Message):
    try:
        if not message.reply_to_message and not (message.text or message.caption):
            await message.reply_text("Please reply to a message or include text to broadcast.")
            return

        content_msg = message.reply_to_message if message.reply_to_message else message
        text = content_msg.text or content_msg.caption or ""
        media_info = await BotUtils.save_media(client, content_msg) if content_msg.media else (None, None)

        cursor = await db.conn.execute("SELECT telegram_id FROM users WHERE is_banned = 0")
        users = [row['telegram_id'] for row in await cursor.fetchall()]
        success_count = 0
        fail_count = 0

        for user_id in users:
            success = await BotUtils.send_message_with_retry(
                client=client,
                target=user_id,
                text=text,
                media_path=media_info[0] if media_info else None,
                media_type=media_info[1] if media_info else None,
                parse_mode="markdown"
            )
            if success:
                success_count += 1
            else:
                fail_count += 1
            await asyncio.sleep(0.1)  # Avoid flooding

        reply_text = (
            f"📢 **Broadcast Completed**\n\n"
            f"✅ Sent to {success_count} users\n"
            f"❌ Failed for {fail_count} users"
        )
        await message.reply_text(reply_text)
        await db.log_usage(message.from_user.id, "/broadcast", f"Sent to {success_count}/{len(users)} users")
    except Exception as e:
        logger.error(f"Error in broadcast command: {e}")
        await message.reply_text("⚠️ Failed to broadcast message.")

@app.on_message(filters.command("stats") & filters.user(Config.ADMIN_IDS))
async def stats_command(client: Client, message: Message):
    try:
        now = datetime.datetime.now(BotUtils.get_timezone())
        start_date = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        end_date = now.strftime("%Y-%m-%d %H:%M:%S")

        stats = await db.get_usage_stats(start_date, end_date)
        
        reply_text = (
            f"📊 **Bot Usage Statistics** (Last 7 Days)\n\n"
            f"👥 **Total Users**: {stats.get('total_users', 0)}\n"
            f"🟢 **Active Users (24h)**: {stats.get('active_users', 0)}\n"
            f"📩 **Spam Tasks Created**: {stats.get('spam_tasks', 0)}\n"
            f"📋 **Command Usage**:\n"
        )
        for cmd, count in stats.get('command_usage', {}).items():
            reply_text += f"- `{cmd}`: {count} times\n"

        await message.reply_text(reply_text)
        await db.log_usage(message.from_user.id, "/stats")
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply_text("⚠️ Failed to retrieve statistics.")

# Callback Query Handlers
@app.on_callback_query()
async def handle_callback_query(client: Client, callback_query: CallbackQuery):
    try:
        user = callback_query.from_user

        if await db.is_user_banned(user.id):
            await callback_query.answer("You are banned from using this bot.", show_alert=True)
            return

        data = callback_query.data

        if data == "schedule":
            await callback_query.message.reply_text(
                "Please use the `/schedule` command to schedule a message.\n"
                "Example: `/schedule 2023-12-31 23:59 @username`"
            )

        elif data == "my_scheduled":
            messages = await db.get_user_scheduled_messages(user.id)
            if not messages:
                await callback_query.message.reply_text("You have no scheduled messages.")
                return

            reply_text = "📊 **Your Scheduled Messages**\n\n"
            for msg in messages:
                reply_text += (
                    f"ID: `{msg['id']}`\n"
                    f"📅 When: {msg['scheduled_time']}\n"
                    f"📩 To: {msg['target']}\n"
                    f"📝 Content: {msg['text'][:50] + '...' if len(msg['text']) > 50 else msg['text']}\n"
                    f"{'📎 Media: ' + msg['media_type'].capitalize() if msg['media_type'] else ''}\n\n"
                )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ])
            await callback_query.message.edit_text(reply_text, reply_markup=keyboard)

        elif data == "spam":
            await callback_query.message.reply_text(
                "Please use the `/spam` command to start spamming.\n"
                "Example: `/spam 60 24 @username` (every 60s for 24h)"
            )

        elif data == "my_spam":
            tasks = await db.get_user_spam_tasks(user.id)
            if not tasks:
                await callback_query.message.reply_text("You have no active spam tasks.")
                return

            reply_text = "📩 **Your Spam Tasks**\n\n"
            for task in tasks:
                reply_text += (
                    f"ID: `{task['id']}`\n"
                    f"📩 To: {task['target']}\n"
                    f"📝 Content: {task['text'][:50] + '...' if len(task['text']) > 50 else task['text']}\n"
                    f"🔄 Interval: Every {task['interval']} seconds\n"
                    f"⏰ Until: {task['end_time']}\n"
                    f"{'📎 Media: ' + task['media_type'].capitalize() if task['media_type'] else ''}\n\n"
                )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ])
            await callback_query.message.edit_text(reply_text, reply_markup=keyboard)

        elif data == "join_group":
            await callback_query.message.reply_text(
                "Please provide a group or channel link using:\n"
                "`/join https://t.me/group_link`"
            )

        elif data == "help":
            await callback_query.message.edit_text(
                "ℹ️ **Help**\n\n"
                "Available commands:\n"
                "- `/start`: Start the bot\n"
                "- `/schedule`: Schedule a one-time message\n"
                "- `/spam`: Start a repeating message\n"
                "- `/join`: Join a group or channel\n\n"
                "Admin commands:\n"
                "- `/ban <user_id>`: Ban a user\n"
                "- `/unban <user_id>`: Unban a user\n"
                "- `/broadcast`: Send a message to all users\n"
                "- `/stats`: View bot usage statistics\n\n"
                "Use the buttons to navigate.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="back")]
                ])
            )

        elif data.startswith("delete_schedule_"):
            message_id = int(data.split("_")[2])
            if await db.cancel_scheduled_message(message_id):
                await callback_query.message.edit_text("✅ Schedule cancelled successfully!")
                await db.log_usage(user.id, "cancel_schedule", f"Cancelled schedule ID: {message_id}")
            else:
                await callback_query.message.edit_text("⚠️ Failed to cancel schedule.")

        elif data.startswith("stop_spam_"):
            task_id = int(data.split("_")[2])
            if await db.stop_spam_task(task_id):
                await callback_query.message.edit_text("✅ Spam task stopped successfully!")
                await db.log_usage(user.id, "stop_spam", f"Stopped spam task ID: {task_id}")
            else:
                await callback_query.message.edit_text("⚠️ Failed to stop spam task.")

        elif data == "back":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
                [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
                [InlineKeyboardButton("📩 Start Spamming", callback_data="spam")],
                [InlineKeyboardButton("📊 My Spam Tasks", callback_data="my_spam")],
                [InlineKeyboardButton("➕ Join Group", callback_data="join_group")],
                [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
            ])
            await callback_query.message.edit_text(
                "🤖 **Advanced Bot System**\n\n"
                "Select an option below:",
                reply_markup=keyboard
            )

        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error in callback query: {e}")
        await callback_query.message.reply_text("⚠️ An error occurred. Please try again later.")

# Background Tasks
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

async def spam_messages_task():
    while True:
        try:
            tasks = await db.get_active_spam_tasks()

            for task in tasks:
                last_sent = datetime.datetime.now(BotUtils.get_timezone())
                end_time = datetime.datetime.strptime(task['end_time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BotUtils.get_timezone())

                if last_sent >= end_time:
                    await db.stop_spam_task(task['id'])
                    logger.info(f"Spam task {task['id']} completed")
                    continue

                success = await BotUtils.send_message_with_retry(
                    client=app,
                    target=task['target'],
                    text=task['text'],
                    media_path=task['media_path'],
                    media_type=task['media_type'],
                    parse_mode=task['parse_mode']
                )

                if success:
                    logger.debug(f"Sent spam message for task {task['id']} to {task['target']}")
                else:
                    logger.error(f"Failed to send spam message for task {task['id']} to {task['target']}")

                await asyncio.sleep(task['interval'])

        except Exception as e:
            logger.error(f"Error in spam messages task: {e}")
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
        asyncio.create_task(spam_messages_task())
        asyncio.create_task(BotUtils.clean_old_media())

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