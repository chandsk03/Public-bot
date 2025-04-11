import os
import asyncio
import datetime
import logging
import aiosqlite
import signal
import sys
from typing import Dict, List, Optional, Union
import pytz
import heapq
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

# Configuration
class Config:
    # WARNING: Hardcoded credentials are used for simplicity. In production, use environment variables or a secure config file.
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
    SPAM_TASK_REFRESH_INTERVAL = 60  # seconds between DB refreshes
    SPAM_MIN_INTERVAL = 30  # Minimum seconds between spam messages
    MAX_SPAM_DURATION = 24 * 3600  # Max spam duration (1 day)
    MAX_SPAM_TASKS_PER_USER = 5  # Max active spam tasks per user
    MAX_SCHEDULED_MESSAGES_PER_USER = 10  # Max scheduled messages per user
    TASK_COOLDOWN = 60  # Seconds between creating new tasks
    DB_RETRY_COUNT = 3
    DB_RETRY_DELAY = 1
    MAX_MEDIA_SIZE = 50 * 1024 * 1024  # 50MB
    CLEANUP_INTERVAL = 24 * 3600  # 1 day

# Logging Setup
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
        self.read_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()

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
            async with self.write_lock:
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
                        is_banned BOOLEAN DEFAULT FALSE,
                        notifications BOOLEAN DEFAULT TRUE,
                        last_task_time TIMESTAMP
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
                        next_send_time TIMESTAMP NOT NULL,
                        priority INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'active' CHECK(status IN ('active', 'stopped', 'completed')),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    CREATE INDEX IF NOT EXISTS idx_spam_next_send ON spam_tasks(next_send_time);
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

    async def execute_with_retry(self, query: str, params: tuple = (), is_select: bool = False):
        for attempt in range(Config.DB_RETRY_COUNT):
            try:
                lock = self.read_lock if is_select else self.write_lock
                async with lock:
                    cursor = await self.conn.execute(query, params)
                    if is_select:
                        return cursor
                    await self.conn.commit()
                    return cursor.lastrowid if "INSERT" in query.upper() else True
            except Exception as e:
                logger.error(f"Database attempt {attempt + 1} failed: {e}")
                if attempt < Config.DB_RETRY_COUNT - 1:
                    await asyncio.sleep(Config.DB_RETRY_DELAY)
                else:
                    raise

    async def create_or_update_user(self, user_data: Dict) -> bool:
        query = """
            INSERT INTO users 
            (telegram_id, username, first_name, last_name, language_code, last_active, is_admin)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            language_code = excluded.language_code,
            last_active = CURRENT_TIMESTAMP
        """
        is_admin = 1 if user_data['id'] in Config.ADMIN_IDS else 0
        return await self.execute_with_retry(query, (
            user_data['id'], user_data.get('username'), 
            user_data.get('first_name'), user_data.get('last_name'),
            user_data.get('language_code', 'en'), is_admin
        ))

    async def update_last_task_time(self, user_id: int) -> bool:
        query = "UPDATE users SET last_task_time = CURRENT_TIMESTAMP WHERE telegram_id = ?"
        return await self.execute_with_retry(query, (user_id,))

    async def get_last_task_time(self, user_id: int) -> Optional[datetime.datetime]:
        query = "SELECT last_task_time FROM users WHERE telegram_id = ?"
        cursor = await self.execute_with_retry(query, (user_id,), is_select=True)
        row = await cursor.fetchone()
        if row and row['last_task_time']:
            return datetime.datetime.strptime(row['last_task_time'], "%Y-%m-%d %H:%M:%S")
        return None

    async def is_user_banned(self, telegram_id: int) -> bool:
        query = "SELECT is_banned FROM users WHERE telegram_id = ?"
        cursor = await self.execute_with_retry(query, (telegram_id,), is_select=True)
        row = await cursor.fetchone()
        return row and row['is_banned']

    async def toggle_notifications(self, telegram_id: int, enabled: bool) -> bool:
        query = "UPDATE users SET notifications = ? WHERE telegram_id = ?"
        return await self.execute_with_retry(query, (1 if enabled else 0, telegram_id))

    async def get_user_settings(self, telegram_id: int) -> Dict:
        query = "SELECT notifications FROM users WHERE telegram_id = ?"
        cursor = await self.execute_with_retry(query, (telegram_id,), is_select=True)
        row = await cursor.fetchone()
        return {'notifications': bool(row['notifications']) if row else True}

    async def schedule_message(self, data: Dict) -> Optional[int]:
        query = """
            INSERT INTO scheduled_messages 
            (user_id, target, target_type, text, media_path, media_type, parse_mode, scheduled_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        return await self.execute_with_retry(query, (
            data['user_id'], data['target'], data['target_type'], data['text'],
            data.get('media_path'), data.get('media_type'), 
            data.get('parse_mode', 'markdown'), data['scheduled_time']
        ))

    async def create_spam_task(self, data: Dict) -> Optional[int]:
        query = """
            INSERT INTO spam_tasks 
            (user_id, target, target_type, text, media_path, media_type, parse_mode, 
             interval, end_time, next_send_time, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return await self.execute_with_retry(query, (
            data['user_id'], data['target'], data['target_type'], data['text'],
            data.get('media_path'), data.get('media_type'), 
            data.get('parse_mode', 'markdown'), data['interval'], 
            data['end_time'], data['next_send_time'], data.get('priority', 0)
        ))

    async def get_pending_messages(self) -> List[Dict]:
        query = """
            SELECT * FROM scheduled_messages
            WHERE status = 'pending' AND scheduled_time <= datetime('now')
            ORDER BY scheduled_time ASC
            LIMIT 100
        """
        cursor = await self.execute_with_retry(query, is_select=True)
        return [dict(row) for row in await cursor.fetchall()]

    async def get_active_spam_tasks(self) -> List[Dict]:
        query = """
            SELECT * FROM spam_tasks
            WHERE status = 'active' AND end_time > datetime('now')
            ORDER BY priority DESC, next_send_time ASC
        """
        cursor = await self.execute_with_retry(query, is_select=True)
        return [dict(row) for row in await cursor.fetchall()]

    async def update_spam_task_next_send(self, task_id: int, next_send_time: str) -> bool:
        query = "UPDATE spam_tasks SET next_send_time = ? WHERE id = ?"
        return await self.execute_with_retry(query, (next_send_time, task_id))

    async def stop_spam_task(self, task_id: int) -> bool:
        query = "UPDATE spam_tasks SET status = 'stopped' WHERE id = ?"
        return await self.execute_with_retry(query, (task_id,))

    async def update_message_status(self, message_id: int, status: str) -> bool:
        query = "UPDATE scheduled_messages SET status = ? WHERE id = ?"
        return await self.execute_with_retry(query, (status, message_id))

    async def get_user_scheduled_messages(self, user_id: int) -> List[Dict]:
        query = """
            SELECT * FROM scheduled_messages
            WHERE user_id = ? AND status = 'pending'
            ORDER BY scheduled_time ASC
        """
        cursor = await self.execute_with_retry(query, (user_id,), is_select=True)
        return [dict(row) for row in await cursor.fetchall()]

    async def get_user_spam_tasks(self, user_id: int) -> List[Dict]:
        query = """
            SELECT * FROM spam_tasks
            WHERE user_id = ? AND status = 'active'
            ORDER BY created_at ASC
        """
        cursor = await self.execute_with_retry(query, (user_id,), is_select=True)
        return [dict(row) for row in await cursor.fetchall()]

    async def count_user_tasks(self, user_id: int, table: str) -> int:
        query = f"SELECT COUNT(*) as count FROM {table} WHERE user_id = ? AND status = 'active'"
        cursor = await self.execute_with_retry(query, (user_id,), is_select=True)
        row = await cursor.fetchone()
        return row['count']

    async def log_usage(self, user_id: int, command: str, details: Optional[str] = None) -> bool:
        query = "INSERT INTO bot_usage (user_id, command, details) VALUES (?, ?, ?)"
        return await self.execute_with_retry(query, (user_id, command, details))

    async def get_usage_stats(self, start_date: str, end_date: str) -> Dict:
        async with self.read_lock:
            cursor = await self.conn.execute("SELECT COUNT(*) as count FROM users")
            total_users = (await cursor.fetchone())['count']

            cursor = await self.conn.execute(
                "SELECT COUNT(DISTINCT telegram_id) as count FROM users WHERE last_active > datetime('now', '-24 hours')"
            )
            active_users = (await cursor.fetchone())['count']

            cursor = await self.conn.execute(
                "SELECT command, COUNT(*) as count FROM bot_usage WHERE timestamp BETWEEN ? AND ? GROUP BY command",
                (start_date, end_date)
            )
            command_usage = {row['command']: row['count'] for row in await cursor.fetchall()}

            cursor = await self.conn.execute(
                "SELECT COUNT(*) as count FROM spam_tasks WHERE created_at BETWEEN ? AND ?",
                (start_date, end_date)
            )
            spam_tasks = (await cursor.fetchone())['count']

            return {
                'total_users': total_users,
                'active_users': active_users,
                'command_usage': command_usage,
                'spam_tasks': spam_tasks
            }

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
                        await client.send_photo(target, full_path, caption=text, parse_mode=parse_mode)
                    elif media_type == "video":
                        await client.send_video(target, full_path, caption=text, parse_mode=parse_mode)
                    elif media_type == "document":
                        await client.send_document(target, full_path, caption=text, parse_mode=parse_mode)
                    elif media_type == "audio":
                        await client.send_audio(target, full_path, caption=text, parse_mode=parse_mode)
                else:
                    await client.send_message(target, text, parse_mode=parse_mode)
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
            chat = await client.get_chat(target)
            if target_type == "user":
                return chat.type == enums.ChatType.PRIVATE
            elif target_type in ("group", "channel"):
                return chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL)
            return False
        except Exception as e:
            logger.error(f"Error validating target {target}: {e}")
            return False

    @staticmethod
    async def notify_user(client: Client, user_id: int, message: str):
        settings = await db.get_user_settings(user_id)
        if settings['notifications']:
            try:
                await client.send_message(user_id, message)
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")

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

# Signal Handler
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

        await db.create_or_update_user({
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'language_code': user.language_code
        })
        await db.log_usage(user.id, "/start")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
            [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
            [InlineKeyboardButton("📩 Start Spamming", callback_data="spam")],
            [InlineKeyboardButton("📊 My Spam Tasks", callback_data="my_spam")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])

        await message.reply_text(
            "🤖 **Advanced Bot System**\n\n"
            "Welcome! I can help you:\n"
            "- Schedule one-time messages with text/media\n"
            "- Send repeated messages (spam) on a timer\n"
            "- Customize your settings\n\n"
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

        scheduled_count = await db.count_user_tasks(user.id, "scheduled_messages")
        if scheduled_count >= Config.MAX_SCHEDULED_MESSAGES_PER_USER:
            await message.reply_text(f"⚠️ You have reached the limit of {Config.MAX_SCHEDULED_MESSAGES_PER_USER} scheduled messages.")
            return

        last_task_time = await db.get_last_task_time(user.id)
        now = datetime.datetime.now(BotUtils.get_timezone())
        if last_task_time and (now - last_task_time).total_seconds() < Config.TASK_COOLDOWN:
            await message.reply_text(f"⏳ Please wait {Config.TASK_COOLDOWN} seconds between creating tasks.")
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
                "`/schedule 23:59 @username` (for today)"
            )
            return

        time_str = f"{args[0]} {args[1]}" if len(args) > 2 else args[0]
        target = args[-1]

        scheduled_time = await BotUtils.parse_schedule_time(time_str)
        if not scheduled_time:
            await message.reply_text("Invalid time format. Use YYYY-MM-DD HH:MM or HH:MM.")
            return

        target_type = "user" if target.startswith("@") else "group" if "+" in target else "channel"
        if not await BotUtils.validate_target(client, target, target_type):
            await message.reply_text("Invalid target.")
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
            await message.reply_text("⚠️ Failed to schedule message.")
            return

        await db.update_last_task_time(user.id)
        await db.log_usage(user.id, "/schedule", f"Scheduled message ID: {message_id}")

        reply_text = (
            f"✅ **Message Scheduled**\n\n"
            f"📅 When: {scheduled_time.strftime('%Y-%m-%d %H:%M')}\n"
            f"📩 To: {target}\n"
            f"📝 Content: {text[:50] + '...' if len(text) > 50 else text}\n"
            f"{'📎 Media: ' + media_info[1].capitalize() if media_info else ''}\n"
            f"ID: `{message_id}`"
        )
        await message.reply_text(reply_text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Cancel", callback_data=f"delete_schedule_{message_id}")]
        ]))
    except Exception as e:
        logger.error(f"Error in schedule command: {e}")
        await message.reply_text("⚠️ Failed to schedule message.")

@app.on_message(filters.command("spam"))
async def spam_command(client: Client, message: Message):
    try:
        user = message.from_user

        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

        spam_count = await db.count_user_tasks(user.id, "spam_tasks")
        if spam_count >= Config.MAX_SPAM_TASKS_PER_USER:
            await message.reply_text(f"⚠️ You have reached the limit of {Config.MAX_SPAM_TASKS_PER_USER} spam tasks.")
            return

        last_task_time = await db.get_last_task_time(user.id)
        now = datetime.datetime.now(BotUtils.get_timezone())
        if last_task_time and (now - last_task_time).total_seconds() < Config.TASK_COOLDOWN:
            await message.reply_text(f"⏳ Please wait {Config.TASK_COOLDOWN} seconds between creating tasks.")
            return

        if not message.reply_to_message and not (message.text or message.caption):
            await message.reply_text("Please reply to a message or include text to spam.")
            return

        args = message.text.split()[1:] if message.text else message.caption.split()[1:]
        if len(args) < 3:
            await message.reply_text(
                "Invalid format. Use:\n"
                "`/spam 60 24 @username` (every 60s for 24h)"
            )
            return

        interval, duration, target = int(args[0]), int(args[1]), args[2]
        if interval < Config.SPAM_MIN_INTERVAL or duration * 3600 > Config.MAX_SPAM_DURATION:
            await message.reply_text(f"Interval must be >= {Config.SPAM_MIN_INTERVAL}s and duration <= {Config.MAX_SPAM_DURATION // 3600}h.")
            return

        target_type = "user" if target.startswith("@") else "group" if "+" in target else "channel"
        if not await BotUtils.validate_target(client, target, target_type):
            await message.reply_text("Invalid target.")
            return

        content_msg = message.reply_to_message if message.reply_to_message else message
        text = content_msg.text or content_msg.caption or ""
        media_info = await BotUtils.save_media(client, content_msg) if content_msg.media else (None, None)

        end_time = now + datetime.timedelta(hours=duration)
        next_send_time = now

        task_id = await db.create_spam_task({
            'user_id': user.id,
            'target': target,
            'target_type': target_type,
            'text': text,
            'media_path': media_info[0] if media_info else None,
            'media_type': media_info[1] if media_info else None,
            'interval': interval,
            'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S"),
            'next_send_time': next_send_time.strftime("%Y-%m-%d %H:%M:%S"),
            'parse_mode': "markdown"
        })

        if not task_id:
            await message.reply_text("⚠️ Failed to start spam task.")
            return

        await db.update_last_task_time(user.id)
        await db.log_usage(user.id, "/spam", f"Spam task ID: {task_id}")

        reply_text = (
            f"✅ **Spam Task Started**\n\n"
            f"📩 To: {target}\n"
            f"📝 Content: {text[:50] + '...' if len(text) > 50 else text}\n"
            f"🔄 Interval: Every {interval}s\n"
            f"⏰ Until: {end_time.strftime('%Y-%m-%d %H:%M')}\n"
            f"{'📎 Media: ' + media_info[1].capitalize() if media_info else ''}\n"
            f"ID: `{task_id}`"
        )
        await message.reply_text(reply_text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🛑 Stop", callback_data=f"stop_spam_{task_id}")]
        ]))
    except Exception as e:
        logger.error(f"Error in spam command: {e}")
        await message.reply_text("⚠️ Failed to start spam task.")

@app.on_message(filters.command("settings"))
async def settings_command(client: Client, message: Message):
    try:
        user = message.from_user
        if await db.is_user_banned(user.id):
            await message.reply_text("🚫 You are banned from using this bot.")
            return

        settings = await db.get_user_settings(user.id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"Notifications: {'On' if settings['notifications'] else 'Off'}", callback_data="toggle_notifications")],
            [InlineKeyboardButton("🔙 Back", callback_data="back")]
        ])
        await message.reply_text("⚙️ **Settings**\n\nCustomize your preferences:", reply_markup=keyboard)
        await db.log_usage(user.id, "/settings")
    except Exception as e:
        logger.error(f"Error in settings command: {e}")
        await message.reply_text("⚠️ An error occurred.")

# Callback Query Handlers
@app.on_callback_query()
async def handle_callback_query(client: Client, callback_query: CallbackQuery):
    try:
        user = callback_query.from_user
        if await db.is_user_banned(user.id):
            await callback_query.answer("You are banned.", show_alert=True)
            return

        data = callback_query.data

        if data == "schedule":
            await callback_query.message.reply_text("Use `/schedule 2023-12-31 23:59 @username`")
        elif data == "my_scheduled":
            messages = await db.get_user_scheduled_messages(user.id)
            if not messages:
                await callback_query.message.reply_text("No scheduled messages.")
                return
            reply_text = "📊 **Your Scheduled Messages**\n\n" + "\n".join(
                f"ID: `{msg['id']}`\nWhen: {msg['scheduled_time']}\nTo: {msg['target']}\n" for msg in messages
            )
            await callback_query.message.edit_text(reply_text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ]))
        elif data == "spam":
            await callback_query.message.reply_text("Use `/spam 60 24 @username`")
        elif data == "my_spam":
            tasks = await db.get_user_spam_tasks(user.id)
            if not tasks:
                await callback_query.message.reply_text("No active spam tasks.")
                return
            reply_text = "📩 **Your Spam Tasks**\n\n" + "\n".join(
                f"ID: `{task['id']}`\nTo: {task['target']}\nInterval: {task['interval']}s\nUntil: {task['end_time']}\n" for task in tasks
            )
            await callback_query.message.edit_text(reply_text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ]))
        elif data == "settings":
            settings = await db.get_user_settings(user.id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"Notifications: {'On' if settings['notifications'] else 'Off'}", callback_data="toggle_notifications")],
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ])
            await callback_query.message.edit_text("⚙️ **Settings**", reply_markup=keyboard)
        elif data == "toggle_notifications":
            settings = await db.get_user_settings(user.id)
            new_value = not settings['notifications']
            await db.toggle_notifications(user.id, new_value)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"Notifications: {'On' if new_value else 'Off'}", callback_data="toggle_notifications")],
                [InlineKeyboardButton("🔙 Back", callback_data="back")]
            ])
            await callback_query.message.edit_text(f"✅ Notifications {'enabled' if new_value else 'disabled'}.", reply_markup=keyboard)
        elif data == "help":
            await callback_query.message.edit_text(
                "ℹ️ **Help**\n\n"
                "Commands:\n"
                "- `/start`: Start bot\n"
                "- `/schedule`: Schedule message\n"
                "- `/spam`: Start spamming\n"
                "- `/settings`: Customize preferences",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back")]])
            )
        elif data.startswith("delete_schedule_"):
            message_id = int(data.split("_")[2])
            await db.update_message_status(message_id, "cancelled")
            await callback_query.message.edit_text("✅ Schedule cancelled!")
            await db.log_usage(user.id, "cancel_schedule", f"ID: {message_id}")
        elif data.startswith("stop_spam_"):
            task_id = int(data.split("_")[2])
            await db.stop_spam_task(task_id)
            await callback_query.message.edit_text("✅ Spam task stopped!")
            await BotUtils.notify_user(client, user.id, f"📩 Spam task `{task_id}` stopped.")
            await db.log_usage(user.id, "stop_spam", f"ID: {task_id}")
        elif data == "back":
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
                [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
                [InlineKeyboardButton("📩 Start Spamming", callback_data="spam")],
                [InlineKeyboardButton("📊 My Spam Tasks", callback_data="my_spam")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
                [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
            ])
            await callback_query.message.edit_text("🤖 **Advanced Bot System**\n\nSelect an option:", reply_markup=keyboard)

        await callback_query.answer()
    except Exception as e:
        logger.error(f"Error in callback query: {e}")
        await callback_query.message.reply_text("⚠️ An error occurred.")

# Background Tasks
async def scheduled_messages_task(client: Client):
    while True:
        try:
            messages = await db.get_pending_messages()
            for msg in messages:
                success = await BotUtils.send_message_with_retry(
                    client, msg['target'], msg['text'], msg['media_path'], msg['media_type'], msg['parse_mode']
                )
                if success:
                    await db.update_message_status(msg['id'], 'sent')
                    await BotUtils.notify_user(client, msg['user_id'], f"✅ Scheduled message `{msg['id']}` sent.")
                    logger.info(f"Sent message {msg['id']}")
                else:
                    await db.update_message_status(msg['id'], 'failed')
                    await BotUtils.notify_user(client, msg['user_id'], f"⚠️ Scheduled message `{msg['id']}` failed.")
                    logger.error(f"Failed message {msg['id']}")
            await asyncio.sleep(Config.SCHEDULE_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"Scheduled task error: {e}")
            await asyncio.sleep(Config.SCHEDULE_CHECK_INTERVAL)

async def spam_messages_task(client: Client):
    task_queue = []
    last_refresh = None

    async def refresh_tasks():
        nonlocal task_queue, last_refresh
        tasks = await db.get_active_spam_tasks()
        task_queue.clear()
        for task in tasks:
            next_send = datetime.datetime.strptime(task['next_send_time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BotUtils.get_timezone())
            heapq.heappush(task_queue, (next_send.timestamp(), task))
        last_refresh = datetime.datetime.now(BotUtils.get_timezone())
        logger.debug("Refreshed spam tasks")

    await refresh_tasks()
    while True:
        try:
            now = datetime.datetime.now(BotUtils.get_timezone())
            if not last_refresh or (now - last_refresh).total_seconds() >= Config.SPAM_TASK_REFRESH_INTERVAL:
                await refresh_tasks()

            if not task_queue:
                await asyncio.sleep(1)
                continue

            next_send_ts, task = task_queue[0]
            next_send = datetime.datetime.fromtimestamp(next_send_ts, tz=BotUtils.get_timezone())

            if now >= next_send:
                heapq.heappop(task_queue)
                end_time = datetime.datetime.strptime(task['end_time'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BotUtils.get_timezone())

                if now >= end_time:
                    await db.stop_spam_task(task['id'])
                    await BotUtils.notify_user(client, task['user_id'], f"🏁 Spam task `{task['id']}` completed.")
                    logger.info(f"Spam task {task['id']} completed")
                    continue

                success = await BotUtils.send_message_with_retry(
                    client, task['target'], task['text'], task['media_path'], task['media_type'], task['parse_mode']
                )
                if success:
                    next_send = now + datetime.timedelta(seconds=task['interval'])
                    await db.update_spam_task_next_send(task['id'], next_send.strftime("%Y-%m-%d %H:%M:%S"))
                    heapq.heappush(task_queue, (next_send.timestamp(), task))
                    logger.debug(f"Sent spam message {task['id']}")
                else:
                    logger.error(f"Failed spam message {task['id']}")

            sleep_time = max(0, (next_send - now).total_seconds())
            await asyncio.sleep(min(sleep_time, 1))
        except Exception as e:
            logger.error(f"Spam task error: {e}")
            await asyncio.sleep(1)

# Main Application
async def main():
    try:
        if not await db.connect():
            logger.error("Database connection failed")
            return

        await app.start()
        me = await app.get_me()
        logger.info(f"Bot started as @{me.username} (ID: {me.id})")

        for admin_id in Config.ADMIN_IDS:
            try:
                await app.send_message(admin_id, "🤖 Bot started successfully!")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")

        asyncio.create_task(scheduled_messages_task(app))
        asyncio.create_task(spam_messages_task(app))
        asyncio.create_task(BotUtils.clean_old_media())

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