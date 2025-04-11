import os
import asyncio
import datetime
import logging
import aiosqlite
from typing import Dict, List, Optional, Union
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    User
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

# Enhanced logging
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

# Database Models
class UserModel:
    def __init__(self, data: Dict):
        self.id = data['id']
        self.telegram_id = data['telegram_id']
        self.username = data['username']
        self.first_name = data['first_name']
        self.last_name = data['last_name']
        self.join_date = data['join_date']
        self.last_active = data['last_active']
        self.language_code = data['language_code']

class ScheduledMessage:
    def __init__(self, data: Dict):
        self.id = data['id']
        self.user_id = data['user_id']
        self.target = data['target']
        self.target_type = data['target_type']
        self.text = data['text']
        self.media_path = data['media_path']
        self.media_type = data['media_type']
        self.parse_mode = data['parse_mode']
        self.scheduled_time = data['scheduled_time']
        self.status = data['status']
        self.created_at = data['created_at']
        self.retry_count = data['retry_count']

# Database Manager
class DatabaseManager:
    def __init__(self):
        self.db_path = Config.DB_NAME
    
    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()
    
    async def _create_tables(self):
        await self.conn.execute("""
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
        
        await self.conn.execute("""
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
        
        await self.conn.execute("""
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
        
        await self.conn.commit()
    
    async def get_user(self, telegram_id: int) -> Optional[UserModel]:
        cursor = await self.conn.execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,)
        )
        row = await cursor.fetchone()
        return UserModel(dict(row)) if row else None
    
    async def create_user(self, user_data: Dict) -> int:
        cursor = await self.conn.execute(
            """INSERT INTO users 
            (telegram_id, username, first_name, last_name, language_code)
            VALUES (?, ?, ?, ?, ?)""",
            (user_data['id'], user_data.get('username'), 
             user_data.get('first_name'), user_data.get('last_name'),
             user_data.get('language_code', 'en'))
        )
        await self.conn.commit()
        return cursor.lastrowid
    
    async def update_user_activity(self, telegram_id: int):
        await self.conn.execute(
            "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE telegram_id = ?",
            (telegram_id,)
        )
        await self.conn.commit()
    
    async def schedule_message(self, data: Dict) -> int:
        cursor = await self.conn.execute(
            """INSERT INTO scheduled_messages 
            (user_id, target, target_type, text, media_path, media_type, parse_mode, scheduled_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (data['user_id'], data['target'], data['target_type'], data['text'],
             data.get('media_path'), data.get('media_type'), data.get('parse_mode', 'markdown'), 
             data['scheduled_time'])
        )
        await self.conn.commit()
        return cursor.lastrowid
    
    async def get_pending_messages(self) -> List[ScheduledMessage]:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor = await self.conn.execute(
            """SELECT * FROM scheduled_messages
            WHERE status = 'pending' AND scheduled_time <= ?""",
            (now,)
        )
        rows = await cursor.fetchall()
        return [ScheduledMessage(dict(row)) for row in rows]
    
    async def update_message_status(self, message_id: int, status: str):
        await self.conn.execute(
            "UPDATE scheduled_messages SET status = ? WHERE id = ?",
            (status, message_id)
        )
        await self.conn.commit()
    
    async def increment_retry_count(self, message_id: int):
        await self.conn.execute(
            "UPDATE scheduled_messages SET retry_count = retry_count + 1 WHERE id = ?",
            (message_id,)
        )
        await self.conn.commit()
    
    async def add_group_participation(self, data: Dict):
        await self.conn.execute(
            """INSERT INTO group_participation 
            (user_id, group_link, group_title, group_id, status)
            VALUES (?, ?, ?, ?, ?)""",
            (data['user_id'], data['group_link'], data['group_title'], 
             data.get('group_id'), data.get('status', 'joined'))
        )
        await self.conn.commit()
    
    async def get_user_scheduled_messages(self, user_id: int, limit: int = 10) -> List[Dict]:
        cursor = await self.conn.execute(
            """SELECT id, target, target_type, scheduled_time, status 
            FROM scheduled_messages 
            WHERE user_id = ? 
            ORDER BY scheduled_time DESC 
            LIMIT ?""",
            (user_id, limit)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    async def delete_scheduled_message(self, message_id: int, user_id: int) -> bool:
        cursor = await self.conn.execute(
            "DELETE FROM scheduled_messages WHERE id = ? AND user_id = ?",
            (message_id, user_id)
        )
        await self.conn.commit()
        return cursor.rowcount > 0
    
    async def close(self):
        await self.conn.close()

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
        
        if not media_type:
            return None
        
        file_id = message.media_group_id or message.id
        file_name = f"{file_id}{file_ext}"
        file_path = os.path.join(Config.MEDIA_DIR, file_name)
        
        try:
            await client.download_media(message, file_name=file_path)
            return file_name, media_type
        except Exception as e:
            logger.error(f"Failed to save media: {str(e)}")
            return None
    
    @staticmethod
    async def send_message_with_retry(
        client: Client,
        target: str,
        text: str,
        media_path: Optional[str] = None,
        media_type: Optional[str] = None,
        parse_mode: str = "markdown"
    ) -> Optional[Message]:
        retries = 0
        last_error = None
        
        while retries < Config.MAX_RETRIES:
            try:
                if media_path:
                    full_path = os.path.join(Config.MEDIA_DIR, media_path)
                    
                    if media_type == "photo":
                        return await client.send_photo(
                            chat_id=target,
                            photo=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "video":
                        return await client.send_video(
                            chat_id=target,
                            video=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "document":
                        return await client.send_document(
                            chat_id=target,
                            document=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                    elif media_type == "audio":
                        return await client.send_audio(
                            chat_id=target,
                            audio=full_path,
                            caption=text,
                            parse_mode=parse_mode
                        )
                else:
                    return await client.send_message(
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
                break
            except RPCError as e:
                logger.error(f"Attempt {retries + 1} failed: {str(e)}")
                last_error = str(e)
                retries += 1
                await asyncio.sleep(Config.RETRY_DELAY)
        
        raise Exception(f"Failed after {retries} retries. Last error: {last_error}")

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

# Background Tasks
async def scheduled_messages_task():
    await db.connect()
    
    while True:
        try:
            messages = await db.get_pending_messages()
            
            for msg in messages:
                try:
                    result = await BotUtils.send_message_with_retry(
                        client=app,
                        target=msg.target,
                        text=msg.text,
                        media_path=msg.media_path,
                        media_type=msg.media_type,
                        parse_mode=msg.parse_mode
                    )
                    
                    await db.update_message_status(msg.id, 'sent')
                    logger.info(f"Successfully sent message {msg.id} to {msg.target}")
                    
                except Exception as e:
                    logger.error(f"Failed to send message {msg.id}: {str(e)}")
                    await db.increment_retry_count(msg.id)
                    
                    if msg.retry_count >= Config.MAX_RETRIES - 1:
                        await db.update_message_status(msg.id, 'failed')
                        logger.error(f"Message {msg.id} marked as failed after max retries")
                    
        except Exception as e:
            logger.error(f"Error in scheduled messages loop: {str(e)}")
        
        await asyncio.sleep(60)

# Command Handlers
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    try:
        user = message.from_user
        logger.info(f"Start command from {user.id} (@{user.username})")
        
        await db.connect()
        await db.update_user_activity(user.id)
        
        if not await db.get_user(user.id):
            await db.create_user({
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
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await message.reply_text("⚠️ An error occurred. Please try again later.")

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
    try:
        user = message.from_user
        await db.connect()
        await db.update_user_activity(user.id)
        
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

@app.on_callback_query(filters.regex(r"^delete_(\d+)$"))
async def delete_schedule_callback(client: Client, callback: CallbackQuery):
    try:
        message_id = int(callback.data.split("_")[1])
        user_id = callback.from_user.id
        
        await db.connect()
        deleted = await db.delete_scheduled_message(message_id, user_id)
        
        if deleted:
            await callback.answer("Schedule deleted successfully!")
            await callback.message.edit_text("🗑 Schedule has been deleted.")
        else:
            await callback.answer("Schedule not found or you don't have permission!", show_alert=True)
    except Exception as e:
        logger.error(f"Error deleting schedule: {e}")
        await callback.answer("Failed to delete schedule!", show_alert=True)

@app.on_message(filters.regex(r"https?://t\.me/") | filters.regex(r"t\.me/"))
async def handle_invite_links(client: Client, message: Message):
    try:
        user = message.from_user
        link = message.text.strip()
        
        await db.connect()
        await db.update_user_activity(user.id)
        
        try:
            chat = await client.join_chat(link)
            
            await db.add_group_participation({
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
    except Exception as e:
        logger.error(f"Error handling invite link: {e}")
        await message.reply_text("⚠️ Failed to process group link. Please try again.")

# Admin Commands
@app.on_message(filters.user(Config.ADMIN_IDS) & filters.command("stats"))
async def admin_stats_command(client: Client, message: Message):
    try:
        await db.connect()
        
        users = await db.conn.execute_fetchall("SELECT COUNT(*) as count FROM users")
        messages = await db.conn.execute_fetchall("SELECT COUNT(*) as count FROM scheduled_messages")
        groups = await db.conn.execute_fetchall("SELECT COUNT(*) as count FROM group_participation")
        
        stats_text = f"""
📊 **Bot Statistics**

👥 Users: {users[0]['count']}
📨 Scheduled Messages: {messages[0]['count']}
👥 Groups/Channels: {groups[0]['count']}
"""
        await message.reply_text(stats_text)
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply_text("⚠️ Failed to get statistics. Please try again.")

# Main Function
async def main():
    try:
        await db.connect()
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
        await asyncio.Event().wait()
        
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    finally:
        await db.close()
        if 'app' in locals() and app.is_initialized:
            await app.stop()
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")