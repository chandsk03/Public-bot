import os
import asyncio
import datetime
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument
)
from pyrogram.errors import FloodWait, PeerIdInvalid
import sqlite3
from typing import Union, List, Dict, Any

# Configuration
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7585970885:AAGgo0Wc1GXEWd6XB_cuQgtp1-q61WAxnvw"
ADMIN_IDS = [7584086775]
DB_NAME = "bot_database.db"
MEDIA_DIR = "media"

# Ensure media directory exists
os.makedirs(MEDIA_DIR, exist_ok=True)

# Initialize SQLite Database
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER UNIQUE,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_active TIMESTAMP
    )
    """)
    
    cursor.execute("""
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
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER REFERENCES users(id),
        target TEXT,
        target_type TEXT CHECK(target_type IN ('user', 'group', 'channel')),
        text TEXT,
        media_path TEXT,
        media_type TEXT CHECK(media_type IN (NULL, 'photo', 'video', 'document')),
        scheduled_time TIMESTAMP,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'sent', 'failed')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS group_participation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER REFERENCES users(id),
        group_link TEXT,
        group_title TEXT,
        join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'joined' CHECK(status IN ('joined', 'left', 'banned'))
    )
    """)
    
    conn.commit()
    conn.close()

init_db()

# Database Helper Functions
class Database:
    @staticmethod
    def execute(query: str, params: tuple = (), fetch_one: bool = False):
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        
        if fetch_one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
            
        conn.close()
        return result
    
    @staticmethod
    def user_exists(telegram_id: int) -> bool:
        return bool(Database.execute(
            "SELECT 1 FROM users WHERE telegram_id = ?",
            (telegram_id,),
            fetch_one=True
        ))
    
    @staticmethod
    def create_user(user_data: Dict[str, Any]) -> int:
        Database.execute(
            """INSERT INTO users (telegram_id, username, first_name, last_name, last_active)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (user_data['id'], user_data.get('username'), 
             user_data.get('first_name'), user_data.get('last_name'))
        )
        return Database.execute("SELECT last_insert_rowid()", fetch_one=True)[0]
    
    @staticmethod
    def update_user_activity(telegram_id: int):
        Database.execute(
            "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE telegram_id = ?",
            (telegram_id,)
        )
    
    @staticmethod
    def schedule_message(data: Dict[str, Any]) -> int:
        Database.execute(
            """INSERT INTO scheduled_messages 
            (user_id, target, target_type, text, media_path, media_type, scheduled_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (data['user_id'], data['target'], data['target_type'], data['text'],
             data.get('media_path'), data.get('media_type'), data['scheduled_time'])
        )
        return Database.execute("SELECT last_insert_rowid()", fetch_one=True)[0]
    
    @staticmethod
    def get_pending_messages() -> List[Dict[str, Any]]:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = Database.execute(
            """SELECT id, user_id, target, target_type, text, media_path, media_type
            FROM scheduled_messages
            WHERE status = 'pending' AND scheduled_time <= ?""",
            (now,)
        )
        
        return [{
            'id': row[0],
            'user_id': row[1],
            'target': row[2],
            'target_type': row[3],
            'text': row[4],
            'media_path': row[5],
            'media_type': row[6]
        } for row in rows]
    
    @staticmethod
    def update_message_status(message_id: int, status: str):
        Database.execute(
            "UPDATE scheduled_messages SET status = ? WHERE id = ?",
            (status, message_id)
        )
    
    @staticmethod
    def add_group_participation(user_id: int, group_link: str, group_title: str):
        Database.execute(
            """INSERT INTO group_participation (user_id, group_link, group_title)
            VALUES (?, ?, ?)""",
            (user_id, group_link, group_title)
        )

# Initialize Pyrogram Client
app = Client(
    "advanced_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Background Task for Scheduled Messages
async def send_scheduled_messages():
    while True:
        try:
            messages = Database.get_pending_messages()
            
            for msg in messages:
                try:
                    if msg['media_path']:
                        media_path = os.path.join(MEDIA_DIR, msg['media_path'])
                        
                        if msg['media_type'] == 'photo':
                            await app.send_photo(
                                msg['target'],
                                photo=media_path,
                                caption=msg['text']
                            )
                        elif msg['media_type'] == 'video':
                            await app.send_video(
                                msg['target'],
                                video=media_path,
                                caption=msg['text']
                            )
                        elif msg['media_type'] == 'document':
                            await app.send_document(
                                msg['target'],
                                document=media_path,
                                caption=msg['text']
                            )
                    else:
                        await app.send_message(
                            msg['target'],
                            msg['text']
                        )
                    
                    Database.update_message_status(msg['id'], 'sent')
                except Exception as e:
                    print(f"Failed to send message {msg['id']}: {str(e)}")
                    Database.update_message_status(msg['id'], 'failed')
                    
        except Exception as e:
            print(f"Error in scheduled messages loop: {str(e)}")
        
        await asyncio.sleep(60)  # Check every minute

# Helper Functions
def parse_schedule_time(time_str: str) -> Union[datetime.datetime, None]:
    try:
        return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        try:
            return datetime.datetime.strptime(time_str, "%H:%M")
        except ValueError:
            return None

def save_media(message: Message) -> Union[str, None]:
    if not (message.photo or message.video or message.document):
        return None
    
    file_ext = ""
    media_type = ""
    
    if message.photo:
        file_ext = ".jpg"
        media_type = "photo"
    elif message.video:
        file_ext = ".mp4"
        media_type = "video"
    elif message.document:
        file_ext = os.path.splitext(message.document.file_name)[1]
        media_type = "document"
    
    file_id = message.media_group_id or message.id
    file_name = f"{file_id}{file_ext}"
    file_path = os.path.join(MEDIA_DIR, file_name)
    
    try:
        app.download_media(message, file_name=file_path)
        return file_name, media_type
    except Exception as e:
        print(f"Failed to save media: {str(e)}")
        return None

# Command Handlers
@app.on_message(filters.command("start"))
async def start(client: Client, message: Message):
    user = message.from_user
    Database.update_user_activity(user.id)
    
    if not Database.user_exists(user.id):
        Database.create_user({
            'id': user.id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name
        })
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Schedule Message", callback_data="schedule")],
        [InlineKeyboardButton("📊 My Scheduled", callback_data="my_scheduled")],
        [InlineKeyboardButton("➕ Join Group", callback_data="join_group")]
    ])
    
    await message.reply_text(
        "🤖 **Advanced Bot System**\n\n"
        "I can help you schedule messages and join groups automatically!\n"
        "Use the buttons below to get started.",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex("^schedule$"))
async def schedule_callback(client: Client, callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "📝 **Schedule a Message**\n\n"
        "Reply to a message or send a new one with the following format:\n"
        "`/schedule 2023-12-31 23:59 @username`\n\n"
        "You can also attach media (photo, video, document).\n"
        "Replace `@username` with a username, group link, or channel link.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")]
        ])
    )

@app.on_message(filters.command("schedule"))
async def schedule_message(client: Client, message: Message):
    user = message.from_user
    Database.update_user_activity(user.id)
    
    if not message.reply_to_message and not (message.text or message.caption):
        await message.reply_text("Please reply to a message or include text to schedule.")
        return
    
    # Parse command arguments
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
    
    scheduled_time = parse_schedule_time(time_str)
    if not scheduled_time:
        await message.reply_text("Invalid time format. Use YYYY-MM-DD HH:MM or HH:MM")
        return
    
    # Determine target type
    if target.startswith("@"):
        target_type = "user"
    elif target.startswith(("https://t.me/", "t.me/")):
        target_type = "group" if "+" in target else "channel"
    else:
        await message.reply_text("Invalid target. Use @username or group link")
        return
    
    # Get message content
    if message.reply_to_message:
        content_msg = message.reply_to_message
    else:
        content_msg = message
    
    text = content_msg.text or content_msg.caption or ""
    media_info = save_media(content_msg) if content_msg.media else (None, None)
    
    # Store in database
    Database.schedule_message({
        'user_id': user.id,
        'target': target,
        'target_type': target_type,
        'text': text,
        'media_path': media_info[0] if media_info else None,
        'media_type': media_info[1] if media_info else None,
        'scheduled_time': scheduled_time.strftime("%Y-%m-%d %H:%M:%S")
    })
    
    await message.reply_text(
        f"✅ Message scheduled for {scheduled_time.strftime('%Y-%m-%d %H:%M')} to {target}"
    )

@app.on_callback_query(filters.regex("^join_group$"))
async def join_group_callback(client: Client, callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(
        "👥 **Join Group/Channel**\n\n"
        "Send me the invite link of the group/channel you want me to join.\n"
        "Example:\n"
        "`https://t.me/joinchat/ABCDEFG12345`\n\n"
        "I'll join and track my participation there.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")]
        ])
    )

@app.on_message(filters.regex(r"https?://t\.me/") | filters.regex(r"t\.me/"))
async def handle_group_link(client: Client, message: Message):
    user = message.from_user
    Database.update_user_activity(user.id)
    
    link = message.text.strip()
    try:
        # Try to join the chat
        chat = await client.join_chat(link)
        
        # Store participation
        Database.add_group_participation(
            user.id,
            link,
            chat.title
        )
        
        await message.reply_text(
            f"✅ Successfully joined {chat.title}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_main")]
            ])
        )
    except Exception as e:
        await message.reply_text(f"❌ Failed to join: {str(e)}")

@app.on_callback_query(filters.regex("^my_scheduled$"))
async def my_scheduled_callback(client: Client, callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    
    scheduled = Database.execute(
        """SELECT target, scheduled_time, status 
        FROM scheduled_messages 
        WHERE user_id = ? 
        ORDER BY scheduled_time DESC 
        LIMIT 10""",
        (user_id,)
    )
    
    if not scheduled:
        text = "📭 You have no scheduled messages."
    else:
        text = "📅 Your Scheduled Messages:\n\n"
        for idx, (target, time, status) in enumerate(scheduled, 1):
            text += f"{idx}. {target} at {time} ({status})\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Back", callback_data="back_to_main")]
        ])
    )

@app.on_callback_query(filters.regex("^back_to_main$"))
async def back_to_main(client: Client, callback: CallbackQuery):
    await callback.answer()
    await start(client, callback.message)

# Admin Commands
@app.on_message(filters.user(ADMIN_IDS) & filters.command("stats"))
async def admin_stats(client: Client, message: Message):
    user_count = Database.execute("SELECT COUNT(*) FROM users", fetch_one=True)[0]
    message_count = Database.execute("SELECT COUNT(*) FROM scheduled_messages", fetch_one=True)[0]
    group_count = Database.execute("SELECT COUNT(*) FROM group_participation", fetch_one=True)[0]
    
    await message.reply_text(
        "📊 **Bot Statistics**\n\n"
        f"👥 Users: {user_count}\n"
        f"📨 Scheduled Messages: {message_count}\n"
        f"👥 Groups/Channels: {group_count}"
    )

# Start the bot
async def main():
    await app.start()
    print("Bot started!")
    
    # Start background tasks
    asyncio.create_task(send_scheduled_messages())
    
    # Keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())