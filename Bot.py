import logging
import sqlite3
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters
)

# Bot Configuration
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
ADMIN_IDS = [7584086775]
BOT_VERSION = "2.4.0"
START_TIME = datetime.now()
DATABASE_FILE = "bot_database.db"
LOG_FILE = "bot.log"

# Rate limiting configuration
RATE_LIMITS = {
    'start': timedelta(seconds=10),
    'stats': timedelta(minutes=1),
    'userinfo': timedelta(seconds=30),
    'broadcast': timedelta(minutes=5),
    'feedback': timedelta(minutes=2)
}

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations with connection pooling"""
    
    _connection = None
    
    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            cls._connection = sqlite3.connect(DATABASE_FILE)
            cls._connection.row_factory = sqlite3.Row
        return cls._connection
    
    @classmethod
    def close_connection(cls):
        if cls._connection is not None:
            cls._connection.close()
            cls._connection = None
    
    @classmethod
    def init_db(cls):
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT,
                is_premium INTEGER DEFAULT 0,
                is_bot INTEGER DEFAULT 0,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_banned INTEGER DEFAULT 0,
                is_limited INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS commands (
                command_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                command TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_settings (
                setting_name TEXT PRIMARY KEY,
                setting_value TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rate_limits (
                user_id INTEGER,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback (
                feedback_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        
        # Insert default settings if not exists
        cursor.execute('''
            INSERT OR IGNORE INTO bot_settings (setting_name, setting_value)
            VALUES ('terms_and_conditions', 'Default Terms and Conditions. Please update through admin panel.'),
                   ('privacy_policy', 'Default Privacy Policy. Please update through admin panel.'),
                   ('welcome_message', 'Welcome to the bot! Use /start to begin.'),
                   ('feedback_message', 'Thank you for your feedback! We appreciate your input.')
        ''')
        
        conn.commit()
    
    @classmethod
    def get_setting(cls, setting_name: str) -> str:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT setting_value FROM bot_settings WHERE setting_name = ?', (setting_name,))
        result = cursor.fetchone()
        return result['setting_value'] if result else ""
    
    @classmethod
    def update_setting(cls, setting_name: str, setting_value: str):
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_settings (setting_name, setting_value)
            VALUES (?, ?)
        ''', (setting_name, setting_value))
        conn.commit()
    
    @classmethod
    def get_user(cls, user_id: int) -> Optional[Dict]:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return dict(result) if result else None
    
    @classmethod
    def update_user(cls, user_data: Dict):
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        # Handle is_premium which might be None
        is_premium = 1 if user_data.get('is_premium') else 0
        
        cursor.execute('''
            INSERT OR REPLACE INTO users (
                user_id, username, first_name, last_name, language_code, 
                is_premium, is_bot, last_seen, is_banned, is_limited
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_data['user_id'],
            user_data.get('username'),
            user_data.get('first_name'),
            user_data.get('last_name'),
            user_data.get('language_code'),
            is_premium,
            int(user_data.get('is_bot', False)),
            datetime.now(),
            int(user_data.get('is_banned', False)),
            int(user_data.get('is_limited', False))
        ))
        conn.commit()
    
    @classmethod
    def log_command(cls, user_id: int, command: str):
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO commands (user_id, command)
            VALUES (?, ?)
        ''', (user_id, command))
        conn.commit()
    
    @classmethod
    def get_user_stats(cls, user_id: int) -> Dict:
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            return {}
        
        cursor.execute('''
            SELECT command, COUNT(*) as count 
            FROM commands 
            WHERE user_id = ?
            GROUP BY command
            ORDER BY count DESC
        ''', (user_id,))
        commands = {row['command']: row['count'] for row in cursor.fetchall()}
        
        cursor.execute('SELECT COUNT(*) FROM commands WHERE user_id = ?', (user_id,))
        total_commands = cursor.fetchone()[0]
        
        return {
            'first_seen': user['first_seen'],
            'last_seen': user['last_seen'],
            'total_commands': total_commands,
            'commands': commands
        }
    
    @classmethod
    def get_global_stats(cls) -> Dict:
        conn = cls.get_connection()
        cursor = conn.cursor()
        
        stats = {
            'total_users': cursor.execute('SELECT COUNT(*) FROM users').fetchone()[0],
            'active_today': cursor.execute('''
                SELECT COUNT(DISTINCT user_id) 
                FROM commands 
                WHERE DATE(timestamp) = DATE('now')
            ''').fetchone()[0],
            'total_commands': cursor.execute('SELECT COUNT(*) FROM commands').fetchone()[0],
            'banned_users': cursor.execute('SELECT COUNT(*) FROM users WHERE is_banned = 1').fetchone()[0],
            'limited_users': cursor.execute('SELECT COUNT(*) FROM users WHERE is_limited = 1').fetchone()[0],
            'feedback_count': cursor.execute('SELECT COUNT(*) FROM feedback').fetchone()[0]
        }
        
        return stats
    
    @classmethod
    def get_all_users(cls) -> List[Dict]:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users')
        return [dict(row) for row in cursor.fetchall()]
    
    @classmethod
    def update_rate_limit(cls, user_id: int, command: str):
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO rate_limits (user_id, command, last_used)
            VALUES (?, ?, ?)
        ''', (user_id, command, datetime.now()))
        conn.commit()
    
    @classmethod
    def check_rate_limit(cls, user_id: int, command: str) -> Optional[timedelta]:
        if command not in RATE_LIMITS:
            return None
            
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_used 
            FROM rate_limits 
            WHERE user_id = ? AND command = ?
        ''', (user_id, command))
        
        result = cursor.fetchone()
        if not result:
            return None
            
        last_time = datetime.strptime(result['last_used'], '%Y-%m-%d %H:%M:%S.%f')
        time_passed = datetime.now() - last_time
        limit_duration = RATE_LIMITS[command]
        
        return limit_duration - time_passed if time_passed < limit_duration else None
    
    @classmethod
    def ban_user(cls, user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        
        if affected == 0:
            cls.update_user({'user_id': user_id, 'is_banned': 1})
        
        return True
    
    @classmethod
    def unban_user(cls, user_id: int) -> bool:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        return affected > 0
    
    @classmethod
    def limit_user(cls, user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_limited = 1 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        
        if affected == 0:
            cls.update_user({'user_id': user_id, 'is_limited': 1})
        
        return True
    
    @classmethod
    def unlimit_user(cls, user_id: int) -> bool:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_limited = 0 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        return affected > 0
    
    @classmethod
    def is_banned(cls, user_id: int) -> bool:
        user = cls.get_user(user_id)
        return bool(user and user.get('is_banned', 0) == 1)
    
    @classmethod
    def is_limited(cls, user_id: int) -> bool:
        user = cls.get_user(user_id)
        return bool(user and user.get('is_limited', 0) == 1)
    
    @classmethod
    def add_feedback(cls, user_id: int, message: str) -> bool:
        conn = cls.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO feedback (user_id, message)
                VALUES (?, ?)
            ''', (user_id, message))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding feedback: {e}")
            return False
    
    @classmethod
    def get_feedback(cls, limit: int = 10) -> List[Dict]:
        conn = cls.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT f.*, u.username, u.first_name, u.last_name 
            FROM feedback f
            LEFT JOIN users u ON f.user_id = u.user_id
            ORDER BY f.timestamp DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]

def format_time_remaining(seconds: int, command: str) -> str:
    """Format time remaining with a progress bar"""
    total_seconds = RATE_LIMITS[command].total_seconds()
    progress = min(int((seconds / total_seconds) * 10), 10)
    bar = "▓" * progress + "░" * (10 - progress)
    return f"⏳ Cooldown: [{bar}] {seconds}s remaining"

# Initialize database
DatabaseManager.init_db()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message with user's Telegram account details."""
    user = update.effective_user
    
    # Check rate limiting
    if (time_remaining := DatabaseManager.check_rate_limit(user.id, 'start')):
        await update.message.reply_text(
            format_time_remaining(int(time_remaining.total_seconds()), 'start')
        )
        return
    
    # Check if user is banned
    if DatabaseManager.is_banned(user.id):
        await update.message.reply_text("⛔ Your account has been banned from using this bot.")
        return
    
    # Update user data
    user_data = {
        'user_id': user.id,
        'username': user.username,
        'first_name': user.first_name,
        'last_name': user.last_name,
        'language_code': user.language_code,
        'is_premium': getattr(user, 'is_premium', False),
        'is_bot': user.is_bot
    }
    DatabaseManager.update_user(user_data)
    DatabaseManager.log_command(user.id, 'start')
    DatabaseManager.update_rate_limit(user.id, 'start')
    
    # Get welcome message
    welcome_message = DatabaseManager.get_setting('welcome_message')
    
    # Prepare user details
    user_details = f"{welcome_message}\n\nUSER DETAILS:\n\n"
    user_details += f"🆔 User ID: {user.id}\n"
    user_details += f"👤 Username: @{user.username}\n" if user.username else "👤 Username: Not set\n"
    user_details += f"📛 First Name: {user.first_name}\n" if user.first_name else "📛 First Name: Not set\n"
    user_details += f"📛 Last Name: {user.last_name}\n" if user.last_name else "📛 Last Name: Not set\n"
    user_details += f"🌐 Language: {user.language_code}\n" if user.language_code else "🌐 Language: Not set\n"
    user_details += f"💎 Premium: {'Yes' if getattr(user, 'is_premium', False) else 'No'}\n"
    user_details += f"🤖 Bot: {'Yes' if user.is_bot else 'No'}\n"
    
    # Get user status safely
    user_status = "❌ Banned" if DatabaseManager.is_banned(user.id) else (
                 "⚠️ Limited" if DatabaseManager.is_limited(user.id) else "✅ Active")
    user_details += f"🔒 Status: {user_status}\n"
    
    # Create keyboard with options
    keyboard = [
        [InlineKeyboardButton("📜 Terms", callback_data="terms"),
         InlineKeyboardButton("🔏 Privacy", callback_data="privacy")],
        [InlineKeyboardButton("ℹ️ Bot Info", callback_data="version"),
         InlineKeyboardButton("💬 Feedback", callback_data="feedback")]
    ]
    
    if user.id in ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="adminpanel")])
    
    await update.message.reply_text(
        text=user_details,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=None
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all button callbacks."""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    user = query.from_user
    
    # Log the button press
    DatabaseManager.log_command(user_id, query.data)
    
    if query.data == "terms":
        terms = DatabaseManager.get_setting('terms_and_conditions')
        await query.edit_message_text(
            text=f"📜 TERMS AND CONDITIONS\n\n{terms}",
            reply_markup=back_button_markup()
        )
    elif query.data == "privacy":
        policy = DatabaseManager.get_setting('privacy_policy')
        await query.edit_message_text(
            text=f"🔏 PRIVACY POLICY\n\n{policy}",
            reply_markup=back_button_markup()
        )
    elif query.data == "version":
        uptime = datetime.now() - START_TIME
        days, seconds = uptime.days, uptime.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        
        version_info = (
            f"ℹ️ BOT INFORMATION\n\n"
            f"🛠 Version: {BOT_VERSION}\n"
            f"⏱ Uptime: {days}d {hours}h {minutes}m\n"
            f"🚀 Started: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"👥 Total Users: {DatabaseManager.get_global_stats()['total_users']}"
        )
        
        await query.edit_message_text(
            text=version_info,
            reply_markup=back_button_markup()
        )
    elif query.data == "feedback":
        if (time_remaining := DatabaseManager.check_rate_limit(user_id, 'feedback')):
            await query.edit_message_text(
                format_time_remaining(int(time_remaining.total_seconds()), 'feedback')
            )
            return
        
        context.user_data['pending_action'] = 'feedback'
        await query.edit_message_text(
            text="💬 Please send your feedback message:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="back")]])
        )
    elif query.data == "adminpanel":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("⛔ Access denied.")
            return
            
        keyboard = [
            [InlineKeyboardButton("📊 Stats", callback_data="adminstats"),
             InlineKeyboardButton("👤 User Lookup", callback_data="userlookup")],
            [InlineKeyboardButton("⛔ Ban User", callback_data="banuser"),
             InlineKeyboardButton("✅ Unban User", callback_data="unbanuser")],
            [InlineKeyboardButton("🔒 Limit User", callback_data="limituser"),
             InlineKeyboardButton("🔓 Unlimit User", callback_data="unlimituser")],
            [InlineKeyboardButton("📝 Update Terms", callback_data="updateterms"),
             InlineKeyboardButton("📝 Update Policy", callback_data="updatepolicy")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("📩 View Feedback", callback_data="viewfeedback")],
            [InlineKeyboardButton("🔙 Back", callback_data="back")]
        ]
        
        await query.edit_message_text(
            text="👑 ADMIN PANEL\n\nSelect an action:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif query.data == "adminstats":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("⛔ Access denied.")
            return
            
        stats = DatabaseManager.get_global_stats()
        uptime = datetime.now() - START_TIME
        days, seconds = uptime.days, uptime.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        
        stats_message = (
            f"📊 ADMIN STATISTICS\n\n"
            f"🛠 Version: {BOT_VERSION}\n"
            f"⏱ Uptime: {days}d {hours}h {minutes}m\n"
            f"👥 Total users: {stats['total_users']}\n"
            f"🟢 Active today: {stats['active_today']}\n"
            f"🔄 Commands processed: {stats['total_commands']}\n"
            f"⛔ Banned users: {stats['banned_users']}\n"
            f"🔒 Limited users: {stats['limited_users']}\n"
            f"📩 Feedback received: {stats['feedback_count']}"
        )
        
        await query.edit_message_text(
            text=stats_message,
            reply_markup=back_to_admin_markup()
        )
    elif query.data == "viewfeedback":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("⛔ Access denied.")
            return
            
        feedback_list = DatabaseManager.get_feedback()
        if not feedback_list:
            await query.edit_message_text("ℹ️ No feedback has been submitted yet.")
            return
            
        feedback_message = "📩 RECENT FEEDBACK\n\n"
        for i, feedback in enumerate(feedback_list, 1):
            user_info = f"@{feedback['username']}" if feedback['username'] else f"{feedback['first_name']} {feedback['last_name']}"
            feedback_message += (
                f"{i}. From: {user_info} (ID: {feedback['user_id']})\n"
                f"   Message: {feedback['message']}\n"
                f"   Date: {feedback['timestamp']}\n\n"
            )
        
        await query.edit_message_text(
            text=feedback_message,
            reply_markup=back_to_admin_markup()
        )
    elif query.data == "back":
        # Recreate the original start message
        user = query.from_user
        welcome_message = DatabaseManager.get_setting('welcome_message')
        
        user_details = f"{welcome_message}\n\nUSER DETAILS:\n\n"
        user_details += f"🆔 User ID: {user.id}\n"
        user_details += f"👤 Username: @{user.username}\n" if user.username else "👤 Username: Not set\n"
        user_details += f"📛 First Name: {user.first_name}\n" if user.first_name else "📛 First Name: Not set\n"
        user_details += f"📛 Last Name: {user.last_name}\n" if user.last_name else "📛 Last Name: Not set\n"
        user_details += f"🌐 Language: {user.language_code}\n" if user.language_code else "🌐 Language: Not set\n"
        user_details += f"💎 Premium: {'Yes' if getattr(user, 'is_premium', False) else 'No'}\n"
        user_details += f"🤖 Bot: {'Yes' if user.is_bot else 'No'}\n"
        
        # Get user status safely
        user_status = "❌ Banned" if DatabaseManager.is_banned(user.id) else (
                     "⚠️ Limited" if DatabaseManager.is_limited(user.id) else "✅ Active")
        user_details += f"🔒 Status: {user_status}\n"
        
        keyboard = [
            [InlineKeyboardButton("📜 Terms", callback_data="terms"),
             InlineKeyboardButton("🔏 Privacy", callback_data="privacy")],
            [InlineKeyboardButton("ℹ️ Bot Info", callback_data="version"),
             InlineKeyboardButton("💬 Feedback", callback_data="feedback")]
        ]
        
        if user.id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="adminpanel")])
        
        await query.edit_message_text(
            text=user_details,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif query.data in ["banuser", "unbanuser", "limituser", "unlimituser", "userlookup", 
                       "updateterms", "updatepolicy", "broadcast"]:
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("⛔ Access denied.")
            return
            
        action_map = {
            "banuser": ("⛔ Ban User", "Send the user ID to ban:", "ban"),
            "unbanuser": ("✅ Unban User", "Send the user ID to unban:", "unban"),
            "limituser": ("🔒 Limit User", "Send the user ID to limit:", "limit"),
            "unlimituser": ("🔓 Unlimit User", "Send the user ID to unlimit:", "unlimit"),
            "userlookup": ("👤 User Lookup", "Send the user ID to lookup:", "userinfo"),
            "updateterms": ("📝 Update Terms", "Send the new Terms and Conditions:", "updateterms"),
            "updatepolicy": ("📝 Update Policy", "Send the new Privacy Policy:", "updatepolicy"),
            "broadcast": ("📢 Broadcast", "Send the message to broadcast to all users:", "broadcast")
        }
        
        title, prompt, action = action_map[query.data]
        context.user_data['pending_action'] = action
        await query.edit_message_text(
            text=f"{title}\n\n{prompt}\n\nType /cancel to abort.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="adminpanel")]])
        )

async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle admin text input for various actions."""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ This command is restricted to administrators.")
        return
    
    if 'pending_action' not in context.user_data:
        return
    
    action = context.user_data['pending_action']
    text = update.message.text
    
    if action in ["ban", "unban", "limit", "unlimit", "userinfo"]:
        try:
            target_id = int(text)
            
            if action == "ban":
                if DatabaseManager.ban_user(target_id):
                    await update.message.reply_text(f"✅ User {target_id} has been banned.")
                else:
                    await update.message.reply_text("⛔ Cannot ban this user (may be an admin).")
            
            elif action == "unban":
                if DatabaseManager.unban_user(target_id):
                    await update.message.reply_text(f"✅ User {target_id} has been unbanned.")
                else:
                    await update.message.reply_text("ℹ️ User was not banned.")
            
            elif action == "limit":
                if DatabaseManager.limit_user(target_id):
                    await update.message.reply_text(f"🔒 User {target_id} has been limited.")
                else:
                    await update.message.reply_text("⛔ Cannot limit this user (may be an admin).")
            
            elif action == "unlimit":
                if DatabaseManager.unlimit_user(target_id):
                    await update.message.reply_text(f"🔓 User {target_id} has been unlimited.")
                else:
                    await update.message.reply_text("ℹ️ User was not limited.")
            
            elif action == "userinfo":
                stats = DatabaseManager.get_user_stats(target_id)
                
                if not stats:
                    await update.message.reply_text("ℹ️ No data available for this user.")
                    return
                
                info_message = (
                    f"👤 USER INFO FOR {target_id}\n\n"
                    f"🕒 First seen: {stats['first_seen']}\n"
                    f"🕒 Last seen: {stats['last_seen']}\n"
                    f"🔄 Total commands: {stats['total_commands']}\n"
                    f"\n📊 COMMAND USAGE:\n"
                )
                
                for cmd, count in stats['commands'].items():
                    info_message += f"{cmd}: {count}\n"
                
                await update.message.reply_text(info_message)
        
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please enter a numeric ID.")
    
    elif action in ["updateterms", "updatepolicy"]:
        setting_name = "terms_and_conditions" if action == "updateterms" else "privacy_policy"
        DatabaseManager.update_setting(setting_name, text)
        await update.message.reply_text(f"✅ {setting_name.replace('_', ' ').title()} updated successfully!")
    
    elif action == "broadcast":
        if (time_remaining := DatabaseManager.check_rate_limit(user.id, 'broadcast')):
            await update.message.reply_text(
                format_time_remaining(int(time_remaining.total_seconds()), 'broadcast')
            )
            return
        
        users = DatabaseManager.get_all_users()
        success = 0
        failed = 0
        
        await update.message.reply_text(f"📢 Starting broadcast to {len(users)} users...")
        
        for user_data in users:
            try:
                await context.bot.send_message(
                    chat_id=user_data['user_id'],
                    text=f"📢 Announcement:\n\n{text}"
                )
                success += 1
            except Exception as e:
                logger.error(f"Failed to send broadcast to {user_data['user_id']}: {e}")
                failed += 1
        
        DatabaseManager.update_rate_limit(user.id, 'broadcast')
        await update.message.reply_text(
            f"📢 Broadcast completed!\n"
            f"✅ Success: {success}\n"
            f"❌ Failed: {failed}"
        )
    
    del context.user_data['pending_action']

async def handle_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle user feedback submission."""
    user = update.effective_user
    
    if 'pending_action' not in context.user_data or context.user_data['pending_action'] != 'feedback':
        return
    
    feedback_message = update.message.text
    
    # Log the feedback
    if DatabaseManager.add_feedback(user.id, feedback_message):
        feedback_response = DatabaseManager.get_setting('feedback_message')
        await update.message.reply_text(feedback_response)
    else:
        await update.message.reply_text("❌ Failed to submit feedback. Please try again later.")
    
    del context.user_data['pending_action']

async def cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel any pending admin action."""
    if 'pending_action' in context.user_data:
        del context.user_data['pending_action']
        await update.message.reply_text("❌ Action cancelled.")

async def version_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /version command."""
    uptime = datetime.now() - START_TIME
    days, seconds = uptime.days, uptime.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    version_info = (
        f"ℹ️ BOT INFORMATION\n\n"
        f"🛠 Version: {BOT_VERSION}\n"
        f"⏱ Uptime: {days}d {hours}h {minutes}m\n"
        f"🚀 Started: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    await update.message.reply_text(version_info)

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stats command for admins."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ This command is restricted to administrators.")
        return
    
    if (time_remaining := DatabaseManager.check_rate_limit(user.id, 'stats')):
        await update.message.reply_text(
            format_time_remaining(int(time_remaining.total_seconds()), 'stats')
        )
        return
    
    DatabaseManager.log_command(user.id, 'stats')
    DatabaseManager.update_rate_limit(user.id, 'stats')
    
    stats = DatabaseManager.get_global_stats()
    uptime = datetime.now() - START_TIME
    days, seconds = uptime.days, uptime.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    stats_message = (
        f"📊 ADMIN STATISTICS\n\n"
        f"🛠 Version: {BOT_VERSION}\n"
        f"⏱ Uptime: {days}d {hours}h {minutes}m\n"
        f"👥 Total users: {stats['total_users']}\n"
        f"🟢 Active today: {stats['active_today']}\n"
        f"🔄 Commands processed: {stats['total_commands']}\n"
        f"⛔ Banned users: {stats['banned_users']}\n"
        f"🔒 Limited users: {stats['limited_users']}\n"
        f"📩 Feedback received: {stats['feedback_count']}"
    )
    
    await update.message.reply_text(stats_message)

async def user_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /userinfo command for admins."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ This command is restricted to administrators.")
        return
    
    if (time_remaining := DatabaseManager.check_rate_limit(user.id, 'userinfo')):
        await update.message.reply_text(
            format_time_remaining(int(time_remaining.total_seconds()), 'userinfo')
        )
        return
    
    if not context.args:
        await update.message.reply_text("ℹ️ Usage: /userinfo <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        DatabaseManager.log_command(user.id, 'userinfo')
        DatabaseManager.update_rate_limit(user.id, 'userinfo')
        
        stats = DatabaseManager.get_user_stats(target_id)
        
        if not stats:
            await update.message.reply_text("ℹ️ No data available for this user.")
            return
        
        info_message = (
            f"👤 USER INFO FOR {target_id}\n\n"
            f"🕒 First seen: {stats['first_seen']}\n"
            f"🕒 Last seen: {stats['last_seen']}\n"
            f"🔄 Total commands: {stats['total_commands']}\n"
            f"\n📊 COMMAND USAGE:\n"
        )
        
        for cmd, count in stats['commands'].items():
            info_message += f"{cmd}: {count}\n"
        
        await update.message.reply_text(info_message)
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID. Please enter a numeric ID.")

def back_button_markup():
    """Helper function to create back button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back")]])

def back_to_admin_markup():
    """Helper function to create back to admin panel button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin", callback_data="adminpanel")]])

def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and handle them gracefully."""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    
    if update and hasattr(update, 'effective_user'):
        user = update.effective_user
        try:
            context.bot.send_message(
                chat_id=user.id,
                text="❌ An error occurred. Please try again later."
            )
        except Exception:
            pass

def main() -> None:
    """Run the bot."""
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add error handler
    application.add_error_handler(error_handler)

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("version", version_command))
    application.add_handler(CommandHandler("stats", admin_stats_command, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("userinfo", user_info_command, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("cancel", cancel_action, filters=filters.User(ADMIN_IDS)))
    
    # Add message handlers
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_IDS),
        handle_admin_action
    ))
    
    # Add message handler for feedback
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        handle_feedback
    ))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Run the bot
    logger.info("Starting bot...")
    application.run_polling()
    
    # Close database connection when bot stops
    DatabaseManager.close_connection()
    logger.info("Bot stopped")

if __name__ == "__main__":
    main()