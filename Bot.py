import logging
import sqlite3
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters
)

# Bot Configuration
API_ID = 25781839
API_HASH = "20a3f2f168739259a180dcdd642e196c"
BOT_TOKEN = "7614305417:AAGyXRK5sPap2V2elxVZQyqwfRpVCW6wOFc"
ADMIN_IDS = [7584086775]
BOT_VERSION = "2.1.0"
START_TIME = datetime.now()
DATABASE_FILE = "bot_database.db"

# Rate limiting configuration
RATE_LIMITS = {
    'start': timedelta(seconds=10),
    'stats': timedelta(minutes=1),
    'userinfo': timedelta(seconds=30)
}

# Logging setup (errors only)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.ERROR
)
logger = logging.getLogger(__name__)

# Initialize database
def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_premium INTEGER,
            is_bot INTEGER,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP,
            is_banned INTEGER DEFAULT 0,
            is_limited INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commands (
            command_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            command TEXT,
            timestamp TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            setting_name TEXT PRIMARY KEY,
            setting_value TEXT
        )
    ''')
    
    # Insert default settings if not exists
    cursor.execute('''
        INSERT OR IGNORE INTO bot_settings (setting_name, setting_value)
        VALUES ('terms_and_conditions', 'Default Terms and Conditions. Please update through admin panel.'),
               ('privacy_policy', 'Default Privacy Policy. Please update through admin panel.')
    ''')
    
    conn.commit()
    conn.close()

init_db()

class DatabaseManager:
    """Handles all database operations"""
    
    @staticmethod
    def get_setting(setting_name: str) -> str:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT setting_value FROM bot_settings WHERE setting_name = ?', (setting_name,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else ""
    
    @staticmethod
    def update_setting(setting_name: str, setting_value: str):
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_settings (setting_name, setting_value)
            VALUES (?, ?)
        ''', (setting_name, setting_value))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_user(user_id: int) -> Optional[Dict]:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return dict(zip(columns, result))
        return None
    
    @staticmethod
    def update_user(user_data: Dict):
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO users (
                user_id, username, first_name, last_name, language_code, 
                is_premium, is_bot, first_seen, last_seen, is_banned, is_limited
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_data['user_id'],
            user_data.get('username'),
            user_data.get('first_name'),
            user_data.get('last_name'),
            user_data.get('language_code'),
            user_data.get('is_premium', 0),
            user_data.get('is_bot', 0),
            user_data.get('first_seen', datetime.now()),
            user_data.get('last_seen', datetime.now()),
            user_data.get('is_banned', 0),
            user_data.get('is_limited', 0)
        ))
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def log_command(user_id: int, command: str):
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO commands (user_id, command, timestamp)
            VALUES (?, ?, ?)
        ''', (user_id, command, datetime.now()))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_user_stats(user_id: int) -> Dict:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Get user info
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return {}
        
        # Get command counts
        cursor.execute('''
            SELECT command, COUNT(*) as count 
            FROM commands 
            WHERE user_id = ?
            GROUP BY command
            ORDER BY count DESC
        ''', (user_id,))
        commands = cursor.fetchall()
        
        # Get total commands
        cursor.execute('SELECT COUNT(*) FROM commands WHERE user_id = ?', (user_id,))
        total_commands = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'first_seen': user[7],
            'last_seen': user[8],
            'total_commands': total_commands,
            'commands': dict(commands)
        }
    
    @staticmethod
    def get_global_stats() -> Dict:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Total users
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        # Active today
        today = datetime.now().date()
        cursor.execute('''
            SELECT COUNT(DISTINCT user_id) 
            FROM commands 
            WHERE DATE(timestamp) = ?
        ''', (today,))
        active_today = cursor.fetchone()[0]
        
        # Total commands
        cursor.execute('SELECT COUNT(*) FROM commands')
        total_commands = cursor.fetchone()[0]
        
        # Banned users
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_banned = 1')
        banned_users = cursor.fetchone()[0]
        
        # Limited users
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_limited = 1')
        limited_users = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_users': total_users,
            'active_today': active_today,
            'total_commands': total_commands,
            'banned_users': banned_users,
            'limited_users': limited_users
        }
    
    @staticmethod
    def ban_user(user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected == 0:
            # User doesn't exist, create with banned status
            DatabaseManager.update_user({
                'user_id': user_id,
                'is_banned': 1
            })
        
        return True
    
    @staticmethod
    def unban_user(user_id: int) -> bool:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    
    @staticmethod
    def limit_user(user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_limited = 1 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        if affected == 0:
            # User doesn't exist, create with limited status
            DatabaseManager.update_user({
                'user_id': user_id,
                'is_limited': 1
            })
        
        return True
    
    @staticmethod
    def unlimit_user(user_id: int) -> bool:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_limited = 0 WHERE user_id = ?', (user_id,))
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    
    @staticmethod
    def is_banned(user_id: int) -> bool:
        user = DatabaseManager.get_user(user_id)
        return user and user['is_banned'] == 1
    
    @staticmethod
    def is_limited(user_id: int) -> bool:
        user = DatabaseManager.get_user(user_id)
        return user and user['is_limited'] == 1

class RateLimiter:
    """Handles rate limiting for commands"""
    
    @staticmethod
    def check_rate_limit(user_id: int, command: str) -> Optional[timedelta]:
        """Check if user is rate limited, returns time remaining or None"""
        if command not in RATE_LIMITS:
            return None
            
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT timestamp 
            FROM commands 
            WHERE user_id = ? AND command = ?
            ORDER BY timestamp DESC 
            LIMIT 1
        ''', (user_id, command))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        last_time = datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
        time_passed = datetime.now() - last_time
        limit_duration = RATE_LIMITS[command]
        
        if time_passed < limit_duration:
            return limit_duration - time_passed
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message with the user's Telegram account details and options."""
    user = update.effective_user
    
    # Check rate limiting
    time_remaining = RateLimiter.check_rate_limit(user.id, 'start')
    if time_remaining:
        await update.message.reply_text(
            f"Please wait {time_remaining.seconds} seconds before using /start again."
        )
        return
    
    # Check if user is banned
    if DatabaseManager.is_banned(user.id):
        await update.message.reply_text("Your account has been banned from using this bot.")
        return
    
    # Save/update user data
    user_data = {
        'user_id': user.id,
        'username': user.username,
        'first_name': user.first_name,
        'last_name': user.last_name,
        'language_code': user.language_code,
        'is_premium': int(getattr(user, 'is_premium', False)),
        'is_bot': int(user.is_bot),
        'last_seen': datetime.now()
    }
    DatabaseManager.update_user(user_data)
    DatabaseManager.log_command(user.id, 'start')
    
    # Prepare user details
    user_details = "USER ACCOUNT DETAILS\n\n"
    user_details += f"User ID: {user.id}\n"
    user_details += f"Username: @{user.username}\n" if user.username else "Username: Not set\n"
    user_details += f"First Name: {user.first_name}\n" if user.first_name else "First Name: Not set\n"
    user_details += f"Last Name: {user.last_name}\n" if user.last_name else "Last Name: Not set\n"
    user_details += f"Language Code: {user.language_code}\n" if user.language_code else "Language Code: Not set\n"
    user_details += f"Is Premium: {'Yes' if getattr(user, 'is_premium', False) else 'No'}\n"
    user_details += f"Is Bot: {'Yes' if user.is_bot else 'No'}\n"
    user_details += f"Account Status: {'Limited' if DatabaseManager.is_limited(user.id) else 'Normal'}\n"
    
    # Create keyboard with options
    keyboard = [
        [
            InlineKeyboardButton("Terms and Conditions", callback_data="terms"),
            InlineKeyboardButton("Privacy Policy", callback_data="privacy")
        ],
        [
            InlineKeyboardButton("Bot Version", callback_data="version")
        ]
    ]
    
    # Add admin buttons if user is admin
    if user.id in ADMIN_IDS:
        keyboard.append([
            InlineKeyboardButton("Admin Panel", callback_data="adminpanel")
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text=user_details,
        reply_markup=reply_markup,
        parse_mode=None
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    user = query.from_user
    
    # Log the button press as a command
    DatabaseManager.log_command(user_id, query.data)
    
    if query.data == "terms":
        terms = DatabaseManager.get_setting('terms_and_conditions')
        await query.edit_message_text(
            text=terms,
            reply_markup=back_button_markup()
        )
    elif query.data == "privacy":
        privacy_policy = DatabaseManager.get_setting('privacy_policy')
        await query.edit_message_text(
            text=privacy_policy,
            reply_markup=back_button_markup()
        )
    elif query.data == "version":
        uptime = datetime.now() - START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        version_info = "BOT VERSION\n\n"
        version_info += f"Version: {BOT_VERSION}\n"
        version_info += f"Uptime: {days}d {hours}h {minutes}m\n"
        version_info += f"Started: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        await query.edit_message_text(
            text=version_info,
            reply_markup=back_button_markup()
        )
    elif query.data == "adminpanel":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Access denied.")
            return
            
        keyboard = [
            [
                InlineKeyboardButton("Global Stats", callback_data="adminstats"),
                InlineKeyboardButton("User Lookup", callback_data="userlookup")
            ],
            [
                InlineKeyboardButton("Ban User", callback_data="banuser"),
                InlineKeyboardButton("Unban User", callback_data="unbanuser")
            ],
            [
                InlineKeyboardButton("Limit User", callback_data="limituser"),
                InlineKeyboardButton("Unlimit User", callback_data="unlimituser")
            ],
            [
                InlineKeyboardButton("Update Terms", callback_data="updateterms"),
                InlineKeyboardButton("Update Policy", callback_data="updatepolicy")
            ],
            [
                InlineKeyboardButton("Back", callback_data="back")
            ]
        ]
        
        await query.edit_message_text(
            text="ADMIN PANEL\n\nSelect an option:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif query.data == "adminstats":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Access denied.")
            return
            
        stats = DatabaseManager.get_global_stats()
        uptime = datetime.now() - START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        stats_message = "ADMIN STATISTICS\n\n"
        stats_message += f"Bot Version: {BOT_VERSION}\n"
        stats_message += f"Uptime: {days}d {hours}h {minutes}m\n"
        stats_message += f"Total users: {stats['total_users']}\n"
        stats_message += f"Active today: {stats['active_today']}\n"
        stats_message += f"Commands processed: {stats['total_commands']}\n"
        stats_message += f"Banned users: {stats['banned_users']}\n"
        stats_message += f"Limited users: {stats['limited_users']}\n"
        
        await query.edit_message_text(
            text=stats_message,
            reply_markup=back_to_admin_markup()
        )
    elif query.data == "back":
        # Recreate the original message with user details
        user_details = "USER ACCOUNT DETAILS\n\n"
        user_details += f"User ID: {user.id}\n"
        user_details += f"Username: @{user.username}\n" if user.username else "Username: Not set\n"
        user_details += f"First Name: {user.first_name}\n" if user.first_name else "First Name: Not set\n"
        user_details += f"Last Name: {user.last_name}\n" if user.last_name else "Last Name: Not set\n"
        user_details += f"Language Code: {user.language_code}\n" if user.language_code else "Language Code: Not set\n"
        user_details += f"Is Premium: {'Yes' if getattr(user, 'is_premium', False) else 'No'}\n"
        user_details += f"Is Bot: {'Yes' if user.is_bot else 'No'}\n"
        user_details += f"Account Status: {'Limited' if DatabaseManager.is_limited(user.id) else 'Normal'}\n"
        
        keyboard = [
            [
                InlineKeyboardButton("Terms and Conditions", callback_data="terms"),
                InlineKeyboardButton("Privacy Policy", callback_data="privacy")
            ],
            [
                InlineKeyboardButton("Bot Version", callback_data="version")
            ]
        ]
        
        if user.id in ADMIN_IDS:
            keyboard.append([
                InlineKeyboardButton("Admin Panel", callback_data="adminpanel")
            ])
        
        await query.edit_message_text(
            text=user_details,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif query.data in ["banuser", "unbanuser", "limituser", "unlimituser", "userlookup", "updateterms", "updatepolicy"]:
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Access denied.")
            return
            
        action_map = {
            "banuser": ("Ban User", "Enter the user ID to ban:", "ban"),
            "unbanuser": ("Unban User", "Enter the user ID to unban:", "unban"),
            "limituser": ("Limit User", "Enter the user ID to limit:", "limit"),
            "unlimituser": ("Unlimit User", "Enter the user ID to unlimit:", "unlimit"),
            "userlookup": ("User Lookup", "Enter the user ID to lookup:", "userinfo"),
            "updateterms": ("Update Terms", "Enter the new Terms and Conditions:", "updateterms"),
            "updatepolicy": ("Update Policy", "Enter the new Privacy Policy:", "updatepolicy")
        }
        
        title, prompt, action = action_map[query.data]
        
        context.user_data['pending_action'] = action
        await query.edit_message_text(
            text=f"{title}\n\n{prompt}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data="adminpanel")]])
        )

async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle admin actions that require text input."""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
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
                    await update.message.reply_text(f"User {target_id} has been banned.")
                else:
                    await update.message.reply_text("Cannot ban this user (may be an admin).")
            
            elif action == "unban":
                if DatabaseManager.unban_user(target_id):
                    await update.message.reply_text(f"User {target_id} has been unbanned.")
                else:
                    await update.message.reply_text("User was not banned.")
            
            elif action == "limit":
                if DatabaseManager.limit_user(target_id):
                    await update.message.reply_text(f"User {target_id} has been limited.")
                else:
                    await update.message.reply_text("Cannot limit this user (may be an admin).")
            
            elif action == "unlimit":
                if DatabaseManager.unlimit_user(target_id):
                    await update.message.reply_text(f"User {target_id} has been unlimited.")
                else:
                    await update.message.reply_text("User was not limited.")
            
            elif action == "userinfo":
                stats = DatabaseManager.get_user_stats(target_id)
                
                if not stats:
                    await update.message.reply_text("No data available for this user.")
                    return
                
                info_message = f"USER INFO FOR {target_id}\n\n"
                info_message += f"First seen: {stats['first_seen']}\n"
                info_message += f"Last seen: {stats['last_seen']}\n"
                info_message += f"Total commands: {stats['total_commands']}\n"
                info_message += "\nCOMMAND USAGE:\n"
                
                for cmd, count in stats['commands'].items():
                    info_message += f"{cmd}: {count}\n"
                
                await update.message.reply_text(info_message)
        
        except ValueError:
            await update.message.reply_text("Invalid user ID. Please enter a numeric user ID.")
    
    elif action in ["updateterms", "updatepolicy"]:
        setting_name = "terms_and_conditions" if action == "updateterms" else "privacy_policy"
        DatabaseManager.update_setting(setting_name, text)
        await update.message.reply_text(f"{setting_name.replace('_', ' ').title()} has been updated successfully.")
    
    del context.user_data['pending_action']

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot version and uptime."""
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    version_info = "BOT VERSION\n\n"
    version_info += f"Version: {BOT_VERSION}\n"
    version_info += f"Uptime: {days}d {hours}h {minutes}m\n"
    version_info += f"Started: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    await update.message.reply_text(version_info)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send admin statistics (only accessible to admins)."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    # Check rate limiting
    time_remaining = RateLimiter.check_rate_limit(user.id, 'stats')
    if time_remaining:
        await update.message.reply_text(
            f"Please wait {time_remaining.seconds} seconds before using /stats again."
        )
        return
    
    # Log command
    DatabaseManager.log_command(user.id, 'stats')
    
    stats = DatabaseManager.get_global_stats()
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    stats_message = "ADMIN STATISTICS\n\n"
    stats_message += f"Bot Version: {BOT_VERSION}\n"
    stats_message += f"Uptime: {days}d {hours}h {minutes}m\n"
    stats_message += f"Total users: {stats['total_users']}\n"
    stats_message += f"Active today: {stats['active_today']}\n"
    stats_message += f"Commands processed: {stats['total_commands']}\n"
    stats_message += f"Banned users: {stats['banned_users']}\n"
    stats_message += f"Limited users: {stats['limited_users']}\n"
    
    await update.message.reply_text(stats_message)

async def user_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Get detailed info about a specific user (admin only)."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    # Check rate limiting
    time_remaining = RateLimiter.check_rate_limit(user.id, 'userinfo')
    if time_remaining:
        await update.message.reply_text(
            f"Please wait {time_remaining.seconds} seconds before using /userinfo again."
        )
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /userinfo <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        stats = DatabaseManager.get_user_stats(target_id)
        
        if not stats:
            await update.message.reply_text("No data available for this user.")
            return
        
        info_message = f"USER INFO FOR {target_id}\n\n"
        info_message += f"First seen: {stats['first_seen']}\n"
        info_message += f"Last seen: {stats['last_seen']}\n"
        info_message += f"Total commands: {stats['total_commands']}\n"
        info_message += "\nCOMMAND USAGE:\n"
        
        for cmd, count in stats['commands'].items():
            info_message += f"{cmd}: {count}\n"
        
        await update.message.reply_text(info_message)
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

def back_button_markup():
    """Helper function to create back button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]])

def back_to_admin_markup():
    """Helper function to create back to admin panel button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back to Admin Panel", callback_data="adminpanel")]])

def main() -> None:
    """Run the bot."""
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("version", version))
    application.add_handler(CommandHandler("stats", admin_stats, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("userinfo", user_info, filters=filters.User(ADMIN_IDS)))
    
    # Add message handler for admin actions
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.User(ADMIN_IDS),
        handle_admin_action
    ))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Run the bot
    application.run_polling()

if __name__ == "__main__":
    main()