import logging
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
BOT_VERSION = "2.0.0"
START_TIME = datetime.now()

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

# In-memory storage
user_data = {}
banned_users = set()
limited_users = set()
user_analytics = {
    'total_users': 0,
    'active_today': set(),
    'commands_processed': 0,
    'user_activity': {}
}
last_command_time = {}  # For rate limiting

class UserAccountManager:
    """Handles user account status and restrictions"""
    
    @staticmethod
    def ban_user(user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        banned_users.add(user_id)
        return True
    
    @staticmethod
    def unban_user(user_id: int) -> bool:
        if user_id in banned_users:
            banned_users.remove(user_id)
            return True
        return False
    
    @staticmethod
    def limit_user(user_id: int) -> bool:
        if user_id in ADMIN_IDS:
            return False
        limited_users.add(user_id)
        return True
    
    @staticmethod
    def unlimit_user(user_id: int) -> bool:
        if user_id in limited_users:
            limited_users.remove(user_id)
            return True
        return False
    
    @staticmethod
    def is_banned(user_id: int) -> bool:
        return user_id in banned_users
    
    @staticmethod
    def is_limited(user_id: int) -> bool:
        return user_id in limited_users

class RateLimiter:
    """Handles rate limiting for commands"""
    
    @staticmethod
    def check_rate_limit(user_id: int, command: str) -> Optional[timedelta]:
        """Check if user is rate limited, returns time remaining or None"""
        if command not in RATE_LIMITS:
            return None
            
        last_time = last_command_time.get((user_id, command))
        if not last_time:
            return None
            
        time_passed = datetime.now() - last_time
        limit_duration = RATE_LIMITS[command]
        
        if time_passed < limit_duration:
            return limit_duration - time_passed
        return None

    @staticmethod
    def update_last_command(user_id: int, command: str):
        """Update the last command time for rate limiting"""
        last_command_time[(user_id, command)] = datetime.now()

class AnalyticsTracker:
    """Tracks and manages user analytics"""
    
    @staticmethod
    def track_command(user_id: int, command: str):
        today = datetime.now().date()
        
        # Update global stats
        user_analytics['commands_processed'] += 1
        
        # Update user activity
        if user_id not in user_analytics['user_activity']:
            user_analytics['user_activity'][user_id] = {
                'first_seen': datetime.now(),
                'last_seen': datetime.now(),
                'command_count': 0,
                'commands': {}
            }
            user_analytics['total_users'] += 1
        
        user_analytics['user_activity'][user_id]['last_seen'] = datetime.now()
        user_analytics['user_activity'][user_id]['command_count'] += 1
        
        if command not in user_analytics['user_activity'][user_id]['commands']:
            user_analytics['user_activity'][user_id]['commands'][command] = 0
        user_analytics['user_activity'][user_id]['commands'][command] += 1
        
        # Track daily active users
        if str(today) not in user_analytics['user_activity'][user_id]:
            user_analytics['user_activity'][user_id][str(today)] = True
            user_analytics['active_today'].add(user_id)
    
    @staticmethod
    def get_user_stats(user_id: int) -> Dict:
        return user_analytics['user_activity'].get(user_id, {})
    
    @staticmethod
    def get_global_stats() -> Dict:
        stats = {
            'total_users': user_analytics['total_users'],
            'active_today': len(user_analytics['active_today']),
            'commands_processed': user_analytics['commands_processed'],
            'banned_users': len(banned_users),
            'limited_users': len(limited_users)
        }
        return stats

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
    
    # Update last command time
    RateLimiter.update_last_command(user.id, 'start')
    
    # Check if user is banned
    if UserAccountManager.is_banned(user.id):
        await update.message.reply_text("Your account has been banned from using this bot.")
        return
    
    # Track analytics
    AnalyticsTracker.track_command(user.id, 'start')
    
    # Prepare user details
    user_details = "USER ACCOUNT DETAILS\n\n"
    user_details += f"User ID: {user.id}\n"
    user_details += f"Username: @{user.username}\n" if user.username else "Username: Not set\n"
    user_details += f"First Name: {user.first_name}\n" if user.first_name else "First Name: Not set\n"
    user_details += f"Last Name: {user.last_name}\n" if user.last_name else "Last Name: Not set\n"
    user_details += f"Language Code: {user.language_code}\n" if user.language_code else "Language Code: Not set\n"
    user_details += f"Is Premium: {user.is_premium}\n" if hasattr(user, 'is_premium') else ""
    user_details += f"Is Bot: {user.is_bot}\n"
    user_details += f"Account Status: {'Limited' if UserAccountManager.is_limited(user.id) else 'Normal'}\n"
    
    # Create keyboard with options
    keyboard = [
        [
            InlineKeyboardButton("Terms and Conditions", callback_data="terms"),
            InlineKeyboardButton("Privacy Policy", callback_data="privacy")
        ],
        [
            InlineKeyboardButton("Bot Version", callback_data="version"),
            InlineKeyboardButton("My Stats", callback_data="mystats")
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
    
    # Track analytics
    AnalyticsTracker.track_command(user_id, query.data)
    
    if query.data == "terms":
        await query.edit_message_text(
            text=TERMS_AND_CONDITIONS,
            reply_markup=back_button_markup()
        )
    elif query.data == "privacy":
        await query.edit_message_text(
            text=PRIVACY_POLICY,
            reply_markup=back_button_markup()
        )
    elif query.data == "version":
        uptime = datetime.now() - START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        version_info = f"BOT VERSION\n\n"
        version_info += f"Version: {BOT_VERSION}\n"
        version_info += f"Uptime: {days}d {hours}h {minutes}m\n"
        version_info += f"Started: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        await query.edit_message_text(
            text=version_info,
            reply_markup=back_button_markup()
        )
    elif query.data == "mystats":
        stats = AnalyticsTracker.get_user_stats(user_id)
        
        if not stats:
            await query.edit_message_text("No statistics available yet.")
            return
        
        stats_message = "YOUR STATISTICS\n\n"
        stats_message += f"First seen: {stats['first_seen']}\n"
        stats_message += f"Last seen: {stats['last_seen']}\n"
        stats_message += f"Total commands: {stats['command_count']}\n"
        stats_message += "\nCOMMAND USAGE:\n"
        
        for cmd, count in stats['commands'].items():
            stats_message += f"{cmd}: {count}\n"
        
        await query.edit_message_text(
            text=stats_message,
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
            
        stats = AnalyticsTracker.get_global_stats()
        uptime = datetime.now() - START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        
        stats_message = "ADMIN STATISTICS\n\n"
        stats_message += f"Bot Version: {BOT_VERSION}\n"
        stats_message += f"Uptime: {days}d {hours}h {minutes}m\n"
        stats_message += f"Total users: {stats['total_users']}\n"
        stats_message += f"Active today: {stats['active_today']}\n"
        stats_message += f"Commands processed: {stats['commands_processed']}\n"
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
        user_details += f"Is Premium: {user.is_premium}\n" if hasattr(user, 'is_premium') else ""
        user_details += f"Is Bot: {user.is_bot}\n"
        user_details += f"Account Status: {'Limited' if UserAccountManager.is_limited(user.id) else 'Normal'}\n"
        
        keyboard = [
            [
                InlineKeyboardButton("Terms and Conditions", callback_data="terms"),
                InlineKeyboardButton("Privacy Policy", callback_data="privacy")
            ],
            [
                InlineKeyboardButton("Bot Version", callback_data="version"),
                InlineKeyboardButton("My Stats", callback_data="mystats")
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

def back_button_markup():
    """Helper function to create back button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]])

def back_to_admin_markup():
    """Helper function to create back to admin panel button markup"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back to Admin Panel", callback_data="adminpanel")]])

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot version and uptime."""
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    version_info = f"BOT VERSION\n\n"
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
    
    # Update last command time
    RateLimiter.update_last_command(user.id, 'stats')
    
    # Track analytics
    AnalyticsTracker.track_command(user.id, 'stats')
    
    stats = AnalyticsTracker.get_global_stats()
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    stats_message = "ADMIN STATISTICS\n\n"
    stats_message += f"Bot Version: {BOT_VERSION}\n"
    stats_message += f"Uptime: {days}d {hours}h {minutes}m\n"
    stats_message += f"Total users: {stats['total_users']}\n"
    stats_message += f"Active today: {stats['active_today']}\n"
    stats_message += f"Commands processed: {stats['commands_processed']}\n"
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
    
    # Update last command time
    RateLimiter.update_last_command(user.id, 'userinfo')
    
    if not context.args:
        await update.message.reply_text("Usage: /userinfo <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        stats = AnalyticsTracker.get_user_stats(target_id)
        
        if not stats:
            await update.message.reply_text("No data available for this user.")
            return
        
        info_message = f"USER INFO FOR {target_id}\n\n"
        info_message += f"First seen: {stats['first_seen']}\n"
        info_message += f"Last seen: {stats['last_seen']}\n"
        info_message += f"Total commands: {stats['command_count']}\n"
        info_message += "\nCOMMAND USAGE:\n"
        
        for cmd, count in stats['commands'].items():
            info_message += f"{cmd}: {count}\n"
        
        await update.message.reply_text(info_message)
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

def main() -> None:
    """Run the bot."""
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("version", version))
    application.add_handler(CommandHandler("stats", admin_stats, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("userinfo", user_info, filters=filters.User(ADMIN_IDS)))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Run the bot
    application.run_polling()

if __name__ == "__main__":
    main()