import logging
from typing import Dict, Optional, List
from datetime import datetime
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

# Logging setup (errors only)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.ERROR
)
logger = logging.getLogger(__name__)

# In-memory storage (replace with database in production)
user_data = {}
banned_users = set()
limited_users = set()
user_analytics = {
    'total_users': 0,
    'active_today': set(),
    'commands_processed': 0,
    'user_activity': {}
}

# Terms and Conditions and Privacy Policy (truncated for brevity)
TERMS_AND_CONDITIONS = """[Your full terms here...]"""
PRIVACY_POLICY = """[Your full privacy policy here...]"""

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
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        text=user_details,
        reply_markup=reply_markup,
        parse_mode=None
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks for terms and privacy."""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Track analytics
    AnalyticsTracker.track_command(user_id, query.data)
    
    if query.data == "terms":
        await query.edit_message_text(
            text=TERMS_AND_CONDITIONS,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]])
        )
    elif query.data == "privacy":
        await query.edit_message_text(
            text=PRIVACY_POLICY,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data="back")]])
        )
    elif query.data == "back":
        # Recreate the original message with user details
        user = query.from_user
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
            ]
        ]
        
        await query.edit_message_text(
            text=user_details,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send admin statistics (only accessible to admins)."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    # Track analytics
    AnalyticsTracker.track_command(user.id, 'stats')
    
    stats = AnalyticsTracker.get_global_stats()
    
    stats_message = "ADMIN STATISTICS\n\n"
    stats_message += f"Total users: {stats['total_users']}\n"
    stats_message += f"Active today: {stats['active_today']}\n"
    stats_message += f"Commands processed: {stats['commands_processed']}\n"
    stats_message += f"Banned users: {stats['banned_users']}\n"
    stats_message += f"Limited users: {stats['limited_users']}\n"
    
    await update.message.reply_text(stats_message)

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ban a user from using the bot."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        if UserAccountManager.ban_user(target_id):
            await update.message.reply_text(f"User {target_id} has been banned.")
        else:
            await update.message.reply_text("Cannot ban this user (may be an admin).")
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unban a user."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        if UserAccountManager.unban_user(target_id):
            await update.message.reply_text(f"User {target_id} has been unbanned.")
        else:
            await update.message.reply_text("User was not banned.")
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

async def limit_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Limit a user's access to certain features."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /limit <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        if UserAccountManager.limit_user(target_id):
            await update.message.reply_text(f"User {target_id} has been limited.")
        else:
            await update.message.reply_text("Cannot limit this user (may be an admin).")
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

async def unlimit_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove limitations from a user."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /unlimit <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
        if UserAccountManager.unlimit_user(target_id):
            await update.message.reply_text(f"User {target_id} has been unlimited.")
        else:
            await update.message.reply_text("User was not limited.")
    except ValueError:
        await update.message.reply_text("Invalid user ID.")

async def user_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Get detailed info about a specific user (admin only)."""
    user = update.effective_user
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("This command is restricted to administrators only.")
        return
    
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
    application.add_handler(CommandHandler("stats", admin_stats, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("ban", ban_user, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("unban", unban_user, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("limit", limit_user, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("unlimit", unlimit_user, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("userinfo", user_info, filters=filters.User(ADMIN_IDS)))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Run the bot
    application.run_polling()

if __name__ == "__main__":
    main()