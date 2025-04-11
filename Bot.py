import logging
from typing import Dict, Optional
from telegram import __version__ as TG_VER
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

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Terms and Conditions
TERMS_AND_CONDITIONS = """
TERMS AND CONDITIONS

1. Acceptance of Terms
By using this bot, you agree to comply with and be bound by these terms and conditions.

2. Data Collection
This bot collects basic Telegram account information necessary for functionality, including but not limited to user ID, username, first name, last name, and language preferences.

3. Data Usage
Collected data is used solely for the purpose of providing bot services and is not shared with third parties without your consent.

4. User Responsibilities
You agree not to use this bot for any unlawful purpose or in any way that might harm, damage, or disparage any other party.

5. Limitation of Liability
The bot developer shall not be liable for any indirect, incidental, special, consequential, or punitive damages resulting from your use of this service.
"""

# Privacy Policy
PRIVACY_POLICY = """
PRIVACY POLICY

1. Information We Collect
We collect information provided by Telegram's API when you interact with our bot, including your user ID, username, first name, last name, and language code.

2. How We Use Information
The information is used to:
- Provide and improve bot functionality
- Respond to user requests
- Ensure compliance with our terms of service

3. Data Storage
User data is stored securely and only for as long as necessary to provide our services.

4. Data Sharing
We do not sell, trade, or otherwise transfer your personally identifiable information to outside parties.

5. Security
We implement appropriate technical and organizational measures to protect user data.
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message with the user's Telegram account details and options."""
    user = update.effective_user
    
    # Prepare user details
    user_details = f"USER ACCOUNT DETAILS\n\n"
    user_details += f"User ID: {user.id}\n"
    user_details += f"Username: @{user.username}\n" if user.username else "Username: Not set\n"
    user_details += f"First Name: {user.first_name}\n" if user.first_name else "First Name: Not set\n"
    user_details += f"Last Name: {user.last_name}\n" if user.last_name else "Last Name: Not set\n"
    user_details += f"Language Code: {user.language_code}\n" if user.language_code else "Language Code: Not set\n"
    user_details += f"Is Premium: {user.is_premium}\n" if hasattr(user, 'is_premium') else ""
    user_details += f"Is Bot: {user.is_bot}\n"
    
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
        user = update.effective_user
        user_details = f"USER ACCOUNT DETAILS\n\n"
        user_details += f"User ID: {user.id}\n"
        user_details += f"Username: @{user.username}\n" if user.username else "Username: Not set\n"
        user_details += f"First Name: {user.first_name}\n" if user.first_name else "First Name: Not set\n"
        user_details += f"Last Name: {user.last_name}\n" if user.last_name else "Last Name: Not set\n"
        user_details += f"Language Code: {user.language_code}\n" if user.language_code else "Language Code: Not set\n"
        user_details += f"Is Premium: {user.is_premium}\n" if hasattr(user, 'is_premium') else ""
        user_details += f"Is Bot: {user.is_bot}\n"
        
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
    
    # Basic stats - in a real bot you would track these metrics
    stats = "ADMIN STATISTICS\n\n"
    stats += "Total users: [Not implemented in this example]\n"
    stats += "Active today: [Not implemented in this example]\n"
    stats += "Commands processed: [Not implemented in this example]\n"
    
    await update.message.reply_text(stats)

def main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", admin_stats, filters=filters.User(ADMIN_IDS)))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()