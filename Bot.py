import os
import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, User, InlineKeyboardMarkup, 
    InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import (
    RPCError, FloodWait, BadRequest, 
    Unauthorized, SessionPasswordNeeded
)
from pyrogram.session import Session
from pyrogram.handlers import (
    MessageHandler, CallbackQueryHandler
)
from pyrogram.raw.types import DataJSON
import sqlite3
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AdvancedTelegramBot:
    def __init__(self):
        # Initialize directories
        self.config_dir = Path("config")
        self.session_dir = Path("sessions")
        self.data_dir = Path("data")
        self._prepare_directories()
        
        # Initialize database
        self.db_path = self.data_dir / "accounts.db"
        self._init_db()
        
        # Load configuration
        self.config = self._load_config()
        
        # Initialize scheduler
        self.scheduler = AsyncIOScheduler()
        
        # Initialize clients
        self.clients: Dict[str, Client] = {}
        self.user_sessions: Dict[int, List[str]] = {}  # user_id: [session_names]
        self.rate_limits: Dict[int, Dict[str, Tuple[int, float]]] = {}  # user_id: {action: (count, timestamp)}
        
        # Initialize the main bot
        self.main_bot = self._init_main_bot()
        
    def _prepare_directories(self):
        """Ensure required directories exist."""
        self.config_dir.mkdir(exist_ok=True)
        self.session_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
    def _init_db(self):
        """Initialize the SQLite database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                session_name TEXT PRIMARY KEY,
                user_id INTEGER,
                phone_number TEXT,
                api_id INTEGER,
                api_hash TEXT,
                proxy TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP,
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                session_name TEXT,
                task_type TEXT,
                parameters TEXT,
                schedule TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(session_name) REFERENCES accounts(session_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                auto_response_enabled INTEGER DEFAULT 0,
                auto_response_text TEXT,
                security_level INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def _load_config(self) -> dict:
        """Load or create configuration."""
        config_file = self.config_dir / "config.json"
        default_config = {
            "api_id": 25781839,  # Replace with your API ID
            "api_hash "20a3f2f168739259a180dcdd642e196c",  # Replace with your API hash
            "bot_token": "7585970885:AAGgo0Wc1GXEWd6XB_cuQgtp1-q61WAxnvw",  # Replace with your bot token
            "admin_ids": [7584086775],  # Replace with admin user IDs
            "owner_proxy": None,  # Optional proxy configuration for owner accounts
            "default_proxy": None,  # Optional default proxy for all accounts
            "rate_limits": {
                "add_account": (1, 3600),  # 1 account per hour
                "send_message": (10, 60),   # 10 messages per minute
                "create_task": (5, 3600)    # 5 tasks per hour
            },
            "security": {
                "min_join_delay": 10,       # Minimum delay between joining chats (seconds)
                "max_join_delay": 60,
                "min_message_delay": 5,     # Minimum delay between messages
                "max_message_delay": 30,
                "randomize_device": True    # Randomize device info for accounts
            }
        }
        
        if not config_file.exists():
            import json
            with open(config_file, "w") as f:
                json.dump(default_config, f, indent=4)
            logger.warning("Created default config file. Please edit it before running.")
            exit(1)
            
        import json
        with open(config_file) as f:
            return json.load(f)
            
    def _init_main_bot(self) -> Client:
        """Initialize the main bot client."""
        return Client(
            "main_bot",
            api_id=self.config["api_id"],
            api_hash=self.config["api_hash"],
            bot_token=self.config["bot_token"],
            workdir=str(self.session_dir),
            proxy=self.config.get("owner_proxy")
        )
        
    async def start(self):
        """Start the bot system."""
        # Add handlers to main bot
        self._add_main_handlers()
        
        # Start the scheduler
        self.scheduler.start()
        
        # Start clients
        await self.main_bot.start()
        
        # Load existing sessions from database
        await self._load_db_sessions()
        
        logger.info("Bot system started successfully!")
        await idle()
        
    def _add_main_handlers(self):
        """Add command handlers to the main bot."""
        # User commands
        self.main_bot.add_handler(MessageHandler(
            self.handle_start,
            filters.command("start") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_help,
            filters.command("help") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_add_account,
            filters.command("add") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_list_accounts,
            filters.command("list") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_remove_account,
            filters.command("remove") & filters.private
        ))
        
        # Task management
        self.main_bot.add_handler(MessageHandler(
            self.handle_create_task,
            filters.command("createtask") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_list_tasks,
            filters.command("tasks") & filters.private
        ))
        
        # Settings
        self.main_bot.add_handler(MessageHandler(
            self.handle_settings,
            filters.command("settings") & filters.private
        ))
        
        # Admin commands
        self.main_bot.add_handler(MessageHandler(
            self.handle_admin_broadcast,
            filters.command("broadcast") & filters.private & filters.user(self.config["admin_ids"])
        ))
        
        # Callback handlers
        self.main_bot.add_handler(CallbackQueryHandler(
            self.handle_callback,
            filters.create(lambda _, __, query: True)
        ))
        
    async def handle_start(self, client: Client, message: Message):
        """Handle /start command."""
        user = message.from_user
        await message.reply_text(
            f"👋 Hello {user.mention()}!\n\n"
            "🤖 Welcome to the Advanced Telegram Account Manager Bot!\n\n"
            "📌 Use /help to see available commands\n"
            "🔒 Your accounts are private and only visible to you",
            reply_markup=self._get_main_menu_keyboard(user.id)
        )
        
    async def handle_help(self, client: Client, message: Message):
        """Handle /help command."""
        help_text = """
<b>Available Commands:</b>

🔹 <b>Account Management</b>
/add - Add a new Telegram account
/list - List your added accounts
/remove - Remove an account

🔹 <b>Task Management</b>
/createtask - Create a scheduled task
/tasks - List your active tasks

🔹 <b>Settings</b>
/settings - Configure bot settings

🔹 <b>Admin Commands</b>
/broadcast - (Admin only) Broadcast message
"""
        await message.reply_text(help_text, parse_mode="HTML")
        
    def _get_main_menu_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        """Generate the main menu inline keyboard."""
        buttons = [
            [
                InlineKeyboardButton("➕ Add Account", callback_data="add_account"),
                InlineKeyboardButton("📋 My Accounts", callback_data="list_accounts")
            ],
            [
                InlineKeyboardButton("⏰ Create Task", callback_data="create_task"),
                InlineKeyboardButton("⚙️ Settings", callback_data="settings")
            ]
        ]
        
        if user_id in self.config["admin_ids"]:
            buttons.append([
                InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")
            ])
            
        return InlineKeyboardMarkup(buttons)
        
    async def handle_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle all callback queries."""
        user_id = callback_query.from_user.id
        data = callback_query.data
        
        try:
            if data == "add_account":
                await self._prompt_add_account(callback_query)
            elif data == "list_accounts":
                await self._show_user_accounts(callback_query)
            elif data.startswith("remove_account_"):
                session_name = data.split("_")[2]
                await self._confirm_remove_account(callback_query, session_name)
            elif data.startswith("confirm_remove_"):
                session_name = data.split("_")[2]
                await self._perform_remove_account(callback_query, user_id, session_name)
            elif data == "create_task":
                await self._prompt_create_task(callback_query)
            elif data == "settings":
                await self._show_settings(callback_query)
            elif data == "admin_panel":
                if user_id in self.config["admin_ids"]:
                    await self._show_admin_panel(callback_query)
            
            await callback_query.answer()
        except Exception as e:
            logger.error(f"Error handling callback: {str(e)}")
            await callback_query.answer("❌ An error occurred", show_alert=True)
        
    async def _prompt_add_account(self, callback_query: CallbackQuery):
        """Prompt user to add an account."""
        await callback_query.message.edit_text(
            "📱 <b>Add Telegram Account</b>\n\n"
            "Please send your phone number in international format:\n"
            "<code>+1234567890</code>\n\n"
            "⚠️ This will create a session file on the bot server.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            )
        )
        
    async def handle_add_account(self, client: Client, message: Message):
        """Handle adding a new account."""
        user_id = message.from_user.id
        
        # Check rate limit
        if not self._check_rate_limit(user_id, "add_account"):
            await message.reply_text(
                "⏳ You can only add 1 account per hour. Please wait.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text(
                "Usage: /add <phone_number>\nExample: /add +1234567890",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        phone_number = args[1]
        session_name = f"user_{user_id}_account_{phone_number}"
        
        # Check if account already exists
        if self._account_exists(session_name):
            await message.reply_text(
                "⚠️ This account is already added!",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        try:
            # Initialize client with randomized device info if enabled
            client_kwargs = {
                "session_name": session_name,
                "api_id": self.config["api_id"],
                "api_hash": self.config["api_hash"],
                "phone_number": phone_number,
                "workdir": str(self.session_dir),
            }
            
            # Add proxy if configured
            if self.config.get("default_proxy"):
                client_kwargs["proxy"] = self.config["default_proxy"]
                
            # Randomize device info for security
            if self.config["security"].get("randomize_device", True):
                client_kwargs["device_model"] = self._random_device_model()
                client_kwargs["system_version"] = self._random_system_version()
                client_kwargs["app_version"] = self._random_app_version()
                
            new_client = Client(**client_kwargs)
            
            await message.reply_text("🔑 Attempting to login...")
            
            # Start the client to initiate login
            await new_client.start()
            
            # If we get here, login was successful
            self._save_account_to_db(
                session_name=session_name,
                user_id=user_id,
                phone_number=phone_number,
                proxy=self.config.get("default_proxy")
            )
            
            self.clients[session_name] = new_client
            self._update_user_sessions(user_id, session_name)
            
            await message.reply_text(
                f"✅ Account {phone_number} added successfully!",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            
            # Update rate limit
            self._update_rate_limit(user_id, "add_account")
            
        except SessionPasswordNeeded:
            await message.reply_text(
                "🔒 This account has 2FA enabled. Please send the password:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
                ])
            )
            
            try:
                password = await client.ask(
                    message.chat.id,
                    "Please enter your 2FA password:",
                    filters=filters.text,
                    timeout=300
                )
                
                if password.text.startswith("/"):
                    await message.reply_text("❌ Setup canceled")
                    return
                    
                await new_client.check_password(password.text)
                self._save_account_to_db(
                    session_name=session_name,
                    user_id=user_id,
                    phone_number=phone_number,
                    proxy=self.config.get("default_proxy")
                )
                
                self.clients[session_name] = new_client
                self._update_user_sessions(user_id, session_name)
                
                await message.reply_text(
                    f"✅ Account {phone_number} added successfully!",
                    reply_markup=self._get_main_menu_keyboard(user_id)
                )
                
                # Update rate limit
                self._update_rate_limit(user_id, "add_account")
                
            except Exception as e:
                await message.reply_text(f"❌ Failed to add account: {str(e)}")
                if 'new_client' in locals() and new_client.is_initialized:
                    await new_client.stop()
                    
        except Exception as e:
            await message.reply_text(f"❌ Failed to add account: {str(e)}")
            if 'new_client' in locals() and new_client.is_initialized:
                await new_client.stop()
                
    def _random_device_model(self) -> str:
        """Generate random device model for security."""
        models = [
            "iPhone 13 Pro", "Samsung Galaxy S22", 
            "Google Pixel 6", "Xiaomi Mi 11",
            "OnePlus 9 Pro", "Huawei P50"
        ]
        return random.choice(models)
        
    def _random_system_version(self) -> str:
        """Generate random system version."""
        versions = [
            "10", "11", "12", "13", 
            "14", "15", "16", "17"
        ]
        return f"{random.choice(versions)}.{random.randint(0, 9)}"
        
    def _random_app_version(self) -> str:
        """Generate random app version."""
        return f"Telegram {random.randint(7, 9)}.{random.randint(0, 99)}"
        
    def _account_exists(self, session_name: str) -> bool:
        """Check if account exists in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM accounts WHERE session_name = ?",
            (session_name,)
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
        
    def _save_account_to_db(self, session_name: str, user_id: int, 
                          phone_number: str, proxy: Optional[str] = None):
        """Save account to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO accounts 
            (session_name, user_id, phone_number, api_id, api_hash, proxy)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (session_name, user_id, phone_number, 
             self.config["api_id"], self.config["api_hash"], proxy)
        )
        conn.commit()
        conn.close()
        
    def _update_user_sessions(self, user_id: int, session_name: str):
        """Update user's session list."""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = []
        if session_name not in self.user_sessions[user_id]:
            self.user_sessions[user_id].append(session_name)
            
    async def _show_user_accounts(self, callback_query: CallbackQuery):
        """Show user's accounts with management options."""
        user_id = callback_query.from_user.id
        accounts = self._get_user_accounts(user_id)
        
        if not accounts:
            await callback_query.message.edit_text(
                "📭 You don't have any accounts added yet.\n"
                "Use /add to add your first account.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Account", callback_data="add_account")],
                    [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
                ])
            )
            return
            
        text = "📋 <b>Your Accounts:</b>\n\n"
        buttons = []
        
        for account in accounts:
            session_name = account[0]
            phone_number = account[1]
            text += f"• {phone_number} (<code>{session_name}</code>)\n"
            buttons.append([
                InlineKeyboardButton(
                    f"❌ Remove {phone_number}",
                    callback_data=f"remove_account_{session_name}")
            ])
            
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        
        await callback_query.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
    def _get_user_accounts(self, user_id: int) -> List[Tuple[str, str]]:
        """Get user's accounts from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT session_name, phone_number FROM accounts WHERE user_id = ?",
            (user_id,)
        )
        accounts = cursor.fetchall()
        conn.close()
        return accounts
        
    async def _confirm_remove_account(self, callback_query: CallbackQuery, session_name: str):
        """Ask for confirmation before removing account."""
        await callback_query.message.edit_text(
            f"⚠️ <b>Confirm Removal</b>\n\n"
            f"Are you sure you want to remove account <code>{session_name}</code>?\n"
            "This will delete the session file and log out the account.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes", callback_data=f"confirm_remove_{session_name}"),
                    InlineKeyboardButton("❌ No", callback_data="list_accounts")
                ]
            ])
        )
        
    async def _perform_remove_account(self, callback_query: CallbackQuery, user_id: int, session_name: str):
        """Remove the specified account."""
        try:
            # Stop and remove client if active
            if session_name in self.clients:
                await self.clients[session_name].stop()
                del self.clients[session_name]
                
            # Remove from user sessions
            if user_id in self.user_sessions and session_name in self.user_sessions[user_id]:
                self.user_sessions[user_id].remove(session_name)
                
            # Delete session file
            session_file = self.session_dir / f"{session_name}.session"
            if session_file.exists():
                session_file.unlink()
                
            # Remove from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM accounts WHERE session_name = ? AND user_id = ?",
                (session_name, user_id)
            )
            conn.commit()
            conn.close()
            
            await callback_query.message.edit_text(
                f"✅ Account <code>{session_name}</code> removed successfully!",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="list_accounts")]
                ])
            )
        except Exception as e:
            await callback_query.message.edit_text(
                f"❌ Failed to remove account: {str(e)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Back", callback_data="list_accounts")]
                ])
            )
            
    async def handle_list_accounts(self, client: Client, message: Message):
        """Handle /list command."""
        user_id = message.from_user.id
        accounts = self._get_user_accounts(user_id)
        
        if not accounts:
            await message.reply_text(
                "📭 You don't have any accounts added yet.\n"
                "Use /add to add your first account.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        text = "📋 <b>Your Accounts:</b>\n\n"
        for account in accounts:
            session_name = account[0]
            phone_number = account[1]
            text += f"• {phone_number} (<code>{session_name}</code>)\n"
            
        await message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=self._get_main_menu_keyboard(user_id)
        )
        
    async def handle_remove_account(self, client: Client, message: Message):
        """Handle /remove command."""
        user_id = message.from_user.id
        args = message.text.split()
        
        if len(args) < 2:
            await message.reply_text(
                "Usage: /remove <session_name>\n"
                "Use /list to see your account session names",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        session_name = args[1]
        
        # Verify user owns this account
        if not self._user_owns_account(user_id, session_name):
            await message.reply_text(
                "❌ You don't own this account or it doesn't exist.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        try:
            # Stop and remove client if active
            if session_name in self.clients:
                await self.clients[session_name].stop()
                del self.clients[session_name]
                
            # Remove from user sessions
            if user_id in self.user_sessions and session_name in self.user_sessions[user_id]:
                self.user_sessions[user_id].remove(session_name)
                
            # Delete session file
            session_file = self.session_dir / f"{session_name}.session"
            if session_file.exists():
                session_file.unlink()
                
            # Remove from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM accounts WHERE session_name = ? AND user_id = ?",
                (session_name, user_id)
            )
            conn.commit()
            conn.close()
            
            await message.reply_text(
                f"✅ Account <code>{session_name}</code> removed successfully!",
                parse_mode="HTML",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
        except Exception as e:
            await message.reply_text(
                f"❌ Failed to remove account: {str(e)}",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            
    def _user_owns_account(self, user_id: int, session_name: str) -> bool:
        """Check if user owns the specified account."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM accounts WHERE session_name = ? AND user_id = ?",
            (session_name, user_id)
        )
        owns = cursor.fetchone() is not None
        conn.close()
        return owns
        
    async def _prompt_create_task(self, callback_query: CallbackQuery):
        """Prompt user to create a task."""
        user_id = callback_query.from_user.id
        accounts = self._get_user_accounts(user_id)
        
        if not accounts:
            await callback_query.message.edit_text(
                "❌ You need to add at least one account first!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add Account", callback_data="add_account")],
                    [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
                ])
            )
            return
            
        await callback_query.message.edit_text(
            "⏰ <b>Create Scheduled Task</b>\n\n"
            "Please reply with the task details in this format:\n\n"
            "<code>/createtask [account_session] [task_type] [parameters] [schedule]</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/createtask user_123_account_+1234567890 send_message -100123456789 Hello! 30m</code>\n\n"
            "<b>Available Task Types:</b>\n"
            "- send_message [chat_id] [text] - Send message\n"
            "- join_chat [chat_id] - Join chat/channel\n"
            "- leave_chat [chat_id] - Leave chat/channel\n\n"
            "<b>Schedule Formats:</b>\n"
            "- 30m (every 30 minutes)\n"
            "- 2h (every 2 hours)\n"
            "- 1d (every day)\n"
            "- 2023-12-31 23:59 (specific datetime)",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ])
        )
        
    async def handle_create_task(self, client: Client, message: Message):
        """Handle task creation."""
        user_id = message.from_user.id
        
        # Check rate limit
        if not self._check_rate_limit(user_id, "create_task"):
            await message.reply_text(
                "⏳ You've reached the task creation limit. Please wait.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        args = message.text.split(maxsplit=4)
        if len(args) < 5:
            await message.reply_text(
                "Invalid format. Usage:\n"
                "<code>/createtask [account_session] [task_type] [parameters] [schedule]</code>\n\n"
                "Example:\n"
                "<code>/createtask user_123_account_+1234567890 send_message -100123456789 Hello! 30m</code>",
                parse_mode="HTML",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        session_name = args[1]
        task_type = args[2]
        parameters = args[3]
        schedule = args[4]
        
        # Verify user owns the account
        if not self._user_owns_account(user_id, session_name):
            await message.reply_text(
                "❌ You don't own this account or it doesn't exist.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        # Validate task type
        valid_task_types = ["send_message", "join_chat", "leave_chat"]
        if task_type not in valid_task_types:
            await message.reply_text(
                f"❌ Invalid task type. Available types: {', '.join(valid_task_types)}",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        # Add task to database
        task_id = self._add_task_to_db(
            user_id=user_id,
            session_name=session_name,
            task_type=task_type,
            parameters=parameters,
            schedule=schedule
        )
        
        # Schedule the task
        await self._schedule_task(task_id)
        
        await message.reply_text(
            f"✅ Task created successfully! (ID: {task_id})",
            reply_markup=self._get_main_menu_keyboard(user_id)
        )
        
        # Update rate limit
        self._update_rate_limit(user_id, "create_task")
        
    def _add_task_to_db(self, user_id: int, session_name: str, 
                       task_type: str, parameters: str, schedule: str) -> int:
        """Add task to database and return task ID."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO tasks 
            (user_id, session_name, task_type, parameters, schedule)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (user_id, session_name, task_type, parameters, schedule)
        )
        task_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return task_id
        
    async def _schedule_task(self, task_id: int):
        """Schedule a task based on its configuration."""
        task = self._get_task_from_db(task_id)
        if not task:
            return
            
        user_id, session_name, task_type, params, schedule = task
        
        # Parse schedule
        if schedule.endswith("m"):
            interval = int(schedule[:-1]) * 60
            self.scheduler.add_job(
                self._execute_task,
                'interval',
                seconds=interval,
                args=[task_id],
                id=f"task_{task_id}"
            )
        elif schedule.endswith("h"):
            interval = int(schedule[:-1]) * 3600
            self.scheduler.add_job(
                self._execute_task,
                'interval',
                seconds=interval,
                args=[task_id],
                id=f"task_{task_id}"
            )
        elif schedule.endswith("d"):
            interval = int(schedule[:-1]) * 86400
            self.scheduler.add_job(
                self._execute_task,
                'interval',
                seconds=interval,
                args=[task_id],
                id=f"task_{task_id}"
            )
        else:
            # Assume it's a specific datetime
            try:
                run_date = datetime.strptime(schedule, "%Y-%m-%d %H:%M")
                self.scheduler.add_job(
                    self._execute_task,
                    'date',
                    run_date=run_date,
                    args=[task_id],
                    id=f"task_{task_id}"
                )
            except ValueError:
                logger.error(f"Invalid schedule format for task {task_id}")
                
    def _get_task_from_db(self, task_id: int) -> Optional[Tuple[int, str, str, str, str]]:
        """Get task details from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, session_name, task_type, parameters, schedule FROM tasks WHERE id = ?",
            (task_id,)
        )
        task = cursor.fetchone()
        conn.close()
        return task
        
    async def _execute_task(self, task_id: int):
        """Execute a scheduled task."""
        task = self._get_task_from_db(task_id)
        if not task:
            logger.error(f"Task {task_id} not found in database")
            return
            
        user_id, session_name, task_type, params, schedule = task
        
        # Check if account is active
        if session_name not in self.clients:
            logger.error(f"Account {session_name} not active for task {task_id}")
            return
            
        client = self.clients[session_name]
        
        try:
            if task_type == "send_message":
                # Parameters: chat_id text
                parts = params.split(maxsplit=1)
                if len(parts) < 2:
                    logger.error(f"Invalid parameters for send_message task {task_id}")
                    return
                    
                chat_id = parts[0]
                text = parts[1]
                
                # Add random delay for security
                delay = random.randint(
                    self.config["security"]["min_message_delay"],
                    self.config["security"]["max_message_delay"]
                )
                await asyncio.sleep(delay)
                
                await client.send_message(chat_id, text)
                logger.info(f"Task {task_id}: Message sent to {chat_id}")
                
            elif task_type == "join_chat":
                # Parameters: chat_id
                chat_id = params
                
                # Add random delay for security
                delay = random.randint(
                    self.config["security"]["min_join_delay"],
                    self.config["security"]["max_join_delay"]
                )
                await asyncio.sleep(delay)
                
                await client.join_chat(chat_id)
                logger.info(f"Task {task_id}: Joined chat {chat_id}")
                
            elif task_type == "leave_chat":
                # Parameters: chat_id
                chat_id = params
                await client.leave_chat(chat_id)
                logger.info(f"Task {task_id}: Left chat {chat_id}")
                
        except FloodWait as e:
            logger.warning(f"Task {task_id}: Flood wait for {e.value} seconds")
            # Reschedule task after flood wait
            self.scheduler.add_job(
                self._execute_task,
                'date',
                run_date=datetime.now() + timedelta(seconds=e.value),
                args=[task_id],
                id=f"task_{task_id}_retry"
            )
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}")
            
    async def handle_list_tasks(self, client: Client, message: Message):
        """List user's active tasks."""
        user_id = message.from_user.id
        tasks = self._get_user_tasks(user_id)
        
        if not tasks:
            await message.reply_text(
                "📭 You don't have any active tasks.",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        text = "⏰ <b>Your Active Tasks:</b>\n\n"
        for task in tasks:
            task_id, session_name, task_type, params, schedule = task
            text += (
                f"<b>ID:</b> {task_id}\n"
                f"<b>Account:</b> <code>{session_name}</code>\n"
                f"<b>Type:</b> {task_type}\n"
                f"<b>Parameters:</b> {params}\n"
                f"<b>Schedule:</b> {schedule}\n\n"
            )
            
        await message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=self._get_main_menu_keyboard(user_id)
        )
        
    def _get_user_tasks(self, user_id: int) -> List[Tuple[int, str, str, str, str]]:
        """Get user's tasks from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, session_name, task_type, parameters, schedule FROM tasks WHERE user_id = ? AND is_active = 1",
            (user_id,)
        )
        tasks = cursor.fetchall()
        conn.close()
        return tasks
        
    async def _show_settings(self, callback_query: CallbackQuery):
        """Show user settings."""
        user_id = callback_query.from_user.id
        settings = self._get_user_settings(user_id)
        
        auto_response_status = "✅ Enabled" if settings[1] else "❌ Disabled"
        auto_response_text = settings[2] or "Not set"
        security_level = settings[3]
        
        await callback_query.message.edit_text(
            "⚙️ <b>Your Settings</b>\n\n"
            f"<b>Auto-response:</b> {auto_response_status}\n"
            f"<b>Auto-response text:</b> {auto_response_text}\n"
            f"<b>Security level:</b> {security_level}\n\n"
            "Use /settings to update these values.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔙 Back", callback_data="main_menu"),
                    InlineKeyboardButton("🔄 Refresh", callback_data="settings")
                ]
            ])
        )
        
    def _get_user_settings(self, user_id: int) -> Tuple[int, int, str, int]:
        """Get user settings from database, creating default if not exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if settings exist
        cursor.execute(
            "SELECT 1 FROM user_settings WHERE user_id = ?",
            (user_id,)
        )
        exists = cursor.fetchone() is not None
        
        if not exists:
            # Create default settings
            cursor.execute(
                '''
                INSERT INTO user_settings 
                (user_id, auto_response_enabled, auto_response_text, security_level)
                VALUES (?, ?, ?, ?)
                ''',
                (user_id, 0, None, 1)
            )
            conn.commit()
            
        # Get settings
        cursor.execute(
            "SELECT * FROM user_settings WHERE user_id = ?",
            (user_id,)
        )
        settings = cursor.fetchone()
        conn.close()
        return settings
        
    async def handle_settings(self, client: Client, message: Message):
        """Handle /settings command."""
        user_id = message.from_user.id
        args = message.text.split(maxsplit=1)
        
        if len(args) < 2:
            # Show current settings
            settings = self._get_user_settings(user_id)
            
            auto_response_status = "✅ Enabled" if settings[1] else "❌ Disabled"
            auto_response_text = settings[2] or "Not set"
            security_level = settings[3]
            
            await message.reply_text(
                "⚙️ <b>Current Settings</b>\n\n"
                f"<b>Auto-response:</b> {auto_response_status}\n"
                f"<b>Auto-response text:</b> {auto_response_text}\n"
                f"<b>Security level:</b> {security_level}\n\n"
                "<b>To update:</b>\n"
                "<code>/settings auto_response [on/off] [text]</code>\n"
                "<code>/settings security [level 1-3]</code>\n\n"
                "<b>Security Levels:</b>\n"
                "1 - Basic (default)\n"
                "2 - Enhanced (random delays)\n"
                "3 - Maximum (strict rate limits)",
                parse_mode="HTML",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            return
            
        # Parse settings update
        parts = args[1].split(maxsplit=2)
        setting_type = parts[0].lower()
        
        try:
            if setting_type == "auto_response":
                if len(parts) < 2:
                    await message.reply_text(
                        "Usage: /settings auto_response [on/off] [text]",
                        reply_markup=self._get_main_menu_keyboard(user_id)
                    )
                    return
                    
                state = parts[1].lower()
                enabled = 1 if state == "on" else 0
                text = parts[2] if len(parts) > 2 else None
                
                self._update_user_setting(
                    user_id=user_id,
                    setting="auto_response_enabled",
                    value=enabled
                )
                
                if text is not None:
                    self._update_user_setting(
                        user_id=user_id,
                        setting="auto_response_text",
                        value=text
                    )
                    
                await message.reply_text(
                    f"✅ Auto-response set to: {state}\n"
                    f"Text: {text or 'Not changed'}",
                    reply_markup=self._get_main_menu_keyboard(user_id)
                )
                
            elif setting_type == "security":
                if len(parts) < 2:
                    await message.reply_text(
                        "Usage: /settings security [level 1-3]",
                        reply_markup=self._get_main_menu_keyboard(user_id)
                    )
                    return
                    
                level = int(parts[1])
                if not 1 <= level <= 3:
                    await message.reply_text(
                        "Security level must be between 1 and 3",
                        reply_markup=self._get_main_menu_keyboard(user_id)
                    )
                    return
                    
                self._update_user_setting(
                    user_id=user_id,
                    setting="security_level",
                    value=level
                )
                
                await message.reply_text(
                    f"✅ Security level set to: {level}",
                    reply_markup=self._get_main_menu_keyboard(user_id)
                )
                
            else:
                await message.reply_text(
                    "Invalid setting type. Use auto_response or security",
                    reply_markup=self._get_main_menu_keyboard(user_id)
                )
                
        except Exception as e:
            await message.reply_text(
                f"❌ Error updating settings: {str(e)}",
                reply_markup=self._get_main_menu_keyboard(user_id)
            )
            
    def _update_user_setting(self, user_id: int, setting: str, value):
        """Update a user setting in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE user_settings SET {setting} = ? WHERE user_id = ?",
            (value, user_id)
        )
        conn.commit()
        conn.close()
        
    async def _show_admin_panel(self, callback_query: CallbackQuery):
        """Show admin panel."""
        if callback_query.from_user.id not in self.config["admin_ids"]:
            await callback_query.answer("❌ Access denied", show_alert=True)
            return
            
        total_users = self._get_total_users()
        active_sessions = len(self.clients)
        
        await callback_query.message.edit_text(
            "👑 <b>Admin Panel</b>\n\n"
            f"<b>Total Users:</b> {total_users}\n"
            f"<b>Active Sessions:</b> {active_sessions}\n\n"
            "<b>Available Commands:</b>\n"
            "/broadcast - Send message to all users\n"
            "/stats - Show bot statistics",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ])
        )
        
    def _get_total_users(self) -> int:
        """Get total number of unique users."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM accounts")
        count = cursor.fetchone()[0]
        conn.close()
        return count
        
    async def handle_admin_broadcast(self, client: Client, message: Message):
        """Handle admin broadcast command."""
        if message.from_user.id not in self.config["admin_ids"]:
            await message.reply_text("❌ Access denied")
            return
            
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /broadcast <message>")
            return
            
        text = args[1]
        user_ids = self._get_all_user_ids()
        
        success = 0
        failed = 0
        
        for user_id in user_ids:
            try:
                await client.send_message(user_id, text)
                success += 1
            except Exception as e:
                logger.error(f"Failed to send broadcast to {user_id}: {str(e)}")
                failed += 1
            await asyncio.sleep(0.1)  # Rate limiting
            
        await message.reply_text(
            f"📢 Broadcast completed!\n"
            f"✅ Success: {success}\n"
            f"❌ Failed: {failed}"
        )
        
    def _get_all_user_ids(self) -> List[int]:
        """Get all user IDs from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT user_id FROM accounts")
        user_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return user_ids
        
    def _check_rate_limit(self, user_id: int, action: str) -> bool:
        """Check if user has exceeded rate limits for an action."""
        if user_id in self.config["admin_ids"]:
            return True  # Admins are exempt from rate limits
            
        # Get user's security level
        settings = self._get_user_settings(user_id)
        security_level = settings[3]
        
        # Adjust rate limits based on security level
        if action in self.config["rate_limits"]:
            base_limit, base_period = self.config["rate_limits"][action]
            
            # More strict limits for higher security levels
            if security_level >= 3:
                base_limit = max(1, base_limit // 2)
                base_period = base_period * 2
            elif security_level == 2:
                base_limit = max(1, int(base_limit * 0.75))
                base_period = int(base_period * 1.5)
                
            # Initialize rate limit tracking for user if needed
            if user_id not in self.rate_limits:
                self.rate_limits[user_id] = {}
                
            if action not in self.rate_limits[user_id]:
                self.rate_limits[user_id][action] = (0, time.time())
                
            count, timestamp = self.rate_limits[user_id][action]
            
            # Reset counter if period has elapsed
            if time.time() - timestamp > base_period:
                self.rate_limits[user_id][action] = (0, time.time())
                return True
                
            # Check if limit reached
            if count >= base_limit:
                return False
                
        return True
        
    def _update_rate_limit(self, user_id: int, action: str):
        """Update rate limit counter for an action."""
        if user_id in self.config["admin_ids"]:
            return  # Admins are exempt from rate limits
            
        if user_id in self.rate_limits and action in self.rate_limits[user_id]:
            count, timestamp = self.rate_limits[user_id][action]
            self.rate_limits[user_id][action] = (count + 1, timestamp)
            
    async def _load_db_sessions(self):
        """Load existing sessions from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT session_name, user_id FROM accounts WHERE is_active = 1")
        accounts = cursor.fetchall()
        conn.close()
        
        for session_name, user_id in accounts:
            try:
                client = Client(
                    session_name,
                    api_id=self.config["api_id"],
                    api_hash=self.config["api_hash"],
                    workdir=str(self.session_dir),
                    proxy=self.config.get("default_proxy")
                )
                
                await client.start()
                self.clients[session_name] = client
                self._update_user_sessions(user_id, session_name)
                logger.info(f"Loaded session: {session_name}")
                
            except Exception as e:
                logger.error(f"Failed to load session {session_name}: {str(e)}")
                
    async def stop_all(self):
        """Stop all clients gracefully."""
        # Stop all scheduled tasks
        self.scheduler.shutdown()
        
        # Stop all account clients
        for client in self.clients.values():
            try:
                await client.stop()
            except Exception as e:
                logger.error(f"Error stopping client: {str(e)}")
                
        # Stop the main bot
        await self.main_bot.stop()

if __name__ == "__main__":
    bot = AdvancedTelegramBot()
    
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        asyncio.run(bot.stop_all())
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        asyncio.run(bot.stop_all())
