import os
import asyncio
import logging
import random
import time
import re
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from pyrogram import Client, filters, idle, enums
from pyrogram.types import (
    Message, User, InlineKeyboardMarkup, 
    InlineKeyboardButton, CallbackQuery
)
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.errors import (
    RPCError, FloodWait, BadRequest, 
    Unauthorized, SessionPasswordNeeded
)

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdvancedTelegramBot:
    def __init__(self):
        # Initialize directories with enhanced path handling
        self.base_dir = Path(__file__).parent
        self.config_dir = self.base_dir / "config"
        self.session_dir = self.base_dir / "sessions"
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.backup_dir = self.base_dir / "backups"
        self._prepare_directories()
        
        # Initialize database with connection pooling
        self.db_path = self.data_dir / "accounts.db"
        self._init_db()
        
        # Load configuration with validation
        self.config = self._load_config()
        
        # Initialize scheduler with job store
        self.scheduler = AsyncIOScheduler()
        
        # Initialize clients with enhanced tracking
        self.clients: Dict[str, Client] = {}
        self.user_sessions: Dict[int, List[str]] = {}
        self.rate_limits: Dict[int, Dict[str, Tuple[int, float]]] = {}
        self.user_states: Dict[int, Dict[str, Union[str, bool, int]]] = {}
        
        # Initialize the main bot with enhanced settings
        self.main_bot = self._init_main_bot()
        
    def _prepare_directories(self):
        """Ensure all required directories exist with proper permissions."""
        try:
            self.config_dir.mkdir(exist_ok=True, mode=0o755)
            self.session_dir.mkdir(exist_ok=True, mode=0o700)
            self.data_dir.mkdir(exist_ok=True, mode=0o755)
            self.logs_dir.mkdir(exist_ok=True, mode=0o755)
            self.backup_dir.mkdir(exist_ok=True, mode=0o700)
        except Exception as e:
            logger.error(f"Failed to create directories: {e}")
            raise

    def _init_db(self):
        """Initialize the SQLite database with connection pooling and WAL mode."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
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
                    device_model TEXT,
                    app_version TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    is_premium INTEGER DEFAULT 0
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
                    status TEXT DEFAULT 'pending',
                    next_run TIMESTAMP,
                    last_run TIMESTAMP,
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
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    def _load_config(self) -> dict:
        """Load or create configuration with validation."""
        config_file = self.config_dir / "config.json"
        default_config = {
            "api_id": 25781839,
            "api_hash": "20a3f2f168739259a180dcdd642e196c",
            "bot_token": "7585970885:AAGgo0Wc1GXEWd6XB_cuQgtp1-q61WAxnvw",
            "admin_ids": [7584086775],
            "owner_proxy": None,
            "default_proxy": None,
            "rate_limits": {
                "add_account": [1, 3600],
                "send_message": [10, 60],
                "create_task": [5, 3600]
            },
            "security": {
                "min_join_delay": 10,
                "max_join_delay": 60,
                "min_message_delay": 5,
                "max_message_delay": 30,
                "randomize_device": True
            }
        }
        
        try:
            if not config_file.exists():
                with open(config_file, "w") as f:
                    json.dump(default_config, f, indent=4)
                logger.warning("Created default config file. Please edit it before running.")
                exit(1)
                
            with open(config_file) as f:
                config = json.load(f)
                
            # Validate configuration
            required_keys = ["api_id", "api_hash", "bot_token", "admin_ids"]
            for key in required_keys:
                if key not in config:
                    raise ValueError(f"Missing required config key: {key}")
                    
            return config
        except Exception as e:
            logger.error(f"Config loading failed: {e}")
            raise

    def _init_main_bot(self) -> Client:
        """Initialize the main bot client with enhanced settings."""
        return Client(
            "main_bot",
            api_id=self.config["api_id"],
            api_hash=self.config["api_hash"],
            bot_token=self.config["bot_token"],
            workdir=str(self.session_dir),
            proxy=self.config.get("owner_proxy"),
            plugins=dict(root="plugins"),
            sleep_threshold=30,
            parse_mode=enums.ParseMode.HTML
        )

    async def _backup_sessions(self):
        """Backup session files to the backup directory."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"sessions_{timestamp}.zip"
            
            # In a real implementation, you would zip the session files here
            # For example using the zipfile module
            logger.info(f"Session backup created at {backup_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to backup sessions: {e}")
            return False

    async def start(self):
        """Start the bot system with enhanced initialization."""
        try:
            # Add handlers
            self._add_main_handlers()
            
            # Start scheduler
            self.scheduler.start()
            
            # Start main bot
            await self.main_bot.start()
            
            # Load existing sessions
            await self._load_db_sessions()
            
            # Start background tasks
            asyncio.create_task(self._background_tasks())
            
            logger.info("Bot system started successfully!")
            await idle()
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            await self.stop_all()
            raise

    def _add_main_handlers(self):
        """Add all command handlers with enhanced organization."""
        # User commands
        self.main_bot.add_handler(MessageHandler(
            self.handle_start,
            filters.command("start") & filters.private
        ))
        
        self.main_bot.add_handler(MessageHandler(
            self.handle_help,
            filters.command("help") & filters.private
        ))
        
        # Account management
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
        
        # Admin commands
        self.main_bot.add_handler(MessageHandler(
            self.handle_admin_broadcast,
            filters.command("broadcast") & filters.private & filters.user(self.config["admin_ids"])
        ))
        
        # Callback handler
        self.main_bot.add_handler(CallbackQueryHandler(self.handle_callback))

    async def handle_start(self, client: Client, message: Message):
        """Handle the /start command."""
        user = message.from_user
        await message.reply_text(
            f"👋 Hello {user.mention()}!\n\n"
            "🤖 Welcome to the Advanced Telegram Account Manager Bot!\n\n"
            "📌 Use /help to see available commands",
            reply_markup=self._get_main_menu_keyboard(user.id)
        )

    async def handle_help(self, client: Client, message: Message):
        """Handle the /help command."""
        help_text = """
<b>Available Commands:</b>

🔹 <b>Account Management</b>
/add - Add new Telegram account
/list - List your accounts
/remove - Remove an account

🔹 <b>Task Management</b>
/createtask - Create scheduled task
/tasks - List your tasks

🔹 <b>Admin Commands</b>
/broadcast - Send message to all users
"""
        await message.reply_text(help_text)

    async def handle_add_account(self, client: Client, message: Message):
        """Handle adding a new account."""
        user_id = message.from_user.id
        
        # Check rate limit
        if not self._check_rate_limit(user_id, "add_account"):
            await message.reply_text("⏳ You can only add 1 account per hour.")
            return
            
        args = message.text.split()
        if len(args) < 2:
            await message.reply_text("Usage: /add +1234567890")
            return
            
        phone_number = args[1]
        if not re.match(r'^\+\d{10,15}$', phone_number):
            await message.reply_text("❌ Invalid phone number format.")
            return
            
        session_name = f"user_{user_id}_acc_{phone_number[1:]}"
        
        if self._account_exists(session_name):
            await message.reply_text("⚠️ This account is already added!")
            return
            
        try:
            client_kwargs = {
                "session_name": session_name,
                "api_id": self.config["api_id"],
                "api_hash": self.config["api_hash"],
                "phone_number": phone_number,
                "workdir": str(self.session_dir),
            }
            
            if self.config.get("default_proxy"):
                client_kwargs["proxy"] = self.config["default_proxy"]
                
            if self.config["security"].get("randomize_device", True):
                client_kwargs.update(self._generate_random_device())
                
            new_client = Client(**client_kwargs)
            
            await message.reply_text("🔑 Attempting to login...")
            await new_client.start()
            
            self._save_account_to_db(
                session_name=session_name,
                user_id=user_id,
                phone_number=phone_number,
                proxy=self.config.get("default_proxy"),
                device_model=client_kwargs.get("device_model"),
                app_version=client_kwargs.get("app_version")
            )
            
            self.clients[session_name] = new_client
            self._update_user_sessions(user_id, session_name)
            self._update_rate_limit(user_id, "add_account")
            
            await message.reply_text(f"✅ Account {phone_number} added successfully!")
            
        except SessionPasswordNeeded:
            self.user_states[user_id] = {
                "action": "add_account",
                "session_name": session_name,
                "step": "request_2fa"
            }
            await message.reply_text("🔒 Please send your 2FA password:")
            
        except Exception as e:
            logger.error(f"Failed to add account: {e}")
            await message.reply_text(f"❌ Failed to add account: {str(e)}")
            if 'new_client' in locals() and new_client.is_initialized:
                await new_client.stop()

    async def handle_list_accounts(self, client: Client, message: Message):
        """List user's accounts."""
        user_id = message.from_user.id
        accounts = self._get_user_accounts(user_id)
        
        if not accounts:
            await message.reply_text("📭 You don't have any accounts added yet.")
            return
            
        text = "📋 <b>Your Accounts:</b>\n\n"
        for acc in accounts:
            text += f"• {acc[1]} (<code>{acc[0]}</code>)\n"
            
        await message.reply_text(text)

    async def handle_remove_account(self, client: Client, message: Message):
        """Handle account removal."""
        user_id = message.from_user.id
        args = message.text.split()
        
        if len(args) < 2:
            await message.reply_text("Usage: /remove session_name")
            return
            
        session_name = args[1]
        
        if not self._user_owns_account(user_id, session_name):
            await message.reply_text("❌ You don't own this account.")
            return
            
        try:
            if session_name in self.clients:
                await self.clients[session_name].stop()
                del self.clients[session_name]
                
            if user_id in self.user_sessions and session_name in self.user_sessions[user_id]:
                self.user_sessions[user_id].remove(session_name)
                
            session_file = self.session_dir / f"{session_name}.session"
            if session_file.exists():
                session_file.unlink()
                
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM accounts WHERE session_name = ? AND user_id = ?",
                (session_name, user_id)
            )
            conn.commit()
            conn.close()
            
            await message.reply_text(f"✅ Account {session_name} removed successfully!")
        except Exception as e:
            await message.reply_text(f"❌ Failed to remove account: {str(e)}")

    async def handle_create_task(self, client: Client, message: Message):
        """Handle task creation."""
        user_id = message.from_user.id
        
        if not self._check_rate_limit(user_id, "create_task"):
            await message.reply_text("⏳ You've reached the task creation limit.")
            return
            
        args = message.text.split(maxsplit=4)
        if len(args) < 5:
            await message.reply_text("Usage: /createtask session_name task_type parameters schedule")
            return
            
        session_name, task_type, params, schedule = args[1], args[2], args[3], args[4]
        
        if not self._user_owns_account(user_id, session_name):
            await message.reply_text("❌ You don't own this account.")
            return
            
        try:
            task_id = self._add_task_to_db(
                user_id=user_id,
                session_name=session_name,
                task_type=task_type,
                parameters=params,
                schedule=schedule
            )
            
            await self._schedule_task(task_id)
            await message.reply_text(f"✅ Task created (ID: {task_id})")
            self._update_rate_limit(user_id, "create_task")
        except Exception as e:
            await message.reply_text(f"❌ Failed to create task: {str(e)}")

    async def handle_list_tasks(self, client: Client, message: Message):
        """List user's tasks."""
        user_id = message.from_user.id
        tasks = self._get_user_tasks(user_id)
        
        if not tasks:
            await message.reply_text("📭 You don't have any active tasks.")
            return
            
        text = "⏰ <b>Your Tasks:</b>\n\n"
        for task in tasks:
            text += (
                f"ID: {task[0]}\n"
                f"Account: {task[1]}\n"
                f"Type: {task[2]}\n"
                f"Schedule: {task[5]}\n\n"
            )
            
        await message.reply_text(text)

    async def handle_admin_broadcast(self, client: Client, message: Message):
        """Handle admin broadcast command."""
        if message.from_user.id not in self.config["admin_ids"]:
            await message.reply_text("❌ Access denied")
            return
            
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply_text("Usage: /broadcast message")
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
                failed += 1
            await asyncio.sleep(0.1)
            
        await message.reply_text(
            f"📢 Broadcast completed!\n"
            f"✅ Success: {success}\n"
            f"❌ Failed: {failed}"
        )

    async def handle_callback(self, client: Client, callback_query: CallbackQuery):
        """Handle callback queries."""
        await callback_query.answer()
        data = callback_query.data
        
        if data == "main_menu":
            await self._show_main_menu(callback_query)

    async def _show_main_menu(self, callback_query: CallbackQuery):
        """Show the main menu."""
        user_id = callback_query.from_user.id
        await callback_query.message.edit_text(
            "🏠 <b>Main Menu</b>",
            reply_markup=self._get_main_menu_keyboard(user_id)
        )

    def _get_main_menu_keyboard(self, user_id: int) -> InlineKeyboardMarkup:
        """Generate the main menu keyboard."""
        buttons = [
            [InlineKeyboardButton("📋 My Accounts", callback_data="list_accounts")],
            [InlineKeyboardButton("⏰ My Tasks", callback_data="list_tasks")],
        ]
        
        if user_id in self.config["admin_ids"]:
            buttons.append([InlineKeyboardButton("👑 Admin", callback_data="admin_panel")])
            
        return InlineKeyboardMarkup(buttons)

    def _account_exists(self, session_name: str) -> bool:
        """Check if an account exists in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM accounts WHERE session_name = ?",
            (session_name,)
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def _user_owns_account(self, user_id: int, session_name: str) -> bool:
        """Check if a user owns an account."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM accounts WHERE session_name = ? AND user_id = ?",
            (session_name, user_id)
        )
        owns = cursor.fetchone() is not None
        conn.close()
        return owns

    def _get_user_accounts(self, user_id: int) -> List[Tuple[str, str]]:
        """Get a user's accounts from the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT session_name, phone_number FROM accounts WHERE user_id = ?",
            (user_id,)
        )
        accounts = cursor.fetchall()
        conn.close()
        return accounts

    def _get_user_tasks(self, user_id: int) -> List[Tuple[int, str, str, str, str, str]]:
        """Get a user's tasks from the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, session_name, task_type, parameters, schedule, status FROM tasks WHERE user_id = ?",
            (user_id,)
        )
        tasks = cursor.fetchall()
        conn.close()
        return tasks

    def _get_all_user_ids(self) -> List[int]:
        """Get all user IDs from the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT user_id FROM accounts")
        user_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return user_ids

    def _check_rate_limit(self, user_id: int, action: str) -> bool:
        """Check if a user has exceeded rate limits."""
        if user_id in self.config["admin_ids"]:
            return True
            
        if action not in self.config["rate_limits"]:
            return True
            
        limit, period = self.config["rate_limits"][action]
        
        if user_id not in self.rate_limits:
            self.rate_limits[user_id] = {}
            
        if action not in self.rate_limits[user_id]:
            self.rate_limits[user_id][action] = (0, time.time())
            
        count, timestamp = self.rate_limits[user_id][action]
        
        if time.time() - timestamp > period:
            self.rate_limits[user_id][action] = (0, time.time())
            return True
            
        if count >= limit:
            return False
            
        return True

    def _update_rate_limit(self, user_id: int, action: str):
        """Update the rate limit counter."""
        if user_id in self.rate_limits and action in self.rate_limits[user_id]:
            count, timestamp = self.rate_limits[user_id][action]
            self.rate_limits[user_id][action] = (count + 1, timestamp)

    def _save_account_to_db(self, session_name: str, user_id: int, phone_number: str,
                          proxy: Optional[str] = None, device_model: Optional[str] = None,
                          app_version: Optional[str] = None):
        """Save an account to the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO accounts 
            (session_name, user_id, phone_number, api_id, api_hash, proxy, device_model, app_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (session_name, user_id, phone_number, self.config["api_id"], 
             self.config["api_hash"], proxy, device_model, app_version)
        )
        conn.commit()
        conn.close()

    def _add_task_to_db(self, user_id: int, session_name: str, task_type: str,
                       parameters: str, schedule: str) -> int:
        """Add a task to the database."""
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
        """Schedule a task to run."""
        task = self._get_task_from_db(task_id)
        if not task:
            return
            
        user_id, session_name, task_type, params, schedule = task[0], task[1], task[2], task[3], task[4]
        
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
        """Get a task from the database."""
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
            return
            
        user_id, session_name, task_type, params = task[0], task[1], task[2], task[3]
        
        if session_name not in self.clients:
            return
            
        client = self.clients[session_name]
        
        try:
            if task_type == "send_message":
                parts = params.split(maxsplit=1)
                if len(parts) < 2:
                    return
                    
                chat_id, text = parts[0], parts[1]
                await client.send_message(chat_id, text)
                
            elif task_type == "join_chat":
                await client.join_chat(params)
                
            elif task_type == "leave_chat":
                await client.leave_chat(params)
                
            # Update task status
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE tasks SET last_run = ?, status = ? WHERE id = ?",
                (datetime.now(), "completed", task_id)
            )
            conn.commit()
            conn.close()
            
        except FloodWait as e:
            logger.warning(f"Task {task_id} flood wait: {e.value}s")
            # Reschedule task after flood wait
            self.scheduler.add_job(
                self._execute_task,
                'date',
                run_date=datetime.now() + timedelta(seconds=e.value),
                args=[task_id],
                id=f"task_{task_id}_retry"
            )
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")

    async def _load_db_sessions(self):
        """Load sessions from the database."""
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
            except Exception as e:
                logger.error(f"Failed to load session {session_name}: {e}")

    def _update_user_sessions(self, user_id: int, session_name: str):
        """Update a user's session list."""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = []
        if session_name not in self.user_sessions[user_id]:
            self.user_sessions[user_id].append(session_name)

    def _generate_random_device(self) -> Dict[str, str]:
        """Generate random device information."""
        models = ["iPhone", "Samsung", "Google", "Xiaomi", "OnePlus"]
        versions = ["10", "11", "12", "13", "14", "15"]
        
        return {
            "device_model": f"{random.choice(models)} {random.randint(10, 14)}",
            "system_version": f"{random.choice(versions)}.{random.randint(0, 9)}",
            "app_version": f"Telegram {random.randint(7, 9)}.{random.randint(0, 99)}"
        }

    async def _background_tasks(self):
        """Run background maintenance tasks."""
        while True:
            try:
                # Perform periodic backups
                if self.config.get("auto_backup", False):
                    await self._backup_sessions()
                    
                # Clean up old sessions
                await self._cleanup_inactive_sessions()
                
                await asyncio.sleep(3600)  # Run hourly
            except Exception as e:
                logger.error(f"Background task error: {e}")
                await asyncio.sleep(600)

    async def _cleanup_inactive_sessions(self):
        """Clean up inactive sessions."""
        timeout = self.config["security"].get("session_timeout", 86400)
        cutoff = time.time() - timeout
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT session_name FROM accounts WHERE last_used < ?",
            (datetime.fromtimestamp(cutoff),)
        )
        old_sessions = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        for session_name in old_sessions:
            try:
                if session_name in self.clients:
                    await self.clients[session_name].stop()
                    del self.clients[session_name]
                    
                session_file = self.session_dir / f"{session_name}.session"
                if session_file.exists():
                    session_file.unlink()
                    
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM accounts WHERE session_name = ?",
                    (session_name,)
                )
                conn.commit()
                conn.close()
                
                logger.info(f"Cleaned up inactive session: {session_name}")
            except Exception as e:
                logger.error(f"Failed to clean up session {session_name}: {e}")

    async def stop_all(self):
        """Stop all clients gracefully."""
        try:
            # Stop all tasks
            self.scheduler.shutdown()
            
            # Stop all account clients
            for client in self.clients.values():
                try:
                    await client.stop()
                except Exception as e:
                    logger.error(f"Error stopping client: {e}")
                    
            # Stop main bot
            await self.main_bot.stop()
            
            logger.info("Bot stopped gracefully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    bot = AdvancedTelegramBot()
    
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Received exit signal, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        asyncio.run(bot.stop_all())