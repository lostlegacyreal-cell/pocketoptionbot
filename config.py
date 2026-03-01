"""
config.py
Configuration and constants for Pocket Option Signal Bot
"""
import os
from datetime import timedelta

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')  # required

# Admin Telegram user IDs (integers)
ADMINS = [123456789]  # replace with real admin IDs

# SQLite DB path
DB_PATH = os.environ.get('POCKET_BOT_DB', 'pocket_signal_bot.db')

# USDT wallet address for manual crypto payments
USDT_WALLET = os.environ.get('USDT_WALLET', 'YOUR_USDT_WALLET_ADDRESS')

# Free user daily signal limit
FREE_DAILY_LIMIT = 3

# Premium subscription defaults
PREMIUM_DAYS_DEFAULT = 30

# Auto-signal interval (seconds) for premium auto mode
AUTO_SIGNAL_INTERVAL = 300  # 5 minutes

# Supported assets sample (user can request other assets too)
SUPPORTED_ASSETS = [
    'EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD', 'BTCUSD'
]

# Timeframes in minutes
TIMEFRAMES = {
    '1M': 1,
    '5M': 5,
    '15M': 15,
}

# Signal confidence threshold (simple calibrated)
MIN_CONFIDENCE = 0.5  # 50% baseline

# Bot messages config
BOT_NAME = os.environ.get('BOT_NAME', 'PocketOptionSignalBot')

# Logging
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Rate limit reset timezone behavior: naive daily reset using local date
DAILY_RESET_HOUR = 0

# Database connection options (future extension)
SQLITE_TIMEOUT = 30
