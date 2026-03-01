"""
db.py
Asynchronous SQLite wrapper using aiosqlite for storing users, payments, signals.
Provides simple helper functions and schema creation.
"""
import aiosqlite
import asyncio
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any
import config

CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    subscription_type TEXT DEFAULT 'free',
    expiry_date TEXT,
    signals_used_today INTEGER DEFAULT 0,
    last_signal_date TEXT,
    created_at TEXT NOT NULL
);
"""

CREATE_PAYMENTS = """
CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    txid TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT NOT NULL
);
"""

CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    asset TEXT,
    direction TEXT,
    timeframe TEXT,
    result TEXT,
    confidence REAL,
    created_at TEXT NOT NULL
);
"""

class Database:
    def __init__(self, db_path: str = config.DB_PATH):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    async def initialize(self):
        async with aiosqlite.connect(self.db_path, timeout=config.SQLITE_TIMEOUT) as db:
            await db.execute(CREATE_USERS)
            await db.execute(CREATE_PAYMENTS)
            await db.execute(CREATE_SIGNALS)
            await db.commit()

    async def _execute(self, query: str, params: tuple = ()):  # helper
        async with self._lock:
            async with aiosqlite.connect(self.db_path, timeout=config.SQLITE_TIMEOUT) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(query, params)
                await db.commit()
                return cur

    # ---- Users ----
    async def get_or_create_user(self, telegram_id: int, username: Optional[str]) -> Dict[str, Any]:
        row = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        if row:
            return dict(row)
        now = datetime.utcnow().isoformat()
        await self._execute(
            "INSERT INTO users (telegram_id, username, created_at) VALUES (?, ?, ?)",
            (telegram_id, username, now)
        )
        row = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        return dict(row) if row else {}

    async def _fetchone(self, query: str, params: tuple = ()):
        async with self._lock:
            async with aiosqlite.connect(self.db_path, timeout=config.SQLITE_TIMEOUT) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(query, params)
                row = await cur.fetchone()
                return row

    async def _fetchall(self, query: str, params: tuple = ()):
        async with self._lock:
            async with aiosqlite.connect(self.db_path, timeout=config.SQLITE_TIMEOUT) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(query, params)
                rows = await cur.fetchall()
                return rows

    async def increment_signal_count(self, telegram_id: int) -> None:
        # Increment signals_used_today, set/reset date as needed
        user = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        if not user:
            return
        today = date.today().isoformat()
        last_date = user['last_signal_date']
        if last_date != today:
            # reset
            await self._execute("UPDATE users SET signals_used_today = 1, last_signal_date = ? WHERE telegram_id = ?", (today, telegram_id))
        else:
            await self._execute("UPDATE users SET signals_used_today = signals_used_today + 1 WHERE telegram_id = ?", (telegram_id,))

    async def get_signal_count(self, telegram_id: int) -> int:
        user = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        if not user:
            return 0
        today = date.today().isoformat()
        if user['last_signal_date'] != today:
            return 0
        return int(user['signals_used_today'] or 0)

    async def add_signal_record(self, telegram_id: int, asset: str, direction: str, timeframe: str, confidence: float, result: Optional[str] = None):
        now = datetime.utcnow().isoformat()
        user = await self._fetchone("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_id = user['id'] if user else None
        await self._execute(
            "INSERT INTO signals (user_id, asset, direction, timeframe, result, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, asset, direction, timeframe, result, confidence, now)
        )

    # ---- Payments ----
    async def create_payment(self, telegram_id: int, txid: str) -> Dict[str, Any]:
        user = await self._fetchone("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_id = user['id'] if user else None
        now = datetime.utcnow().isoformat()
        await self._execute("INSERT INTO payments (user_id, txid, status, created_at) VALUES (?, ?, ?, ?)", (user_id, txid, 'pending', now))
        row = await self._fetchone("SELECT * FROM payments WHERE txid = ?", (txid,))
        return dict(row) if row else {}

    async def list_pending_payments(self) -> List[Dict[str, Any]]:
        rows = await self._fetchall("SELECT payments.*, users.telegram_id, users.username FROM payments LEFT JOIN users ON payments.user_id = users.id WHERE status = 'pending'")
        return [dict(r) for r in rows]

    async def set_payment_status(self, txid: str, status: str) -> None:
        await self._execute("UPDATE payments SET status = ? WHERE txid = ?", (status, txid))

    # ---- Subscription management ----
    async def add_premium(self, telegram_id: int, days: int):
        user = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        if not user:
            return
        now = datetime.utcnow()
        expiry = None
        if user['expiry_date']:
            try:
                expiry = datetime.fromisoformat(user['expiry_date'])
            except Exception:
                expiry = now
        if expiry and expiry > now:
            new_expiry = expiry + timedelta(days=days)
        else:
            new_expiry = now + timedelta(days=days)
        await self._execute("UPDATE users SET subscription_type = 'premium', expiry_date = ? WHERE telegram_id = ?", (new_expiry.isoformat(), telegram_id))

    async def remove_premium(self, telegram_id: int):
        await self._execute("UPDATE users SET subscription_type = 'free', expiry_date = NULL WHERE telegram_id = ?", (telegram_id,))

    async def get_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        row = await self._fetchone("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        return dict(row) if row else None

    async def get_stats(self) -> Dict[str, Any]:
        total_users = await self._fetchone("SELECT COUNT(*) as c FROM users")
        total_payments = await self._fetchone("SELECT COUNT(*) as c FROM payments")
        total_signals = await self._fetchone("SELECT COUNT(*) as c FROM signals")
        return {
            'total_users': total_users['c'] if total_users else 0,
            'total_payments': total_payments['c'] if total_payments else 0,
            'total_signals': total_signals['c'] if total_signals else 0,
        }
