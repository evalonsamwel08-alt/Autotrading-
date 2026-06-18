"""
Database module — Evalon AutoTrader
Uses asyncpg with synchronous wrapper for Python 3.14 compatibility.
"""
import os, json, asyncio, logging
from datetime import datetime, timedelta
import asyncpg

logger = logging.getLogger(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def _run(coro):
    """Run async coroutine synchronously."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=30)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)

async def _get_conn():
    return await asyncpg.connect(DATABASE_URL)

async def _init_db_async():
    conn = await _get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS licenses (
                id         SERIAL PRIMARY KEY,
                key        VARCHAR(100) UNIQUE NOT NULL,
                type       VARCHAR(20)  NOT NULL,
                created_at TIMESTAMP    DEFAULT NOW(),
                expires_at TIMESTAMP    NOT NULL,
                is_active  BOOLEAN      DEFAULT TRUE,
                note       VARCHAR(200)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id          SERIAL PRIMARY KEY,
                email       VARCHAR(200) UNIQUE NOT NULL,
                license_key VARCHAR(100),
                broker      VARCHAR(50),
                last_login  TIMESTAMP DEFAULT NOW(),
                settings    TEXT DEFAULT '{}'
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id         SERIAL PRIMARY KEY,
                email      VARCHAR(200),
                broker     VARCHAR(50),
                account    VARCHAR(10),
                asset      VARCHAR(50),
                direction  VARCHAR(5),
                amount     NUMERIC(12,2),
                profit     NUMERIC(12,2),
                won        BOOLEAN,
                trade_id   VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    finally:
        await conn.close()

def init_db():
    _run(_init_db_async())
    logger.info("✅ Database initialized")

# ── LICENSE ───────────────────────────────────────────────────────────────────

def create_license(key: str, days: int, note: str = "") -> dict:
    async def _do():
        now     = datetime.utcnow()
        expires = now + timedelta(days=days)
        conn = await _get_conn()
        try:
            row = await conn.fetchrow("""
                INSERT INTO licenses (key, type, expires_at, note)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (key) DO UPDATE
                    SET type=EXCLUDED.type, expires_at=EXCLUDED.expires_at,
                        is_active=TRUE, note=EXCLUDED.note
                RETURNING *
            """, key, f"{days}d", expires, note)
            return dict(row)
        finally:
            await conn.close()
    return _run(_do())

def verify_license(key: str):
    async def _do():
        if not key:
            return False, "No license key", None
        master = os.environ.get("ADMIN_MASTER_KEY", "")
        if master and key == master:
            return True, "Admin key", None
        conn = await _get_conn()
        try:
            row = await conn.fetchrow(
                "SELECT * FROM licenses WHERE key=$1 AND is_active=TRUE", key)
            if not row:
                return False, "❌ Invalid license key", None
            row = dict(row)
            now = datetime.utcnow()
            exp = row["expires_at"]
            if hasattr(exp, "tzinfo") and exp.tzinfo:
                exp = exp.replace(tzinfo=None)
            if exp < now:
                return False, f"❌ License expired", None
            days_left = (exp - now).days
            hours_left = int((exp - now).total_seconds() / 3600)
            if days_left == 0:
                msg = f"✅ Valid — {hours_left}h remaining"
            else:
                msg = f"✅ Valid — {days_left} days remaining"
            return True, msg, row
        finally:
            await conn.close()
    return _run(_do())

def revoke_license(key: str):
    async def _do():
        conn = await _get_conn()
        try:
            r = await conn.execute(
                "UPDATE licenses SET is_active=FALSE WHERE key=$1", key)
            return "UPDATE 1" in r
        finally:
            await conn.close()
    return _run(_do())

def delete_license(key: str):
    async def _do():
        conn = await _get_conn()
        try:
            r = await conn.execute("DELETE FROM licenses WHERE key=$1", key)
            return "DELETE 1" in r
        finally:
            await conn.close()
    return _run(_do())

def list_licenses(active_only=False):
    async def _do():
        conn = await _get_conn()
        try:
            if active_only:
                rows = await conn.fetch(
                    "SELECT * FROM licenses WHERE is_active=TRUE ORDER BY expires_at DESC")
            else:
                rows = await conn.fetch(
                    "SELECT * FROM licenses ORDER BY created_at DESC")
            now = datetime.utcnow()
            result = []
            for r in rows:
                r = dict(r)
                exp = r["expires_at"]
                if hasattr(exp, "tzinfo") and exp.tzinfo:
                    exp = exp.replace(tzinfo=None)
                r["expired"]    = exp < now
                r["days_left"]  = max(0, (exp - now).days)
                r["expires_at"] = exp.strftime("%Y-%m-%d %H:%M")
                r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M") if r.get("created_at") else ""
                result.append(r)
            return result
        finally:
            await conn.close()
    return _run(_do())

# ── USERS ─────────────────────────────────────────────────────────────────────

def upsert_user(email, license_key, broker, settings=None):
    async def _do():
        conn = await _get_conn()
        try:
            await conn.execute("""
                INSERT INTO users (email, license_key, broker, last_login, settings)
                VALUES ($1,$2,$3,NOW(),$4)
                ON CONFLICT (email) DO UPDATE
                    SET license_key=$2, broker=$3, last_login=NOW(),
                        settings=COALESCE($4, users.settings)
            """, email, license_key, broker, json.dumps(settings or {}))
        finally:
            await conn.close()
    _run(_do())

def get_user_settings(email):
    async def _do():
        conn = await _get_conn()
        try:
            row = await conn.fetchrow(
                "SELECT settings FROM users WHERE email=$1", email)
            return json.loads(row["settings"]) if row and row["settings"] else {}
        finally:
            await conn.close()
    return _run(_do())

def save_user_settings(email, settings):
    async def _do():
        conn = await _get_conn()
        try:
            await conn.execute(
                "UPDATE users SET settings=$1 WHERE email=$2",
                json.dumps(settings), email)
        finally:
            await conn.close()
    _run(_do())

# ── TRADES ────────────────────────────────────────────────────────────────────

def save_trade(email, broker, account, asset, direction, amount, profit, won, trade_id=None):
    async def _do():
        conn = await _get_conn()
        try:
            await conn.execute("""
                INSERT INTO trades
                    (email,broker,account,asset,direction,amount,profit,won,trade_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, email, broker, account, asset, direction,
                float(amount), float(profit), won, str(trade_id or ""))
        finally:
            await conn.close()
    _run(_do())

def get_trade_history(email, limit=50):
    async def _do():
        conn = await _get_conn()
        try:
            rows = await conn.fetch(
                "SELECT * FROM trades WHERE email=$1 ORDER BY created_at DESC LIMIT $2",
                email, limit)
            result = []
            for r in rows:
                r = dict(r)
                r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else ""
                result.append(r)
            return result
        finally:
            await conn.close()
    return _run(_do())

def get_user_stats(email):
    async def _do():
        conn = await _get_conn()
        try:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN NOT won THEN 1 ELSE 0 END) AS losses,
                       SUM(CASE WHEN won THEN profit ELSE -amount END) AS pnl
                FROM trades WHERE email=$1
            """, email)
            if row:
                total = int(row["total"] or 0)
                wins  = int(row["wins"] or 0)
                return {
                    "total": total, "wins": wins,
                    "losses": int(row["losses"] or 0),
                    "pnl": float(row["pnl"] or 0),
                    "winrate": round(wins/total*100, 1) if total else 0
                }
            return {"total":0,"wins":0,"losses":0,"pnl":0,"winrate":0}
        finally:
            await conn.close()
    return _run(_do())
