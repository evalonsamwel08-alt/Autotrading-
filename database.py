"""
Database module — Evalon AutoTrader
PostgreSQL via Neon (or any PostgreSQL URL).
Tables: licenses, users, trades
"""

import os
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    """Create all tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # ── Licenses table ────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS licenses (
                    id          SERIAL PRIMARY KEY,
                    key         VARCHAR(100) UNIQUE NOT NULL,
                    type        VARCHAR(10)  NOT NULL CHECK (type IN ('daily','monthly','yearly')),
                    created_at  TIMESTAMP    DEFAULT NOW(),
                    expires_at  TIMESTAMP    NOT NULL,
                    is_active   BOOLEAN      DEFAULT TRUE,
                    note        VARCHAR(200)
                );
            """)

            # ── Users table ───────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id          SERIAL PRIMARY KEY,
                    email       VARCHAR(200) UNIQUE NOT NULL,
                    license_key VARCHAR(100),
                    broker      VARCHAR(50),
                    last_login  TIMESTAMP    DEFAULT NOW(),
                    settings    JSONB        DEFAULT '{}'::jsonb
                );
            """)

            # ── Trades table ──────────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          SERIAL PRIMARY KEY,
                    email       VARCHAR(200),
                    broker      VARCHAR(50),
                    account     VARCHAR(10),
                    asset       VARCHAR(50),
                    direction   VARCHAR(5),
                    amount      NUMERIC(12,2),
                    profit      NUMERIC(12,2),
                    won         BOOLEAN,
                    trade_id    VARCHAR(100),
                    created_at  TIMESTAMP DEFAULT NOW()
                );
            """)

            conn.commit()
            logger.info("Database tables ready.")


# ── LICENSE FUNCTIONS ─────────────────────────────────────────────────────────

def create_license(key: str, days: int, note: str = "") -> dict:
    """
    Create a new license key valid for `days` number of days.
    days: any positive integer (e.g. 1, 6, 15, 30, 365)
    Returns the created license row.
    """
    if days < 1:
        raise ValueError("Days must be at least 1")

    now     = datetime.utcnow()
    expires = now + timedelta(days=days)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO licenses (key, type, expires_at, note)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (key) DO UPDATE
                    SET type       = EXCLUDED.type,
                        expires_at = EXCLUDED.expires_at,
                        is_active  = TRUE,
                        note       = EXCLUDED.note
                RETURNING *;
            """, (key, f"{days}d", expires, note))
            row = dict(cur.fetchone())
            conn.commit()
            return row


def verify_license(key: str) -> tuple:
    """
    Check if license key is valid and not expired.
    Returns (valid: bool, message: str, license_row: dict|None)
    """
    if not key:
        return False, "No license key provided", None

    # Always allow admin master key
    admin_master = os.environ.get("ADMIN_MASTER_KEY", "")
    if admin_master and key == admin_master:
        return True, "Admin key", None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM licenses
                WHERE key = %s AND is_active = TRUE
                LIMIT 1;
            """, (key,))
            row = cur.fetchone()

    if not row:
        return False, "❌ Invalid license key", None

    row = dict(row)
    now = datetime.utcnow()
    expires = row["expires_at"]

    # Make timezone-naive for comparison
    if hasattr(expires, "tzinfo") and expires.tzinfo:
        from datetime import timezone
        expires = expires.replace(tzinfo=None)

    if expires < now:
        days_ago = (now - expires).days
        return False, f"❌ License expired {days_ago} day(s) ago", None

    # Calculate days remaining
    days_left = (expires - now).days
    hours_left = int((expires - now).total_seconds() / 3600)

    if days_left == 0:
        msg = f"✅ License valid — {hours_left}h remaining"
    elif days_left == 1:
        msg = "✅ License valid — 1 day remaining"
    else:
        msg = f"✅ License valid — {days_left} days remaining"

    return True, msg, row


def revoke_license(key: str) -> bool:
    """Deactivate a license key."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE licenses SET is_active = FALSE
                WHERE key = %s;
            """, (key,))
            conn.commit()
            return cur.rowcount > 0


def delete_license(key: str) -> bool:
    """Permanently delete a license key."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM licenses WHERE key = %s;", (key,))
            conn.commit()
            return cur.rowcount > 0


def list_licenses(active_only: bool = False) -> list:
    """List all licenses."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if active_only:
                cur.execute("""
                    SELECT * FROM licenses
                    WHERE is_active = TRUE
                    ORDER BY expires_at DESC;
                """)
            else:
                cur.execute("SELECT * FROM licenses ORDER BY created_at DESC;")
            rows = cur.fetchall()
            now = datetime.utcnow()
            result = []
            for r in rows:
                r = dict(r)
                exp = r["expires_at"]
                if hasattr(exp, "tzinfo") and exp.tzinfo:
                    exp = exp.replace(tzinfo=None)
                r["expired"]   = exp < now
                r["days_left"] = max(0, (exp - now).days)
                r["expires_at"] = exp.strftime("%Y-%m-%d %H:%M") if exp else ""
                r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M") if r.get("created_at") else ""
                result.append(r)
            return result


# ── USER FUNCTIONS ────────────────────────────────────────────────────────────

def upsert_user(email: str, license_key: str, broker: str, settings: dict = None):
    """Save or update user login info and settings."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            import json
            cur.execute("""
                INSERT INTO users (email, license_key, broker, last_login, settings)
                VALUES (%s, %s, %s, NOW(), %s::jsonb)
                ON CONFLICT (email) DO UPDATE
                    SET license_key = EXCLUDED.license_key,
                        broker      = EXCLUDED.broker,
                        last_login  = NOW(),
                        settings    = COALESCE(EXCLUDED.settings, users.settings);
            """, (email, license_key, broker, json.dumps(settings or {})))
            conn.commit()


def get_user_settings(email: str) -> dict:
    """Retrieve saved settings for a user."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT settings FROM users WHERE email = %s;", (email,))
            row = cur.fetchone()
            return dict(row["settings"]) if row and row["settings"] else {}


def save_user_settings(email: str, settings: dict):
    """Update settings for a user."""
    import json
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET settings = %s::jsonb
                WHERE email = %s;
            """, (json.dumps(settings), email))
            conn.commit()


# ── TRADE FUNCTIONS ───────────────────────────────────────────────────────────

def save_trade(email: str, broker: str, account: str, asset: str,
               direction: str, amount: float, profit: float,
               won: bool, trade_id: str = None):
    """Save a completed trade to DB."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades
                    (email, broker, account, asset, direction, amount, profit, won, trade_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (email, broker, account, asset, direction,
                  amount, profit, won, str(trade_id or "")))
            conn.commit()


def get_trade_history(email: str, limit: int = 50) -> list:
    """Get recent trades for a user."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades
                WHERE email = %s
                ORDER BY created_at DESC
                LIMIT %s;
            """, (email, limit))
            rows = cur.fetchall()
            result = []
            for r in rows:
                r = dict(r)
                r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else ""
                result.append(r)
            return result


def get_user_stats(email: str) -> dict:
    """Get aggregated win/loss stats for a user."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                           AS total,
                    SUM(CASE WHEN won THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN NOT won THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN won THEN profit ELSE -amount END) AS pnl
                FROM trades
                WHERE email = %s;
            """, (email,))
            row = cur.fetchone()
            if row:
                r = dict(row)
                total = int(r["total"] or 0)
                wins  = int(r["wins"] or 0)
                return {
                    "total":   total,
                    "wins":    wins,
                    "losses":  int(r["losses"] or 0),
                    "pnl":     float(r["pnl"] or 0),
                    "winrate": round((wins / total * 100), 1) if total > 0 else 0,
                }
            return {"total": 0, "wins": 0, "losses": 0, "pnl": 0, "winrate": 0}
