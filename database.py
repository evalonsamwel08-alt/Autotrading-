"""
Database module — Evalon AutoTrader
PostgreSQL via psycopg2 (Python 3.11)
"""
import os, json, logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor, 
                             sslmode="require")

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS licenses (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(100) UNIQUE NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    expires_at TIMESTAMP NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    note VARCHAR(200)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(200) UNIQUE NOT NULL,
                    license_key VARCHAR(100),
                    broker VARCHAR(50),
                    last_login TIMESTAMP DEFAULT NOW(),
                    settings TEXT DEFAULT '{}'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(200),
                    broker VARCHAR(50),
                    account VARCHAR(10),
                    asset VARCHAR(50),
                    direction VARCHAR(5),
                    amount NUMERIC(12,2),
                    profit NUMERIC(12,2),
                    won BOOLEAN,
                    trade_id VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
    logger.info("✅ Database initialized")

def create_license(key, days, note=""):
    now = datetime.utcnow()
    expires = now + timedelta(days=days)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO licenses (key, type, expires_at, note)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (key) DO UPDATE
                    SET type=EXCLUDED.type, expires_at=EXCLUDED.expires_at,
                        is_active=TRUE, note=EXCLUDED.note
                RETURNING *
            """, (key, f"{days}d", expires, note))
            row = dict(cur.fetchone())
            conn.commit()
            return row

def verify_license(key):
    if not key:
        return False, "No license key", None
    master = os.environ.get("ADMIN_MASTER_KEY", "")
    if master and key == master:
        return True, "Admin key", None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM licenses WHERE key=%s AND is_active=TRUE", (key,))
            row = cur.fetchone()
    if not row:
        return False, "❌ Invalid license key", None
    row = dict(row)
    now = datetime.utcnow()
    exp = row["expires_at"]
    if hasattr(exp, "tzinfo") and exp.tzinfo:
        exp = exp.replace(tzinfo=None)
    if exp < now:
        return False, "❌ License expired", None
    days_left = (exp - now).days
    hours_left = int((exp - now).total_seconds() / 3600)
    msg = f"✅ Valid — {hours_left}h left" if days_left == 0 else f"✅ Valid — {days_left} days left"
    return True, msg, row

def revoke_license(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE licenses SET is_active=FALSE WHERE key=%s", (key,))
            conn.commit()
            return cur.rowcount > 0

def delete_license(key):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM licenses WHERE key=%s", (key,))
            conn.commit()
            return cur.rowcount > 0

def list_licenses(active_only=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if active_only:
                cur.execute("SELECT * FROM licenses WHERE is_active=TRUE ORDER BY expires_at DESC")
            else:
                cur.execute("SELECT * FROM licenses ORDER BY created_at DESC")
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
        r["expires_at"] = exp.strftime("%Y-%m-%d %H:%M")
        r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M") if r.get("created_at") else ""
        result.append(r)
    return result

def upsert_user(email, license_key, broker, settings=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (email, license_key, broker, last_login, settings)
                VALUES (%s,%s,%s,NOW(),%s)
                ON CONFLICT (email) DO UPDATE
                    SET license_key=%s, broker=%s, last_login=NOW(),
                        settings=COALESCE(%s, users.settings)
            """, (email, license_key, broker, json.dumps(settings or {}),
                  license_key, broker, json.dumps(settings or {})))
            conn.commit()

def get_user_settings(email):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT settings FROM users WHERE email=%s", (email,))
            row = cur.fetchone()
    return json.loads(row["settings"]) if row and row["settings"] else {}

def save_user_settings(email, settings):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET settings=%s WHERE email=%s",
                       (json.dumps(settings), email))
            conn.commit()

def save_trade(email, broker, account, asset, direction, amount, profit, won, trade_id=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades
                    (email,broker,account,asset,direction,amount,profit,won,trade_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (email, broker, account, asset, direction,
                  float(amount), float(profit), won, str(trade_id or "")))
            conn.commit()

def get_trade_history(email, limit=50):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM trades WHERE email=%s
                ORDER BY created_at DESC LIMIT %s
            """, (email, limit))
            rows = cur.fetchall()
    result = []
    for r in rows:
        r = dict(r)
        r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r.get("created_at") else ""
        result.append(r)
    return result

def get_user_stats(email):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN won THEN 1 ELSE 0 END) AS wins,
                       SUM(CASE WHEN NOT won THEN 1 ELSE 0 END) AS losses,
                       SUM(CASE WHEN won THEN profit ELSE -amount END) AS pnl
                FROM trades WHERE email=%s
            """, (email,))
            row = cur.fetchone()
    if row:
        total = int(row["total"] or 0)
        wins  = int(row["wins"] or 0)
        return {"total": total, "wins": wins,
                "losses": int(row["losses"] or 0),
                "pnl": float(row["pnl"] or 0),
                "winrate": round(wins/total*100,1) if total else 0}
    return {"total":0,"wins":0,"losses":0,"pnl":0,"winrate":0}
