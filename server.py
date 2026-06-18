"""
Evalon AutoTrader — Flask + SocketIO Server
Handles login, trading loop, and real-time UI updates.
"""

import os
import time
import threading
import logging
from flask import Flask, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from database import (
    init_db, verify_license, create_license, delete_license,
    revoke_license, list_licenses, upsert_user, get_user_settings,
    save_user_settings, save_trade, get_trade_history, get_user_stats
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="static")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "evalon2024secret")
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Init DB on startup
try:
    init_db()
    logger.info("✅ Database initialized")
except Exception as e:
    logger.error(f"❌ DB init failed: {e}")

# ── Active sessions: sid → {trader, settings, running, thread} ──────────────
sessions = {}

LICENSE_KEY = os.environ.get("LICENSE_KEY", "kenteboy")
ADMIN_PW    = os.environ.get("ADMIN_PW", "evalon@2024#admin")

# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/health")
def health():
    return {"status": "ok"}, 200

# ── SocketIO Events ──────────────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    logger.info(f"Client connected: {socketio.request.sid}")

@socketio.on("disconnect")
def on_disconnect():
    sid = socketio.request.sid
    _stop_bot(sid)
    if sid in sessions:
        try:
            sessions[sid]["trader"].disconnect()
        except Exception:
            pass
        del sessions[sid]
    logger.info(f"Client disconnected: {sid}")

# ── LOGIN ─────────────────────────────────────────────────────────────────────
@socketio.on("login")
def on_login(data):
    sid = socketio.request.sid
    email    = data.get("email", "").strip()
    password = data.get("password", "").strip()
    broker   = data.get("broker", "quotex")
    account  = data.get("account", "demo")   # demo | real
    lic_key  = data.get("license_key", "").strip()

    if lic_key != LICENSE_KEY:
        # Check DB license
        valid, lic_msg, lic_row = verify_license(lic_key)
        if not valid:
            emit("login_result", {"success": False, "message": lic_msg})
            return
    emit("login_result", {"success": None, "message": f"⏳ Connecting to broker..."})

    try:
        trader = _create_trader(broker, email, password, account)
    except ValueError as e:
        emit("login_result", {"success": False, "message": str(e)})
        return
    except Exception as e:
        emit("login_result", {"success": False, "message": f"❌ Error: {e}"})
        return

    try:
        result = trader.connect()
        # Quotex returns 3 values, others return 2
        if len(result) == 3:
            success, status, message = result
        else:
            success, message = result
            status = "connected" if success else "failed"

        # OTP required — ask user for code
        if status == "otp_required":
            # Keep trader in session so OTP submit can use it
            sessions[sid] = {
                "trader":  trader,
                "broker":  broker,
                "account": account,
                "email":   email,
                "running": False,
                "thread":  None,
                "settings": {},
                "stats":   {"wins":0,"losses":0,"pnl":0.0,"demo":0,"real":0,"cur_amt":10,"cur_round":0,"comp_step":0},
            }
            emit("otp_required", {"message": "📩 Check your email for a verification code"})
            return

        if not success:
            emit("login_result", {"success": False, "message": f"❌ {message}"})
            return

        # fetch real balances
        balances = trader.get_balance()

        # Save user to DB
        try:
            upsert_user(email, lic_key, broker)
            saved_settings = get_user_settings(email)
        except Exception as e:
            logger.error(f"DB user save error: {e}")
            saved_settings = {}

        sessions[sid] = {
            "trader":  trader,
            "broker":  broker,
            "account": account,
            "email":   email,
            "running": False,
            "thread":  None,
            "settings": {
                "amount":    saved_settings.get("amount", 10.0),
                "martingale": saved_settings.get("martingale", "off"),
                "mtg_mult":   saved_settings.get("mtg_mult", 2.0),
                "max_rounds": saved_settings.get("max_rounds", 1),
                "compound":   saved_settings.get("compound", "off"),
                "compound_steps": saved_settings.get("compound_steps", 5),
                "compound_base":  saved_settings.get("compound_base", 10.0),
                "stop_loss":  saved_settings.get("stop_loss", 0),
                "take_profit": saved_settings.get("take_profit", 0),
                "market":     saved_settings.get("market", "otc"),
                "pairs":      saved_settings.get("pairs", "random"),
                "single_pair": saved_settings.get("single_pair", None),
                "score":      saved_settings.get("score", 3),
                "min_payout": saved_settings.get("min_payout", 77),
            },
            "stats": {
                "wins": 0, "losses": 0, "pnl": 0.0,
                "demo": balances["demo"], "real": balances["real"],
                "cur_amt": 10.0, "cur_round": 0, "comp_step": 0,
            }
        }

        emit("login_result", {
            "success":  True,
            "message":  "✅ Connected!",
            "broker":   broker,
            "account":  account,
            "balances": balances,
        })

    except Exception as e:
        logger.error(f"Login error: {e}")
        emit("login_result", {"success": False, "message": f"❌ Error: {e}"})

# ── SWITCH ACCOUNT (demo ↔ real) ──────────────────────────────────────────────
@socketio.on("switch_account")
def on_switch_account(data):
    sid = socketio.request.sid
    sess = sessions.get(sid)
    if not sess:
        return
    account = data.get("account", "demo")
    sess["account"] = account
    sess["trader"].switch_account(account)
    balances = sess["trader"].get_balance()
    emit("balance_update", balances)

# ── SETTINGS ──────────────────────────────────────────────────────────────────
@socketio.on("update_settings")
def on_settings(data):
    sid = socketio.request.sid
    sess = sessions.get(sid)
    if not sess:
        return
    s = sess["settings"]
    s.update({k: v for k, v in data.items() if k in s})
    # Save to DB
    try:
        save_user_settings(sess["email"], s)
    except Exception as e:
        logger.error(f"Settings save error: {e}")
    emit("settings_ack", {"ok": True})

# ── START BOT ─────────────────────────────────────────────────────────────────
@socketio.on("start_bot")
def on_start(data):
    sid = socketio.request.sid
    sess = sessions.get(sid)
    if not sess:
        emit("bot_error", {"message": "Not logged in"})
        return
    if sess.get("running"):
        emit("bot_error", {"message": "Bot already running"})
        return

    # apply any last-minute settings
    if data:
        sess["settings"].update({k: v for k, v in data.items() if k in sess["settings"]})

    # reset stats
    sess["stats"].update({"wins": 0, "losses": 0, "pnl": 0.0,
                           "cur_round": 0, "comp_step": 0,
                           "cur_amt": sess["settings"]["amount"]})
    sess["running"] = True

    t = threading.Thread(target=_bot_loop, args=(sid,), daemon=True)
    sess["thread"] = t
    t.start()
    emit("bot_started", {})

# ── STOP BOT ──────────────────────────────────────────────────────────────────
@socketio.on("stop_bot")
def on_stop():
    sid = socketio.request.sid
    _stop_bot(sid)
    emit("bot_stopped", {})

def _stop_bot(sid):
    sess = sessions.get(sid)
    if sess:
        sess["running"] = False

# ── LOGOUT ────────────────────────────────────────────────────────────────────
@socketio.on("logout")
def on_logout():
    sid = socketio.request.sid
    _stop_bot(sid)
    if sid in sessions:
        try:
            sessions[sid]["trader"].disconnect()
        except Exception:
            pass
        del sessions[sid]
    emit("logged_out", {})

# ── OTP HANDLER ───────────────────────────────────────────────────────────────
@socketio.on("submit_otp")
def on_submit_otp(data):
    """User submits OTP code sent by broker (Quotex email/SMS code)."""
    sid = socketio.request.sid
    otp = data.get("code", "").strip()
    sess = sessions.get(sid)
    if not sess:
        emit("otp_result", {"success": False, "message": "❌ Session expired"})
        return
    try:
        trader = sess["trader"]
        broker = sess.get("broker", "quotex")
        if broker == "quotex":
            import asyncio
            future = asyncio.run_coroutine_threadsafe(
                trader.client.send_ssid(otp), trader.loop
            )
            future.result(timeout=15)
        emit("otp_result", {"success": True})
        # Fetch balance and complete login
        balances = trader.get_balance()
        sess["stats"]["demo"] = balances["demo"]
        sess["stats"]["real"] = balances["real"]
        emit("login_result", {
            "success":  True,
            "message":  "✅ Connected!",
            "broker":   sess["broker"],
            "account":  sess["account"],
            "balances": balances,
        })
    except Exception as e:
        emit("otp_result", {"success": False, "message": f"❌ Wrong code: {e}"})
@socketio.on("refresh_balance")
def on_refresh():
    sid = socketio.request.sid
    sess = sessions.get(sid)
    if not sess:
        return
    try:
        bal = sess["trader"].get_balance()
        sess["stats"]["demo"] = bal["demo"]
        sess["stats"]["real"] = bal["real"]
        emit("balance_update", bal)
    except Exception:
        pass

# ── ADMIN ─────────────────────────────────────────────────────────────────────
@socketio.on("admin_auth")
def on_admin_auth(data):
    pw = data.get("password", "")
    if pw == ADMIN_PW:
        emit("admin_result", {"success": True})
    else:
        emit("admin_result", {"success": False, "message": "Wrong password"})

@socketio.on("admin_set_balance")
def on_admin_balance(data):
    sid = socketio.request.sid
    sess = sessions.get(sid)
    if not sess:
        return
    if "demo" in data:
        sess["stats"]["demo"] = float(data["demo"])
    if "real" in data:
        sess["stats"]["real"] = float(data["real"])
    emit("balance_update", {"demo": sess["stats"]["demo"], "real": sess["stats"]["real"]})

# ── LICENSE MANAGEMENT ────────────────────────────────────────────────────────

@socketio.on("admin_create_license")
def on_create_license(data):
    try:
        key  = data.get("key", "").strip()
        days = int(data.get("days", 30))
        note = data.get("note", "")
        if not key:
            emit("license_result", {"success": False, "message": "❌ Key cannot be empty"})
            return
        if days < 1:
            emit("license_result", {"success": False, "message": "❌ Days must be at least 1"})
            return
        row = create_license(key, days, note)
        emit("license_result", {"success": True, "message": f"✅ License created ({days} days)", "license": row})
        emit("license_list", {"licenses": list_licenses()})
    except Exception as e:
        emit("license_result", {"success": False, "message": f"❌ {e}"})

@socketio.on("admin_delete_license")
def on_delete_license(data):
    try:
        key = data.get("key", "").strip()
        ok  = delete_license(key)
        if ok:
            emit("license_result", {"success": True, "message": f"✅ License deleted"})
        else:
            emit("license_result", {"success": False, "message": "❌ License not found"})
        emit("license_list", {"licenses": list_licenses()})
    except Exception as e:
        emit("license_result", {"success": False, "message": f"❌ {e}"})

@socketio.on("admin_revoke_license")
def on_revoke_license(data):
    try:
        key = data.get("key", "").strip()
        ok  = revoke_license(key)
        msg = "✅ License revoked" if ok else "❌ License not found"
        emit("license_result", {"success": ok, "message": msg})
        emit("license_list", {"licenses": list_licenses()})
    except Exception as e:
        emit("license_result", {"success": False, "message": f"❌ {e}"})

@socketio.on("admin_list_licenses")
def on_list_licenses():
    try:
        licenses = list_licenses()
        emit("license_list", {"licenses": licenses})
    except Exception as e:
        emit("license_list", {"licenses": [], "error": str(e)})

@socketio.on("admin_get_stats")
def on_admin_stats(data):
    try:
        email = data.get("email", "")
        stats = get_user_stats(email) if email else {}
        history = get_trade_history(email, limit=50) if email else []
        emit("admin_stats", {"stats": stats, "history": history})
    except Exception as e:
        emit("admin_stats", {"stats": {}, "history": [], "error": str(e)})

# ── BOT LOOP ─────────────────────────────────────────────────────────────────
def _bot_loop(sid):
    """
    Main trading loop.
    Pre-analyzes candle 1.5s before close, fires trade instantly at close.
    No candle is skipped.
    """
    logger.info(f"Bot loop started for {sid}")

    while True:
        sess = sessions.get(sid)
        if not sess or not sess.get("running"):
            break

        settings = sess["settings"]
        stats    = sess["stats"]
        trader   = sess["trader"]
        market   = settings.get("market", "otc")

        if market == "otc":
            period    = 15
            timeframe = 15
            duration  = 15
        else:
            period    = 60
            timeframe = 60
            duration  = 60

        # ── Step 1: Calculate exact sleep until 1.5s before candle close ────
        now       = time.time()
        remainder = now % period
        sleep_for = period - remainder - 1.5
        if sleep_for < 0.05:
            sleep_for += period
        time.sleep(sleep_for)

        # ── Step 2: Re-check session still active ────────────────────────────
        sess = sessions.get(sid)
        if not sess or not sess.get("running"):
            break

        # ── Step 3: Pick asset and pre-fetch signal (1.5s before close) ──────
        asset = _pick_asset(sess)
        if not asset:
            _emit_log(sid, "⚠️ No asset available — retrying...", "warn")
            time.sleep(1)
            continue

        _emit_log(sid, f"🔍 Analyzing {asset}...", "info")
        signal = trader.analyze_signal(asset, timeframe)

        # ── Step 4: Sleep remaining 1.5s until exact candle close ────────────
        now       = time.time()
        remainder = now % period
        wait_left = period - remainder
        if wait_left > 0.05:
            time.sleep(wait_left)

        # ── Step 5: Re-check session ─────────────────────────────────────────
        sess = sessions.get(sid)
        if not sess or not sess.get("running"):
            break

        if signal is None:
            _emit_log(sid, f"⏭ {asset}: Flat/doji — skipped", "skip")
            continue

        # ── Step 6: Fire trade INSTANTLY at candle close ─────────────────────
        amount = _calc_amount(sess)
        success, trade_id, msg = trader.place_trade(asset, signal, amount, duration)

        if not success:
            _emit_log(sid, f"❌ Trade failed: {msg}", "error")
            time.sleep(2)
            continue

        # Notify UI
        socketio.emit("trade_opened", {
            "id":     trade_id,
            "pair":   asset,
            "dir":    signal,
            "amount": amount,
            "payout": settings.get("min_payout", 80),
            "dur":    duration,
        }, room=sid)

        _deduct_balance(sess, amount)
        socketio.emit("balance_update", {
            "demo": stats["demo"], "real": stats["real"]
        }, room=sid)

        # ── Step 7: Wait for result (non-blocking — next candle runs in parallel) ─
        def _wait_result(sid, trade_id, amount, asset, signal, duration):
            time.sleep(duration + 2)
            sess = sessions.get(sid)
            if not sess:
                return
            won, profit = sess["trader"].check_result(trade_id)
            _settle_trade(sess, won, amount, profit)
            # Save trade to DB
            try:
                save_trade(
                    email=sess["email"],
                    broker=sess["broker"],
                    account=sess["account"],
                    asset=asset,
                    direction=signal,
                    amount=amount,
                    profit=profit if won else 0,
                    won=won,
                    trade_id=trade_id,
                )
            except Exception as e:
                logger.error(f"Trade save error: {e}")
            socketio.emit("trade_result", {
                "id":     trade_id,
                "won":    won,
                "profit": profit,
                "amount": amount,
                "pair":   asset,
                "dir":    signal,
            }, room=sid)
            s = sess["stats"]
            socketio.emit("stats_update", {
                "wins":   s["wins"],
                "losses": s["losses"],
                "pnl":    round(s["pnl"], 2),
                "demo":   s["demo"],
                "real":   s["real"],
            }, room=sid)
            # Stop loss / Take profit check
            settings = sess["settings"]
            sl = float(settings.get("stop_loss", 0) or 0)
            tp = float(settings.get("take_profit", 0) or 0)
            if sl > 0 and s["pnl"] <= -sl:
                socketio.emit("bot_stopped", {"reason": "stop_loss"}, room=sid)
                sess["running"] = False
            if tp > 0 and s["pnl"] >= tp:
                socketio.emit("bot_stopped", {"reason": "take_profit"}, room=sid)
                sess["running"] = False

        import threading as _th
        _th.Thread(target=_wait_result,
                   args=(sid, trade_id, amount, asset, signal, duration),
                   daemon=True).start()

        # Loop immediately to next candle — no blocking wait

    logger.info(f"Bot loop ended for {sid}")


# ── Helpers ───────────────────────────────────────────────────────────────────

# Brokers with real trading support
LIVE_BROKERS = {"quotex", "pocketoption", "iqoption", "deriv"}

# Coming Soon brokers — UI only, no trading
COMING_SOON = {"binomo", "expert", "stockity", "olymptrade", "binolla"}

def _create_trader(broker, email, password, account):
    if broker in COMING_SOON:
        raise ValueError(f"🚧 {broker.title()} coming soon! Use Quotex, Pocket Option, IQ Option or Deriv.")

    if broker == "deriv":
        from deriv_trader import DerivTrader
        return DerivTrader(api_token=password, account_type=account)

    if broker == "pocketoption":
        from pocket_option_trader import PocketOptionTrader
        return PocketOptionTrader(email=email, password=password, account_type=account)

    if broker == "iqoption":
        from iqoption_trader import IQOptionTrader
        return IQOptionTrader(email=email, password=password, account_type=account)

    from quotex_trader import QuotexTrader
    return QuotexTrader(email=email, password=password, account_type=account)

def _wait_candle_close(period_seconds, pre_wake=1.5):
    """
    Sleep until just before the next candle close boundary.
    pre_wake: wake up this many seconds BEFORE close to pre-fetch data.
    This ensures trade fires the instant candle closes.
    """
    now = time.time()
    remainder = now % period_seconds
    sleep_time = period_seconds - remainder - pre_wake
    if sleep_time < 0.1:
        sleep_time += period_seconds
    time.sleep(sleep_time)
    # Now we are pre_wake seconds before candle close — analyze now
    # Then sleep the remaining pre_wake seconds
    time.sleep(pre_wake)

def _pick_asset(sess):
    """Pick trading asset based on broker, market, and user settings."""
    settings = sess["settings"]
    single   = settings.get("single_pair")
    if single:
        return single

    broker = sess.get("broker", "quotex")
    market = settings.get("market", "otc")

    QUOTEX_OTC = [
        "EURUSD_otc","GBPUSD_otc","AUDUSD_otc","USDJPY_otc",
        "USDCAD_otc","USDCHF_otc","EURJPY_otc","GBPJPY_otc",
        "EURGBP_otc","AUDJPY_otc",
    ]
    QUOTEX_REAL = [
        "EURUSD","GBPUSD","AUDUSD","USDJPY",
        "USDCAD","USDCHF","EURJPY","GBPJPY",
    ]
    PO_OTC = [
        "#EURUSD_otc","#GBPUSD_otc","#AUDUSD_otc","#USDJPY_otc",
        "#USDCAD_otc","#USDCHF_otc","#EURJPY_otc","#GBPJPY_otc",
    ]
    PO_REAL = [
        "EURUSD","GBPUSD","AUDUSD","USDJPY",
        "USDCAD","USDCHF","EURJPY","GBPJPY",
    ]
    IQ_OTC = [
        "EURUSD-OTC","GBPUSD-OTC","AUDUSD-OTC","USDJPY-OTC",
        "USDCAD-OTC","USDCHF-OTC","EURJPY-OTC","GBPJPY-OTC",
    ]
    IQ_REAL = [
        "EURUSD","GBPUSD","AUDUSD","USDJPY",
        "USDCAD","USDCHF","EURJPY","GBPJPY",
    ]
    DERIV_REAL = [
        "frxEURUSD","frxGBPUSD","frxAUDUSD","frxUSDJPY",
        "frxUSDCAD","frxUSDCHF","frxEURJPY",
    ]

    if broker == "deriv":
        pool = DERIV_REAL
    elif broker == "pocketoption":
        pool = PO_OTC if market == "otc" else PO_REAL
    elif broker == "iqoption":
        pool = IQ_OTC if market == "otc" else IQ_REAL
    else:
        # quotex default
        pool = QUOTEX_OTC if market == "otc" else QUOTEX_REAL

    import random
    return random.choice(pool) if pool else None

def _calc_amount(sess):
    settings = sess["settings"]
    stats    = sess["stats"]
    base     = float(settings.get("amount", 10))
    compound = settings.get("compound", "off")

    if compound == "on":
        step  = stats.get("comp_step", 0)
        steps = int(settings.get("compound_steps", 5))
        base  = float(settings.get("compound_base", 10))
        payout = float(settings.get("min_payout", 80)) / 100
        amt = base
        for _ in range(min(step, steps - 1)):
            amt += amt * payout
        return round(amt, 2)

    return round(stats.get("cur_amt", base), 2)

def _deduct_balance(sess, amount):
    stats   = sess["stats"]
    account = sess["account"]
    if account == "real":
        stats["real"] = max(0, stats["real"] - amount)
    else:
        stats["demo"] = max(0, stats["demo"] - amount)

def _settle_trade(sess, won, amount, profit):
    stats    = sess["stats"]
    settings = sess["settings"]
    account  = sess["account"]
    compound = settings.get("compound", "off")
    mtg      = settings.get("martingale", "off")

    if won:
        stats["wins"] += 1
        stats["pnl"]  += profit
        if account == "real":
            stats["real"] += amount + profit
        else:
            stats["demo"] += amount + profit

        if compound == "on":
            step  = stats.get("comp_step", 0) + 1
            steps = int(settings.get("compound_steps", 5))
            stats["comp_step"] = 0 if step >= steps else step

        # reset martingale on win
        stats["cur_round"] = 0
        stats["cur_amt"]   = float(settings.get("amount", 10))
    else:
        stats["losses"] += 1
        stats["pnl"]    -= amount

        if compound == "on":
            stats["comp_step"] = 0
        elif mtg != "off":
            mult       = float(mtg)
            max_rounds = int(settings.get("max_rounds", 1))
            if stats["cur_round"] < max_rounds:
                stats["cur_amt"]   = round(stats["cur_amt"] * mult, 2)
                stats["cur_round"] += 1
            else:
                stats["cur_round"] = 0
                stats["cur_amt"]   = float(settings.get("amount", 10))

def _emit_log(sid, message, level="info"):
    socketio.emit("bot_log", {"message": message, "level": level}, room=sid)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
