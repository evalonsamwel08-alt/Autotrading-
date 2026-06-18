"""
Pocket Option Trader — Direct WebSocket Implementation
No external library needed.
"""
import json, time, threading, logging, requests
import websocket

logger = logging.getLogger(__name__)

PO_WS = "wss://api-l.po.market/trade"

class PocketOptionTrader:
    def __init__(self, email, password, account_type="demo"):
        self.email = email
        self.password = password
        self.account_type = account_type
        self.ws = None
        self.connected = False
        self.authorized = False
        self._thread = None
        self.balance_demo = 0.0
        self.balance_real = 0.0
        self.session_token = None
        self._pending_trades = {}

    def connect(self):
        try:
            # Login via HTTP
            session = requests.Session()
            session.headers.update({"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"})
            resp = session.post("https://po.market/api/v1/users/login", json={
                "email": self.email,
                "password": self.password
            }, timeout=20)
            
            data = resp.json() if resp.content else {}
            
            # Extract token
            token = (data.get("token") or 
                     data.get("data", {}).get("token") or
                     data.get("access_token") or
                     session.cookies.get("user_auth"))
            
            if not token:
                if "captcha" in str(data).lower():
                    return False, "failed", "❌ CAPTCHA required — try again later"
                return False, "failed", f"Login failed: {resp.status_code}"

            self.session_token = token
            self._connect_ws()
            return True, "connected", None

        except Exception as e:
            logger.error(f"PO connect: {e}")
            return False, "failed", str(e)

    def _connect_ws(self):
        headers = {
            "Authorization": f"Bearer {self.session_token}",
            "User-Agent": "Mozilla/5.0"
        }
        self.ws = websocket.WebSocketApp(
            PO_WS,
            header=headers,
            on_message=self._on_message,
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
        )
        self._thread = threading.Thread(target=self.ws.run_forever,
                                         kwargs={"ping_interval": 25},
                                         daemon=True)
        self._thread.start()
        for _ in range(20):
            if self.connected: break
            time.sleep(0.5)

    def _on_open(self, ws):
        self.connected = True
        self.authorized = True
        ws.send(json.dumps({"action": "auth", "message": {"token": self.session_token}}))

    def _on_close(self, ws, *a):
        self.connected = False

    def _on_error(self, ws, err):
        logger.error(f"PO WS: {err}")

    def _on_message(self, ws, msg):
        try:
            data = json.loads(msg)
            action = data.get("action", "")
            message = data.get("message", {})
            
            if action == "balance":
                if isinstance(message, list):
                    for b in message:
                        if b.get("is_demo"):
                            self.balance_demo = float(b.get("amount", 0))
                        else:
                            self.balance_real = float(b.get("amount", 0))
            
            if action in ("win", "lose", "close-option"):
                tid = str(message.get("id", ""))
                if tid in self._pending_trades:
                    self._pending_trades[tid]["result"] = message
                    self._pending_trades[tid]["event"].set()
        except Exception as e:
            logger.error(f"PO msg: {e}")

    def disconnect(self):
        try:
            if self.ws: self.ws.close()
        except: pass
        self.connected = False

    def get_balance(self):
        return {"demo": self.balance_demo, "real": self.balance_real}

    def switch_account(self, account_type):
        self.account_type = account_type
        if self.ws and self.connected:
            try:
                self.ws.send(json.dumps({
                    "action": "switch-balance",
                    "message": {"is_demo": account_type == "demo"}
                }))
            except: pass

    def get_candles(self, asset, timeframe=60, count=5):
        try:
            evt = threading.Event()
            key = f"c_{asset}_{time.time()}"
            self._pending_trades[key] = {"event": evt, "data": None}
            self.ws.send(json.dumps({
                "action": "history",
                "message": {
                    "symbol": asset,
                    "period": timeframe,
                    "time": int(time.time()),
                    "count": count + 2
                }
            }))
            evt.wait(timeout=10)
            raw = self._pending_trades.pop(key, {}).get("data", [])
            candles = []
            for c in (raw or []):
                candles.append({
                    "open":  float(c.get("open", 0)),
                    "close": float(c.get("close", 0)),
                    "high":  float(c.get("high", 0)),
                    "low":   float(c.get("low", 0)),
                })
            return candles[:-1]
        except Exception as e:
            logger.error(f"PO candles: {e}")
            return []

    def analyze_signal(self, asset, timeframe=60):
        candles = self.get_candles(asset, timeframe, count=3)
        if not candles: return None
        last = candles[-1]
        o, c, h, l = last["open"], last["close"], last["high"], last["low"]
        body = abs(c - o)
        rng  = h - l
        if rng < 0.00001: return None
        if body / rng < 0.20: return None
        return "call" if c > o else "put" if c < o else None

    def place_trade(self, asset, direction, amount, duration=60):
        if not self.connected:
            return False, None, "Not connected"
        try:
            trade_id = str(int(time.time() * 1000))
            evt = threading.Event()
            self._pending_trades[trade_id] = {"event": evt, "result": None}
            self.ws.send(json.dumps({
                "action": "open-option",
                "message": {
                    "asset": asset,
                    "amount": amount,
                    "time": duration,
                    "action": direction,
                    "is_demo": self.account_type == "demo",
                    "request_id": trade_id,
                }
            }))
            evt.wait(timeout=10)
            result = self._pending_trades.get(trade_id, {}).get("result")
            real_id = str(result.get("id", trade_id)) if result else trade_id
            return True, real_id, "Trade placed"
        except Exception as e:
            return False, None, str(e)

    def check_result(self, trade_id):
        try:
            r = self._pending_trades.pop(trade_id, {}).get("result")
            if r:
                profit = float(r.get("profit", r.get("win", 0)))
                return profit > 0, abs(profit)
            return False, 0.0
        except:
            return False, 0.0
