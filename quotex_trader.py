"""
Quotex Trader — Direct WebSocket Implementation
No external broker library needed.
"""
import json, time, threading, logging, requests
import websocket

logger = logging.getLogger(__name__)

QUOTEX_WS = "wss://ws2.trade.app/echo/websocket"
QUOTEX_LOGIN = "https://qxbroker.com/api/v2/user/login"

class QuotexTrader:
    def __init__(self, email, password, account_type="demo"):
        self.email = email
        self.password = password
        self.account_type = account_type
        self.ws = None
        self.connected = False
        self.authorized = False
        self._lock = threading.Lock()
        self._responses = {}
        self._req_id = 0
        self._thread = None
        self.balance_demo = 0.0
        self.balance_real = 0.0
        self.ssid = None
        self._pending_trades = {}

    def connect(self):
        try:
            # Step 1: Login via HTTP
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Content-Type": "application/json",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://qxbroker.com",
                "Referer": "https://qxbroker.com/en/sign-in",
            })
            resp = session.post(QUOTEX_LOGIN, json={
                "email": self.email,
                "password": self.password
            }, timeout=20)
            
            # Parse response safely
            try:
                data = resp.json()
            except Exception:
                data = {}

            if "token" in data:
                self.ssid = data["token"]
            elif "data" in data and isinstance(data["data"], dict) and "token" in data["data"]:
                self.ssid = data["data"]["token"]
            else:
                # Try cookies
                for cookie_name in ["session", "token", "auth", "ssid"]:
                    val = resp.cookies.get(cookie_name)
                    if val:
                        self.ssid = val
                        break

            if not self.ssid:
                text = resp.text.lower()
                if "code" in text or "verify" in text or "otp" in text:
                    return False, "otp_required", None
                if "captcha" in text:
                    return False, "failed", "❌ CAPTCHA required — try again later"
                if resp.status_code == 401:
                    return False, "failed", "❌ Wrong email or password"
                return False, "failed", f"❌ Login failed (status {resp.status_code})"
            
            # Step 2: Connect WebSocket
            self._connect_ws()
            return True, "connected", None

        except requests.exceptions.ConnectionError:
            return False, "failed", "No internet connection"
        except Exception as e:
            logger.error(f"Quotex connect: {e}")
            return False, "failed", str(e)

    def _connect_ws(self):
        self.ws = websocket.WebSocketApp(
            QUOTEX_WS,
            header={"Cookie": f"session={self.ssid}"},
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
        ws.send(json.dumps({"ssid": self.ssid, "action": "ssid"}))

    def _on_close(self, ws, *a):
        self.connected = False

    def _on_error(self, ws, err):
        logger.error(f"Quotex WS: {err}")

    def _on_message(self, ws, msg):
        try:
            data = json.loads(msg)
            # Balance updates
            if data.get("action") == "balance":
                bal = data.get("data", {})
                if isinstance(bal, dict):
                    t = bal.get("type", 1)
                    amount = float(bal.get("amount", 0))
                    if t == 1:
                        self.balance_real = amount
                    else:
                        self.balance_demo = amount
            # Trade results
            if data.get("action") == "buy-complete":
                d = data.get("data", {})
                tid = str(d.get("id", ""))
                if tid in self._pending_trades:
                    self._pending_trades[tid]["result"] = d
                    self._pending_trades[tid]["event"].set()
        except Exception as e:
            logger.error(f"Quotex msg: {e}")

    def disconnect(self):
        try:
            if self.ws: self.ws.close()
        except: pass
        self.connected = False

    def get_balance(self):
        # Request balance
        if self.ws and self.connected:
            try:
                self.ws.send(json.dumps({"action": "account-information", "data": {}}))
                time.sleep(1)
            except: pass
        return {"demo": self.balance_demo, "real": self.balance_real}

    def switch_account(self, account_type):
        self.account_type = account_type
        acc_id = 2 if account_type == "demo" else 1
        if self.ws and self.connected:
            try:
                self.ws.send(json.dumps({"action": "switch-account", "data": {"acc_id": acc_id}}))
            except: pass

    def get_candles(self, asset, timeframe=60, count=5):
        try:
            evt = threading.Event()
            req_id = f"candles_{time.time()}"
            self._responses[req_id] = {"event": evt, "data": None}
            
            payload = json.dumps({
                "action": "history",
                "data": {
                    "asset": asset,
                    "period": timeframe,
                    "time": int(time.time()),
                    "offset": count * timeframe
                }
            })
            self.ws.send(payload)
            evt.wait(timeout=10)
            raw = self._responses.pop(req_id, {}).get("data", [])
            candles = []
            for c in (raw or []):
                candles.append({
                    "open":  float(c.get("open", 0)),
                    "close": float(c.get("close", 0)),
                    "high":  float(c.get("high", 0)),
                    "low":   float(c.get("low", 0)),
                })
            return candles
        except Exception as e:
            logger.error(f"Quotex candles: {e}")
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
            acc = 2 if self.account_type == "demo" else 1
            trade_id = str(int(time.time() * 1000))
            evt = threading.Event()
            self._pending_trades[trade_id] = {"event": evt, "result": None}
            
            self.ws.send(json.dumps({
                "action": "buy",
                "data": {
                    "asset": asset,
                    "amount": amount,
                    "time": duration,
                    "action": direction,
                    "isDemo": acc == 2,
                    "requestId": trade_id,
                }
            }))
            evt.wait(timeout=10)
            result = self._pending_trades.pop(trade_id, {}).get("result")
            if result:
                real_id = str(result.get("id", trade_id))
                return True, real_id, "Trade placed"
            return True, trade_id, "Trade placed (pending)"
        except Exception as e:
            return False, None, str(e)

    def check_result(self, trade_id):
        # After duration, check balance change
        # Quotex sends win/loss via WebSocket automatically
        try:
            if trade_id in self._pending_trades:
                result = self._pending_trades[trade_id].get("result")
                if result:
                    profit = float(result.get("profit", 0))
                    return profit > 0, abs(profit)
            return False, 0.0
        except:
            return False, 0.0
