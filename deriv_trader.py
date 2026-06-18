"""
Deriv Trader — Evalon AutoTrader
Uses Deriv Official WebSocket API.
Candle-based logic: closed bullish → CALL (DIGITOVER), bearish → PUT (DIGITUNDER)
For binary options: uses CALL/PUT contract types.
"""

import json
import time
import threading
import logging
import websocket

logger = logging.getLogger(__name__)

DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3?app_id=1089"


class DerivTrader:
    def __init__(self, api_token, account_type="demo"):
        # api_token comes from the password field in the login UI
        self.api_token = api_token
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
        self.account_currency = "USD"

    # ── WebSocket Core ────────────────────────────────────────────────────────
    def connect(self):
        """Connect and authorize. Returns (success, message)"""
        try:
            self.ws = websocket.WebSocketApp(
                DERIV_WS_URL,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            self._thread = threading.Thread(
                target=self.ws.run_forever,
                kwargs={"ping_interval": 30, "ping_timeout": 10},
                daemon=True,
            )
            self._thread.start()
            # wait for open
            for _ in range(30):
                if self.connected:
                    break
                time.sleep(0.5)
            if not self.connected:
                return False, "WebSocket connection timeout"
            # authorize
            resp = self._send_sync({"authorize": self.api_token})
            if resp and "authorize" in resp:
                self.authorized = True
                self._load_balances()
                return True, "Connected & Authorized"
            err = resp.get("error", {}).get("message", "Auth failed") if resp else "Auth timeout"
            return False, err
        except Exception as e:
            logger.error(f"Deriv connect: {e}")
            return False, str(e)

    def _on_open(self, ws):
        self.connected = True

    def _on_close(self, ws, code, msg):
        self.connected = False
        self.authorized = False

    def _on_error(self, ws, error):
        logger.error(f"Deriv WS error: {error}")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            req_id = data.get("req_id")
            if req_id and req_id in self._responses:
                self._responses[req_id]["data"] = data
                self._responses[req_id]["event"].set()
            # live balance updates
            if "balance" in data and "balance" in data.get("balance", {}):
                bal = data["balance"]["balance"]
                if data["balance"].get("account_type") == "demo":
                    self.balance_demo = float(bal)
                else:
                    self.balance_real = float(bal)
        except Exception as e:
            logger.error(f"Deriv message parse: {e}")

    def _next_req_id(self):
        with self._lock:
            self._req_id += 1
            return self._req_id

    def _send_sync(self, payload, timeout=15):
        req_id = self._next_req_id()
        payload["req_id"] = req_id
        evt = threading.Event()
        self._responses[req_id] = {"event": evt, "data": None}
        try:
            self.ws.send(json.dumps(payload))
            evt.wait(timeout=timeout)
            return self._responses.pop(req_id, {}).get("data")
        except Exception as e:
            logger.error(f"Deriv send: {e}")
            self._responses.pop(req_id, None)
            return None

    def disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        self.connected = False
        self.authorized = False

    # ── Balance ───────────────────────────────────────────────────────────────
    def _load_balances(self):
        resp = self._send_sync({"balance": 1, "account": "all", "subscribe": 1})
        if resp and "balance" in resp:
            accs = resp["balance"].get("accounts", {})
            for acc_id, info in accs.items():
                bal = float(info.get("balance", 0))
                if info.get("type") == "virtual":
                    self.balance_demo = bal
                else:
                    self.balance_real = bal

    def get_balance(self):
        return {"demo": self.balance_demo, "real": self.balance_real}

    def switch_account(self, account_type):
        self.account_type = account_type

    # ── Candles ───────────────────────────────────────────────────────────────
    def get_candles(self, symbol, granularity=60, count=5):
        """
        Fetch last `count` closed OHLC candles.
        granularity: seconds per candle (60 = 1m, 120 = 2m)
        Returns list of {'open','close','high','low'}
        """
        end_time = int(time.time())
        start_time = end_time - (granularity * (count + 2))
        resp = self._send_sync({
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": count + 2,
            "end": "latest",
            "start": start_time,
            "granularity": granularity,
            "style": "candles",
        }, timeout=15)
        if not resp or "candles" not in resp:
            return []
        candles = []
        for c in resp["candles"]:
            candles.append({
                "open":  float(c.get("open", 0)),
                "close": float(c.get("close", 0)),
                "high":  float(c.get("high", 0)),
                "low":   float(c.get("low", 0)),
            })
        # return last `count` candles only (exclude last possibly open candle)
        return candles[:-1][-count:]

    def analyze_signal(self, symbol, granularity=60):
        """
        Candle-based signal:
        - Last closed candle bullish (close > open) → CALL
        - Last closed candle bearish (close < open) → PUT
        - Flat market filter: body < 20% of range → skip
        Returns: 'call', 'put', or None
        """
        candles = self.get_candles(symbol, granularity)
        if not candles:
            return None
        last = candles[-1]
        open_p  = last["open"]
        close_p = last["close"]
        high    = last["high"]
        low     = last["low"]

        body = abs(close_p - open_p)
        full_range = high - low

        if full_range < 0.00001:
            logger.info(f"{symbol}: Flat market — skip")
            return None

        if (body / full_range) < 0.20:
            logger.info(f"{symbol}: Flat candle ({body/full_range:.1%}) — skip")
            return None

        if close_p > open_p:
            return "call"
        elif close_p < open_p:
            return "put"
        return None

    # ── Place Trade ───────────────────────────────────────────────────────────
    def place_trade(self, symbol, direction, amount, duration=60):
        """
        Place binary options trade via Deriv API.
        direction: 'call' or 'put'
        duration: seconds
        Returns (success, contract_id, message)
        """
        if not self.authorized:
            return False, None, "Not authorized"

        contract_type = "CALL" if direction == "call" else "PUT"
        currency = self.account_currency

        # For virtual/demo accounts use virtual=1
        is_virtual = self.account_type == "demo"

        payload = {
            "buy": 1,
            "price": amount,
            "parameters": {
                "amount": amount,
                "basis": "stake",
                "contract_type": contract_type,
                "currency": currency,
                "duration": duration,
                "duration_unit": "s",
                "symbol": symbol,
            }
        }
        if is_virtual:
            payload["parameters"]["account_type"] = "virtual"

        resp = self._send_sync(payload, timeout=20)
        if not resp:
            return False, None, "No response from Deriv"
        if "error" in resp:
            return False, None, resp["error"].get("message", "Trade error")
        if "buy" in resp:
            contract_id = resp["buy"].get("contract_id")
            return True, contract_id, "Trade placed"
        return False, None, "Unknown response"

    def check_result(self, contract_id):
        """
        Poll contract result. Call after duration has passed.
        Returns (won: bool, profit: float)
        """
        if not contract_id:
            return False, 0.0
        resp = self._send_sync({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
        }, timeout=15)
        if not resp or "proposal_open_contract" not in resp:
            return False, 0.0
        contract = resp["proposal_open_contract"]
        status = contract.get("status", "")
        if status == "won":
            profit = float(contract.get("profit", 0))
            return True, abs(profit)
        elif status == "lost":
            return False, 0.0
        # still open
        return False, 0.0

    # ── Open Symbols ─────────────────────────────────────────────────────────
    def get_open_assets(self, market="forex"):
        """Return tradeable symbols for the given market"""
        resp = self._send_sync({
            "active_symbols": "brief",
            "product_type": "basic",
        }, timeout=10)
        if not resp or "active_symbols" not in resp:
            return []
        symbols = []
        for s in resp["active_symbols"]:
            if s.get("exchange_is_open") and market in s.get("market", ""):
                symbols.append({
                    "name": s.get("symbol"),
                    "display": s.get("display_name"),
                    "payout": 0,
                })
        return symbols
