"""
Quotex Trader — using quotexpy (maintained library)
https://github.com/SantiiRepair/quotexpy
"""
import asyncio, threading, time, logging

logger = logging.getLogger(__name__)


class QuotexTrader:
    def __init__(self, email, password, account_type="demo"):
        self.email = email
        self.password = password
        self.account_type = "PRACTICE" if account_type == "demo" else "REAL"
        self.client = None
        self.connected = False
        self.loop = None
        self.thread = None

    def connect(self):
        try:
            from quotexpy import Quotex
            self.client = Quotex(email=self.email, password=self.password, lang="en")
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self.thread.start()
            future = asyncio.run_coroutine_threadsafe(self._async_connect(), self.loop)
            return future.result(timeout=30)
        except ImportError as e:
            return False, "failed", f"Library missing: {e}"
        except Exception as e:
            logger.error(f"Quotex connect: {e}")
            return False, "failed", str(e)

    async def _async_connect(self):
        try:
            check, reason = await self.client.connect()
            if check:
                self.client.change_account(self.account_type)
                self.connected = True
                return True, "connected", None
            reason_str = str(reason).lower()
            if any(x in reason_str for x in ["code", "otp", "verify", "email"]):
                return False, "otp_required", None
            if "captcha" in reason_str:
                return False, "failed", "❌ CAPTCHA required — try again in a few minutes"
            return False, "failed", str(reason)
        except Exception as e:
            return False, "failed", str(e)

    def disconnect(self):
        try:
            if self.client and self.loop:
                asyncio.run_coroutine_threadsafe(self.client.close(), self.loop)
        except Exception:
            pass
        self.connected = False

    def get_balance(self):
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_balance(), self.loop)
            return future.result(timeout=10)
        except Exception as e:
            logger.error(f"Quotex balance: {e}")
            return {"demo": 0, "real": 0}

    async def _async_balance(self):
        try:
            self.client.change_account("PRACTICE")
            demo = await self.client.get_balance()
            self.client.change_account("REAL")
            real = await self.client.get_balance()
            self.client.change_account(self.account_type)
            return {"demo": float(demo or 0), "real": float(real or 0)}
        except Exception as e:
            logger.error(f"Quotex async balance: {e}")
            return {"demo": 0, "real": 0}

    def switch_account(self, account_type):
        self.account_type = "PRACTICE" if account_type == "demo" else "REAL"
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_switch(), self.loop)
            future.result(timeout=5)
        except Exception:
            pass

    async def _async_switch(self):
        self.client.change_account(self.account_type)

    def get_candles(self, asset, timeframe=60, count=5):
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_candles(asset, timeframe, count), self.loop)
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"Quotex candles: {e}")
            return []

    async def _async_candles(self, asset, timeframe, count):
        try:
            candles = await self.client.get_candles(asset, timeframe, count, time.time())
            result = []
            for c in candles:
                result.append({
                    "open":  float(c.get("open", 0)),
                    "close": float(c.get("close", 0)),
                    "high":  float(c.get("max", c.get("high", 0))),
                    "low":   float(c.get("min", c.get("low", 0))),
                })
            return result
        except Exception as e:
            logger.error(f"Quotex async candles: {e}")
            return []

    def analyze_signal(self, asset, timeframe=60):
        candles = self.get_candles(asset, timeframe, count=3)
        if not candles:
            return None
        last = candles[-1]
        o, c, h, l = last["open"], last["close"], last["high"], last["low"]
        body, rng = abs(c - o), (h - l)
        if rng < 0.00001:
            return None
        if body / rng < 0.20:
            return None
        return "call" if c > o else "put" if c < o else None

    def place_trade(self, asset, direction, amount, duration=60):
        if not self.connected:
            return False, None, "Not connected"
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_trade(asset, direction, amount, duration), self.loop)
            return future.result(timeout=20)
        except Exception as e:
            logger.error(f"Quotex trade: {e}")
            return False, None, str(e)

    async def _async_trade(self, asset, direction, amount, duration):
        try:
            status, trade_id = await self.client.buy(
                amount=amount, asset=asset, direction=direction, duration=duration)
            if status:
                return True, trade_id, "Trade placed"
            return False, None, "Trade rejected by broker"
        except Exception as e:
            return False, None, str(e)

    def check_result(self, trade_id):
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_result(trade_id), self.loop)
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"Quotex result: {e}")
            return False, 0.0

    async def _async_result(self, trade_id):
        try:
            result = await self.client.check_win(trade_id)
            if result is None:
                return False, 0.0
            profit = float(result)
            return profit > 0, abs(profit)
        except Exception as e:
            logger.error(f"Quotex async result: {e}")
            return False, 0.0
