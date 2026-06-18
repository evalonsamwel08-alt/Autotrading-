"""
Pocket Option Trader — Evalon AutoTrader
Uses pocketoptionapi library.
Candle-based logic: bullish close → CALL, bearish close → PUT
Flat market filter included.
"""

import asyncio
import threading
import time
import logging

logger = logging.getLogger(__name__)


class PocketOptionTrader:
    def __init__(self, email, password, account_type="demo"):
        self.email = email
        self.password = password
        self.account_type = account_type  # demo | real
        self.client = None
        self.connected = False
        self.loop = None
        self._thread = None

    def connect(self):
        """Connect to Pocket Option. Returns (success, message)"""
        try:
            from pocketoptionapi.stable_api import PocketOption
            self.client = PocketOption(self.email, self.password)
            self.loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self._thread.start()
            future = asyncio.run_coroutine_threadsafe(self._async_connect(), self.loop)
            return future.result(timeout=30)
        except ImportError:
            return False, "pocketoptionapi not installed"
        except Exception as e:
            logger.error(f"PocketOption connect: {e}")
            return False, str(e)

    async def _async_connect(self):
        try:
            check, reason = await self.client.connect()
            if check:
                self.connected = True
                if self.account_type == "real":
                    self.client.change_account("REAL")
                else:
                    self.client.change_account("PRACTICE")
                return True, "Connected"
            return False, str(reason)
        except Exception as e:
            return False, str(e)

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
            logger.error(f"PO balance: {e}")
            return {"demo": 0, "real": 0}

    async def _async_balance(self):
        try:
            self.client.change_account("PRACTICE")
            demo = await self.client.get_balance()
            self.client.change_account("REAL")
            real = await self.client.get_balance()
            self.client.change_account("REAL" if self.account_type == "real" else "PRACTICE")
            return {"demo": float(demo or 0), "real": float(real or 0)}
        except Exception as e:
            logger.error(f"PO async balance: {e}")
            return {"demo": 0, "real": 0}

    def switch_account(self, account_type):
        self.account_type = account_type
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_switch(), self.loop)
            future.result(timeout=5)
        except Exception:
            pass

    async def _async_switch(self):
        acct = "REAL" if self.account_type == "real" else "PRACTICE"
        self.client.change_account(acct)

    def get_candles(self, asset, timeframe=60, count=5):
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_candles(asset, timeframe, count), self.loop
            )
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"PO candles: {e}")
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
            logger.error(f"PO async candles: {e}")
            return []

    def analyze_signal(self, asset, timeframe=60):
        """
        Candle close logic + flat market filter.
        Returns: 'call', 'put', or None
        """
        candles = self.get_candles(asset, timeframe, count=3)
        if not candles:
            return None
        last = candles[-1]
        open_p  = last["open"]
        close_p = last["close"]
        high    = last["high"]
        low     = last["low"]
        body       = abs(close_p - open_p)
        full_range = high - low
        if full_range < 0.00001:
            logger.info(f"{asset}: Flat market — skip")
            return None
        if (body / full_range) < 0.20:
            logger.info(f"{asset}: Flat candle — skip")
            return None
        if close_p > open_p:
            return "call"
        elif close_p < open_p:
            return "put"
        return None

    def place_trade(self, asset, direction, amount, duration=60):
        if not self.connected:
            return False, None, "Not connected"
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_trade(asset, direction, amount, duration), self.loop
            )
            return future.result(timeout=20)
        except Exception as e:
            logger.error(f"PO trade: {e}")
            return False, None, str(e)

    async def _async_trade(self, asset, direction, amount, duration):
        try:
            status, trade_id = await self.client.buy(
                price=amount,
                active=asset,
                action=direction,
                expirations=duration
            )
            if status:
                return True, trade_id, "Trade placed"
            return False, None, "Rejected by broker"
        except Exception as e:
            return False, None, str(e)

    def check_result(self, trade_id):
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_result(trade_id), self.loop
            )
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"PO result: {e}")
            return False, 0.0

    async def _async_result(self, trade_id):
        try:
            result = await self.client.check_win(trade_id)
            if result is None:
                return False, 0.0
            profit = float(result)
            return profit > 0, abs(profit)
        except Exception as e:
            logger.error(f"PO async result: {e}")
            return False, 0.0

    def get_open_assets(self):
        try:
            future = asyncio.run_coroutine_threadsafe(self._async_assets(), self.loop)
            return future.result(timeout=10)
        except Exception:
            return []

    async def _async_assets(self):
        try:
            raw = await self.client.get_all_open_time()
            assets = []
            for category in raw.values():
                for name, info in category.items():
                    if info.get("open"):
                        assets.append({"name": name, "payout": 0})
            return assets
        except Exception:
            return []
