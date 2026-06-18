"""
Quotex Trader — Evalon AutoTrader
Candle-based logic: closed bullish candle → CALL, closed bearish candle → PUT
Flat market filter included.
"""

import asyncio
import threading
import time
import logging
from quotexapi.stable_api import Quotex

logger = logging.getLogger(__name__)


class QuotexTrader:
    def __init__(self, email, password, account_type="PRACTICE"):
        self.email = email
        self.password = password
        # PRACTICE = demo, REAL = real
        self.account_type = "PRACTICE" if account_type == "demo" else "REAL"
        self.client = None
        self.connected = False
        self.loop = None
        self.thread = None

    # ── Connect ──────────────────────────────────────────────────────────────
    def connect(self):
        """Connect to Quotex. Returns (success: bool, message: str)"""
        try:
            self.client = Quotex(
                email=self.email,
                password=self.password,
                lang="en"
            )
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self.thread.start()

            future = asyncio.run_coroutine_threadsafe(self._async_connect(), self.loop)
            result = future.result(timeout=30)
            return result
        except Exception as e:
            logger.error(f"Quotex connect error: {e}")
            return False, str(e)

    async def _async_connect(self):
        try:
            check, reason = await self.client.connect()
            if check:
                self.client.change_account(self.account_type)
                self.connected = True
                return True, "connected", None
            # Check if OTP is required
            reason_str = str(reason).lower()
            if "code" in reason_str or "otp" in reason_str or "verify" in reason_str or "email" in reason_str:
                return False, "otp_required", None
            return False, "failed", str(reason)
        except Exception as e:
            return False, "failed", str(e)

    def connect(self):
        """Connect to Quotex. Returns (success, status, message)
        status: 'connected' | 'otp_required' | 'failed'
        """
        try:
            self.client = Quotex(
                email=self.email,
                password=self.password,
                lang="en"
            )
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self.thread.start()
            future = asyncio.run_coroutine_threadsafe(self._async_connect(), self.loop)
            success, status, msg = future.result(timeout=30)
            return success, status, msg
        except Exception as e:
            logger.error(f"Quotex connect error: {e}")
            return False, "failed", str(e)

    def disconnect(self):
        try:
            if self.client:
                future = asyncio.run_coroutine_threadsafe(
                    self.client.close(), self.loop
                )
                future.result(timeout=5)
        except Exception:
            pass
        self.connected = False

    # ── Account Info ─────────────────────────────────────────────────────────
    def get_balance(self):
        """Returns {'demo': float, 'real': float}"""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_balance(), self.loop
            )
            return future.result(timeout=10)
        except Exception as e:
            logger.error(f"Balance error: {e}")
            return {"demo": 0, "real": 0}

    async def _async_balance(self):
        try:
            self.client.change_account("PRACTICE")
            demo_bal = await self.client.get_balance()
            self.client.change_account("REAL")
            real_bal = await self.client.get_balance()
            # restore original
            self.client.change_account(self.account_type)
            return {"demo": float(demo_bal or 0), "real": float(real_bal or 0)}
        except Exception as e:
            logger.error(f"Async balance error: {e}")
            return {"demo": 0, "real": 0}

    def switch_account(self, account_type):
        """Switch between demo and real"""
        self.account_type = "PRACTICE" if account_type == "demo" else "REAL"
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_switch(), self.loop
            )
            future.result(timeout=5)
        except Exception as e:
            logger.error(f"Switch account error: {e}")

    async def _async_switch(self):
        self.client.change_account(self.account_type)

    # ── Candles & Signal ─────────────────────────────────────────────────────
    def get_candles(self, asset, timeframe=60, count=5):
        """
        Fetch last `count` closed candles.
        timeframe in seconds (60 = 1 minute).
        Returns list of {'open', 'close', 'high', 'low', 'volume'}
        """
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_candles(asset, timeframe, count), self.loop
            )
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"Candles error: {e}")
            return []

    async def _async_candles(self, asset, timeframe, count):
        try:
            candles = await self.client.get_candles(asset, timeframe, count, time.time())
            result = []
            for c in candles:
                result.append({
                    "open":  float(c.get("open", 0)),
                    "close": float(c.get("close", 0)),
                    "high":  float(c.get("max", 0)),
                    "low":   float(c.get("min", 0)),
                    "volume": float(c.get("volume", 0)),
                })
            return result
        except Exception as e:
            logger.error(f"Async candles error: {e}")
            return []

    def analyze_signal(self, asset, timeframe=60):
        """
        Core logic:
        - Get last 3 closed candles
        - If last candle is bullish (close > open) → CALL
        - If last candle is bearish (close < open) → PUT
        - Flat market filter: if candle body < 20% of (high-low range) → SKIP
        Returns: 'call', 'put', or None (skip)
        """
        candles = self.get_candles(asset, timeframe, count=3)
        if not candles or len(candles) < 1:
            return None

        last = candles[-1]
        open_price  = last["open"]
        close_price = last["close"]
        high        = last["high"]
        low         = last["low"]

        body = abs(close_price - open_price)
        full_range = high - low

        # ── Flat market filter ────────────────────────────────────────────────
        # If the candle body is less than 20% of the full range, market is flat
        # Also skip if full_range is essentially zero
        if full_range < 0.00001:
            logger.info(f"{asset}: Flat market (zero range) — skipping")
            return None

        body_ratio = body / full_range
        if body_ratio < 0.20:
            logger.info(f"{asset}: Flat candle (body ratio {body_ratio:.2%}) — skipping")
            return None

        # ── Direction ─────────────────────────────────────────────────────────
        if close_price > open_price:
            return "call"
        elif close_price < open_price:
            return "put"
        else:
            return None  # doji — skip

    # ── Place Trade ──────────────────────────────────────────────────────────
    def place_trade(self, asset, direction, amount, duration=60):
        """
        Place a binary options trade.
        direction: 'call' or 'put'
        duration: seconds (60 = 1 minute)
        Returns (success: bool, trade_id, message: str)
        """
        if not self.connected:
            return False, None, "Not connected"
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_trade(asset, direction, amount, duration), self.loop
            )
            return future.result(timeout=20)
        except Exception as e:
            logger.error(f"Trade error: {e}")
            return False, None, str(e)

    async def _async_trade(self, asset, direction, amount, duration):
        try:
            # quotexapi uses 'call' / 'put' direction strings
            status, trade_id = await self.client.buy(
                amount=amount,
                asset=asset,
                direction=direction,
                duration=duration
            )
            if status:
                return True, trade_id, "Trade placed"
            return False, None, "Trade rejected by broker"
        except Exception as e:
            logger.error(f"Async trade error: {e}")
            return False, None, str(e)

    # ── Check Result ─────────────────────────────────────────────────────────
    def check_result(self, trade_id):
        """
        Check win/loss for a completed trade.
        Returns (won: bool, profit: float)
        """
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_result(trade_id), self.loop
            )
            return future.result(timeout=15)
        except Exception as e:
            logger.error(f"Result check error: {e}")
            return False, 0.0

    async def _async_result(self, trade_id):
        try:
            result = await self.client.check_win(trade_id)
            if result is None:
                return False, 0.0
            profit = float(result)
            won = profit > 0
            return won, abs(profit)
        except Exception as e:
            logger.error(f"Async result error: {e}")
            return False, 0.0

    # ── Asset List ───────────────────────────────────────────────────────────
    def get_open_assets(self):
        """Return list of currently open/tradeable assets with payout %"""
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._async_assets(), self.loop
            )
            return future.result(timeout=10)
        except Exception as e:
            logger.error(f"Assets error: {e}")
            return []

    async def _async_assets(self):
        try:
            raw = await self.client.get_available_asset(
                "binary",
                is_open=True
            )
            assets = []
            for name, data in (raw or {}).items():
                payout = data.get("profit", {}).get("1", 0) if isinstance(data, dict) else 0
                assets.append({"name": name, "payout": int(payout)})
            return sorted(assets, key=lambda x: x["payout"], reverse=True)
        except Exception as e:
            logger.error(f"Async assets error: {e}")
            return []
