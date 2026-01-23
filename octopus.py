#!/usr/bin/env python3
"""
Try3: Execution Engine
- Base: Octopus (Architecture)
- Logic: Model2xx (8 Limit Steps + 1 Market Sweep)
- Signals: Multi-URL (try3{asset})
- Schedule: Hourly @ :15s
"""

import os
import sys
import time
import logging
import requests
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple

# --- Local Imports ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found.")
    sys.exit(1)

# --- Configuration ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# API Keys
KF_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KF_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# Global Settings
LEVERAGE = 1.0
MAX_WORKERS = 3  # For BTC, XRP, SOL

# Signal Sources
# Maps the internal asset ticker to the specific Railway URL
SIGNAL_URLS = {
    "BTC": "https://try3btc.up.railway.app/api/current",
    "XRP": "https://try3xrp.up.railway.app/api/current",
    "SOL": "https://try3sol.up.railway.app/api/current"
}

# Asset Mapping (Internal Symbol -> Kraken Futures Symbol)
# Using Perpetuals (pf_) for consistency. Change to ff_ if fixed maturity is desired.
SYMBOL_MAP = {
    "BTC": "pf_xbtusd",
    "XRP": "pf_xrpusd",
    "SOL": "pf_solusd",
}

# Execution Constants (From Model2xx)
MAX_STEPS = 8           # 8 Limit Order updates
STEP_INTERVAL = 5       # 5 seconds per step
INITIAL_OFFSET = 0.0002 # 0.02% (2 bps)
OFFSET_DECAY = 0.90     # Reduce offset by 10% per step

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("try3.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Try3")
LOG_LOCK = threading.Lock()

def bot_log(msg, level="info"):
    """Thread-safe logging helper."""
    with LOG_LOCK:
        if level == "info": logger.info(msg)
        elif level == "warning": logger.warning(msg)
        elif level == "error": logger.error(msg)
        # Custom delay not strictly necessary for functionality, but kept for readability if needed
        # time.sleep(0.01) 

# --- Signal Fetcher ---

class MultiSourceFetcher:
    def fetch_signals(self) -> Tuple[Dict[str, int], int]:
        """
        Fetches signals from individual asset URLs.
        Returns: ({ 'BTC': 1, 'XRP': -1, ... }, valid_count)
        """
        votes = {}
        valid_count = 0
        
        bot_log(f"Fetching signals for {list(SIGNAL_URLS.keys())}...")

        for asset, url in SIGNAL_URLS.items():
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    # Expecting: {"pred_dir": 0, "matches": 0, ...}
                    # We use 'pred_dir' directly (-1, 0, 1)
                    direction = int(data.get("pred_dir", 0))
                    
                    votes[asset] = direction
                    valid_count += 1
                    bot_log(f"[{asset}] Signal: {direction} (Matches: {data.get('matches', '?')})")
                else:
                    bot_log(f"[{asset}] HTTP Error {resp.status_code}", level="warning")
            except Exception as e:
                bot_log(f"[{asset}] Fetch Failed: {e}", level="error")
        
        return votes, valid_count

# --- Main Engine ---

class Try3Bot:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = MultiSourceFetcher()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.instrument_specs = {}

    def initialize(self):
        bot_log("--- Initializing Try3 Bot (Hourly/Model2xx Logic) ---")
        self._fetch_instrument_specs()
        
        # Connection Check
        try:
            acc = self.kf.get_accounts()
            if "error" in acc:
                bot_log(f"API Error: {acc}", level="error")
                sys.exit(1)
            else:
                bot_log("API Connection Successful.")
        except Exception as e:
            bot_log(f"API Connection Failed: {e}", level="error")
            sys.exit(1)

    def _fetch_instrument_specs(self):
        """Fetches tick size and precision for all symbols."""
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper() # Store uppercase for matching
                    precision = inst.get("contractValueTradePrecision", 3)
                    
                    self.instrument_specs[sym] = {
                        "sizeStep": 10 ** (-int(precision)) if precision is not None else 1.0,
                        "tickSize": float(inst.get("tickSize", 0.1))
                    }
        except Exception as e:
            bot_log(f"Error fetching specs: {e}", level="error")

    def _round_to_step(self, value: float, step: float) -> float:
        if step == 0: return value
        rounded = round(value / step) * step
        # Handle floating point errors slightly for clean logs
        return float(f"{rounded:.10g}")

    def _get_current_state(self, kf_symbol_upper: str) -> Tuple[float, float]:
        """Gets position size and mark price. Returns (size, price)."""
        current_pos_size = 0.0
        mark_price = 0.0
        
        try:
            # 1. Get Position
            open_pos = self.kf.get_open_positions()
            if "openPositions" in open_pos:
                for p in open_pos["openPositions"]:
                    if p["symbol"].upper() == kf_symbol_upper:
                        size = float(p["size"])
                        if p["side"] == "short": size = -size
                        current_pos_size = size
                        break
                        
            # 2. Get Mark Price
            tickers = self.kf.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == kf_symbol_upper:
                    mark_price = float(t["markPrice"])
                    break
                    
        except Exception as e:
            bot_log(f"[{kf_symbol_upper}] State Fetch Error: {e}", level="error")
            return 0.0, 0.0
            
        return current_pos_size, mark_price

    def run(self):
        bot_log("Bot started. Syncing to XX:00:15...")
        while True:
            now = datetime.now(timezone.utc)
            
            # Trigger at Minute 0, Second 15
            if now.minute == 0 and 15 <= now.second < 20:
                bot_log(f">>> TRIGGER: {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                time.sleep(50) # Sleep to avoid double trigger
                
            time.sleep(1)

    def _process_cycle(self):
        # 1. Fetch Signals
        votes, active_count = self.fetcher.fetch_signals()
        
        if active_count == 0:
            bot_log("No active signals found. Skipping.", level="warning")
            return

        # 2. Get Account Equity
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            else:
                # Fallback for some account types
                first_acc = list(acc.get("accounts", {}).values())[0]
                equity = float(first_acc.get("marginEquity", 0))
                
            if equity <= 0:
                bot_log("Equity <= 0. Aborting.", level="error")
                return
        except Exception as e:
            bot_log(f"Account fetch failed: {e}", level="error")
            return

        # 3. Calculate Base Unit (USD per Asset)
        # Logic: If 3 assets active, each gets 1/3 * leverage * signal_direction
        unit_usd = (equity * LEVERAGE) / active_count
        bot_log(f"Equity: ${equity:.2f} | Assets: {active_count} | Unit: ${unit_usd:.2f}")

        # 4. Execute concurrently
        for asset, direction in votes.items():
            if asset not in SYMBOL_MAP:
                continue
                
            kf_symbol = SYMBOL_MAP[asset]
            target_usd = unit_usd * direction # Can be negative (short)
            
            self.executor.submit(self._execute_dynamic_sequence, kf_symbol, target_usd)

    def _execute_dynamic_sequence(self, symbol_upper: str, target_usd: float):
        """
        Model2xx Execution Logic:
        - 8 Limit Order updates (start passive, get aggressive)
        - 1 Market Order sweep (if needed)
        - Recalculates contracts needed based on Mark Price at every step
        """
        symbol_upper = symbol_upper.upper() # Strict Upper for Specs/Matching
        symbol_lower = symbol_upper.lower() # Strict Lower for Order API
        
        specs = self.instrument_specs.get(symbol_upper)
        if not specs:
            bot_log(f"[{symbol_upper}] Specs missing. Skipping.", level="error")
            return

        order_id = None
        current_offset = INITIAL_OFFSET
        
        bot_log(f"[{symbol_upper}] Logic Start. Target Value: ${target_usd:.2f}")

        # --- Steps 1 to 8: Limit Orders ---
        for step in range(MAX_STEPS):
            # A. Get State
            curr_pos, mark_price = self._get_current_state(symbol_upper)
            if mark_price == 0:
                time.sleep(1)
                continue

            # B. Calculate Target Contracts
            target_contracts = target_usd / mark_price
            delta = target_contracts - curr_pos
            
            # C. Check Completion
            if abs(delta) < specs["sizeStep"]:
                bot_log(f"[{symbol_upper}] Target Reached (Delta: {delta:.4f}).")
                if order_id: self._cancel_order(order_id, symbol_lower)
                return

            # D. Order Parameters
            side = "buy" if delta > 0 else "sell"
            abs_delta = abs(delta)
            size = self._round_to_step(abs_delta, specs["sizeStep"])
            
            # Price Calculation
            price_mult = (1 - current_offset) if side == "buy" else (1 + current_offset)
            limit_price = self._round_to_step(mark_price * price_mult, specs["tickSize"])

            # E. Place/Update Order
            try:
                if not order_id:
                    resp = self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": side,
                        "size": size, "limitPrice": limit_price
                    })
                    if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                        order_id = resp["sendStatus"]["order_id"]
                        bot_log(f"[{symbol_upper}] Step {step+1}: Placed {side} {size} @ {limit_price}")
                else:
                    self.kf.edit_order({
                        "orderId": order_id, "symbol": symbol_lower,
                        "limitPrice": limit_price, "size": size
                    })
            except Exception as e:
                bot_log(f"[{symbol_upper}] Order Update Failed: {e}", level="warning")
                order_id = None # Assume filled or lost, reset for next loop

            # F. Decay & Wait
            current_offset *= OFFSET_DECAY
            time.sleep(STEP_INTERVAL)

        # --- Step 9: Market Sweep ---
        if order_id: self._cancel_order(order_id, symbol_lower)
        time.sleep(0.5)
        
        # Final Check
        curr_pos, mark_price = self._get_current_state(symbol_upper)
        if mark_price > 0:
            target_contracts = target_usd / mark_price
            delta = target_contracts - curr_pos
            
            if abs(delta) >= specs["sizeStep"]:
                side = "buy" if delta > 0 else "sell"
                size = self._round_to_step(abs(delta), specs["sizeStep"])
                bot_log(f"[{symbol_upper}] SWEEPING MKT: {side} {size} (Delta: {delta:.4f})")
                try:
                    self.kf.send_order({
                        "orderType": "mkt", "symbol": symbol_lower,
                        "side": side, "size": size
                    })
                except Exception as e:
                    bot_log(f"[{symbol_upper}] Sweep Failed: {e}", level="error")
            else:
                bot_log(f"[{symbol_upper}] Sweep not needed. Done.")

    def _cancel_order(self, order_id, symbol_lower):
        try:
            self.kf.cancel_order({"order_id": order_id, "symbol": symbol_lower})
        except:
            pass

if __name__ == "__main__":
    bot = Try3Bot()
    bot.initialize()
    bot.run()
