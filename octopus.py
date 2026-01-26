#!/usr/bin/env python3
"""
Try4: Execution Engine with SL/TP (Adapted for Machine Learning Server)
- Source: https://machine-learning.up.railway.app/api/signals
- Logic: Model2xx (Entry) + 1% SL / 2% TP (Bracket)
- Schedule: Every 15 mins (00, 15, 30, 45) @ :15s
- Assets: 15 Multi-Asset Support
"""

import os
import sys
import time
import logging
import requests
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, Any

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
LEVERAGE = 50
MAX_WORKERS = 10  # Increased for multi-asset coverage

# Skip List: Add symbols here to ignore them during execution
SKIP_ASSETS = [] 

# Signal Source
SIGNAL_URL = "https://machine-learning.up.railway.app/api/signals"

# Asset Mapping (Binance Symbol from Server -> Kraken Futures Symbol)
# Note: Kraken symbols are usually lower case 'pf_'. 
# Ensure these exist on your specific Kraken Futures feed.
SYMBOL_MAP = {
    "BTCUSDT": "pf_xbtusd",
    "ETHUSDT": "pf_ethusd",
    "XRPUSDT": "pf_xrpusd",
    "SOLUSDT": "pf_solusd",
    "DOGEUSDT": "pf_dogeusd",
    "ADAUSDT": "pf_adausd",
    "BCHUSDT": "pf_bchusd",
    "LINKUSDT": "pf_linkusd",
    "XLMUSDT": "pf_xlmusd",
    "SUIUSDT": "pf_suiusd",
    "AVAXUSDT": "pf_avaxusd",
    "LTCUSDT": "pf_ltcusd",
    "HBARUSDT": "pf_hbarusd",
    "SHIBUSDT": "pf_shibusd", # Verify if 1000SHIB or SHIB on your feed
    "TONUSDT": "pf_tonusd"
}

# Execution Constants
MAX_STEPS = 30          # 30 Limit Order updates (Total 5 mins)
STEP_INTERVAL = 10      # 10 seconds per step
INITIAL_OFFSET = 0.0002 # 0.02% start offset
OFFSET_DECAY = 0.90     # Decay offset
SL_PCT = 0.01           # 1% Stop Loss
TP_PCT = 0.02           # 2% Take Profit

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    handlers=[logging.FileHandler("try4.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Try4")
LOG_LOCK = threading.Lock()

def bot_log(msg, level="info"):
    with LOG_LOCK:
        if level == "info": logger.info(msg)
        elif level == "warning": logger.warning(msg)
        elif level == "error": logger.error(msg)

# --- Signal Fetcher ---

class CentralSignalFetcher:
    def fetch_signals(self) -> Dict[str, Any]:
        """
        Fetches signals from the central ML server.
        Expected format: 
        { 
          "BTCUSDT": { "direction": "UP", "entry_price": 50000, ... }, 
          "ETHUSDT": { ... } 
        }
        """
        votes = {}
        
        bot_log(f"Fetching signals from {SIGNAL_URL}...")

        try:
            resp = requests.get(SIGNAL_URL, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                
                for symbol, signal in data.items():
                    # Parse Direction
                    raw_dir = signal.get("direction", "FLAT")
                    direction = 0
                    if raw_dir == "UP": direction = 1
                    elif raw_dir == "DOWN": direction = -1
                    
                    # Parse Entry
                    entry_price = float(signal.get("entry_price", 0.0))
                    
                    if direction != 0:
                        votes[symbol] = {
                            "dir": direction,
                            "entry": entry_price
                        }
                        bot_log(f"[{symbol}] Sig: {raw_dir} @ {entry_price}")
            else:
                bot_log(f"HTTP Error {resp.status_code}", level="warning")
        except Exception as e:
            bot_log(f"Fetch Failed: {e}", level="error")
        
        return votes

# --- Main Engine ---

class Try4Bot:
    def __init__(self):
        self.kf = KrakenFuturesApi(KF_KEY, KF_SECRET)
        self.fetcher = CentralSignalFetcher()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.instrument_specs = {}

    def initialize(self):
        bot_log("--- Initializing Try4 Bot (15m Cycle + Multi-Asset) ---")
        if SKIP_ASSETS:
            bot_log(f"WARNING: Skipping Assets: {SKIP_ASSETS}")

        self._fetch_instrument_specs()
        
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
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    sym = inst["symbol"].upper()
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
        return float(f"{rounded:.10g}")

    def _get_current_state(self, kf_symbol_upper: str) -> Tuple[float, float]:
        try:
            current_pos_size = 0.0
            mark_price = 0.0
            
            # Position
            open_pos = self.kf.get_open_positions()
            if "openPositions" in open_pos:
                for p in open_pos["openPositions"]:
                    if p["symbol"].upper() == kf_symbol_upper:
                        size = float(p["size"])
                        if p["side"] == "short": size = -size
                        current_pos_size = size
                        break
            
            # Mark Price
            tickers = self.kf.get_tickers()
            for t in tickers.get("tickers", []):
                if t["symbol"].upper() == kf_symbol_upper:
                    mark_price = float(t["markPrice"])
                    break
                    
            return current_pos_size, mark_price
        except Exception as e:
            bot_log(f"[{kf_symbol_upper}] State Fetch Error: {e}", level="error")
            return 0.0, 0.0

    def run(self):
        bot_log("Bot started. Syncing to 15m intervals (00, 15, 30, 45)...")
        while True:
            now = datetime.now(timezone.utc)
            # Trigger on 15 minute boundaries at :15 seconds
            if now.minute % 15 == 0 and 15 <= now.second < 20:
                bot_log(f">>> TRIGGER: {now.strftime('%H:%M:%S')} <<<")
                self._process_cycle()
                time.sleep(50) # Prevent double trigger
            time.sleep(1)

    def _process_cycle(self):
        # 1. Fetch Raw Signals
        raw_votes = self.fetcher.fetch_signals()
        
        # 2. Filter out SKIPPED assets and Map to Kraken Symbols
        votes = {}
        for binance_sym, data in raw_votes.items():
            if binance_sym in SKIP_ASSETS: continue
            if binance_sym in SYMBOL_MAP:
                votes[binance_sym] = data # Keep binance key for reference, map later
            else:
                # Optional: Log missing map if needed
                pass

        # 3. Calculate Active Count (only non-zero direction)
        active_count = sum(1 for v in votes.values() if v["dir"] != 0)
        
        if active_count == 0: active_count = 1 

        # 4. Get Account Equity
        try:
            acc = self.kf.get_accounts()
            if "flex" in acc.get("accounts", {}):
                equity = float(acc["accounts"]["flex"].get("marginEquity", 0))
            else:
                first_acc = list(acc.get("accounts", {}).values())[0]
                equity = float(first_acc.get("marginEquity", 0))
                
            if equity <= 0:
                bot_log("Equity <= 0. Aborting.", level="error")
                return
        except Exception as e:
            bot_log(f"Account fetch failed: {e}", level="error")
            return

        # 5. Calculate Base Unit
        unit_usd = (equity * LEVERAGE) / active_count if active_count > 0 else 0
        bot_log(f"Equity: ${equity:.2f} | Active Assets: {active_count} | Unit: ${unit_usd:.2f}")

        # 6. Execute concurrently
        for binance_sym, data in votes.items():
            if binance_sym not in SYMBOL_MAP: continue
                
            kf_symbol = SYMBOL_MAP[binance_sym]
            direction = data["dir"]
            entry_price = data["entry"]
            target_usd = unit_usd * direction

            # If the spec doesn't exist (e.g. unsupported asset on this account), skip safely
            if kf_symbol.upper() not in self.instrument_specs:
                bot_log(f"[{kf_symbol}] Specs not found. Skipping.", level="warning")
                continue

            self.executor.submit(self._execute_dynamic_sequence, kf_symbol, target_usd, entry_price)

    def _execute_dynamic_sequence(self, symbol_upper: str, target_usd: float, entry_price: float):
        symbol_upper = symbol_upper.upper()
        symbol_lower = symbol_upper.lower()
        
        specs = self.instrument_specs.get(symbol_upper)
        if not specs: return

        # 1. Cancel ALL existing orders for this symbol (Cleanup old SL/TPs/Limits)
        try:
            self.kf.cancel_all_orders({"symbol": symbol_lower})
        except: pass

        # 2. Entry Logic (Model2xx)
        order_id = None
        current_offset = INITIAL_OFFSET
        bot_log(f"[{symbol_upper}] Entry Start. Target: ${target_usd:.2f}")

        # --- Entry Loop ---
        for step in range(MAX_STEPS):
            curr_pos, mark_price = self._get_current_state(symbol_upper)
            if mark_price == 0: time.sleep(1); continue

            # If target is 0 and we are 0, we are done (Flat)
            if target_usd == 0 and abs(curr_pos) < specs["sizeStep"]:
                bot_log(f"[{symbol_upper}] Position Closed (Flat).")
                return

            target_contracts = target_usd / mark_price
            delta = target_contracts - curr_pos
            
            if abs(delta) < specs["sizeStep"]:
                bot_log(f"[{symbol_upper}] Target Reached (Delta: {delta:.4f}).")
                if order_id: self._cancel_order(order_id, symbol_lower)
                break # Exit loop to place brackets

            side = "buy" if delta > 0 else "sell"
            size = self._round_to_step(abs(delta), specs["sizeStep"])
            
            price_mult = (1 - current_offset) if side == "buy" else (1 + current_offset)
            limit_price = self._round_to_step(mark_price * price_mult, specs["tickSize"])

            try:
                if not order_id:
                    resp = self.kf.send_order({
                        "orderType": "lmt", "symbol": symbol_lower, "side": side,
                        "size": size, "limitPrice": limit_price
                    })
                    if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                        order_id = resp["sendStatus"]["order_id"]
                else:
                    self.kf.edit_order({
                        "orderId": order_id, "symbol": symbol_lower,
                        "limitPrice": limit_price, "size": size
                    })
            except:
                order_id = None

            current_offset *= OFFSET_DECAY
            time.sleep(STEP_INTERVAL)

        if order_id: self._cancel_order(order_id, symbol_lower)
        
        # --- Market Sweep ---
        curr_pos, mark_price = self._get_current_state(symbol_upper)
        target_contracts = target_usd / mark_price
        delta = target_contracts - curr_pos
        
        if abs(delta) >= specs["sizeStep"]:
            side = "buy" if delta > 0 else "sell"
            size = self._round_to_step(abs(delta), specs["sizeStep"])
            bot_log(f"[{symbol_upper}] SWEEP MKT: {side} {size}")
            try:
                self.kf.send_order({"orderType": "mkt", "symbol": symbol_lower, "side": side, "size": size})
            except Exception as e:
                bot_log(f"[{symbol_upper}] Sweep Error: {e}", level="error")

        # --- 3. Place Bracket Orders (SL / TP) ---
        # Only if we have a significant open position
        time.sleep(1) # Let fills settle
        final_pos, _ = self._get_current_state(symbol_upper)
        
        if abs(final_pos) > specs["sizeStep"] and entry_price > 0:
            self._place_bracket_orders(symbol_lower, final_pos, entry_price, specs["tickSize"])

    def _place_bracket_orders(self, symbol: str, position_size: float, entry_price: float, tick_size: float):
        """
        Places OCO-like Stop Loss and Take Profit orders.
        Uses reduceOnly to ensure we don't flip position.
        """
        # Determine Direction
        is_long = position_size > 0
        side = "sell" if is_long else "buy"
        abs_size = abs(position_size)

        # Calculate Prices based on SIGNAL ENTRY PRICE (from the server)
        # Using server entry price ensures alignment with the strategy logic
        if is_long:
            sl_price = entry_price * (1 - SL_PCT) # 1% below entry
            tp_price = entry_price * (1 + TP_PCT) # 2% above entry
        else:
            sl_price = entry_price * (1 + SL_PCT) # 1% above entry
            tp_price = entry_price * (1 - TP_PCT) # 2% below entry

        sl_price = self._round_to_step(sl_price, tick_size)
        tp_price = self._round_to_step(tp_price, tick_size)

        bot_log(f"[{symbol.upper()}] Placing Bracket | Entry: {entry_price} | SL: {sl_price} | TP: {tp_price}")

        # Send Stop Loss
        try:
            self.kf.send_order({
                "orderType": "stp", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": sl_price,
                "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] SL Failed: {e}", level="error")

        # Send Take Profit
        try:
            self.kf.send_order({
                "orderType": "take_profit", 
                "symbol": symbol, 
                "side": side, 
                "size": abs_size, 
                "stopPrice": tp_price, 
                "reduceOnly": True
            })
        except Exception as e:
            bot_log(f"[{symbol.upper()}] TP Failed: {e}", level="error")

    def _cancel_order(self, order_id, symbol_lower):
        try: self.kf.cancel_order({"order_id": order_id, "symbol": symbol_lower})
        except: pass

if __name__ == "__main__":
    bot = Try4Bot()
    bot.initialize()
    bot.run()
