#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║  WYCKOFF 2.0 + MALAYSIAN SNR TRADING BOT                           ║
║  Multi-Environment: Pydroid 3 (Android) · Render (Cloud) · PC     ║
╠══════════════════════════════════════════════════════════════════════╣
║  ARCHITECTURE                                                      ║
║  ┌─────────────────────────────────────────────────────────┐       ║
║  │  Section 1 : Imports & Logging                          │       ║
║  │  Section 2 : CONFIG                                     │       ║
║  │  Section 3 : Enums & Dataclasses (Signal, SNRLevel…)   │       ║
║  │  Section 4 : HTTP Helpers                               │       ║
║  │  Section 5 : Broker Layer                               │       ║
║  │               MetaAPIBroker | OANDABroker               │       ║
║  │               YFinanceBroker | YFinanceMT5Broker        │       ║
║  │               PaperBroker | create_broker()             │       ║
║  │  Section 6 : Binance Data Layer (crypto volume/OB/FP)  │       ║
║  │  Section 7 : Data Helpers & Indicators                  │       ║
║  │  Section 8 : Malaysian SNR Detection                    │       ║
║  │  Section 9 : Wyckoff Phase Analysis                     │       ║
║  │  Section 10: Bias Cascade & Confluence                  │       ║
║  │  Section 11: Signal Scoring (5-layer)                   │       ║
║  │  Section 12: Risk & Control (lot size, daily loss…)    │       ║
║  │  Section 13: PnL & Equity Tracking                      │       ║
║  │  Section 14: Price Feed (MT5 bridge → broker fallback)  │       ║
║  │  Section 15: Trade Lifecycle Manager                    │       ║
║  │               open_trade_record()                       │       ║
║  │               update_trade_record() — trailing stop     │       ║
║  │               close_trade_record()                      │       ║
║  │               sync_lifecycle_with_broker()              │       ║
║  │  Section 16: Execution Pipeline                         │       ║
║  │  Section 17: Main Scan Loop (run / run_main_loop)       │       ║
║  │  Section 18: Render Hybrid (Flask + session bot loop)  │       ║
║  │  Section 19: Session Manager (/session/start|end)       │       ║
║  │  Section 20: Entry Point                                │       ║
║  └─────────────────────────────────────────────────────────┘       ║
╠══════════════════════════════════════════════════════════════════════╣
║  DEPENDENCIES (pip install)                                        ║
║    numpy  pandas  scipy  requests  flask                           ║
║                                                                    ║
║  NO TA-Lib  | NO MetaTrader5 DLL  | NO colorama | NO dotenv       ║
╠══════════════════════════════════════════════════════════════════════╣
║  RUNNING THE BOT                                                   ║
║  Local / Pydroid  : python wyckoff_snr_bot_pydroid-4-1.py         ║
║  Render (cloud)   : set start command to the same file             ║
║                     set env vars: BOT_SECRET, WEBAPP_API_URL       ║
╠══════════════════════════════════════════════════════════════════════╣
║  ENV VARIABLES (Render dashboard → Environment)                    ║
║    BOT_SECRET      shared secret between bot and webapp            ║
║    WEBAPP_API_URL  your webapp's push endpoint (optional)          ║
║    PORT            set automatically by Render (default 10000)     ║
╠══════════════════════════════════════════════════════════════════════╣
║  FLASK ENDPOINTS                                                   ║
║    GET  /                  bot status + session state              ║
║    GET  /health            Render health check                     ║
║    GET  /signals           last 20 signals                         ║
║    GET  /analysis          last 10 chart analysis snapshots        ║
║    GET  /trades            live open trades (lifecycle manager)    ║
║    GET  /trades/history    last 50 closed trades                   ║
║    POST /session/start     inject MetaAPI credentials (secured)    ║
║    POST /session/end       wipe credentials, stop trading          ║
║    GET  /session/status    session active/idle check               ║
╚══════════════════════════════════════════════════════════════════════╝
"""

# ── STANDARD LIBRARY ONLY (all built into Python 3.8) ────────────────
import os
import sys
import json
import math
import time
import logging
import threading
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Any
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
from urllib.parse import urljoin
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ── PYDROID-AVAILABLE SCIENTIFIC LIBRARIES ────────────────────────────
try:
    import numpy as np
    NUMPY_OK = True
except ImportError:
    NUMPY_OK = False
    print("[ERROR] numpy not found. Run in Pydroid pip menu: pip install numpy")
    sys.exit(1)

try:
    import pandas as pd
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False
    print("[ERROR] pandas not found. Run: pip install pandas")
    sys.exit(1)

try:
    from scipy.signal import argrelextrema
    SCIPY_OK = True
except ImportError:
    SCIPY_OK = False
    print("[WARN] scipy not found. Using built-in pivot detection fallback.")
    print("       To fix: pip install scipy")

try:
    import requests as req_lib
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False
    print("[WARN] requests not found. Using urllib fallback.")
    print("       To fix: pip install requests")

# ════════════════════════════════════════════════════════════════════
#  LOGGING — Android/Pydroid safe (no colorama, plain text)
# ════════════════════════════════════════════════════════════════════
LOG_FILE = "bot_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("WyckoffSNRBot")

def log_info(msg):    log.info(msg)
def log_warn(msg):    log.warning(msg)
def log_error(msg):   log.error(msg)
def log_signal(msg):  log.info(">>> SIGNAL: " + msg)
def log_trade(msg):   log.info("=== TRADE: " + msg)

# ════════════════════════════════════════════════════════════════════
#  INTERNET CONNECTIVITY  — hard gate, no silent fallback
# ════════════════════════════════════════════════════════════════════

# Tracks live internet state — checked at startup and every scan
_INTERNET_OK: bool = False

# Lightweight hosts to ping — tries each in order
_PING_HOSTS = [
    ("8.8.8.8",         53),   # Google DNS
    ("1.1.1.1",         53),   # Cloudflare DNS
    ("query1.finance.yahoo.com", 443),  # Yahoo Finance directly
]

# ════════════════════════════════════════════════════════════════════
#  GLOBAL STATES — Equity, Daily Loss, Cooldown
# ════════════════════════════════════════════════════════════════════
EQUITY_STATE: Dict = {"equity": [], "last_balance": 10000.0}
DAILY_STATE:  Dict = {"date": datetime.now().date(), "loss": 0.0}
LAST_TRADE_TIME: Dict = {}

def check_internet(timeout: int = 5) -> bool:
    """
    Try to open a TCP socket to known public hosts.
    Returns True only if at least one host is reachable.
    Works on Android/Pydroid — no ICMP ping needed.
    """
    import socket
    for host, port in _PING_HOSTS:
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            return True
        except OSError:
            continue
    return False


def require_internet(retry_secs: int = 30) -> None:
    """
    Block until internet is available.
    Logs a clear error every retry_secs seconds while offline.
    Called at bot startup AND at the top of every scan cycle.
    """
    global _INTERNET_OK
    first_check = True
    while True:
        if check_internet():
            if not _INTERNET_OK or first_check:
                log_info("Internet connection: OK")
            _INTERNET_OK = True
            return
        _INTERNET_OK = False
        if first_check:
            print("")
            print("=" * 60)
            print("  [NO INTERNET] Bot is PAUSED.")
            print("  Live price data requires an active connection.")
            print("  PAPER mode will NOT be used as a fallback.")
            print(f"  Retrying every {retry_secs}s — connect to WiFi/mobile data.")
            print("=" * 60)
            first_check = False
        log_warn(f"No internet — waiting {retry_secs}s before retry...")
        time.sleep(retry_secs)

# ════════════════════════════════════════════════════════════════════
#  CONFIGURATION  — Edit these values directly (no .env needed)
# ════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Broker Connection ─────────────────────────────────────────
    # RECOMMENDED SETUP:
    #   broker_mode = "YFINANCE_MT5"
    #   → Price data:   Yahoo Finance (free, real live prices, no account)
    #   → Trade execution: MT5 EA on your PC reads signal_queue.json
    #
    # Other modes:
    #   METAAPI  — MT5 via REST (metaapi.cloud)
    #   OANDA    — OANDA REST API
    #   PAPER    — synthetic data, no real trades
    #   YFINANCE — Yahoo Finance data, signal-only (no MT5)

    "broker_mode":    "YFINANCE_MT5",   # <── change this

    # MetaAPI (only needed if broker_mode = METAAPI)
    "metaapi_token":  "YOUR_METAAPI_TOKEN_HERE",
    "metaapi_account":"YOUR_ACCOUNT_ID_HERE",
    "metaapi_url":    "https://mt-client-api-v1.london.agiliumtrade.ai",

    # OANDA (only needed if broker_mode = OANDA)
    "oanda_token":    "YOUR_OANDA_TOKEN_HERE",
    "oanda_account":  "YOUR_OANDA_ACCOUNT_HERE",
    "oanda_url":      "https://api-fxtrade.oanda.com",

    # MT5 Signal Bridge (used when broker_mode = YFINANCE_MT5)
    # The bot writes signals here; your MT5 EA on PC reads this file.
    # Use a shared folder path — e.g. Google Drive, Dropbox, or LAN share.
    # On Android Pydroid, use a path your phone can write to.
    "mt5_signal_file": "signal_queue.json",   # bot writes here
    "mt5_magic":       20250422,              # EA magic number (any integer)

    # MT5 Bridge HTTP server (optional — EA runs a local HTTP server on PC)
    # When running: bot fetches live prices + sends orders via REST instead of file.
    # Leave as default if you are not running the bridge HTTP server.
    "mt5_bridge_url":  "http://localhost:8000",

    # ── Instruments ───────────────────────────────────────────────
    # Yahoo Finance symbol map — bot uses these internally.
    # MT5 symbols use standard broker names (set in EA below).
    "symbols": [
        "EUR_USD", "GBP_USD", "XAU_USD",
        "USD_JPY", "GBP_JPY", "BTC_USD"
    ],

    # ── Risk ──────────────────────────────────────────────────────
    "risk_pct":        1.0,
    "max_trades":      3,
    "atr_sl_mult":     1.5,
    "atr_tp_mult":     3.0,
    "max_spread_pips": 5,

    # ── Strategy thresholds ───────────────────────────────────────
    "wyckoff_min":    4,
    "snr_min":        5,
    "combined_min":   9,

    # ── Timeframes ────────────────────────────────────────────────
    "tf_weekly":  "W",
    "tf_daily":   "D",
    "tf_h4":      "H4",
    "tf_h2":      "H2",
    "tf_h1":      "H1",
    "tf_m45":     "M45",
    "tf_m30":     "M30",
    "tf_m15":     "M15",

    # ── Bias cascade (BIAS only — not confluence/entry) ───────────
    "bias_cascade":   ["H4", "H2", "H1", "M45", "M30", "M15"],
    "bias_min_score": 2,

    # ── Scan & timing ─────────────────────────────────────────────
    "scan_secs":  300,

    # ── Telegram alerts (optional) ────────────────────────────────
    "tg_token":  "",
    "tg_chat":   "",

    # ── File paths ────────────────────────────────────────────────
    "log_file":      "bot_log.txt",
    "trades_file":   "open_trades.json",
    "signals_file":  "signals_log.json",
    "analysis_file": "chart_analysis.json",
}

# ════════════════════════════════════════════════════════════════════
#  ENUMS & DATA CLASSES
# ════════════════════════════════════════════════════════════════════
class Direction(Enum):
    BUY  = "BUY"
    SELL = "SELL"

class PatternType(Enum):
    REGULAR_SNR      = "REGULAR_SNR"
    BREAKOUT_FLIPPED = "BREAKOUT_FLIPPED"
    QML_HNS          = "QML_HNS"
    GAP_HIDDEN       = "GAP_HIDDEN"
    CONGESTION       = "CONGESTION"
    X_FACTOR         = "X_FACTOR"
    QMX              = "QMX"
    TL_DIVERGENCE    = "TL_DIVERGENCE"
    BREAKOUT_TL      = "BREAKOUT_TL"

@dataclass
class SNRLevel:
    price:      float
    stype:      str
    shape:      str
    pattern:    PatternType = PatternType.REGULAR_SNR
    fresh:      bool  = True
    miss_count: int   = 0
    idx:        int   = 0

@dataclass
class Trendline:
    p1: float
    p2: float
    p3: float
    i1: int
    i2: int
    i3: int
    ttype:  str = "REGULAR"   # REGULAR | BREAKOUT | DIVERGENCE
    direction: str = "UP"

@dataclass
class Signal:
    symbol:    str
    direction: Direction
    score_w:   int
    score_s:   int
    score:     int
    pattern:   PatternType
    phase:     str
    snr_price: float
    sl:        float
    tp:        float
    vpoc:      float
    vwap:      float
    reasons:   List[str] = field(default_factory=list)
    tl_type:   str = ""
    qmx:       bool = False
    timestamp: str = ""

    def to_dict(self):
        return {
            "symbol": self.symbol,
            "direction": self.direction.value,
            "score": self.score,
            "pattern": self.pattern.value,
            "phase": self.phase,
            "snr_price": self.snr_price,
            "sl": self.sl,
            "tp": self.tp,
            "reasons": self.reasons,
            "timestamp": self.timestamp or datetime.now().isoformat(),
        }

# ════════════════════════════════════════════════════════════════════
#  HTTP HELPERS  (works without requests library via urllib fallback)
# ════════════════════════════════════════════════════════════════════
def http_get(url, headers=None, timeout=10):
    """HTTP GET — uses requests if available, else urllib."""
    headers = headers or {}
    if REQUESTS_OK:
        try:
            r = req_lib.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log_error(f"HTTP GET error {url}: {e}")
            return None
    else:
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            log_error(f"urllib GET error {url}: {e}")
            return None

def http_post(url, data, headers=None, timeout=10):
    """HTTP POST."""
    headers = headers or {"Content-Type": "application/json"}
    body = json.dumps(data).encode("utf-8")
    if REQUESTS_OK:
        try:
            r = req_lib.post(url, json=data, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log_error(f"HTTP POST error {url}: {e}")
            return None
    else:
        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            log_error(f"urllib POST error {url}: {e}")
            return None

# ════════════════════════════════════════════════════════════════════
#  BROKER LAYER  — MetaAPI / OANDA / Paper
# ════════════════════════════════════════════════════════════════════
class BrokerBase:
    def get_candles(self, symbol, timeframe, count=200):
        raise NotImplementedError
    def get_account_info(self):
        raise NotImplementedError
    def place_order(self, symbol, direction, lot, sl, tp):
        raise NotImplementedError
    def get_open_positions(self):
        raise NotImplementedError
    def get_tick(self, symbol):
        raise NotImplementedError

# ── MetaAPI broker (connects to MT5 account via REST) ────────────────
class MetaAPIBroker(BrokerBase):
    """
    Uses MetaAPI (metaapi.cloud) REST API.
    Free tier available. Connect your MT5 account there.
    Docs: https://metaapi.cloud/docs/client/
    """
    def __init__(self, token, account_id, base_url):
        self.token      = token
        self.account_id = account_id
        self.base_url   = base_url.rstrip("/")
        self.headers    = {
            "auth-token": token,
            "Content-Type": "application/json"
        }

    def _url(self, path):
        return f"{self.base_url}{path}"

    def get_candles(self, symbol, timeframe, count=200):
        """Fetch OHLCV candles from MetaAPI."""
        path = f"/users/current/accounts/{self.account_id}/historical-market-data/symbols/{symbol}/timeframes/{timeframe}/candles"
        params = f"?limit={count}"
        data = http_get(self._url(path + params), self.headers)
        if not data or "candles" not in data:
            return None
        rows = []
        for c in data["candles"]:
            rows.append({
                "time":        c.get("time", ""),
                "open":        float(c.get("open",  0)),
                "high":        float(c.get("high",  0)),
                "low":         float(c.get("low",   0)),
                "close":       float(c.get("close", 0)),
                "tick_volume": float(c.get("tickVolume", 1)),
            })
        df = pd.DataFrame(rows)
        return _enrich_df(df)

    def get_account_info(self):
        path = f"/users/current/accounts/{self.account_id}/account-information"
        return http_get(self._url(path), self.headers)

    def place_order(self, symbol, direction, lot, sl, tp):
        path = f"/users/current/accounts/{self.account_id}/trade"
        action = "ORDER_TYPE_BUY" if direction == Direction.BUY else "ORDER_TYPE_SELL"
        data = {
            "actionType": action,
            "symbol": symbol,
            "volume": lot,
            "stopLoss": sl,
            "takeProfit": tp,
            "comment": "WyckoffSNR"
        }
        return http_post(self._url(path), data, self.headers)

    def get_open_positions(self):
        path = f"/users/current/accounts/{self.account_id}/positions"
        data = http_get(self._url(path), self.headers)
        return data if data else []

    def get_tick(self, symbol):
        path = f"/users/current/accounts/{self.account_id}/symbols/{symbol}/current-price"
        return http_get(self._url(path), self.headers)

# ── OANDA broker ──────────────────────────────────────────────────────
class OANDABroker(BrokerBase):
    """
    OANDA v20 REST API. Free demo account at oanda.com.
    Docs: https://developer.oanda.com/rest-live-v20/introduction/
    """
    TF_MAP = {
        "W": "W", "D": "D", "H4": "H4", "H2": "H2",
        "H1": "H1", "M45": "M45", "M30": "M30", "M15": "M15"
    }

    def __init__(self, token, account_id, base_url):
        self.token      = token
        self.account_id = account_id
        self.base_url   = base_url.rstrip("/")
        self.headers    = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _url(self, path):
        return f"{self.base_url}/v3{path}"

    def get_candles(self, symbol, timeframe, count=200):
        gran = self.TF_MAP.get(timeframe, "H1")
        path = f"/instruments/{symbol}/candles?count={count}&granularity={gran}&price=M"
        data = http_get(self._url(path), self.headers)
        if not data or "candles" not in data:
            return None
        rows = []
        for c in data["candles"]:
            if not c.get("complete", True):
                continue
            m = c.get("mid", {})
            rows.append({
                "time":        c.get("time", ""),
                "open":        float(m.get("o", 0)),
                "high":        float(m.get("h", 0)),
                "low":         float(m.get("l", 0)),
                "close":       float(m.get("c", 0)),
                "tick_volume": float(c.get("volume", 1)),
            })
        df = pd.DataFrame(rows)
        return _enrich_df(df)

    def get_account_info(self):
        path = f"/accounts/{self.account_id}/summary"
        data = http_get(self._url(path), self.headers)
        return data.get("account") if data else None

    def place_order(self, symbol, direction, lot, sl, tp):
        path = f"/accounts/{self.account_id}/orders"
        side = "buy" if direction == Direction.BUY else "sell"
        data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(lot) if side == "buy" else str(-lot),
                "stopLossOnFill":   {"price": str(round(sl, 5))},
                "takeProfitOnFill": {"price": str(round(tp, 5))},
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
            }
        }
        return http_post(self._url(path), data, self.headers)

    def get_open_positions(self):
        path = f"/accounts/{self.account_id}/openPositions"
        data = http_get(self._url(path), self.headers)
        return data.get("positions", []) if data else []

    def get_tick(self, symbol):
        path = f"/accounts/{self.account_id}/pricing?instruments={symbol}"
        data = http_get(self._url(path), self.headers)
        if data and "prices" in data and data["prices"]:
            p = data["prices"][0]
            return {
                "bid": float(p.get("bids", [{"price": 0}])[0]["price"]),
                "ask": float(p.get("asks", [{"price": 0}])[0]["price"]),
            }
        return None

# ── Paper Broker (no real connection — for testing on Pydroid) ────────
class PaperBroker(BrokerBase):
    """No live connection. Logs signals and simulates trades locally."""
    def __init__(self):
        self.balance    = 10000.0
        self.positions  = []
        self.trade_log  = []
        self._price_cache: Dict[str, float] = {}

    def _simulate_closes(self, symbol: str, current_price: float):
        """
        Check open paper positions for SL/TP hits using latest price.
        Removes closed positions so max_trades doesn't permanently block.
        """
        still_open = []
        for pos in self.positions:
            if pos["symbol"] != symbol:
                still_open.append(pos)
                continue
            sl = pos.get("sl", 0); tp = pos.get("tp", 0)
            direction = pos.get("direction", "")
            hit = None
            if direction == "BUY":
                if current_price <= sl:
                    hit = f"SL hit at {current_price:.5f} (sl={sl:.5f})"
                elif current_price >= tp:
                    hit = f"TP hit at {current_price:.5f} (tp={tp:.5f})"
            elif direction == "SELL":
                if current_price >= sl:
                    hit = f"SL hit at {current_price:.5f} (sl={sl:.5f})"
                elif current_price <= tp:
                    hit = f"TP hit at {current_price:.5f} (tp={tp:.5f})"
            if hit:
                log_trade(f"[PAPER] CLOSED {direction} {symbol} — {hit}")
                pos["close_reason"] = hit
                pos["close_price"]  = current_price
                pos["close_time"]   = datetime.now().isoformat()
            else:
                still_open.append(pos)
        self.positions = still_open

    def get_candles(self, symbol, timeframe, count=200):
        """Generate synthetic OHLCV for testing without internet."""
        # Use a time-varying seed so each scan gets slightly different data
        seed = hash(symbol + timeframe + str(int(time.time() // 300))) % (2**31)
        np.random.seed(seed)
        closes = np.cumsum(np.random.randn(count) * 0.0005) + 1.1
        opens  = np.roll(closes, 1); opens[0] = closes[0]
        highs  = np.maximum(opens, closes) + np.abs(np.random.randn(count)) * 0.0003
        lows   = np.minimum(opens, closes) - np.abs(np.random.randn(count)) * 0.0003
        vols   = np.random.randint(100, 10000, count).astype(float)
        df = pd.DataFrame({
            "time": [str(datetime.now() - timedelta(hours=i)) for i in range(count-1, -1, -1)],
            "open": opens, "high": highs, "low": lows,
            "close": closes, "tick_volume": vols,
        })
        df = _enrich_df(df)
        # Cache last close price per symbol for SL/TP simulation
        self._price_cache[symbol] = float(df.iloc[-1]["close"])
        return df

    def get_account_info(self):
        return {"balance": self.balance, "equity": self.balance, "currency": "USD"}

    def place_order(self, symbol, direction, lot, sl, tp):
        # Don't open duplicate positions on same symbol
        already = [p for p in self.positions if p["symbol"] == symbol]
        if already:
            log_warn(f"[PAPER] Already have open position on {symbol}, skipping.")
            return None
        trade = {
            "id":        len(self.trade_log) + 1,
            "symbol":    symbol,
            "direction": direction.value,
            "lot":       lot,
            "sl":        sl,
            "tp":        tp,
            "entry":     self._price_cache.get(symbol, 0),
            "time":      datetime.now().isoformat(),
        }
        self.positions.append(trade)
        self.trade_log.append(trade)
        log_trade(f"[PAPER] {direction.value} {symbol} lot:{lot} sl:{sl:.5f} tp:{tp:.5f}")
        _save_json(CONFIG["trades_file"], self.trade_log)
        return {"retcode": "DONE", "trade": trade}

    def get_open_positions(self):
        # Simulate SL/TP hits for all cached prices before reporting
        for symbol, price in list(self._price_cache.items()):
            self._simulate_closes(symbol, price)
        return self.positions

    def get_tick(self, symbol):
        price = self._price_cache.get(symbol, 1.09500)
        spread = 0.00010
        return {"bid": round(price, 5), "ask": round(price + spread, 5)}

# ── Yahoo Finance Broker (free real-time price data, no account) ──────
class YFinanceBroker(BrokerBase):
    """
    Pulls live candles from Yahoo Finance — completely free, no account.
    Prices are real market data (15-min delayed for free tier, but
    close enough for H4/H1/M30 analysis timeframes).

    Symbol mapping (bot internal → Yahoo Finance):
      EUR_USD → EURUSD=X    XAU_USD → GC=F
      GBP_USD → GBPUSD=X    BTC_USD → BTC-USD
      USD_JPY → USDJPY=X    GBP_JPY → GBPJPY=X
    """
    SYM_MAP = {
        "EUR_USD": "EURUSD=X",  "GBP_USD": "GBPUSD=X",
        "USD_JPY": "USDJPY=X",  "GBP_JPY": "GBPJPY=X",
        "XAU_USD": "GC=F",      "BTC_USD": "BTC-USD",
        "NAS_USD": "NQ=F",      "SPX_USD": "ES=F",
    }
    # Yahoo intervals and the range needed to get enough bars
    TF_MAP = {
        "W":   ("1wk",  "2y"),
        "D":   ("1d",   "1y"),
        "H4":  ("1h",   "7d"),   # H4 resampled from H1
        "H2":  ("1h",   "7d"),   # H2 resampled from H1
        "H1":  ("1h",   "7d"),
        "M45": ("30m",  "5d"),   # M45 resampled from M30
        "M30": ("30m",  "5d"),
        "M15": ("15m",  "5d"),
    }
    # Which TFs need resampling from a coarser native interval
    RESAMPLE = {"H4": 4, "H2": 2, "M45": None}  # None = special case

    def _yahoo_fetch(self, ysym: str, interval: str,
                     period: str) -> Optional[pd.DataFrame]:
        """
        Raw fetch from Yahoo Finance chart API.
        Raises ConnectionError if network is down (not a data issue).
        Returns None if data is simply unavailable for this symbol/TF.
        """
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ysym}"
               f"?interval={interval}&range={period}"
               f"&includePrePost=false")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
            )
        }
        # ── Distinguish network failure from bad data ─────────────
        if REQUESTS_OK:
            try:
                r = req_lib.get(url, headers=headers, timeout=15)
                r.raise_for_status()
                data = r.json()
            except req_lib.exceptions.ConnectionError as e:
                raise ConnectionError(f"No internet: {e}") from e
            except req_lib.exceptions.Timeout as e:
                raise ConnectionError(f"Timeout reaching Yahoo Finance: {e}") from e
            except Exception as e:
                log_error(f"YFinance HTTP error ({ysym} {interval}): {e}")
                return None
        else:
            try:
                req_obj = Request(url, headers=headers)
                with urlopen(req_obj, timeout=15) as resp:
                    data = json.loads(resp.read().decode())
            except (URLError, HTTPError) as e:
                # URLError covers socket errors (no internet) on urllib
                reason = str(e)
                if any(x in reason.lower() for x in
                       ["errno", "timed out", "network", "refused",
                        "unreachable", "no route", "connect"]):
                    raise ConnectionError(f"No internet: {e}") from e
                log_error(f"YFinance urllib error ({ysym} {interval}): {e}")
                return None
            except Exception as e:
                log_error(f"YFinance fetch error ({ysym} {interval}): {e}")
                return None

        if not data:
            return None
        try:
            res = data["chart"]["result"]
            if not res:
                return None
            res = res[0]
            ts  = res.get("timestamp", [])
            q   = res["indicators"]["quote"][0]
            rows = []
            for i in range(len(ts)):
                o = q["open"][i];  h = q["high"][i]
                l = q["low"][i];   c = q["close"][i]
                v = (q.get("volume") or [None]*len(ts))[i]
                if None in (o, h, l, c):
                    continue
                rows.append({
                    "time":        str(datetime.fromtimestamp(int(ts[i]))),
                    "open":        float(o),
                    "high":        float(h),
                    "low":         float(l),
                    "close":       float(c),
                    "tick_volume": float(v) if v else 1.0,
                })
            return pd.DataFrame(rows) if rows else None
        except Exception as e:
            log_error(f"YFinance parse error ({ysym} {interval}): {e}")
            return None

    def _resample_to_h4(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample H1 bars into H4 bars (group every 4 rows)."""
        if df is None or len(df) < 4:
            return df
        groups = [df.iloc[i:i+4] for i in range(0, len(df)-3, 4)]
        rows = []
        for g in groups:
            rows.append({
                "time":        g.iloc[-1]["time"],
                "open":        float(g.iloc[0]["open"]),
                "high":        float(g["high"].max()),
                "low":         float(g["low"].min()),
                "close":       float(g.iloc[-1]["close"]),
                "tick_volume": float(g["tick_volume"].sum()),
            })
        return pd.DataFrame(rows)

    def _resample_to_h2(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample H1 bars into H2 bars (group every 2 rows)."""
        if df is None or len(df) < 2:
            return df
        groups = [df.iloc[i:i+2] for i in range(0, len(df)-1, 2)]
        rows = []
        for g in groups:
            rows.append({
                "time":        g.iloc[-1]["time"],
                "open":        float(g.iloc[0]["open"]),
                "high":        float(g["high"].max()),
                "low":         float(g["low"].min()),
                "close":       float(g.iloc[-1]["close"]),
                "tick_volume": float(g["tick_volume"].sum()),
            })
        return pd.DataFrame(rows)

    def _resample_to_m45(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample M30 bars into M45 bars (group 3 bars → 90min, skip 1)."""
        # Approximate M45 by taking every M30 bar with 1.5x weight
        # True M45 requires M15 data — this is close enough for bias
        if df is None or len(df) < 3:
            return df
        groups = [df.iloc[i:i+3] for i in range(0, len(df)-2, 3)]
        rows = []
        for g in groups:
            rows.append({
                "time":        g.iloc[-1]["time"],
                "open":        float(g.iloc[0]["open"]),
                "high":        float(g["high"].max()),
                "low":         float(g["low"].min()),
                "close":       float(g.iloc[-1]["close"]),
                "tick_volume": float(g["tick_volume"].sum()),
            })
        return pd.DataFrame(rows)

    def get_candles(self, symbol: str, timeframe: str,
                    count: int = 200) -> Optional[pd.DataFrame]:
        ysym = self.SYM_MAP.get(symbol)
        if not ysym:
            log_warn(f"YFinance: no symbol mapping for {symbol}")
            return None

        interval, period = self.TF_MAP.get(timeframe, ("1h", "7d"))
        raw_tf = timeframe

        # ConnectionError propagates up — caller (main loop) handles it
        df = self._yahoo_fetch(ysym, interval, period)
        if df is None:
            log_warn(f"YFinance: no data for {symbol} {timeframe} "
                     f"(symbol may be unavailable on Yahoo)")
            return None

        # Resample where needed
        if raw_tf == "H4":
            df = self._resample_to_h4(df)
        elif raw_tf == "H2":
            df = self._resample_to_h2(df)
        elif raw_tf == "M45":
            df = self._resample_to_m45(df)

        if df is None or len(df) == 0:
            return None

        df = df.tail(count).reset_index(drop=True)
        return _enrich_df(df)

    def get_account_info(self):
        return {"balance": 10000.0, "currency": "USD",
                "note": "YFinance data-only mode"}

    def place_order(self, symbol, direction, lot, sl, tp):
        # No order execution in pure YFinance mode
        log_warn(f"[YFINANCE] place_order called but no execution "
                 f"broker configured. Use YFINANCE_MT5 mode.")
        return None

    def get_open_positions(self):
        return []

    def get_tick(self, symbol: str) -> Optional[Dict]:
        """Get latest price using M15 candles."""
        df = self.get_candles(symbol, "M15", 2)
        if df is not None and len(df) > 0:
            p = float(df.iloc[-1]["close"])
            # Estimate spread per instrument type
            if "JPY" in symbol:
                spread = 0.010
            elif "XAU" in symbol:
                spread = 0.30
            elif "BTC" in symbol:
                spread = 5.0
            else:
                spread = 0.00010
            return {"bid": round(p, 5), "ask": round(p + spread, 5)}
        return None


# ── MT5 Signal Bridge (hybrid: YFinance data + MT5 execution) ─────────
class YFinanceMT5Broker(YFinanceBroker):
    """
    RECOMMENDED MODE — YFINANCE_MT5

    Price data  : Yahoo Finance (free, real market prices)
    Execution   : Writes signals to signal_queue.json
                  Your MT5 EA (WyckoffSNR_EA.mq5) on PC reads the file
                  and places real trades in MT5.

    Setup:
      1. Copy signal_queue.json path to a shared location
         (Google Drive folder, Dropbox, or LAN share accessible to your PC)
      2. Set "mt5_signal_file" in CONFIG to that shared path
      3. In MT5 EA input, set SignalFile to the same path
      4. Attach EA to any chart in MT5 (e.g. EURUSD M1)
    """

    def __init__(self):
        super().__init__()
        self._open_signals: List[Dict] = _load_json(
            CONFIG["mt5_signal_file"], []
        )

    def get_candles(self, symbol: str, timeframe: str,
                    count: int = 200) -> Optional[pd.DataFrame]:
        """
        Use Binance for crypto (real volume), Yahoo Finance for everything else.
        """
        if symbol in BINANCE_SYM_MAP:
            df = binance_get_candles(symbol, timeframe, count)
            if df is not None:
                return df
            log_warn(f"Binance candles failed for {symbol} {timeframe}, "
                     f"falling back to Yahoo Finance")
        return super().get_candles(symbol, timeframe, count)

    def place_order(self, symbol: str, direction, lot: float,
                    sl: float, tp: float):
        """
        Write signal to the queue file.
        MT5 EA polls this file and executes on the next tick.
        """
        # Map bot symbol → MT5 symbol format
        MT5_SYM = {
            "EUR_USD": "EURUSD", "GBP_USD": "GBPUSD",
            "USD_JPY": "USDJPY", "GBP_JPY": "GBPJPY",
            "XAU_USD": "XAUUSD", "BTC_USD": "BTCUSD",
        }
        mt5_sym = MT5_SYM.get(symbol, symbol.replace("_", ""))

        signal = {
            "id":        int(time.time()),
            "symbol":    mt5_sym,
            "action":    direction.value,   # "BUY" or "SELL"
            "lot":       lot,
            "sl":        round(sl, 5),
            "tp":        round(tp, 5),
            "magic":     CONFIG["mt5_magic"],
            "comment":   "WyckoffSNR",
            "status":    "PENDING",         # EA changes to EXECUTED/REJECTED
            "timestamp": datetime.now().isoformat(),
        }

        self._open_signals.append(signal)
        # Keep last 50 signals in the file
        self._open_signals = self._open_signals[-50:]
        _save_json(CONFIG["mt5_signal_file"], self._open_signals)

        log_trade(f"[MT5 SIGNAL] {direction.value} {mt5_sym} "
                  f"lot={lot} sl={sl:.5f} tp={tp:.5f} "
                  f"→ written to {CONFIG['mt5_signal_file']}")
        return {"retcode": "SIGNAL_QUEUED", "signal": signal}

    def get_open_positions(self) -> List[Dict]:
        """
        Read executed signals back from queue file.
        EA marks them EXECUTED — bot counts those as open positions.
        """
        try:
            data = _load_json(CONFIG["mt5_signal_file"], [])
            self._open_signals = data
            return [s for s in data if s.get("status") == "EXECUTED"]
        except Exception:
            return []



# ════════════════════════════════════════════════════════════════════
#  BINANCE DATA LAYER  — real volume, order book, footprint
#  Free, no API key needed for public market data endpoints.
#  Used for BTC_USD and any crypto symbol.
#  Forex/Gold still uses Yahoo Finance (no public volume exists).
# ════════════════════════════════════════════════════════════════════

# Binance symbol map: bot internal → Binance trading pair
BINANCE_SYM_MAP = {
    "BTC_USD": "BTCUSDT",
    "ETH_USD": "ETHUSDT",
    "BNB_USD": "BNBUSDT",
    "SOL_USD": "SOLUSDT",
    "XRP_USD": "XRPUSDT",
}

BINANCE_TF_MAP = {
    "W": "1w", "D": "1d",
    "H4": "4h", "H2": "2h",
    "H1": "1h", "M45": "45m",
    "M30": "30m", "M15": "15m",
    "M5": "5m",
}

BINANCE_BASE = "https://api.binance.com/api/v3"

def binance_get_candles(symbol: str, timeframe: str,
                         count: int = 200) -> Optional[pd.DataFrame]:
    """
    Fetch real OHLCV candles from Binance.
    Volume here is REAL traded base-asset volume — not tick count.
    No API key needed for public klines endpoint.
    """
    bsym = BINANCE_SYM_MAP.get(symbol)
    if not bsym:
        return None
    btf = BINANCE_TF_MAP.get(timeframe, "1h")
    url = f"{BINANCE_BASE}/klines?symbol={bsym}&interval={btf}&limit={count}"
    try:
        data = http_get(url, timeout=10)
        if not data or not isinstance(data, list):
            return None
        rows = []
        for k in data:
            # Binance kline: [openTime,o,h,l,c,vol,closeTime,quoteVol,trades,...]
            rows.append({
                "time":        str(datetime.fromtimestamp(int(k[0]) / 1000)),
                "open":        float(k[1]),
                "high":        float(k[2]),
                "low":         float(k[3]),
                "close":       float(k[4]),
                "tick_volume": float(k[5]),   # real base-asset volume
                "quote_volume":float(k[7]),   # real quote volume (USD)
                "num_trades":  int(k[8]),
            })
        df = pd.DataFrame(rows)
        return _enrich_df(df)
    except ConnectionError:
        raise
    except Exception as e:
        log_error(f"Binance candles error ({symbol} {timeframe}): {e}")
        return None


def binance_get_orderbook(symbol: str,
                           depth: int = 20) -> Optional[Dict]:
    """
    Fetch real order book (bid/ask depth) from Binance.
    Returns dict with bids, asks, spread, imbalance ratio.
    No API key needed.
    """
    bsym = BINANCE_SYM_MAP.get(symbol)
    if not bsym:
        return None
    url = f"{BINANCE_BASE}/depth?symbol={bsym}&limit={depth}"
    try:
        data = http_get(url, timeout=8)
        if not data or "bids" not in data:
            return None

        bids = [(float(p), float(q)) for p, q in data["bids"]]
        asks = [(float(p), float(q)) for p, q in data["asks"]]

        if not bids or not asks:
            return None

        best_bid   = bids[0][0]
        best_ask   = asks[0][0]
        spread     = round(best_ask - best_bid, 5)
        bid_vol    = sum(q for _, q in bids)
        ask_vol    = sum(q for _, q in asks)
        total_vol  = bid_vol + ask_vol
        # Imbalance: >0.6 = more buyers, <0.4 = more sellers
        imbalance  = round(bid_vol / total_vol, 3) if total_vol > 0 else 0.5

        # Wall detection: single level with >15% of total volume
        bid_walls  = [(p, q) for p, q in bids if q / total_vol > 0.15]
        ask_walls  = [(p, q) for p, q in asks if q / total_vol > 0.15]

        bias = "NEUTRAL"
        if imbalance > 0.62:
            bias = "BUY_PRESSURE"
        elif imbalance < 0.38:
            bias = "SELL_PRESSURE"

        return {
            "best_bid":   best_bid,
            "best_ask":   best_ask,
            "spread":     spread,
            "bid_volume": round(bid_vol, 2),
            "ask_volume": round(ask_vol, 2),
            "imbalance":  imbalance,
            "bias":       bias,
            "bid_walls":  bid_walls[:3],
            "ask_walls":  ask_walls[:3],
            "depth":      depth,
        }
    except ConnectionError:
        raise
    except Exception as e:
        log_error(f"Binance order book error ({symbol}): {e}")
        return None


def binance_get_footprint(symbol: str,
                           timeframe: str = "M15",
                           candles: int = 10) -> Optional[List[Dict]]:
    """
    Approximate footprint chart using Binance aggTrades endpoint.
    True footprint requires tick-level data — this bins recent
    aggregate trades by price level to show buy/sell volume distribution.
    Works best on M15 and lower timeframes.
    No API key needed.
    """
    bsym = BINANCE_SYM_MAP.get(symbol)
    if not bsym:
        return None
    # Fetch recent aggregate trades (last 500)
    url = f"{BINANCE_BASE}/aggTrades?symbol={bsym}&limit=500"
    try:
        trades = http_get(url, timeout=10)
        if not trades or not isinstance(trades, list):
            return None

        # Group trades into price bins (0.05% width)
        from collections import defaultdict
        buy_vol:  Dict[float, float] = defaultdict(float)
        sell_vol: Dict[float, float] = defaultdict(float)
        prices = []

        for t in trades:
            price    = float(t["p"])
            qty      = float(t["q"])
            is_sell  = bool(t["m"])   # maker = sell aggressor
            bin_size = price * 0.0005  # 0.05% bin width
            price_bin = round(price / bin_size) * bin_size
            prices.append(price)
            if is_sell:
                sell_vol[price_bin] += qty
            else:
                buy_vol[price_bin]  += qty

        if not prices:
            return None

        # Build footprint levels sorted by price descending
        all_bins = sorted(
            set(list(buy_vol.keys()) + list(sell_vol.keys())),
            reverse=True
        )
        footprint = []
        for bin_price in all_bins[:30]:  # top 30 price levels
            bv = buy_vol.get(bin_price, 0)
            sv = sell_vol.get(bin_price, 0)
            total = bv + sv
            delta = bv - sv
            footprint.append({
                "price":      round(bin_price, 2),
                "buy_vol":    round(bv, 4),
                "sell_vol":   round(sv, 4),
                "delta":      round(delta, 4),
                "total_vol":  round(total, 4),
                "poc":        False,  # marked below
            })

        # Mark POC (highest total volume bin)
        if footprint:
            poc_idx = max(range(len(footprint)),
                          key=lambda i: footprint[i]["total_vol"])
            footprint[poc_idx]["poc"] = True

        return footprint

    except ConnectionError:
        raise
    except Exception as e:
        log_error(f"Binance footprint error ({symbol}): {e}")
        return None


def enrich_with_binance(symbol: str, analysis: Dict) -> Dict:
    """
    For crypto symbols: fetch real order book + footprint from Binance
    and add them to the analysis report. For forex/gold: skip silently.
    Called inside run_chart_analysis when symbol is in BINANCE_SYM_MAP.
    """
    if symbol not in BINANCE_SYM_MAP:
        return analysis   # forex/gold — no Binance data available

    log_info(f"  [{symbol}] Fetching Binance order book + footprint...")

    # Order book
    ob = binance_get_orderbook(symbol, depth=20)
    if ob:
        analysis["order_book"] = ob
        log_info(f"  [{symbol}] Order book: imbalance={ob['imbalance']} "
                 f"bias={ob['bias']} spread={ob['spread']}")
        if ob["bid_walls"]:
            log_info(f"  [{symbol}] BID WALLS: "
                     f"{[round(p,2) for p,_ in ob['bid_walls']]}")
        if ob["ask_walls"]:
            log_info(f"  [{symbol}] ASK WALLS: "
                     f"{[round(p,2) for p,_ in ob['ask_walls']]}")
    else:
        analysis["order_book"] = None

    # Footprint
    fp = binance_get_footprint(symbol)
    if fp:
        analysis["footprint"] = fp[:10]  # top 10 levels for log
        poc = next((l for l in fp if l["poc"]), None)
        if poc:
            log_info(f"  [{symbol}] Footprint POC: "
                     f"${poc['price']} buy={poc['buy_vol']} "
                     f"sell={poc['sell_vol']} delta={poc['delta']}")
    else:
        analysis["footprint"] = None

    return analysis


def binance_order_book_bias(symbol: str) -> str:
    """
    Quick helper: returns BUY_PRESSURE / SELL_PRESSURE / NEUTRAL
    from the order book. Used inside score_bias_on_tf for crypto.
    """
    ob = binance_get_orderbook(symbol, depth=10)
    if ob:
        return ob.get("bias", "NEUTRAL")
    return "NEUTRAL"

# ════════════════════════════════════════════════════════════════════
#  BROKER FACTORY
# ════════════════════════════════════════════════════════════════════
def create_broker() -> BrokerBase:
    mode = CONFIG.get("broker_mode", "PAPER").upper()
    if mode == "YFINANCE_MT5":
        log_info("Broker mode: Yahoo Finance (prices) + MT5 EA (execution)")
        return YFinanceMT5Broker()
    elif mode == "YFINANCE":
        log_info("Broker mode: Yahoo Finance (prices, signal-only)")
        return YFinanceBroker()
    elif mode == "METAAPI":
        log_info("Broker mode: MetaAPI (MT5 REST bridge)")
        return MetaAPIBroker(
            CONFIG["metaapi_token"],
            CONFIG["metaapi_account"],
            CONFIG["metaapi_url"]
        )
    elif mode == "OANDA":
        log_info("Broker mode: OANDA REST API")
        return OANDABroker(
            CONFIG["oanda_token"],
            CONFIG["oanda_account"],
            CONFIG["oanda_url"]
        )
    else:
        log_info("Broker mode: PAPER (no real trades)")
        return PaperBroker()

# ════════════════════════════════════════════════════════════════════
#  DATA HELPERS
# ════════════════════════════════════════════════════════════════════
def _enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns used throughout the bot."""
    if df is None or len(df) == 0:
        return df
    df = df.copy()
    df["body_high"] = df[["open", "close"]].max(axis=1)
    df["body_low"]  = df[["open", "close"]].min(axis=1)
    df["bullish"]   = df["close"] > df["open"]
    df["range"]     = df["high"] - df["low"]
    df["body_size"] = (df["body_high"] - df["body_low"]).abs()
    return df.reset_index(drop=True)

def _save_json(filename, data):
    """Save JSON to script directory (Pydroid-safe path)."""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filepath   = os.path.join(script_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        log_error(f"Cannot save {filename}: {e}")

def _load_json(filename, default=None):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filepath   = os.path.join(script_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default if default is not None else []

# ════════════════════════════════════════════════════════════════════
#  INDICATORS  (pure Python / numpy — no TA-Lib)
# ════════════════════════════════════════════════════════════════════
def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Average True Range — pure numpy, no TA-Lib."""
    if len(df) < period + 1:
        return float(df["range"].mean()) if len(df) > 0 else 0.001
    h = df["high"].values
    l = df["low"].values
    c = df["close"].values
    tr = np.maximum(
        h[1:] - l[1:],
        np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1]))
    )
    atr_vals = np.convolve(tr, np.ones(period) / period, mode="valid")
    return float(atr_vals[-1]) if len(atr_vals) > 0 else 0.001

def calc_ema(series: np.ndarray, period: int) -> np.ndarray:
    """Exponential Moving Average — pure numpy."""
    alpha  = 2.0 / (period + 1)
    result = np.zeros_like(series, dtype=float)
    result[0] = series[0]
    for i in range(1, len(series)):
        result[i] = alpha * series[i] + (1 - alpha) * result[i-1]
    return result

def calc_vwap(df: pd.DataFrame) -> float:
    """Volume Weighted Average Price."""
    tp  = (df["high"] + df["low"] + df["close"]) / 3.0
    num = float((tp * df["tick_volume"]).sum())
    den = float(df["tick_volume"].sum())
    return num / den if den > 0 else float(df["close"].iloc[-1])

def calc_vpoc(df: pd.DataFrame, bins: int = 50) -> float:
    """Volume Point of Control."""
    lo = float(df["low"].min())
    hi = float(df["high"].max())
    if hi <= lo:
        return float(df["close"].iloc[-1])
    edges = np.linspace(lo, hi, bins + 1)
    vols  = []
    for i in range(bins):
        mask = (df["close"] >= edges[i]) & (df["close"] < edges[i+1])
        vols.append(float(df.loc[mask, "tick_volume"].sum()))
    best = int(np.argmax(vols))
    return float((edges[best] + edges[best+1]) / 2.0)

def local_extrema(arr: np.ndarray, order: int = 5, mode: str = "min") -> np.ndarray:
    """Find local minima or maxima — uses scipy if available, else pure numpy."""
    if SCIPY_OK:
        if mode == "min":
            return argrelextrema(arr, np.less_equal, order=order)[0]
        else:
            return argrelextrema(arr, np.greater_equal, order=order)[0]
    # Pure numpy fallback
    result = []
    for i in range(order, len(arr) - order):
        window = arr[i - order: i + order + 1]
        if mode == "min" and arr[i] == window.min():
            result.append(i)
        elif mode == "max" and arr[i] == window.max():
            result.append(i)
    return np.array(result, dtype=int)

# ════════════════════════════════════════════════════════════════════
#  MALAYSIAN SNR DETECTION (including pages 47-57 patterns)
# ════════════════════════════════════════════════════════════════════

def detect_basic_snr(df: pd.DataFrame) -> List[SNRLevel]:
    """Detect A-shape (resistance) and V-shape (support) SNR levels."""
    levels = []
    for i in range(1, len(df) - 1):
        c1 = df.iloc[i - 1]
        c2 = df.iloc[i]
        # Resistance: bullish candle → bearish candle (A-shape)
        if bool(c1["bullish"]) and not bool(c2["bullish"]):
            levels.append(SNRLevel(
                price=float(c1["close"]),
                stype="RESISTANCE", shape="A_SHAPE", idx=i
            ))
        # Support: bearish candle → bullish candle (V-shape)
        if not bool(c1["bullish"]) and bool(c2["bullish"]):
            levels.append(SNRLevel(
                price=float(c1["close"]),
                stype="SUPPORT", shape="V_SHAPE", idx=i
            ))
    return levels

def mark_freshness(levels: List[SNRLevel], df: pd.DataFrame) -> List[SNRLevel]:
    """Mark each SNR as fresh or broken. Count MISS candles."""
    for snr in levels:
        for j in range(snr.idx + 1, len(df)):
            row = df.iloc[j]
            if snr.stype == "RESISTANCE":
                if float(row["body_high"]) > snr.price:
                    snr.fresh = False
                    break
                if float(row["high"]) > snr.price:
                    snr.miss_count += 1
            else:
                if float(row["body_low"]) < snr.price:
                    snr.fresh = False
                    break
                if float(row["low"]) < snr.price:
                    snr.miss_count += 1
    return [s for s in levels if s.fresh]

def has_liquidity_sweep(df: pd.DataFrame, price: float,
                         stype: str, lookback: int = 8) -> bool:
    """Detect if price grabbed liquidity beyond SNR then returned."""
    tail = df.tail(lookback)
    for _, row in tail.iterrows():
        if stype == "SUPPORT":
            if float(row["low"]) < price and float(row["close"]) > price:
                return True
        else:
            if float(row["high"]) > price and float(row["close"]) < price:
                return True
    return False

def detect_engulfing(df: pd.DataFrame, idx: int = -1) -> Optional[str]:
    """Detect all engulfing types including gap engulfing."""
    n = len(df)
    if abs(idx) > n - 1 or n < 2:
        return None
    actual_idx = idx if idx >= 0 else n + idx
    if actual_idx < 1:
        return None
    c1 = df.iloc[actual_idx - 1]
    c2 = df.iloc[actual_idx]
    c1h = float(c1["body_high"]); c1l = float(c1["body_low"])
    c2h = float(c2["body_high"]); c2l = float(c2["body_low"])
    # Full body engulfing
    if c2h > c1h and c2l < c1l:
        return "BULLISH_ENGULFING" if bool(c2["bullish"]) else "BEARISH_ENGULFING"
    # Gap-type engulfing (hidden engulfing)
    if bool(c2["bullish"]) and float(c2["open"]) < c1l and float(c2["close"]) > c1h:
        return "BULLISH_GAP_ENGULFING"
    if not bool(c2["bullish"]) and float(c2["open"]) > c1h and float(c2["close"]) < c1l:
        return "BEARISH_GAP_ENGULFING"
    return None

def detect_flipped_engulfing(df: pd.DataFrame) -> Optional[Dict]:
    """Failed/Flipped engulfing — broken engulfing zone flips role."""
    n = len(df)
    for i in range(max(2, n - 5), n - 1):
        eng = detect_engulfing(df, i)
        if eng is None:
            continue
        c_eng  = df.iloc[i]
        c_next = df.iloc[i + 1] if i + 1 < n else None
        if c_next is None:
            continue
        if "BULLISH" in eng and float(c_next["body_low"]) < float(c_eng["body_low"]):
            return {
                "type": "FLIPPED_BULLISH",
                "zone_high": float(c_eng["body_high"]),
                "zone_low":  float(c_eng["body_low"]),
                "direction": "SELL"
            }
        if "BEARISH" in eng and float(c_next["body_high"]) > float(c_eng["body_high"]):
            return {
                "type": "FLIPPED_BEARISH",
                "zone_high": float(c_eng["body_high"]),
                "zone_low":  float(c_eng["body_low"]),
                "direction": "BUY"
            }
    return None

def detect_gap_snr(df: pd.DataFrame) -> List[SNRLevel]:
    """GAP / Hidden SNR — open-close gap between consecutive candles."""
    gaps = []
    for i in range(1, len(df)):
        c1 = df.iloc[i - 1]
        c2 = df.iloc[i]
        gap_size = abs(float(c2["open"]) - float(c1["close"]))
        if gap_size < float(c1["range"]) * 0.3:
            continue
        if float(c2["open"]) > float(c1["close"]):
            gaps.append(SNRLevel(
                price=float(c1["close"]), stype="SUPPORT",
                shape="GAP_UP", pattern=PatternType.GAP_HIDDEN, idx=i
            ))
        elif float(c2["open"]) < float(c1["close"]):
            gaps.append(SNRLevel(
                price=float(c1["close"]), stype="RESISTANCE",
                shape="GAP_DOWN", pattern=PatternType.GAP_HIDDEN, idx=i
            ))
    return gaps

def find_congestion(all_levels: List[SNRLevel],
                    tolerance: float = 0.002) -> List[Dict]:
    """SNR Congestion Zone — 3+ SNR levels at same price area."""
    zones = []
    used  = set()
    for i, s1 in enumerate(all_levels):
        if i in used:
            continue
        cluster = [s1]
        for j, s2 in enumerate(all_levels):
            if i == j or j in used:
                continue
            if s1.price > 0 and abs(s1.price - s2.price) / s1.price < tolerance:
                cluster.append(s2)
                used.add(j)
        if len(cluster) >= 3:
            avg_price = sum(s.price for s in cluster) / len(cluster)
            zones.append({
                "price":   avg_price,
                "count":   len(cluster),
                "levels":  cluster,
                "pattern": PatternType.CONGESTION
            })
    return zones

# ── TRENDLINES (page 47-52) ───────────────────────────────────────────
def detect_trendlines(df: pd.DataFrame, order: int = 5) -> List[Trendline]:
    """
    Detect REGULAR, BREAKOUT and DIVERGENCE trendlines.
    Connects minimum 2 SNR V-shapes (support) or A-shapes (resistance).
    Page 47: Regular, Breakout, Divergence types.
    """
    trendlines = []
    n      = len(df)
    if n < order * 3:
        return trendlines

    bl  = df["body_low"].values.astype(float)
    bh  = df["body_high"].values.astype(float)
    lo_idx = local_extrema(bl, order=order, mode="min")
    hi_idx = local_extrema(bh, order=order, mode="max")

    # Support trendlines (V-shapes)
    for i in range(len(lo_idx) - 1):
        p1_i = int(lo_idx[i]); p2_i = int(lo_idx[i + 1])
        if p2_i - p1_i < 3:
            continue
        p1_v = float(bl[p1_i]); p2_v = float(bl[p2_i])
        slope = (p2_v - p1_v) / (p2_i - p1_i)
        # Validate: no body close below the line
        broken = False
        for k in range(p1_i + 1, min(p2_i, n)):
            proj = p1_v + slope * (k - p1_i)
            if float(bl[k]) < proj - abs(proj) * 0.003:
                broken = True
                break
        if broken:
            continue
        p3_i   = min(p2_i + (p2_i - p1_i), n - 1)
        p3_v   = p2_v + slope * (p3_i - p2_i)
        direct = "UP" if slope >= 0 else "DOWN"
        ttype  = "REGULAR" if direct == "UP" else "BREAKOUT"
        trendlines.append(Trendline(
            p1=p1_v, p2=p2_v, p3=p3_v,
            i1=p1_i, i2=p2_i, i3=p3_i,
            ttype=ttype, direction=direct
        ))

    # Resistance trendlines (A-shapes)
    for i in range(len(hi_idx) - 1):
        p1_i = int(hi_idx[i]); p2_i = int(hi_idx[i + 1])
        if p2_i - p1_i < 3:
            continue
        p1_v = float(bh[p1_i]); p2_v = float(bh[p2_i])
        slope = (p2_v - p1_v) / (p2_i - p1_i)
        broken = False
        for k in range(p1_i + 1, min(p2_i, n)):
            proj = p1_v + slope * (k - p1_i)
            if float(bh[k]) > proj + abs(proj) * 0.003:
                broken = True
                break
        if broken:
            continue
        p3_i   = min(p2_i + (p2_i - p1_i), n - 1)
        p3_v   = p2_v + slope * (p3_i - p2_i)
        direct = "DOWN" if slope <= 0 else "UP"
        ttype  = "REGULAR" if direct == "DOWN" else "BREAKOUT"
        trendlines.append(Trendline(
            p1=p1_v, p2=p2_v, p3=p3_v,
            i1=p1_i, i2=p2_i, i3=p3_i,
            ttype=ttype, direction=direct
        ))
    return trendlines

def detect_tl_divergence(trendlines: List[Trendline],
                           df: pd.DataFrame) -> List[Dict]:
    """
    TRENDLINE DIVERGENCE (page 47, 49 right):
    Price moves one way, but trendline moves opposite.
    """
    divs = []
    closes = df["close"].values.astype(float)
    for tl in trendlines:
        if tl.ttype != "REGULAR":
            continue
        if tl.i2 >= len(closes):
            continue
        cur = closes[-1]; prev = closes[tl.i2]
        if tl.direction == "UP":
            if cur > prev and tl.p3 < tl.p2:
                divs.append({
                    "type": "BEARISH_DIVERGENCE", "tl": tl,
                    "signal": "SELL", "poi": tl.p3
                })
        elif tl.direction == "DOWN":
            if cur < prev and tl.p3 > tl.p2:
                divs.append({
                    "type": "BULLISH_DIVERGENCE", "tl": tl,
                    "signal": "BUY", "poi": tl.p3
                })
    return divs

def find_x_factor(trendlines: List[Trendline], snr_levels: List[SNRLevel],
                   df: pd.DataFrame, tol: float = 0.0015) -> List[Dict]:
    """
    X-FACTOR CONFLUENCE (page 55):
    TL point #3 coincides with an SNR level = strongest entry.
    """
    xfs = []
    cur = float(df.iloc[-1]["close"])
    for tl in trendlines:
        for snr in snr_levels:
            if not snr.fresh:
                continue
            if snr.price <= 0:
                continue
            dist = abs(tl.p3 - snr.price) / snr.price
            if dist > tol:
                continue
            direction = "BUY" if snr.stype == "SUPPORT" else "SELL"
            entry_ok  = snr.price > 0 and abs(cur - snr.price) / snr.price < 0.005
            xfs.append({
                "type":      PatternType.X_FACTOR,
                "tl":        tl,
                "snr":       snr,
                "direction": direction,
                "entry_ok":  entry_ok,
                "price":     snr.price,
                "tl_type":   tl.ttype,
            })
    return xfs

def detect_qml_hns(df: pd.DataFrame) -> List[Dict]:
    """
    QML / HNS DETECTION (page 53):
    Head and Shoulders (bearish) and Inverse HNS (bullish).
    Entry at right shoulder. Peak must show engulfing.
    """
    results = []
    n = len(df)
    if n < 20:
        return results
    bh  = df["body_high"].values.astype(float)
    bl  = df["body_low"].values.astype(float)
    hi_idx = local_extrema(bh, order=4, mode="max")
    lo_idx = local_extrema(bl, order=4, mode="min")

    # Bearish HNS
    for i in range(len(hi_idx) - 2):
        ls_i = int(hi_idx[i]); h_i = int(hi_idx[i+1]); rs_i = int(hi_idx[i+2])
        ls_v = float(bh[ls_i]); h_v = float(bh[h_i]); rs_v = float(bh[rs_i])
        if h_v <= max(ls_v, rs_v):
            continue
        if ls_v > 0 and abs(ls_v - rs_v) / ls_v > 0.03:
            continue
        neckline  = float(np.min(bl[ls_i:rs_i+1]))
        qml_price = rs_v
        eng = detect_engulfing(df, rs_i)
        results.append({
            "type": PatternType.QML_HNS, "direction": "SELL",
            "ls": ls_v, "head": h_v, "rs": rs_v,
            "neckline": neckline, "qml_price": qml_price,
            "engulf": eng, "rs_idx": rs_i
        })

    # Bullish Inverse HNS
    for i in range(len(lo_idx) - 2):
        ls_i = int(lo_idx[i]); h_i = int(lo_idx[i+1]); rs_i = int(lo_idx[i+2])
        ls_v = float(bl[ls_i]); h_v = float(bl[h_i]); rs_v = float(bl[rs_i])
        if h_v >= min(ls_v, rs_v):
            continue
        if ls_v > 0 and abs(ls_v - rs_v) / ls_v > 0.03:
            continue
        neckline  = float(np.max(bh[ls_i:rs_i+1]))
        qml_price = rs_v
        eng = detect_engulfing(df, rs_i)
        results.append({
            "type": PatternType.QML_HNS, "direction": "BUY",
            "ls": ls_v, "head": h_v, "rs": rs_v,
            "neckline": neckline, "qml_price": qml_price,
            "engulf": eng, "rs_idx": rs_i
        })
    return results

def detect_qmx(qml_list: List[Dict], trendlines: List[Trendline],
                tol: float = 0.003) -> List[Dict]:
    """
    QMX (page 56) = QML/HNS + Trendline crossing at pt#3.
    Highest priority pattern. Score bonus +3.
    """
    qmx_list = []
    for qml in qml_list:
        for tl in trendlines:
            qml_p = qml.get("qml_price", 0)
            if qml_p <= 0:
                continue
            dist = abs(tl.p3 - qml_p) / qml_p
            if dist > tol:
                continue
            dir_match = (
                (qml["direction"] == "SELL" and tl.direction == "DOWN") or
                (qml["direction"] == "BUY"  and tl.direction == "UP")
            )
            if not dir_match:
                continue
            qmx_list.append({
                "type":        PatternType.QMX,
                "direction":   qml["direction"],
                "price":       qml_p,
                "qml":         qml,
                "tl":          tl,
                "engulf":      qml.get("engulf"),
                "score_bonus": 3,
            })
    return qmx_list

# ════════════════════════════════════════════════════════════════════
#  WYCKOFF PHASE DETECTION
# ════════════════════════════════════════════════════════════════════
def detect_wyckoff(df: pd.DataFrame) -> Tuple[str, int, List[str]]:
    """Detect Wyckoff accumulation/distribution phase and score it."""
    reasons: List[str] = []
    score = 0
    phase = "UNKNOWN"
    if len(df) < 50:
        return phase, score, reasons

    recent = df.tail(60).copy()
    av_vol = float(recent["tick_volume"].mean())
    rng    = float(recent["high"].max() - recent["low"].min())
    mid    = float((recent["high"].max() + recent["low"].min()) / 2.0)
    cur    = float(df.iloc[-1]["close"])

    first_half_vol = float(recent.head(30)["tick_volume"].mean())
    sec_half_vol   = float(recent.tail(30)["tick_volume"].mean())
    vol_decreasing = sec_half_vol < first_half_vol * 0.85

    # Selling Climax detection
    for i in range(len(recent) - 10, len(recent)):
        row = recent.iloc[i]
        cond1 = float(row["tick_volume"]) > av_vol * 2.5
        cond2 = not bool(row["bullish"])
        cond3 = rng > 0 and (float(row["open"]) - float(row["close"])) > rng * 0.04
        if cond1 and cond2 and cond3:
            score += 2; phase = "PHASE_A_ACCUM"
            reasons.append("+2 Selling Climax detected")
            break

    # Volume decreasing (Phase B accumulation)
    if vol_decreasing:
        score += 1
        reasons.append("+1 Volume decreasing (Phase B)")
        if phase == "UNKNOWN":
            phase = "PHASE_B"

    # Spring detection
    rec_lo = float(df.tail(20)["low"].min())
    prv_lo = float(df.head(40)["low"].min())
    if rec_lo < prv_lo and cur > prv_lo:
        score += 2; phase = "PHASE_C_SPRING"
        reasons.append("+2 Spring detected (Phase C)")

    # Buying Climax detection (distribution)
    for i in range(len(recent) - 10, len(recent)):
        row = recent.iloc[i]
        cond1 = float(row["tick_volume"]) > av_vol * 2.5
        cond2 = bool(row["bullish"])
        cond3 = rng > 0 and (float(row["close"]) - float(row["open"])) > rng * 0.04
        if cond1 and cond2 and cond3:
            score += 2; phase = "PHASE_A_DIST"
            reasons.append("+2 Buying Climax detected (Distribution)")
            break

    if cur > mid:
        score += 1
        reasons.append("+1 Price above structure midpoint")

    return phase, score, reasons

# ════════════════════════════════════════════════════════════════════
#  RISK MANAGEMENT
# ════════════════════════════════════════════════════════════════════
def calc_lot_size(broker: BrokerBase, sl_price: float,
                   entry_price: float) -> float:
    """Calculate position size based on account risk %."""
    try:
        info    = broker.get_account_info()
        balance = float(info.get("balance", 1000)) if info else 1000.0
        risk    = balance * CONFIG["risk_pct"] / 100.0
        pips    = abs(entry_price - sl_price)
        if pips <= 0:
            return 0.01
        # Conservative fixed pip value assumption (adjust per instrument)
        pip_val  = 10.0    # USD per pip per 1 lot (forex standard)
        lot      = risk / (pips * 10000 * pip_val)
        lot      = max(0.01, min(10.0, round(lot, 2)))
        return lot
    except Exception:
        return 0.01

# ════════════════════════════════════════════════════════════════════
#  CHART ANALYSIS ENGINE  — real-time pattern recognition
# ════════════════════════════════════════════════════════════════════

def analyse_candle_patterns(df: pd.DataFrame) -> List[str]:
    """
    Identify the last 3 candles for classic candle patterns.
    Returns list of pattern names found.
    """
    found = []
    n = len(df)
    if n < 3:
        return found
    c1 = df.iloc[-3]; c2 = df.iloc[-2]; c3 = df.iloc[-1]

    o1,h1,l1,cl1 = float(c1["open"]),float(c1["high"]),float(c1["low"]),float(c1["close"])
    o2,h2,l2,cl2 = float(c2["open"]),float(c2["high"]),float(c2["low"]),float(c2["close"])
    o3,h3,l3,cl3 = float(c3["open"]),float(c3["high"]),float(c3["low"]),float(c3["close"])
    rng3 = h3 - l3
    body3 = abs(cl3 - o3)
    upper_wick3 = h3 - max(o3, cl3)
    lower_wick3 = min(o3, cl3) - l3

    # ── Single candle patterns ────────────────────────────────────
    if rng3 > 0:
        # Doji (body < 10% of range)
        if body3 / rng3 < 0.1:
            found.append("DOJI")
        # Hammer (bullish): small body at top, long lower wick >= 2x body
        if cl3 > o3 and lower_wick3 >= body3 * 2 and upper_wick3 < body3:
            found.append("HAMMER_BULLISH")
        # Shooting Star (bearish): small body at bottom, long upper wick
        if cl3 < o3 and upper_wick3 >= body3 * 2 and lower_wick3 < body3:
            found.append("SHOOTING_STAR_BEARISH")
        # Marubozu Bullish: almost no wicks, strong close
        if cl3 > o3 and body3 / rng3 > 0.9:
            found.append("MARUBOZU_BULLISH")
        # Marubozu Bearish
        if cl3 < o3 and body3 / rng3 > 0.9:
            found.append("MARUBOZU_BEARISH")
        # Pin Bar Bullish: lower wick >= 60% of range, closes upper half
        if lower_wick3 / rng3 >= 0.6 and cl3 > (l3 + rng3 * 0.6):
            found.append("PIN_BAR_BULLISH")
        # Pin Bar Bearish: upper wick >= 60% of range, closes lower half
        if upper_wick3 / rng3 >= 0.6 and cl3 < (h3 - rng3 * 0.6):
            found.append("PIN_BAR_BEARISH")

    # ── Two candle patterns ───────────────────────────────────────
    body2 = abs(cl2 - o2)
    # Bullish Engulfing
    if cl2 < o2 and cl3 > o3 and o3 <= cl2 and cl3 >= o2:
        found.append("ENGULFING_BULLISH")
    # Bearish Engulfing
    if cl2 > o2 and cl3 < o3 and o3 >= cl2 and cl3 <= o2:
        found.append("ENGULFING_BEARISH")
    # Tweezer Bottom (bullish): two candles with same low
    if abs(l2 - l3) / max(l2, 0.00001) < 0.001:
        found.append("TWEEZER_BOTTOM_BULLISH")
    # Tweezer Top (bearish): two candles with same high
    if abs(h2 - h3) / max(h2, 0.00001) < 0.001:
        found.append("TWEEZER_TOP_BEARISH")

    # ── Three candle patterns ─────────────────────────────────────
    body1 = abs(cl1 - o1)
    # Morning Star (bullish reversal)
    if (cl1 < o1 and body1 > 0 and              # C1: bearish
        body2 / max(h2-l2,0.00001) < 0.3 and    # C2: small body (star)
        cl3 > o3 and                             # C3: bullish
        cl3 > (o1 + cl1) / 2):                  # C3 closes above C1 midpoint
        found.append("MORNING_STAR_BULLISH")
    # Evening Star (bearish reversal)
    if (cl1 > o1 and body1 > 0 and
        body2 / max(h2-l2,0.00001) < 0.3 and
        cl3 < o3 and
        cl3 < (o1 + cl1) / 2):
        found.append("EVENING_STAR_BEARISH")
    # Three White Soldiers (strong bullish)
    if cl1 > o1 and cl2 > o2 and cl3 > o3 and cl2 > cl1 and cl3 > cl2:
        found.append("THREE_WHITE_SOLDIERS_BULLISH")
    # Three Black Crows (strong bearish)
    if cl1 < o1 and cl2 < o2 and cl3 < o3 and cl2 < cl1 and cl3 < cl2:
        found.append("THREE_BLACK_CROWS_BEARISH")

    return found


def analyse_market_structure(df: pd.DataFrame) -> Dict:
    """
    Detect market structure: trend, HH/HL/LH/LL sequence,
    Break of Structure (BOS), Change of Character (CHoCH).
    """
    result = {
        "trend":   "RANGING",
        "swings":  [],
        "bos":     False,
        "choch":   False,
        "bos_dir": "",
        "last_hh": 0.0,
        "last_ll": 0.0,
    }
    if len(df) < 20:
        return result

    hi = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    hi_idx = local_extrema(hi, order=3, mode="max")
    lo_idx = local_extrema(lo, order=3, mode="min")

    # Build swing sequence (alternating highs and lows)
    pivots = (
        [(int(i), "H", float(hi[i])) for i in hi_idx] +
        [(int(i), "L", float(lo[i])) for i in lo_idx]
    )
    pivots.sort(key=lambda x: x[0])

    highs = [p for p in pivots if p[1] == "H"]
    lows  = [p for p in pivots if p[1] == "L"]

    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1][2] > highs[-2][2]   # Higher High
        hl = lows[-1][2]  > lows[-2][2]    # Higher Low
        lh = highs[-1][2] < highs[-2][2]   # Lower High
        ll = lows[-1][2]  < lows[-2][2]    # Lower Low

        result["last_hh"] = float(highs[-1][2])
        result["last_ll"] = float(lows[-1][2])

        if hh and hl:
            result["trend"] = "UPTREND"
            result["swings"].append("HH+HL confirmed")
        elif lh and ll:
            result["trend"] = "DOWNTREND"
            result["swings"].append("LH+LL confirmed")
        elif hh and ll:
            result["trend"] = "RANGING"
            result["swings"].append("Mixed: HH+LL")
        elif lh and hl:
            result["trend"] = "RANGING"
            result["swings"].append("Mixed: LH+HL")

        # Break of Structure: price closes beyond last swing
        cur = float(df.iloc[-1]["close"])
        if cur > highs[-1][2] and result["trend"] in ("DOWNTREND", "RANGING"):
            result["bos"]     = True
            result["bos_dir"] = "BULLISH_BOS"
            result["swings"].append("BOS BULLISH — broke last swing high")
        elif cur < lows[-1][2] and result["trend"] in ("UPTREND", "RANGING"):
            result["bos"]     = True
            result["bos_dir"] = "BEARISH_BOS"
            result["swings"].append("BOS BEARISH — broke last swing low")

        # Change of Character: trend reversal signal
        if len(highs) >= 3 and len(lows) >= 3:
            prev_trend_up = highs[-2][2] > highs[-3][2] and lows[-2][2] > lows[-3][2]
            prev_trend_dn = highs[-2][2] < highs[-3][2] and lows[-2][2] < lows[-3][2]
            if prev_trend_up and lh:
                result["choch"] = True
                result["swings"].append("CHoCH BEARISH — trend flipping")
            elif prev_trend_dn and hh:
                result["choch"] = True
                result["swings"].append("CHoCH BULLISH — trend flipping")

    return result


def analyse_key_levels(df: pd.DataFrame) -> Dict:
    """
    Identify key price levels: recent highs/lows, equal highs/lows
    (liquidity pools), psychological round numbers.
    """
    levels = {"resistance": [], "support": [], "equal_highs": [], "equal_lows": []}
    if len(df) < 10:
        return levels

    hi = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    hi_idx = local_extrema(hi, order=5, mode="max")
    lo_idx = local_extrema(lo, order=5, mode="min")

    for i in hi_idx[-5:]:
        levels["resistance"].append(round(float(hi[i]), 5))
    for i in lo_idx[-5:]:
        levels["support"].append(round(float(lo[i]), 5))

    # Equal highs (liquidity above): two swing highs within 0.05%
    for i in range(len(hi_idx) - 1):
        a = float(hi[int(hi_idx[i])]); b = float(hi[int(hi_idx[i+1])])
        if a > 0 and abs(a - b) / a < 0.0005:
            levels["equal_highs"].append(round((a + b) / 2, 5))
    # Equal lows (liquidity below)
    for i in range(len(lo_idx) - 1):
        a = float(lo[int(lo_idx[i])]); b = float(lo[int(lo_idx[i+1])])
        if a > 0 and abs(a - b) / a < 0.0005:
            levels["equal_lows"].append(round((a + b) / 2, 5))

    return levels


def analyse_volume_profile(df: pd.DataFrame) -> Dict:
    """
    High/low volume nodes, volume trend, climax bars.
    """
    result = {"vol_trend": "NEUTRAL", "climax_bars": [], "hvn": 0.0, "lvn": 0.0}
    if len(df) < 10:
        return result

    vols  = df["tick_volume"].values.astype(float)
    av    = float(np.mean(vols))
    close = df["close"].values.astype(float)

    # Volume trend: compare first half vs second half
    half = len(vols) // 2
    if float(np.mean(vols[half:])) > float(np.mean(vols[:half])) * 1.1:
        result["vol_trend"] = "INCREASING"
    elif float(np.mean(vols[half:])) < float(np.mean(vols[:half])) * 0.9:
        result["vol_trend"] = "DECREASING"

    # Climax bars: volume > 2.5x average
    for i in range(max(0, len(df)-10), len(df)):
        if vols[i] > av * 2.5:
            direction = "BULLISH" if close[i] > df["open"].values[i] else "BEARISH"
            result["climax_bars"].append({
                "idx": i, "price": round(float(close[i]), 5),
                "direction": direction,
                "vol_ratio": round(float(vols[i] / av), 1)
            })

    result["hvn"] = float(calc_vpoc(df))   # highest volume node = VPOC
    return result


def run_chart_analysis(symbol: str, df: pd.DataFrame,
                        timeframe: str) -> Dict:
    """
    Master chart analysis function. Runs all pattern engines on one
    dataframe and returns a structured analysis report.
    """
    if df is None or len(df) < 20:
        return {}

    candle_patterns = analyse_candle_patterns(df)
    structure       = analyse_market_structure(df)
    key_levels      = analyse_key_levels(df)
    vol_profile     = analyse_volume_profile(df)
    trendlines      = detect_trendlines(df)
    snr_levels      = mark_freshness(detect_basic_snr(df), df)
    gap_snr         = mark_freshness(detect_gap_snr(df), df)
    qml_patterns    = detect_qml_hns(df)
    engulf          = detect_engulfing(df)
    vwap            = calc_vwap(df)
    atr             = calc_atr(df)
    cur             = float(df.iloc[-1]["close"])

    # Summarise trendlines
    tl_summary = []
    for tl in trendlines[-4:]:
        tl_summary.append({
            "type":      tl.ttype,
            "direction": tl.direction,
            "pt3_price": round(tl.p3, 5)
        })

    # Summarise SNR levels near price
    near_snr = []
    for s in snr_levels + gap_snr:
        if s.price > 0:
            dist_pct = abs(cur - s.price) / cur * 100
            if dist_pct < 1.0:
                near_snr.append({
                    "price":      round(s.price, 5),
                    "type":       s.stype,
                    "pattern":    s.pattern.value,
                    "fresh":      s.fresh,
                    "miss_count": s.miss_count,
                    "dist_pct":   round(dist_pct, 3)
                })

    report = {
        "symbol":          symbol,
        "timeframe":       timeframe,
        "timestamp":       datetime.now().isoformat(),
        "price":           round(cur, 5),
        "atr":             round(atr, 5),
        "vwap":            round(vwap, 5),
        "above_vwap":      cur > vwap,
        "candle_patterns": candle_patterns,
        "last_engulfing":  engulf or "NONE",
        "structure":       structure,
        "key_levels":      key_levels,
        "volume_profile":  vol_profile,
        "trendlines":      tl_summary,
        "near_snr_levels": near_snr,
        "qml_patterns":    len(qml_patterns),
        "order_book":      None,   # populated below for crypto
        "footprint":       None,   # populated below for crypto
    }

    # ── Binance real volume/order book enrichment (crypto only) ──
    # Only runs on H4 and lower to avoid excessive API calls.
    # Forex/gold symbols are skipped — no public order book exists.
    if symbol in BINANCE_SYM_MAP and timeframe in ("H4","H1","M30","M15"):
        report = enrich_with_binance(symbol, report)

    return report


def print_chart_analysis(report: Dict):
    """Print chart analysis to console in a clean readable format."""
    if not report:
        return
    sym = report.get("symbol", "?")
    tf  = report.get("timeframe", "?")
    s   = report.get("structure", {})

    log_info(f"  [CHART {sym} {tf}] Trend:{s.get('trend','?')} | "
             f"Price:{report.get('price')} | ATR:{report.get('atr')}")

    if s.get("bos"):
        log_info(f"  [CHART] *** {s.get('bos_dir')} ***")
    if s.get("choch"):
        log_info(f"  [CHART] *** CHoCH detected — possible reversal ***")

    pats = report.get("candle_patterns", [])
    if pats:
        log_info(f"  [CHART] Candle patterns: {', '.join(pats)}")

    eng = report.get("last_engulfing", "NONE")
    if eng != "NONE":
        log_info(f"  [CHART] Last engulfing: {eng}")

    near = report.get("near_snr_levels", [])
    for lvl in near:
        log_info(f"  [CHART] Near SNR: {lvl['type']} @ {lvl['price']} "
                 f"(fresh={lvl['fresh']}, miss={lvl['miss_count']}, "
                 f"dist={lvl['dist_pct']}%)")

    swings = s.get("swings", [])
    for sw in swings:
        log_info(f"  [CHART] Structure: {sw}")

    vp = report.get("volume_profile", {})
    if vp.get("climax_bars"):
        for cb in vp["climax_bars"]:
            log_info(f"  [CHART] VOLUME CLIMAX {cb['direction']} "
                     f"@ {cb['price']} ({cb['vol_ratio']}x avg)")

    eq_hi = report.get("key_levels", {}).get("equal_highs", [])
    eq_lo = report.get("key_levels", {}).get("equal_lows", [])
    if eq_hi:
        log_info(f"  [CHART] Equal Highs (liquidity above): {eq_hi}")
    if eq_lo:
        log_info(f"  [CHART] Equal Lows  (liquidity below): {eq_lo}")


# ════════════════════════════════════════════════════════════════════
#  ADAPTIVE BIAS CASCADE  — H4 → H2 → H1 → M45 → M30 → M15
# ════════════════════════════════════════════════════════════════════

def score_bias_on_tf(df: pd.DataFrame) -> Tuple[str, int, List[str]]:
    """
    Score the directional bias on a single timeframe dataframe.
    Returns (direction, score, reasons). Direction = BUY | SELL | NONE.
    Score must reach bias_min_score to be accepted.
    """
    if df is None or len(df) < 20:
        return "NONE", 0, []

    score   = 0
    reasons = []
    votes_b = 0   # bullish votes
    votes_s = 0   # bearish votes

    cur   = float(df.iloc[-1]["close"])
    prev  = float(df.iloc[-2]["close"])
    vwap  = calc_vwap(df)
    atr   = calc_atr(df)

    # ── 1. Price vs VWAP ─────────────────────────────────────────
    if cur > vwap:
        votes_b += 1; reasons.append("Price above VWAP (bullish)")
    else:
        votes_s += 1; reasons.append("Price below VWAP (bearish)")

    # ── 2. EMA trend (20/50) ──────────────────────────────────────
    closes = df["close"].values.astype(float)
    if len(closes) >= 50:
        ema20 = calc_ema(closes, 20)[-1]
        ema50 = calc_ema(closes, 50)[-1]
        if ema20 > ema50:
            votes_b += 1; reasons.append("EMA20 > EMA50 (bullish)")
        else:
            votes_s += 1; reasons.append("EMA20 < EMA50 (bearish)")
        if cur > ema20:
            votes_b += 1; reasons.append("Price above EMA20")
        else:
            votes_s += 1; reasons.append("Price below EMA20")

    # ── 3. Market structure ───────────────────────────────────────
    ms = analyse_market_structure(df)
    if ms["trend"] == "UPTREND":
        votes_b += 2; reasons.append("Market structure: UPTREND (HH+HL)")
    elif ms["trend"] == "DOWNTREND":
        votes_s += 2; reasons.append("Market structure: DOWNTREND (LH+LL)")
    if ms.get("bos") and ms.get("bos_dir") == "BULLISH_BOS":
        votes_b += 2; reasons.append("Bullish Break of Structure")
    elif ms.get("bos") and ms.get("bos_dir") == "BEARISH_BOS":
        votes_s += 2; reasons.append("Bearish Break of Structure")
    if ms.get("choch"):
        # CHoCH adds weight to the opposing direction
        if ms["trend"] == "UPTREND":
            votes_s += 1; reasons.append("CHoCH warning — potential reversal down")
        else:
            votes_b += 1; reasons.append("CHoCH warning — potential reversal up")

    # ── 4. Candle momentum (last 3 closes) ────────────────────────
    if len(df) >= 4:
        last3 = df["close"].values[-4:].astype(float)
        bull_count = int(sum(last3[i] > last3[i-1] for i in range(1,4)))
        if bull_count >= 3:
            votes_b += 1; reasons.append("3 consecutive bullish closes")
        elif bull_count == 0:
            votes_s += 1; reasons.append("3 consecutive bearish closes")

    # ── 5. Engulfing confirmation ─────────────────────────────────
    eng = detect_engulfing(df)
    if eng and "BULLISH" in eng:
        votes_b += 1; reasons.append(f"Bullish engulfing: {eng}")
    elif eng and "BEARISH" in eng:
        votes_s += 1; reasons.append(f"Bearish engulfing: {eng}")

    # ── 6. Wyckoff phase contribution ────────────────────────────
    phase, w_sc, _ = detect_wyckoff(df)
    if phase in ("PHASE_C_SPRING", "PHASE_A_ACCUM", "PHASE_B"):
        votes_b += 1; reasons.append(f"Wyckoff phase: {phase} (bullish context)")
    elif phase == "PHASE_A_DIST":
        votes_s += 1; reasons.append(f"Wyckoff phase: {phase} (bearish context)")

    # ── 7. Order book bias (crypto only via Binance) ──────────────
    # Checked from the df's symbol attribute if available — otherwise
    # this step is skipped for forex/gold (no public order book).
    # The caller (find_bias_cascade) passes symbol separately;
    # here we use a module-level hint set by find_bias_cascade.
    ob_sym = getattr(score_bias_on_tf, "_current_symbol", None)
    if ob_sym and ob_sym in BINANCE_SYM_MAP:
        ob_bias = binance_order_book_bias(ob_sym)
        if ob_bias == "BUY_PRESSURE":
            votes_b += 2
            reasons.append("+2 Binance order book: BUY pressure (more bids)")
        elif ob_bias == "SELL_PRESSURE":
            votes_s += 2
            reasons.append("+2 Binance order book: SELL pressure (more asks)")
        else:
            reasons.append("Order book: neutral imbalance")

    # ── Decision ──────────────────────────────────────────────────
    net = votes_b - votes_s
    if net >= CONFIG["bias_min_score"]:
        return "BUY",  net, reasons
    elif net <= -CONFIG["bias_min_score"]:
        return "SELL", abs(net), reasons
    else:
        return "NONE", 0, reasons


def find_bias_cascade(symbol: str, broker) -> Tuple[str, str, int, List[str]]:
    """
    Walk down the bias cascade: H4 → H2 → H1 → M45 → M30 → M15.
    Returns (bias_direction, bias_tf, bias_score, reasons).
    Stops at the first timeframe that gives a clear bias.
    """
    # Set symbol hint for order book bias inside score_bias_on_tf
    score_bias_on_tf._current_symbol = symbol  # type: ignore
    cascade = CONFIG.get("bias_cascade", ["H4", "H2", "H1", "M45", "M30", "M15"])
    for tf in cascade:
        df = broker.get_candles(symbol, tf, 100)
        if df is None or len(df) < 20:
            log_warn(f"  [{symbol}] Bias cascade: no data on {tf}, dropping down...")
            continue
        direction, score, reasons = score_bias_on_tf(df)
        if direction != "NONE":
            log_info(f"  [{symbol}] Bias found on {tf}: {direction} "
                     f"(net votes={score})")
            return direction, tf, score, reasons
        else:
            log_info(f"  [{symbol}] No clear bias on {tf}, dropping to next TF...")

    log_warn(f"  [{symbol}] No bias found on any timeframe in cascade.")
    return "NONE", "NONE", 0, ["No bias found on any cascade TF"]


# ════════════════════════════════════════════════════════════════════
#  FULL SIGNAL SCORING ENGINE
# ════════════════════════════════════════════════════════════════════
def score_signal(symbol: str,
                 df_w: pd.DataFrame,
                 df_d: pd.DataFrame,
                 df_h4: pd.DataFrame,
                 df_h1: Optional[pd.DataFrame],
                 bias_dir: str = "NONE",
                 bias_tf:  str = "NONE",
                 bias_score: int = 0) -> Optional[Signal]:
    """
    Combined Wyckoff 2.0 + Malaysian SNR scoring.
    bias_dir/tf/score come from the adaptive bias cascade.
    Returns Signal if combined score >= combined_min, else None.
    """
    if df_d is None or df_h4 is None or df_w is None:
        return None
    if len(df_h4) < 20 or len(df_d) < 20:
        return None

    cur = float(df_h4.iloc[-1]["close"])
    weekly_bullish = bool(df_w.iloc[-1]["close"] > df_w.iloc[-2]["close"])
    a_w  = calc_atr(df_h4)
    vpoc = calc_vpoc(df_d)
    vwap = calc_vwap(df_h4)

    # ── Wyckoff scoring ───────────────────────────────────────────
    phase, w_sc, w_reasons = detect_wyckoff(df_d)
    if vpoc > 0:
        if cur > vpoc:
            w_sc += 1; w_reasons.append("+1 Price above VPOC")
        else:
            w_sc -= 2; w_reasons.append("-2 Price below VPOC")
    if cur > vwap:
        w_sc += 1; w_reasons.append("+1 Price above VWAP")

    # ── Adaptive Bias cascade contribution ───────────────────────
    if bias_dir != "NONE":
        w_reasons.append(f"Bias: {bias_dir} from {bias_tf} (votes={bias_score})")

    # ── SNR detection ─────────────────────────────────────────────
    base_snr = mark_freshness(detect_basic_snr(df_d), df_d)
    gap_snr  = mark_freshness(detect_gap_snr(df_d), df_d)
    all_snr  = base_snr + gap_snr
    cong     = find_congestion(all_snr)

    # ── Chart patterns (pg 47-57) ─────────────────────────────────
    trendlines  = detect_trendlines(df_h4)
    x_factors   = find_x_factor(trendlines, all_snr, df_h4)
    qml_list    = detect_qml_hns(df_h4)
    qmx_list    = detect_qmx(qml_list, trendlines)
    divergences = detect_tl_divergence(trendlines, df_h4)

    best: Optional[Dict] = None
    best_total = 0

    # ── 1. QMX (highest priority) ─────────────────────────────────
    for qmx in qmx_list:
        dist = abs(cur - qmx["price"]) / cur if cur > 0 else 1
        if dist > 0.03:
            continue
        s_sc = 3; s_r = ["+3 QMX Pattern (QML + TL Crossing)"]
        direction = qmx["direction"]
        if qmx.get("engulf"):
            s_sc += 2; s_r.append("+2 Engulfing at QML peak")
        if has_liquidity_sweep(df_h4, qmx["price"],
                                "SUPPORT" if direction == "BUY" else "RESISTANCE"):
            s_sc += 2; s_r.append("+2 Liquidity Sweep")
        if (direction == "BUY") == weekly_bullish:
            s_sc += 1; s_r.append("+1 Weekly storyline aligned")
        else:
            s_sc -= 3; s_r.append("-3 Opposing Weekly Storyline")
        # Bias cascade alignment
        if bias_dir != "NONE":
            if direction == bias_dir:
                s_sc += 2; s_r.append(f"+2 Bias aligned ({bias_dir} on {bias_tf})")
            else:
                s_sc -= 2; s_r.append(f"-2 Bias opposes signal ({bias_dir} on {bias_tf})")
        total = w_sc + s_sc
        if total > best_total:
            best_total = total
            best = {
                "direction": direction, "price": qmx["price"],
                "w_sc": w_sc, "s_sc": s_sc,
                "reasons": w_reasons + s_r,
                "pattern": PatternType.QMX,
                "tl_type": "QMX", "qmx": True
            }

    # ── 2. X-Factor confluence ────────────────────────────────────
    for xf in x_factors:
        if not xf["entry_ok"]:
            continue
        direction = xf["direction"]
        snr = xf["snr"]
        s_sc = 3; s_r = ["+3 X-Factor: TL + SNR at point #3"]
        if snr.miss_count >= 2:
            s_sc += 2; s_r.append(f"+2 {snr.miss_count} MISS candles")
        elif snr.miss_count == 1:
            s_sc += 1; s_r.append("+1 MISS candle")
        if has_liquidity_sweep(df_h4, snr.price, snr.stype):
            s_sc += 2; s_r.append("+2 Liquidity Sweep")
        eng = detect_engulfing(df_h4)
        if eng and (("BUY" == direction and "BULLISH" in eng) or
                    ("SELL" == direction and "BEARISH" in eng)):
            s_sc += 2; s_r.append(f"+2 Engulfing: {eng}")
        cong_hit = any(
            snr.price > 0 and abs(c["price"] - snr.price) / snr.price < 0.003
            for c in cong
        )
        if cong_hit:
            s_sc += 1; s_r.append("+1 Congestion Zone")
        if (direction == "BUY") == weekly_bullish:
            s_sc += 1; s_r.append("+1 Weekly Storyline aligned")
        else:
            s_sc -= 3; s_r.append("-3 Opposing Weekly Storyline")
        if bias_dir != "NONE":
            if direction == bias_dir:
                s_sc += 2; s_r.append(f"+2 Bias aligned ({bias_dir} on {bias_tf})")
            else:
                s_sc -= 2; s_r.append(f"-2 Bias opposes signal ({bias_dir} on {bias_tf})")
        total = w_sc + s_sc
        if total > best_total:
            best_total = total
            best = {
                "direction": direction, "price": snr.price,
                "w_sc": w_sc, "s_sc": s_sc,
                "reasons": w_reasons + s_r,
                "pattern": PatternType.X_FACTOR,
                "tl_type": xf.get("tl_type", ""), "qmx": False
            }

    # ── 3. QML/HNS ────────────────────────────────────────────────
    for qml in qml_list:
        qml_p = qml.get("qml_price", 0)
        if qml_p <= 0:
            continue
        dist = abs(cur - qml_p) / cur if cur > 0 else 1
        if dist > 0.025:
            continue
        direction = qml["direction"]
        s_sc = 2; s_r = ["+2 QML/HNS pattern"]
        if qml.get("engulf"):
            s_sc += 2; s_r.append(f"+2 Engulfing at right shoulder")
        if has_liquidity_sweep(df_h4, qml_p,
                                "SUPPORT" if direction == "BUY" else "RESISTANCE"):
            s_sc += 2; s_r.append("+2 Liquidity Sweep at right shoulder")
        if (direction == "BUY") == weekly_bullish:
            s_sc += 1; s_r.append("+1 Weekly Storyline aligned")
        else:
            s_sc -= 3; s_r.append("-3 Opposing Weekly Storyline")
        if bias_dir != "NONE":
            if direction == bias_dir:
                s_sc += 2; s_r.append(f"+2 Bias aligned ({bias_dir} on {bias_tf})")
            else:
                s_sc -= 2; s_r.append(f"-2 Bias opposes signal ({bias_dir} on {bias_tf})")
        total = w_sc + s_sc
        if total > best_total:
            best_total = total
            best = {
                "direction": direction, "price": qml_p,
                "w_sc": w_sc, "s_sc": s_sc,
                "reasons": w_reasons + s_r,
                "pattern": PatternType.QML_HNS,
                "tl_type": "QML", "qmx": False
            }

    # ── 4. Trendline Divergence ───────────────────────────────────
    for div in divergences:
        poi  = div.get("poi", 0)
        if poi <= 0:
            continue
        dist = abs(cur - poi) / cur if cur > 0 else 1
        if dist > 0.02:
            continue
        direction = div["signal"]
        s_sc = 2; s_r = [f"+2 TL Divergence: {div['type']}"]
        if has_liquidity_sweep(df_h4, poi,
                                "SUPPORT" if direction == "BUY" else "RESISTANCE"):
            s_sc += 2; s_r.append("+2 Liquidity Sweep at POI")
        if (direction == "BUY") == weekly_bullish:
            s_sc += 1; s_r.append("+1 Weekly Storyline aligned")
        else:
            s_sc -= 3; s_r.append("-3 Opposing Weekly Storyline")
        if bias_dir != "NONE":
            if direction == bias_dir:
                s_sc += 2; s_r.append(f"+2 Bias aligned ({bias_dir} on {bias_tf})")
            else:
                s_sc -= 2; s_r.append(f"-2 Bias opposes signal ({bias_dir} on {bias_tf})")
        total = w_sc + s_sc
        if total > best_total:
            best_total = total
            best = {
                "direction": direction, "price": poi,
                "w_sc": w_sc, "s_sc": s_sc,
                "reasons": w_reasons + s_r,
                "pattern": PatternType.TL_DIVERGENCE,
                "tl_type": "DIVERGENCE", "qmx": False
            }

    # ── 5. Regular SNR ────────────────────────────────────────────
    sorted_snr = sorted(all_snr, key=lambda x: x.miss_count, reverse=True)
    for snr in sorted_snr:
        if cur <= 0 or snr.price <= 0:
            continue
        dist = abs(cur - snr.price) / cur
        if dist > 0.02:
            continue
        direction = "BUY" if snr.stype == "SUPPORT" else "SELL"
        s_sc = 0; s_r = []
        if snr.fresh:
            s_sc += 3; s_r.append("+3 Fresh HTF SNR")
        else:
            s_sc -= 2; s_r.append("-2 Unfresh SNR")
        if has_liquidity_sweep(df_h4, snr.price, snr.stype):
            s_sc += 2; s_r.append("+2 Liquidity Sweep")
        if snr.miss_count >= 2:
            s_sc += 2; s_r.append(f"+2 {snr.miss_count} MISS candles")
        elif snr.miss_count == 1:
            s_sc += 1; s_r.append("+1 MISS candle")
        eng = detect_engulfing(df_h4)
        if eng and (("BUY" == direction and "BULLISH" in eng) or
                    ("SELL" == direction and "BEARISH" in eng)):
            s_sc += 2; s_r.append(f"+2 Engulfing: {eng}")
        cong_hit = any(
            snr.price > 0 and abs(c["price"] - snr.price) / snr.price < 0.003
            for c in cong
        )
        if cong_hit:
            s_sc += 1; s_r.append("+1 Congestion Zone")
        if snr.pattern == PatternType.GAP_HIDDEN:
            s_sc += 1; s_r.append("+1 GAP/Hidden SNR")
        if df_h1 is not None and len(df_h1) > 1:
            lh   = df_h1.iloc[-1]
            wick = float(lh["high"]) - float(lh["low"])
            body = float(lh["body_size"]) if "body_size" in lh else abs(float(lh["close"]) - float(lh["open"]))
            if wick > 0 and body / wick < 0.4:
                s_sc += 1; s_r.append("+1 H1 wick rejection")
        if (direction == "BUY") == weekly_bullish:
            s_sc += 1; s_r.append("+1 Weekly Storyline aligned")
        else:
            s_sc -= 3; s_r.append("-3 Opposing Weekly Storyline")
        if bias_dir != "NONE":
            if direction == bias_dir:
                s_sc += 2; s_r.append(f"+2 Bias aligned ({bias_dir} on {bias_tf})")
            else:
                s_sc -= 2; s_r.append(f"-2 Bias opposes signal ({bias_dir} on {bias_tf})")
        total = w_sc + s_sc
        if total > best_total:
            best_total = total
            best = {
                "direction": direction, "price": snr.price,
                "w_sc": w_sc, "s_sc": s_sc,
                "reasons": w_reasons + s_r,
                "pattern": snr.pattern,
                "tl_type": "", "qmx": False
            }

    # ── Build Signal ──────────────────────────────────────────────
    if best is None or best_total < CONFIG["combined_min"]:
        return None

    d = Direction(best["direction"])
    if d == Direction.BUY:
        sl = best["price"] - a_w * CONFIG["atr_sl_mult"]
        tp = cur           + a_w * CONFIG["atr_tp_mult"]
    else:
        sl = best["price"] + a_w * CONFIG["atr_sl_mult"]
        tp = cur           - a_w * CONFIG["atr_tp_mult"]

    return Signal(
        symbol=symbol,
        direction=d,
        score_w=best["w_sc"],
        score_s=best["s_sc"],
        score=best_total,
        pattern=best["pattern"],
        phase=phase,
        snr_price=best["price"],
        sl=round(sl, 5),
        tp=round(tp, 5),
        vpoc=vpoc,
        vwap=vwap,
        reasons=best["reasons"],
        tl_type=best["tl_type"],
        qmx=best["qmx"],
        timestamp=datetime.now().isoformat(),
    )

# ════════════════════════════════════════════════════════════════════
#  NOTIFICATIONS
# ════════════════════════════════════════════════════════════════════
def send_telegram(msg: str):
    """Send Telegram message if token is configured."""
    token = CONFIG.get("tg_token", "")
    chat  = CONFIG.get("tg_chat", "")
    if not token or not chat:
        return
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat, "text": msg, "parse_mode": "HTML"}
    try:
        if REQUESTS_OK:
            req_lib.post(url, json=data, timeout=5)
        else:
            body = json.dumps(data).encode()
            req  = Request(url, data=body,
                           headers={"Content-Type": "application/json"},
                           method="POST")
            urlopen(req, timeout=5)
    except Exception as e:
        log_error(f"Telegram error: {e}")

# ════════════════════════════════════════════════════════════════════
#  MAIN BOT LOOP
# ════════════════════════════════════════════════════════════════════

# ── Confluence Analysis (H4 — pattern confirmation layer) ─────────────
def analyse_confluence(symbol: str, df_h4: pd.DataFrame,
                        bias_dir: str) -> Dict:
    """
    Confluence layer: confirms bias using H4 chart patterns.
    Looks for: SNR freshness, trendline touches, QML/HNS at bias POI,
    engulfing alignment, VWAP/VPOC position, congestion zones.
    Returns a dict with score and list of confluence reasons.
    """
    result = {
        "symbol":   symbol,
        "tf":       "H4",
        "bias":     bias_dir,
        "score":    0,
        "reasons":  [],
        "poi":      0.0,
        "valid":    False,
    }
    if df_h4 is None or len(df_h4) < 20 or bias_dir == "NONE":
        return result

    score   = 0
    reasons = []
    cur     = float(df_h4.iloc[-1]["close"])
    vwap    = calc_vwap(df_h4)
    vpoc    = calc_vpoc(df_h4)
    atr     = calc_atr(df_h4)

    # ── 1. VWAP alignment with bias ───────────────────────────────
    if bias_dir == "BUY" and cur > vwap:
        score += 1; reasons.append("+1 H4 price above VWAP (BUY bias confirmed)")
    elif bias_dir == "SELL" and cur < vwap:
        score += 1; reasons.append("+1 H4 price below VWAP (SELL bias confirmed)")
    else:
        score -= 1; reasons.append("-1 VWAP opposes bias direction")

    # ── 2. VPOC alignment ─────────────────────────────────────────
    if bias_dir == "BUY" and cur > vpoc:
        score += 1; reasons.append("+1 Price above H4 VPOC (high-vol support)")
    elif bias_dir == "SELL" and cur < vpoc:
        score += 1; reasons.append("+1 Price below H4 VPOC (high-vol resistance)")

    # ── 3. Fresh SNR at current price ─────────────────────────────
    snr_levels = mark_freshness(detect_basic_snr(df_h4), df_h4)
    gap_snr    = mark_freshness(detect_gap_snr(df_h4),   df_h4)
    all_snr    = snr_levels + gap_snr
    poi = 0.0
    for snr in all_snr:
        if snr.price <= 0:
            continue
        dist = abs(cur - snr.price) / cur
        if dist > 0.015:
            continue
        snr_bias = "BUY" if snr.stype == "SUPPORT" else "SELL"
        if snr_bias == bias_dir:
            if snr.fresh:
                score += 2; reasons.append(
                    f"+2 Fresh H4 SNR @ {snr.price:.5f} aligns with {bias_dir}")
                poi = snr.price
            if snr.miss_count >= 2:
                score += 1; reasons.append(f"+1 {snr.miss_count} MISS candles at SNR")

    # ── 4. Engulfing in bias direction ────────────────────────────
    eng = detect_engulfing(df_h4)
    if eng:
        if bias_dir == "BUY" and "BULLISH" in eng:
            score += 2; reasons.append(f"+2 H4 bullish engulfing: {eng}")
        elif bias_dir == "SELL" and "BEARISH" in eng:
            score += 2; reasons.append(f"+2 H4 bearish engulfing: {eng}")
        else:
            score -= 1; reasons.append(f"-1 H4 engulfing opposes bias: {eng}")

    # ── 5. Trendline at point #3 (X-Factor) ──────────────────────
    trendlines = detect_trendlines(df_h4)
    for tl in trendlines:
        dist = abs(cur - tl.p3) / cur if cur > 0 else 1
        if dist < 0.005:
            tl_ok = (bias_dir == "BUY" and tl.direction == "UP") or \
                    (bias_dir == "SELL" and tl.direction == "DOWN")
            if tl_ok:
                score += 2; reasons.append(
                    f"+2 Trendline pt#3 @ {tl.p3:.5f} aligns with bias")

    # ── 6. Market structure BOS in bias direction ─────────────────
    ms = analyse_market_structure(df_h4)
    if ms.get("bos"):
        bos_ok = (bias_dir == "BUY" and ms["bos_dir"] == "BULLISH_BOS") or \
                 (bias_dir == "SELL" and ms["bos_dir"] == "BEARISH_BOS")
        if bos_ok:
            score += 2; reasons.append(f"+2 H4 BOS confirms {bias_dir} bias")

    # ── 7. QML / HNS at POI ──────────────────────────────────────
    qml_list = detect_qml_hns(df_h4)
    for qml in qml_list:
        if qml["direction"] == bias_dir:
            qml_p = qml.get("qml_price", 0)
            if qml_p > 0 and abs(cur - qml_p) / cur < 0.02:
                score += 2; reasons.append(
                    f"+2 QML/HNS confirms {bias_dir} @ {qml_p:.5f}")

    result["score"]   = score
    result["reasons"] = reasons
    result["poi"]     = poi
    result["valid"]   = score >= 3   # needs at least 3 pts to be valid confluence
    return result


def print_confluence(c: Dict):
    """Print confluence report to console."""
    if not c or not c.get("reasons"):
        return
    sym   = c.get("symbol", "?")
    valid = "VALID" if c.get("valid") else "WEAK"
    log_info(f"  [CONFLUENCE {sym} H4] Score:{c['score']} [{valid}] "
             f"Bias:{c.get('bias')} POI:{c.get('poi', 0):.5f}")
    for r in c.get("reasons", []):
        log_info(f"    {r}")


# ── Entry Analysis (M15/M30 — precision trigger layer) ────────────────
def analyse_entry(symbol: str, broker, bias_dir: str,
                   poi: float) -> Dict:
    """
    Entry layer: drops to M30 then M15 to find a precision trigger.
    Looks for: engulfing, pin bar, BOS on LTF, SNR touch + rejection,
    wick rejection, volume climax at POI.
    Returns entry details if trigger found.
    """
    result = {
        "symbol":    symbol,
        "bias":      bias_dir,
        "trigger":   "NONE",
        "entry_tf":  "NONE",
        "score":     0,
        "reasons":   [],
        "valid":     False,
    }
    if bias_dir == "NONE":
        return result

    for tf in ["M30", "M15"]:
        df = broker.get_candles(symbol, tf, 60)
        if df is None or len(df) < 10:
            continue

        score   = 0
        reasons = []
        cur     = float(df.iloc[-1]["close"])

        # ── 1. Price near POI ─────────────────────────────────────
        if poi > 0:
            poi_dist = abs(cur - poi) / cur
            if poi_dist < 0.005:
                score += 2; reasons.append(
                    f"+2 Price within 0.5% of POI @ {poi:.5f}")
            elif poi_dist > 0.015:
                # Too far from POI — not a valid entry TF yet
                continue

        # ── 2. Candle trigger patterns ────────────────────────────
        pats = analyse_candle_patterns(df)
        for pat in pats:
            if bias_dir == "BUY" and any(x in pat for x in
               ["BULLISH", "HAMMER", "MORNING_STAR", "THREE_WHITE"]):
                score += 2; reasons.append(f"+2 {tf} trigger pattern: {pat}")
                result["trigger"] = pat
            elif bias_dir == "SELL" and any(x in pat for x in
                 ["BEARISH", "SHOOTING", "EVENING_STAR", "THREE_BLACK"]):
                score += 2; reasons.append(f"+2 {tf} trigger pattern: {pat}")
                result["trigger"] = pat

        # ── 3. LTF BOS in bias direction ──────────────────────────
        ms = analyse_market_structure(df)
        if ms.get("bos"):
            bos_ok = (bias_dir == "BUY" and ms["bos_dir"] == "BULLISH_BOS") or \
                     (bias_dir == "SELL" and ms["bos_dir"] == "BEARISH_BOS")
            if bos_ok:
                score += 2; reasons.append(f"+2 {tf} LTF BOS: {ms['bos_dir']}")

        # ── 4. Engulfing trigger ──────────────────────────────────
        eng = detect_engulfing(df)
        if eng:
            if bias_dir == "BUY" and "BULLISH" in eng:
                score += 2; reasons.append(f"+2 {tf} bullish engulf trigger: {eng}")
            elif bias_dir == "SELL" and "BEARISH" in eng:
                score += 2; reasons.append(f"+2 {tf} bearish engulf trigger: {eng}")

        # ── 5. Wick rejection at POI ──────────────────────────────
        last = df.iloc[-1]
        rng  = float(last["high"]) - float(last["low"])
        if rng > 0:
            body = abs(float(last["close"]) - float(last["open"]))
            lower_wick = float(min(last["open"], last["close"])) - float(last["low"])
            upper_wick = float(last["high"]) - float(max(last["open"], last["close"]))
            if bias_dir == "BUY" and lower_wick / rng > 0.5:
                score += 1; reasons.append(f"+1 {tf} strong lower wick rejection")
            elif bias_dir == "SELL" and upper_wick / rng > 0.5:
                score += 1; reasons.append(f"+1 {tf} strong upper wick rejection")

        # ── 6. Volume climax confirmation ─────────────────────────
        vp = analyse_volume_profile(df)
        for cb in vp.get("climax_bars", []):
            if bias_dir == "BUY" and cb["direction"] == "BULLISH":
                score += 1; reasons.append(
                    f"+1 {tf} bullish volume climax ({cb['vol_ratio']}x avg)")
            elif bias_dir == "SELL" and cb["direction"] == "BEARISH":
                score += 1; reasons.append(
                    f"+1 {tf} bearish volume climax ({cb['vol_ratio']}x avg)")

        if score >= 3:
            result["score"]    = score
            result["reasons"]  = reasons
            result["entry_tf"] = tf
            result["valid"]    = True
            return result

    return result


def print_entry(e: Dict):
    """Print entry analysis to console."""
    if not e or not e.get("reasons"):
        return
    sym   = e.get("symbol", "?")
    valid = "VALID TRIGGER" if e.get("valid") else "NO TRIGGER"
    log_info(f"  [ENTRY {sym} {e.get('entry_tf','?')}] Score:{e.get('score')} "
             f"[{valid}] Bias:{e.get('bias')} Trigger:{e.get('trigger')}")
    for r in e.get("reasons", []):
        log_info(f"    {r}")

def responsive_sleep(seconds: int):
    """Sleep in 1-second ticks so bot stays responsive and shows countdown."""
    for remaining in range(seconds, 0, -1):
        if remaining % 30 == 0 or remaining <= 5:
            log_info(f"  Next scan in {remaining}s...")
        time.sleep(1)

def print_banner():
    print("=" * 60)
    print("  WYCKOFF 2.0 + MALAYSIAN SNR BOT")
    print("  Pydroid 3 / Android Compatible Version")
    print("=" * 60)
    print(f"  Broker mode : {CONFIG['broker_mode']}")
    print(f"  Symbols     : {', '.join(CONFIG['symbols'])}")
    print(f"  Risk %      : {CONFIG['risk_pct']}%")
    print(f"  Min score   : {CONFIG['combined_min']}")
    print(f"  Scan every  : {CONFIG['scan_secs']}s")
    print(f"  Log file    : {CONFIG['log_file']}")
    print("=" * 60)

# ════════════════════════════════════════════════════════════════════
#  RISK & CONTROL
# ════════════════════════════════════════════════════════════════════
def calculate_lot_size(balance: float, risk_pct: float,
                       entry: float, sl: float,
                       symbol: str = "EUR_USD") -> float:
    """Risk-based lot sizing: risk_pct% of balance per trade."""
    risk_amount   = balance * (risk_pct / 100)
    pip_value     = 10
    stop_distance = abs(entry - sl)
    if stop_distance == 0:
        return 0.01
    lot = risk_amount / (stop_distance * pip_value)
    return round(max(lot, 0.01), 2)


def check_daily_loss(limit_pct: float = 5.0) -> bool:
    """Returns False and warns when daily drawdown exceeds limit_pct%."""
    global DAILY_STATE
    today = datetime.now().date()
    if DAILY_STATE["date"] != today:
        DAILY_STATE = {"date": today, "loss": 0.0}
    if DAILY_STATE["loss"] >= limit_pct:
        log_warn("Daily loss limit reached — no new trades today.")
        return False
    return True


def cooldown_ok(symbol: str, cooldown: int = 1800) -> bool:
    """Returns False if a trade was placed on this symbol within cooldown seconds."""
    global LAST_TRADE_TIME
    now  = time.time()
    last = LAST_TRADE_TIME.get(symbol, 0)
    if now - last < cooldown:
        log_info(f"  [{symbol}] Cooldown active — "
                 f"{int(cooldown - (now - last))}s remaining.")
        return False
    LAST_TRADE_TIME[symbol] = now
    return True


# ════════════════════════════════════════════════════════════════════
#  PnL SYSTEM
# ════════════════════════════════════════════════════════════════════
def calculate_pnl(position: Dict, current_price: float) -> float:
    """Estimate floating P&L for a single open position."""
    entry     = position.get("entry", 0)
    lot       = position.get("lot", 0.01)
    direction = position.get("direction", "")
    pip_value = 10
    if direction == "BUY":
        return (current_price - entry) * lot * pip_value
    else:
        return (entry - current_price) * lot * pip_value


def update_equity(broker) -> float:
    """
    Recalculate account equity (balance + floating P&L),
    append to EQUITY_STATE, persist equity_curve.json,
    and update DAILY_STATE loss tracker.
    """
    global EQUITY_STATE, DAILY_STATE
    account = broker.get_account_info()
    balance = float(account.get("balance", 10000)) if account else 10000.0

    positions    = broker.get_open_positions()
    floating_pnl = 0.0

    for pos in (positions or []):
        symbol = pos.get("symbol")
        tick   = broker.get_tick(symbol) if symbol else None
        if not tick:
            continue
        floating_pnl += calculate_pnl(pos, tick["bid"])

    equity = balance + floating_pnl

    point = {
        "time":         datetime.now().isoformat(),
        "balance":      balance,
        "floating_pnl": floating_pnl,
        "equity":       equity,
    }

    EQUITY_STATE["equity"].append(point)
    EQUITY_STATE["equity"] = EQUITY_STATE["equity"][-500:]
    _save_json("equity_curve.json", EQUITY_STATE["equity"])

    DAILY_STATE["loss"] = max(
        0, EQUITY_STATE["last_balance"] - equity
    )

    log_info(
        f"Equity | Bal:{balance:.2f}  "
        f"Float:{floating_pnl:.2f}  Eq:{equity:.2f}  "
        f"DayLoss:{DAILY_STATE['loss']:.2f}"
    )
    return equity


# ════════════════════════════════════════════════════════════════════
#  PRICE FEED — live price from active broker (MT5 bridge or YFinance)
# ════════════════════════════════════════════════════════════════════

# MT5 local bridge URL (running on your PC when the EA bridge server
# is active). Bot checks this first; falls back to broker.get_tick().
MT5_BRIDGE_URL = "http://localhost:8000"

def is_mt5_bridge_online() -> bool:
    """
    Check if the local MT5 bridge server is reachable.
    The bridge runs on your PC alongside MT5 and exposes a REST API.
    Returns False silently if the bridge is offline (e.g. PC is off).
    """
    try:
        if REQUESTS_OK:
            r = req_lib.get(f"{MT5_BRIDGE_URL}/health", timeout=2)
            return r.status_code == 200
        else:
            urlopen(f"{MT5_BRIDGE_URL}/health", timeout=2)
            return True
    except Exception:
        return False


def get_live_price(broker: BrokerBase, symbol: str) -> Optional[Dict]:
    """
    Get the best available live price for a symbol.

    Priority order:
      1. MT5 bridge (localhost:8000/price/<symbol>) — most accurate,
         uses your real broker's live tick when PC bridge is running.
      2. broker.get_tick() — MetaAPI REST, YFinance M15 close, or
         PaperBroker spread simulation depending on active broker mode.

    Returns dict with keys: bid, ask
    Returns None if both sources fail.
    """
    # 1 ── Try MT5 bridge first (only if bridge is online)
    if is_mt5_bridge_online():
        try:
            mt5_sym = symbol.replace("_", "")   # EUR_USD → EURUSD
            url     = f"{MT5_BRIDGE_URL}/price/{mt5_sym}"
            data    = http_get(url, timeout=3)
            if data and "bid" in data and "ask" in data:
                log_info(f"  [{symbol}] Price via MT5 bridge: "
                         f"bid={data['bid']} ask={data['ask']}")
                return {"bid": float(data["bid"]), "ask": float(data["ask"])}
        except Exception as e:
            log_warn(f"  [{symbol}] MT5 bridge price failed: {e}")

    # 2 ── Fall back to broker's own tick method
    tick = broker.get_tick(symbol)
    if tick:
        return tick

    log_warn(f"  [{symbol}] No live price available from any source.")
    return None


# ════════════════════════════════════════════════════════════════════
#  TRADE LIFECYCLE MANAGER
#  Tracks open trades in memory + JSON, handles trailing stops,
#  pushes updates to the webapp, and syncs with the broker.
#
#  Complements execute_trade_pipeline():
#    execute_trade_pipeline() → places the order (entry gate)
#    open_trade_record()      → registers it in lifecycle state
#    update_trade_record()    → trails SL, checks SL/TP, pushes data
#    close_trade_record()     → marks closed, writes history
# ════════════════════════════════════════════════════════════════════

# In-memory trade store (also persisted to open_trades.json)
_ACTIVE_TRADES: Dict[str, Dict] = {}
_TRADE_HISTORY: List[Dict]      = []
_TRADE_LOCK    = threading.Lock()

# Small-account threshold — trailing logic tightens below this balance
SMALL_ACCOUNT_LIMIT: float = 50.0


def _push_trade_event(event_type: str, payload: Dict) -> None:
    """
    Send a trade event to the webapp's push endpoint.
    Non-blocking — failures are logged but don't interrupt the bot.
    """
    if not WEBAPP_API_URL:
        return
    try:
        http_post(WEBAPP_API_URL, {"type": event_type, **payload}, timeout=3)
    except Exception as e:
        log_warn(f"Trade event push failed ({event_type}): {e}")


def open_trade_record(signal: "Signal", broker: BrokerBase) -> Optional[str]:
    """
    Register a newly placed trade in the lifecycle state.
    Called immediately after execute_trade_pipeline() succeeds.

    Returns the trade_id string, or None if broker tick unavailable.
    """
    tick = get_live_price(broker, signal.symbol)
    if not tick:
        return None

    entry    = tick["ask"] if signal.direction.value == "BUY" else tick["bid"]
    trade_id = f"{signal.symbol}_{int(time.time())}"

    trade: Dict = {
        "id":         trade_id,
        "symbol":     signal.symbol,
        "direction":  signal.direction.value,
        "entry":      entry,
        "sl":         signal.sl,
        "tp":         signal.tp,
        "lot":        0.0,          # filled by execute_trade_pipeline
        "status":     "OPEN",
        "open_time":  datetime.now().isoformat(),
        "close_time": None,
        "close_reason": None,
        "profit":     0.0,
        "pattern":    signal.pattern.value,
        "score":      signal.score,
    }

    with _TRADE_LOCK:
        _ACTIVE_TRADES[trade_id] = trade
        _save_json(CONFIG["trades_file"],
                   list(_ACTIVE_TRADES.values()))

    log_trade(f"[LIFECYCLE] Registered trade {trade_id}")
    _push_trade_event("TRADE_OPEN", {"trade": trade})
    return trade_id


def update_trade_record(trade_id: str, broker: BrokerBase) -> None:
    """
    Update an open trade:
      • Fetch live price via get_live_price() (bridge → broker fallback)
      • Calculate floating P&L
      • Apply trailing stop on small accounts (balance < SMALL_ACCOUNT_LIMIT)
      • Detect SL/TP hit and call close_trade_record() automatically
      • Push TRAIL_UPDATE event to webapp if SL moved

    Called once per scan cycle for every open trade.
    """
    with _TRADE_LOCK:
        trade = _ACTIVE_TRADES.get(trade_id)
        if not trade or trade["status"] != "OPEN":
            return

    tick = get_live_price(broker, trade["symbol"])
    if not tick:
        return

    direction = trade["direction"]
    price     = tick["bid"] if direction == "BUY" else tick["ask"]
    entry     = trade["entry"]
    sl        = trade["sl"]
    tp        = trade["tp"]

    # ── Floating P&L ─────────────────────────────────────────────
    profit_move = (price - entry) if direction == "BUY" else (entry - price)

    account     = broker.get_account_info()
    balance     = float(account.get("balance", 10000)) if account else 10000.0

    # ── Trailing stop (tightened on small accounts) ───────────────
    sl_updated = False
    if profit_move > 0:
        if balance < SMALL_ACCOUNT_LIMIT:
            # Small account: trail after 0.5 pip profit, tight 0.5 pip distance
            if profit_move > 0.0005:
                trail_dist = 0.0005
                if direction == "BUY":
                    new_sl = price - trail_dist
                    if new_sl > sl:
                        trade["sl"] = new_sl
                        sl_updated  = True
                else:
                    new_sl = price + trail_dist
                    if new_sl < sl:
                        trade["sl"] = new_sl
                        sl_updated  = True
        else:
            # Standard account: trail at 1 ATR distance after 2 ATR profit
            atr = calc_atr(broker.get_candles(
                trade["symbol"], CONFIG["tf_h1"], 20
            ) or pd.DataFrame(columns=["high","low","close","range"]))
            if atr > 0 and profit_move > atr * 2:
                trail_dist = atr
                if direction == "BUY":
                    new_sl = price - trail_dist
                    if new_sl > sl:
                        trade["sl"] = new_sl
                        sl_updated  = True
                else:
                    new_sl = price + trail_dist
                    if new_sl < sl:
                        trade["sl"] = new_sl
                        sl_updated  = True

    if sl_updated:
        log_trade(f"[TRAIL] {trade_id} SL → {trade['sl']:.5f}")
        _push_trade_event("TRAIL_UPDATE", {
            "trade_id": trade_id,
            "new_sl":   trade["sl"],
        })

    # ── SL / TP hit detection ─────────────────────────────────────
    sl = trade["sl"]
    tp = trade["tp"]
    close_reason = None

    if direction == "BUY":
        if price <= sl:
            close_reason = "SL_HIT"
        elif price >= tp:
            close_reason = "TP_HIT"
    else:
        if price >= sl:
            close_reason = "SL_HIT"
        elif price <= tp:
            close_reason = "TP_HIT"

    # ── Persist updated SL ────────────────────────────────────────
    with _TRADE_LOCK:
        if trade_id in _ACTIVE_TRADES:
            _ACTIVE_TRADES[trade_id]["sl"]     = trade["sl"]
            _ACTIVE_TRADES[trade_id]["profit"]  = round(
                calculate_pnl(trade, price), 2
            )
            _save_json(CONFIG["trades_file"],
                       list(_ACTIVE_TRADES.values()))

    if close_reason:
        close_trade_record(trade_id, close_reason, price)


def close_trade_record(trade_id: str,
                        reason: str = "manual",
                        close_price: float = 0.0) -> None:
    """
    Mark a trade as closed, move it to history, persist files,
    update DAILY_STATE loss, and notify the webapp.
    """
    with _TRADE_LOCK:
        trade = _ACTIVE_TRADES.pop(trade_id, None)

    if not trade:
        log_warn(f"[LIFECYCLE] close_trade_record: {trade_id} not found.")
        return

    trade["status"]       = "CLOSED"
    trade["close_time"]   = datetime.now().isoformat()
    trade["close_reason"] = reason
    trade["close_price"]  = close_price

    if close_price and trade.get("entry"):
        trade["profit"] = round(calculate_pnl(trade, close_price), 2)

    _TRADE_HISTORY.append(trade)

    # Update daily loss tracker
    if trade["profit"] < 0:
        global DAILY_STATE
        DAILY_STATE["loss"] = DAILY_STATE.get("loss", 0) + abs(trade["profit"])

    with _TRADE_LOCK:
        _save_json(CONFIG["trades_file"],
                   list(_ACTIVE_TRADES.values()))
        _save_json("trade_history.json", _TRADE_HISTORY[-200:])

    log_trade(f"[LIFECYCLE] CLOSED {trade_id} | "
              f"reason={reason} | profit={trade['profit']:.2f}")
    _push_trade_event("TRADE_CLOSE", {"trade": trade})


def sync_lifecycle_with_broker(broker: BrokerBase) -> None:
    """
    Cross-check _ACTIVE_TRADES with broker's open positions.
    Any trade in our lifecycle dict that the broker no longer reports
    as open is assumed closed (broker-side SL/TP hit or manual close).
    Call this at the top of every scan cycle.
    """
    broker_positions = broker.get_open_positions() or []
    broker_symbols   = {p.get("symbol", "").replace("_", "")
                        for p in broker_positions}

    with _TRADE_LOCK:
        stale_ids = [
            tid for tid, t in _ACTIVE_TRADES.items()
            if t["symbol"].replace("_", "") not in broker_symbols
        ]

    for tid in stale_ids:
        log_warn(f"[LIFECYCLE] Trade {tid} no longer at broker — closing.")
        close_trade_record(tid, reason="BROKER_CLOSED")


# ════════════════════════════════════════════════════════════════════
#  EXECUTION PIPELINE
# ════════════════════════════════════════════════════════════════════
def execute_trade_pipeline(broker, signal) -> None:
    """
    Full pre-trade gate before placing an order:
      daily-loss → spread → max-trades → duplicate → cooldown → RR → lot → execute
    Replaces the inline order block inside the main scan loop.
    """
    symbol    = signal.symbol
    direction = signal.direction
    sl        = signal.sl
    tp        = signal.tp

    try:
        # 1. Daily loss guard
        if not check_daily_loss():
            return

        # 2. Live tick
        tick = broker.get_tick(symbol)
        if not tick:
            log_warn(f"  [{symbol}] No tick data — skipping.")
            return

        entry  = tick["ask"] if direction.value == "BUY" else tick["bid"]
        spread = abs(tick["ask"] - tick["bid"])

        # 3. Spread filter
        if spread > CONFIG["max_spread_pips"] * 0.0001:
            log_info(f"  [{symbol}] Spread too wide "
                     f"({spread:.5f}) — skipping.")
            return

        # 4. Max-trades guard
        open_positions = broker.get_open_positions() or []
        if len(open_positions) >= CONFIG["max_trades"]:
            log_info(f"  [{symbol}] Max trades reached — skipping.")
            return

        # 5. Duplicate-position guard
        if any(p.get("symbol") == symbol for p in open_positions):
            log_info(f"  [{symbol}] Position already open — skipping.")
            return

        # 6. Cooldown
        if not cooldown_ok(symbol):
            return

        # 7. Risk:Reward filter (min 1.5R)
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0:
            return
        rr = reward / risk
        if rr < 1.5:
            log_info(f"  [{symbol}] RR {rr:.2f} < 1.5 — skipping.")
            return

        # 8. Lot sizing
        account = broker.get_account_info()
        balance = float(account.get("balance", 10000)) if account else 10000.0
        lot     = calculate_lot_size(
            balance, CONFIG["risk_pct"], entry, sl, symbol
        )

        # 9. Place order
        result = broker.place_order(symbol, direction, lot, sl, tp)
        if not result or "retcode" not in result:
            log_error(f"  [{symbol}] Trade placement failed: {result}")
            return

        # 10. Register in trade lifecycle (trail, PnL, webapp push)
        trade_id = open_trade_record(signal, broker)
        if trade_id:
            # Backfill lot size into the lifecycle record
            with _TRADE_LOCK:
                if trade_id in _ACTIVE_TRADES:
                    _ACTIVE_TRADES[trade_id]["lot"] = lot

        # 11. Persist signal record
        signals = _load_json(CONFIG["signals_file"], [])
        signals.append(signal.to_dict())
        _save_json(CONFIG["signals_file"], signals[-200:])

        msg = (
            f"EXECUTED {direction.value} {symbol} | "
            f"lot={lot} | entry={entry:.5f} | "
            f"SL={sl:.5f} | TP={tp:.5f} | RR={rr:.2f}"
        )
        log_trade(msg)
        send_telegram(msg)

    except Exception as e:
        log_error(f"execute_trade_pipeline error [{symbol}]: {e}")
        if "--debug" in sys.argv:
            traceback.print_exc()


# ════════════════════════════════════════════════════════════════════
#  TRADE LIFECYCLE  — open / update (trailing) / close
#
#  These functions wrap the existing execute_trade_pipeline with a
#  clean lifecycle layer that:
#    • Maintains an in-memory active_trades dict (also pushed to webapp)
#    • Applies small-account trailing stop logic on every scan tick
#    • Checks the MT5 bridge health before attempting direct execution
#    • Routes price feed through MT5 bridge → Yahoo Finance (fallback)
#  Compatible with MetaAPI, YFINANCE_MT5, OANDA, and PAPER modes.
# ════════════════════════════════════════════════════════════════════

# ── Runtime state (in-memory, not persisted between restarts) ─────────
_active_trades:  Dict[str, Dict] = {}   # trade_id → trade dict
_trade_history:  List[Dict]      = []   # closed trades this session

# ── Small-account threshold (USD balance) ─────────────────────────────
SMALL_ACCOUNT_LIMIT: float = 50.0


# ── Price feed: MT5 bridge → Yahoo Finance fallback ───────────────────

def _get_bridge_url() -> str:
    """Return the MT5 local bridge base URL from CONFIG (default localhost:8000)."""
    return CONFIG.get("mt5_bridge_url", "http://localhost:8000")


def _is_bridge_online() -> bool:
    """
    Check if the MT5 local bridge (WyckoffSNR_EA HTTP server) is reachable.
    Fast 2-second timeout — called before every bridge price fetch.
    """
    try:
        if REQUESTS_OK:
            r = req_lib.get(f"{_get_bridge_url()}/health", timeout=2)
            return r.status_code == 200
        else:
            url = f"{_get_bridge_url()}/health"
            req = Request(url)
            res = urlopen(req, timeout=2)
            return res.status == 200
    except Exception:
        return False


def _get_price_from_bridge(symbol: str) -> Optional[Dict]:
    """
    Fetch live bid/ask from the MT5 bridge EA HTTP endpoint.
    Returns {"bid": float, "ask": float} or None if bridge is offline.
    Expected bridge endpoint: GET /price?symbol=EURUSD
    """
    try:
        mt5_sym = symbol.replace("_", "")   # EUR_USD → EURUSD
        url = f"{_get_bridge_url()}/price?symbol={mt5_sym}"
        if REQUESTS_OK:
            r = req_lib.get(url, timeout=3)
            if r.status_code == 200:
                data = r.json()
                return {
                    "bid": float(data["bid"]),
                    "ask": float(data["ask"]),
                }
        else:
            req = Request(url)
            res = urlopen(req, timeout=3)
            data = json.loads(res.read().decode())
            return {
                "bid": float(data["bid"]),
                "ask": float(data["ask"]),
            }
    except Exception:
        return None


def _get_price_from_yfinance(symbol: str) -> Optional[Dict]:
    """
    Fetch live price from Yahoo Finance as fallback when bridge is offline.
    Uses the last close of the M15 bar as a proxy for mid-price.
    """
    try:
        yf_broker = YFinanceBroker()
        tick = yf_broker.get_tick(symbol)
        return tick   # already {"bid": ..., "ask": ...}
    except Exception:
        return None


def get_current_price(symbol: str, broker: Optional[BrokerBase] = None) -> Optional[Dict]:
    """
    Unified price feed with automatic fallback chain:
      1. MT5 bridge (real-time tick via local EA HTTP server)
      2. Broker.get_tick()  (MetaAPI / OANDA live tick)
      3. Yahoo Finance M15 close (free, ~15-min delayed)

    Returns {"bid": float, "ask": float} or None if all sources fail.
    """
    # 1. MT5 bridge — fastest, real-time if EA is running locally
    if _is_bridge_online():
        tick = _get_price_from_bridge(symbol)
        if tick:
            log_info(f"  [{symbol}] Price via MT5 bridge: "
                     f"bid={tick['bid']:.5f} ask={tick['ask']:.5f}")
            return tick

    # 2. Broker tick (MetaAPI / OANDA)
    if broker is not None:
        try:
            tick = broker.get_tick(symbol)
            if tick:
                log_info(f"  [{symbol}] Price via broker tick: "
                         f"bid={tick['bid']:.5f} ask={tick['ask']:.5f}")
                return tick
        except Exception:
            pass

    # 3. Yahoo Finance fallback
    tick = _get_price_from_yfinance(symbol)
    if tick:
        log_info(f"  [{symbol}] Price via Yahoo Finance (fallback): "
                 f"bid={tick['bid']:.5f} ask={tick['ask']:.5f}")
        return tick

    log_warn(f"  [{symbol}] All price sources failed — cannot get current price.")
    return None


def _get_account_balance(broker: Optional[BrokerBase] = None) -> float:
    """
    Get current account balance from broker.
    Falls back to last known balance from EQUITY_STATE.
    """
    if broker is not None:
        try:
            info = broker.get_account_info()
            if info:
                return float(info.get("balance", 10000.0))
        except Exception:
            pass
    # Fallback: last known balance
    eq = EQUITY_STATE.get("equity", [])
    if eq:
        return float(eq[-1].get("balance", 10000.0))
    return 10000.0


# ── Trade open ────────────────────────────────────────────────────────

def open_trade(signal_obj, broker: Optional[BrokerBase] = None) -> Optional[str]:
    """
    Open a new trade from a scored Signal object.

    1. Fetches live price via unified get_current_price()
    2. Executes via MT5 bridge if online, else via broker.place_order()
    3. Registers the trade in _active_trades and pushes to webapp

    Returns trade_id string on success, None on failure.
    Wraps execute_trade_pipeline — do NOT call both for the same signal.
    """
    symbol    = signal_obj.symbol
    direction = signal_obj.direction.value   # "BUY" or "SELL"
    sl        = signal_obj.sl
    tp        = signal_obj.tp

    # Live price
    tick = get_current_price(symbol, broker)
    if not tick:
        log_warn(f"  [{symbol}] open_trade: no price available, aborting.")
        return None

    entry = tick["ask"] if direction == "BUY" else tick["bid"]

    trade_id = f"{symbol}_{int(time.time())}"
    trade: Dict = {
        "id":        trade_id,
        "symbol":    symbol,
        "direction": direction,
        "entry":     round(entry, 5),
        "sl":        round(sl, 5),
        "tp":        round(tp, 5),
        "lot":       signal_obj.lot if hasattr(signal_obj, "lot") else 0.01,
        "status":    "OPEN",
        "open_time": datetime.now().isoformat(),
        "profit":    0.0,
    }

    # Execution: bridge → broker.place_order() → SIGNAL_QUEUED
    executed = False
    if _is_bridge_online():
        try:
            bridge_payload = {
                "symbol":    symbol.replace("_", ""),
                "direction": direction,
                "lot":       trade["lot"],
                "sl":        sl,
                "tp":        tp,
            }
            if REQUESTS_OK:
                r = req_lib.post(
                    f"{_get_bridge_url()}/trade",
                    json=bridge_payload, timeout=5
                )
                executed = r.status_code == 200
            else:
                data = json.dumps(bridge_payload).encode()
                req  = Request(
                    f"{_get_bridge_url()}/trade",
                    data=data,
                    headers={"Content-Type": "application/json"}
                )
                urlopen(req, timeout=5)
                executed = True
            if executed:
                log_trade(f"[BRIDGE] {direction} {symbol} "
                          f"entry={entry:.5f} sl={sl:.5f} tp={tp:.5f}")
        except Exception as e:
            log_warn(f"  [{symbol}] Bridge execution failed: {e} — "
                     f"falling back to broker.place_order()")

    if not executed and broker is not None:
        from dataclasses import fields as dc_fields
        try:
            result = broker.place_order(
                symbol, signal_obj.direction, trade["lot"], sl, tp
            )
            executed = result is not None
        except Exception as e:
            log_error(f"  [{symbol}] broker.place_order failed: {e}")

    if not executed:
        log_warn(f"  [{symbol}] open_trade: no execution path succeeded.")
        return None

    _active_trades[trade_id] = trade
    push_all_data({
        "signals":  [],
        "analysis": [],
        "trades":   list(_active_trades.values()),
        "metrics":  compute_metrics(_trade_history),
        "event":    {"type": "TRADE_OPEN", "trade": trade},
    })
    log_trade(f"[OPEN] {trade_id} | {direction} {symbol} "
              f"entry={entry:.5f} sl={sl:.5f} tp={tp:.5f}")
    return trade_id


# ── Trade update — trailing stop ──────────────────────────────────────

def update_trade(trade: Dict, broker: Optional[BrokerBase] = None) -> None:
    """
    Called every scan tick for each open trade.

    Applies small-account trailing stop:
      • Triggers after profit_move > 0.5 pips (adjustable)
      • Trail distance = 0.5× ATR of the symbol on M15
        (falls back to 5-pip fixed trail if ATR unavailable)
      • Trailing only moves SL in the profit direction (never back)

    Also updates floating profit on the trade dict and pushes
    a TRAIL_UPDATE event to the webapp when SL moves.
    """
    symbol    = trade["symbol"]
    direction = trade["direction"]

    tick = get_current_price(symbol, broker)
    if not tick:
        return

    # Mid price for P&L estimate
    mid_price  = (tick["bid"] + tick["ask"]) / 2.0
    entry      = trade.get("entry", mid_price)

    profit_move = (mid_price - entry) if direction == "BUY" \
                  else (entry - mid_price)

    # Update floating profit
    lot        = trade.get("lot", 0.01)
    pip_value  = 10
    trade["profit"] = round(profit_move * lot * pip_value, 2)

    # ── Small-account trailing stop ───────────────────────────────────
    balance = _get_account_balance(broker)
    if balance < SMALL_ACCOUNT_LIMIT and profit_move > 0.0005:
        # Dynamic trail: 0.5× M15 ATR, fallback to 5 pips
        try:
            yf = YFinanceBroker()
            df_m15 = yf.get_candles(symbol, "M15", 30)
            trail_distance = calc_atr(df_m15, 14) * 0.5 if df_m15 is not None \
                             else 0.0005
        except Exception:
            trail_distance = 0.0005   # 5 pips fallback

        sl_moved = False
        if direction == "BUY":
            new_sl = mid_price - trail_distance
            if new_sl > trade["sl"]:
                trade["sl"] = round(new_sl, 5)
                sl_moved = True
        else:
            new_sl = mid_price + trail_distance
            if new_sl < trade["sl"]:
                trade["sl"] = round(new_sl, 5)
                sl_moved = True

        if sl_moved:
            log_trade(f"[TRAIL] {trade['id']} | new SL={trade['sl']:.5f} "
                      f"| profit_move={profit_move:.5f} "
                      f"| trail_dist={trail_distance:.5f}")
            push_all_data({
                "signals": [], "analysis": [],
                "trades":  list(_active_trades.values()),
                "metrics": compute_metrics(_trade_history),
                "event": {
                    "type":     "TRAIL_UPDATE",
                    "trade_id": trade["id"],
                    "new_sl":   trade["sl"],
                },
            })


# ── Trade close ───────────────────────────────────────────────────────

def close_trade(trade_id: str, reason: str = "manual",
                broker: Optional[BrokerBase] = None) -> None:
    """
    Close an open trade by ID.

    Marks trade CLOSED, moves it to _trade_history,
    removes from _active_trades, pushes TRADE_CLOSE event to webapp.
    Does NOT send a close order to the broker — MetaAPI/MT5 manages
    SL/TP exits natively. Call this when you detect a position is gone.
    """
    trade = _active_trades.get(trade_id)
    if not trade:
        log_warn(f"close_trade: trade_id {trade_id} not found in active trades.")
        return

    trade["status"]       = "CLOSED"
    trade["close_time"]   = datetime.now().isoformat()
    trade["close_reason"] = reason

    _trade_history.append(trade)
    del _active_trades[trade_id]

    # Persist to trade history file
    history = _load_json(CONFIG["trades_file"], [])
    history.append(trade)
    _save_json(CONFIG["trades_file"], history[-200:])

    push_all_data({
        "signals": [], "analysis": [],
        "trades":  list(_active_trades.values()),
        "metrics": compute_metrics(_trade_history),
        "event":   {"type": "TRADE_CLOSE", "trade": trade},
    })
    log_trade(f"[CLOSE] {trade_id} | reason={reason} | profit={trade.get('profit', 0):.2f}")


# ── SL/TP hit detection — called in the scan loop ─────────────────────

def check_trade_exits(broker: Optional[BrokerBase] = None) -> None:
    """
    For each active trade, fetch current price and check if SL or TP
    has been hit. Auto-closes the trade if so.

    This matters mainly for PAPER mode and YFINANCE_MT5 where the broker
    doesn't push close events back to the bot automatically.
    For MetaAPI/OANDA the broker's own SL/TP system handles this,
    but running this check adds a safety net.
    """
    for trade_id in list(_active_trades.keys()):
        trade     = _active_trades[trade_id]
        symbol    = trade["symbol"]
        direction = trade["direction"]
        sl        = trade.get("sl", 0.0)
        tp        = trade.get("tp", 0.0)

        tick = get_current_price(symbol, broker)
        if not tick:
            continue

        price = tick["bid"] if direction == "BUY" else tick["ask"]

        if direction == "BUY":
            if price <= sl:
                close_trade(trade_id, "SL_HIT", broker)
            elif price >= tp:
                close_trade(trade_id, "TP_HIT", broker)
        else:
            if price >= sl:
                close_trade(trade_id, "SL_HIT", broker)
            elif price <= tp:
                close_trade(trade_id, "TP_HIT", broker)


def run():
    print_banner()

    # ── Hard internet check before doing anything ─────────────────
    mode = CONFIG.get("broker_mode", "PAPER").upper()
    needs_internet = mode in ("YFINANCE_MT5", "YFINANCE", "METAAPI", "OANDA")
    if needs_internet:
        log_info(f"Broker mode requires internet ({mode}). Checking connection...")
        require_internet(retry_secs=30)
    else:
        log_info("PAPER mode — internet not required.")

    broker = create_broker()

    # ── Connection test ───────────────────────────────────────────
    info = broker.get_account_info()
    if info:
        bal = info.get("balance", "N/A")
        log_info(f"Connected. Balance: {bal}")
    else:
        if mode not in ("PAPER", "YFINANCE", "YFINANCE_MT5"):
            # For REST brokers (MetaAPI/OANDA), bad credentials → halt
            log_error("Cannot connect to broker. Check credentials. Halting.")
            sys.exit(1)

    all_signals: List[Dict] = _load_json(CONFIG["signals_file"], [])
    scan_count = 0

    run_main_loop(broker)

def run_main_loop(broker):
    """Main scan loop — separated for clean restart and crash recovery."""
    mode           = CONFIG.get("broker_mode", "PAPER").upper()
    needs_internet = mode in ("YFINANCE_MT5", "YFINANCE", "METAAPI", "OANDA")
    all_signals: List[Dict] = _load_json(CONFIG["signals_file"], [])
    scan_count = 0

    while True:
        try:
            require_internet()
            update_equity(broker)

            # ── Sync lifecycle state with broker ──────────────────────
            sync_lifecycle_with_broker(broker)

            # ── Update all open trades (trailing, SL/TP check) ────────
            for tid in list(_ACTIVE_TRADES.keys()):
                update_trade_record(tid, broker)

            open_pos = broker.get_open_positions()
            n_open   = len(open_pos) if isinstance(open_pos, list) else 0
            log_info(f"Open positions: {n_open} / {CONFIG['max_trades']}")
            # NOTE: We always continue scanning for bias/confluence/entry analysis
            # even when max_trades is reached — we just skip the order placement.
            can_trade = n_open < CONFIG["max_trades"]
            if not can_trade:
                log_info("Max trades active — scanning for analysis only (no new orders).")

            for symbol in CONFIG["symbols"]:
                log_info(f"Scanning {symbol}...")
                try:
                    df_w  = broker.get_candles(symbol, CONFIG["tf_weekly"],  50)
                    df_d  = broker.get_candles(symbol, CONFIG["tf_daily"],   200)
                    df_h4 = broker.get_candles(symbol, CONFIG["tf_h4"],      200)
                    df_h1 = broker.get_candles(symbol, CONFIG["tf_h1"],      100)

                    if df_d is None or df_h4 is None or df_w is None:
                        log_warn(f"  {symbol}: No data returned, skipping")
                        continue

                    # ── LAYER 1: Chart Analysis (H4 + H1) ────────
                    log_info(f"  [{symbol}] LAYER 1: Chart Analysis...")
                    h4_report = run_chart_analysis(symbol, df_h4, "H4")
                    h1_report = run_chart_analysis(symbol, df_h1, "H1") \
                                if df_h1 is not None else {}
                    print_chart_analysis(h4_report)
                    print_chart_analysis(h1_report)

                    # ── LAYER 2: Adaptive Bias Cascade ────────────
                    log_info(f"  [{symbol}] LAYER 2: Bias Cascade "
                             f"(H4->H2->H1->M45->M30->M15)...")
                    bias_dir, bias_tf, bias_score, bias_reasons = \
                        find_bias_cascade(symbol, broker)
                    log_info(f"  [{symbol}] Bias: {bias_dir} "
                             f"(TF={bias_tf}, votes={bias_score})")

                    # ── LAYER 3: Confluence Analysis (H4) ─────────
                    log_info(f"  [{symbol}] LAYER 3: Confluence Analysis (H4)...")
                    conf = analyse_confluence(symbol, df_h4, bias_dir)
                    print_confluence(conf)

                    # ── LAYER 4: Entry Analysis (M30/M15) ─────────
                    log_info(f"  [{symbol}] LAYER 4: Entry Trigger (M30/M15)...")
                    entry_analysis = analyse_entry(
                        symbol, broker, bias_dir, conf.get("poi", 0.0)
                    )
                    print_entry(entry_analysis)

                    # ── LAYER 5: Full Signal Scoring ───────────────
                    signal = score_signal(
                        symbol, df_w, df_d, df_h4, df_h1,
                        bias_dir=bias_dir,
                        bias_tf=bias_tf,
                        bias_score=bias_score,
                    )

                    # ── Save analysis snapshot ─────────────────────
                    analysis_entry = {
                        "symbol":    symbol,
                        "timestamp": datetime.now().isoformat(),
                        "bias":      {"dir": bias_dir, "tf": bias_tf,
                                      "score": bias_score},
                        "confluence": conf,
                        "entry":      entry_analysis,
                        "H4":         h4_report,
                        "H1":         h1_report,
                        "signal":     signal.to_dict() if signal else None,
                    }
                    prev_analysis = _load_json(CONFIG["analysis_file"], [])
                    prev_analysis.append(analysis_entry)
                    _save_json(CONFIG["analysis_file"], prev_analysis[-50:])

                    if signal:
                        log_signal(
                            f"{signal.direction.value} {symbol} | "
                            f"Score:{signal.score} (W:{signal.score_w}+S:{signal.score_s}) | "
                            f"Pattern:{signal.pattern.value} | "
                            f"Bias:{bias_dir}@{bias_tf} | "
                            f"Confluence:{'OK' if conf['valid'] else 'WEAK'} | "
                            f"Entry:{'TRIGGER' if entry_analysis['valid'] else 'WAIT'}"
                        )
                        for r in signal.reasons:
                            log_info(f"    {r}")

                        sig_dict = signal.to_dict()
                        all_signals.append(sig_dict)
                        _save_json(CONFIG["signals_file"], all_signals[-100:])

                        # Only place trade if all layers align AND slot available
                        if not can_trade:
                            log_info(f"  [{symbol}] Signal valid but max trades "
                                     f"reached — queued for next open slot.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Signal valid but confluence "
                                     f"too weak (score={conf['score']}) — skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] Signal valid, confluence OK, "
                                     f"but no entry trigger yet — waiting...")
                        else:
                            # All three layers aligned — route through pipeline
                            execute_trade_pipeline(broker, signal)
                            can_trade = (
                                len(broker.get_open_positions() or [])
                                < CONFIG["max_trades"]
                            )
                    else:
                        log_info(f"  [{symbol}]: No valid signal "
                                 f"(score < {CONFIG['combined_min']})")

                except ConnectionError as e:
                    log_error(f"  [{symbol}] Internet connection lost: {e}")
                    log_warn("  Pausing all scans until internet is restored...")
                    require_internet(retry_secs=30)
                    log_info("  Internet restored — continuing scan cycle.")
                    break   # break inner symbol loop, outer loop will re-check
                except Exception as e:
                    log_error(f"  Error scanning {symbol}: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()

            scan_count += 1
            log_info(f"Scan #{scan_count} complete. Next scan in "
                     f"{CONFIG['scan_secs']}s...")
            responsive_sleep(CONFIG["scan_secs"])

        except KeyboardInterrupt:
            log_info("Bot stopped by user (Ctrl+C)")
            break
        except Exception as e:
            log_error(f"Main loop error: {e}")
            if "--debug" in sys.argv:
                traceback.print_exc()
            time.sleep(10)

# ════════════════════════════════════════════════════════════════════
#  HYBRID BOT REFACTOR — RENDER READY
#  Merges original bot logic with Flask web server + data pipeline.
#  Run locally:  python wyckoff_snr_bot_pydroid-4-1.py
#  Run on Render: set start command to the same; port 10000 is exposed.
# ════════════════════════════════════════════════════════════════════

# ── Flask (install: pip install flask) ───────────────────────────────
try:
    from flask import Flask, jsonify
    FLASK_OK = True
except ImportError:
    FLASK_OK = False
    print("[WARN] flask not found. Web server disabled.")
    print("       To fix: pip install flask")

# ── Requests already imported above as req_lib ───────────────────────

# ════════════════════════════════════════════════════════════════════
#  DATA PIPELINE — push snapshot to external webapp API
# ════════════════════════════════════════════════════════════════════

# Set this to your webapp API endpoint (or leave blank to disable push)
WEBAPP_API_URL = "https://project--d0c75dff-8649-4cc5-8dad-c578ae78b21c.lovable.app/api/public/bot-ingest"   # e.g. "https://your-webapp.onrender.com/api/bot/update"

def push_all_data(data: Dict) -> None:
    """
    POST a full bot snapshot to the external webapp API.
    Skipped silently if WEBAPP_API_URL is empty.
    """
    if not WEBAPP_API_URL:
        return
    payload = {
        "timestamp": datetime.now().isoformat(),
        "signals":   data.get("signals",  []),
        "analysis":  data.get("analysis", []),
        "trades":    data.get("trades",   []),
        "metrics":   data.get("metrics",  {}),
    }
    try:
        http_post(WEBAPP_API_URL, payload, timeout=10)
        log_info("Data push: OK")
    except Exception as e:
        log_warn(f"Data push failed: {e}")

# ════════════════════════════════════════════════════════════════════
#  ANALYTICS ENGINE — win-rate and PnL from trades list
# ════════════════════════════════════════════════════════════════════

def compute_metrics(trades: List[Dict]) -> Dict:
    """
    Compute win-rate and cumulative PnL from a list of trade dicts.
    Each trade dict should have a 'profit' key (float).
    """
    wins  = [t for t in trades if t.get("profit", 0) > 0]
    total = len(trades)
    return {
        "total_trades": total,
        "winrate":      round(len(wins) / total, 2) if total else 0.0,
        "pnl":          round(sum(t.get("profit", 0) for t in trades), 2),
    }

# ════════════════════════════════════════════════════════════════════
#  BOT LOOP — background engine (wraps original run_main_loop)
# ════════════════════════════════════════════════════════════════════

def run_bot_loop() -> None:
    """
    Background thread entry point for Render deployment.
    Replicates the startup sequence from run(), then enters
    run_main_loop_with_push() which adds data-pipeline calls.
    """
    print_banner()

    mode = CONFIG.get("broker_mode", "PAPER").upper()
    needs_internet = mode in ("YFINANCE_MT5", "YFINANCE", "METAAPI", "OANDA")
    if needs_internet:
        log_info(f"Broker mode requires internet ({mode}). Checking connection...")
        require_internet(retry_secs=30)
    else:
        log_info("PAPER mode — internet not required.")

    broker = create_broker()

    info = broker.get_account_info()
    if info:
        log_info(f"Connected. Balance: {info.get('balance', 'N/A')}")
    else:
        if mode not in ("PAPER", "YFINANCE", "YFINANCE_MT5"):
            log_error("Cannot connect to broker. Check credentials. Halting.")
            return   # thread exits cleanly instead of sys.exit

    run_main_loop_with_push(broker)


def run_main_loop_with_push(broker: BrokerBase) -> None:
    """
    Extended main loop: runs the full 5-layer analysis per symbol,
    then pushes a snapshot to the webapp API after every scan cycle.
    This is a drop-in replacement for run_main_loop() when running
    on Render (or any server where you want the data pipeline active).
    """
    mode           = CONFIG.get("broker_mode", "PAPER").upper()
    needs_internet = mode in ("YFINANCE_MT5", "YFINANCE", "METAAPI", "OANDA")
    all_signals: List[Dict] = _load_json(CONFIG["signals_file"], [])
    scan_count = 0

    while True:
        try:
            if needs_internet:
                require_internet()
            update_equity(broker)

            open_pos = broker.get_open_positions()
            n_open   = len(open_pos) if isinstance(open_pos, list) else 0
            log_info(f"Open positions: {n_open} / {CONFIG['max_trades']}")
            can_trade = n_open < CONFIG["max_trades"]
            if not can_trade:
                log_info("Max trades active — scanning for analysis only.")

            # ── Snapshot accumulator for this scan cycle ─────────────
            cycle_data: Dict = {
                "signals":  [],
                "analysis": [],
                "trades":   list(open_pos) if isinstance(open_pos, list) else [],
                "metrics":  {},
            }

            for symbol in CONFIG["symbols"]:
                log_info(f"Scanning {symbol}...")
                try:
                    df_w  = broker.get_candles(symbol, CONFIG["tf_weekly"],  50)
                    df_d  = broker.get_candles(symbol, CONFIG["tf_daily"],   200)
                    df_h4 = broker.get_candles(symbol, CONFIG["tf_h4"],      200)
                    df_h1 = broker.get_candles(symbol, CONFIG["tf_h1"],      100)

                    if df_d is None or df_h4 is None or df_w is None:
                        log_warn(f"  {symbol}: No data returned, skipping")
                        continue

                    # ── LAYER 1: Chart Analysis ───────────────────────
                    log_info(f"  [{symbol}] LAYER 1: Chart Analysis...")
                    h4_report = run_chart_analysis(symbol, df_h4, "H4")
                    h1_report = run_chart_analysis(symbol, df_h1, "H1") \
                                if df_h1 is not None else {}
                    print_chart_analysis(h4_report)
                    print_chart_analysis(h1_report)

                    # ── LAYER 2: Adaptive Bias Cascade ────────────────
                    log_info(f"  [{symbol}] LAYER 2: Bias Cascade...")
                    bias_dir, bias_tf, bias_score, bias_reasons = \
                        find_bias_cascade(symbol, broker)
                    log_info(f"  [{symbol}] Bias: {bias_dir} "
                             f"(TF={bias_tf}, votes={bias_score})")

                    # ── LAYER 3: Confluence Analysis (H4) ─────────────
                    log_info(f"  [{symbol}] LAYER 3: Confluence Analysis...")
                    conf = analyse_confluence(symbol, df_h4, bias_dir)
                    print_confluence(conf)

                    # ── LAYER 4: Entry Analysis (M30/M15) ─────────────
                    log_info(f"  [{symbol}] LAYER 4: Entry Trigger...")
                    entry_analysis = analyse_entry(
                        symbol, broker, bias_dir, conf.get("poi", 0.0)
                    )
                    print_entry(entry_analysis)

                    # ── LAYER 5: Full Signal Scoring ───────────────────
                    signal = score_signal(
                        symbol, df_w, df_d, df_h4, df_h1,
                        bias_dir=bias_dir,
                        bias_tf=bias_tf,
                        bias_score=bias_score,
                    )

                    # ── Save analysis snapshot ─────────────────────────
                    analysis_entry = {
                        "symbol":    symbol,
                        "timestamp": datetime.now().isoformat(),
                        "bias":      {"dir": bias_dir, "tf": bias_tf,
                                      "score": bias_score},
                        "confluence": conf,
                        "entry":      entry_analysis,
                        "H4":         h4_report,
                        "H1":         h1_report,
                        "signal":     signal.to_dict() if signal else None,
                    }
                    cycle_data["analysis"].append(analysis_entry)

                    prev_analysis = _load_json(CONFIG["analysis_file"], [])
                    prev_analysis.append(analysis_entry)
                    _save_json(CONFIG["analysis_file"], prev_analysis[-50:])

                    if signal:
                        log_signal(
                            f"{signal.direction.value} {symbol} | "
                            f"Score:{signal.score} (W:{signal.score_w}+S:{signal.score_s}) | "
                            f"Pattern:{signal.pattern.value} | "
                            f"Bias:{bias_dir}@{bias_tf} | "
                            f"Confluence:{'OK' if conf['valid'] else 'WEAK'} | "
                            f"Entry:{'TRIGGER' if entry_analysis['valid'] else 'WAIT'}"
                        )
                        for r in signal.reasons:
                            log_info(f"    {r}")

                        sig_dict = signal.to_dict()
                        all_signals.append(sig_dict)
                        cycle_data["signals"].append(sig_dict)
                        _save_json(CONFIG["signals_file"], all_signals[-100:])

                        if not can_trade:
                            log_info(f"  [{symbol}] Signal valid but max trades "
                                     f"reached — queued for next open slot.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Signal valid but confluence "
                                     f"too weak (score={conf['score']}) — skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] Signal valid, confluence OK, "
                                     f"but no entry trigger yet — waiting...")
                        else:
                            execute_trade_pipeline(broker, signal)
                            can_trade = (
                                len(broker.get_open_positions() or [])
                                < CONFIG["max_trades"]
                            )
                    else:
                        log_info(f"  [{symbol}]: No valid signal "
                                 f"(score < {CONFIG['combined_min']})")

                except ConnectionError as e:
                    log_error(f"  [{symbol}] Internet connection lost: {e}")
                    log_warn("  Pausing all scans until internet is restored...")
                    require_internet(retry_secs=30)
                    log_info("  Internet restored — continuing scan cycle.")
                    break
                except Exception as e:
                    log_error(f"  Error scanning {symbol}: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()

            # ── End of cycle: compute metrics + push data ─────────────
            cycle_data["metrics"] = compute_metrics(cycle_data["trades"])
            push_all_data(cycle_data)

            scan_count += 1
            log_info(f"Scan #{scan_count} complete. Next scan in "
                     f"{CONFIG['scan_secs']}s...")
            responsive_sleep(CONFIG["scan_secs"])

        except KeyboardInterrupt:
            log_info("Bot stopped by user (Ctrl+C)")
            break
        except Exception as e:
            log_error(f"Main loop error: {e}")
            if "--debug" in sys.argv:
                traceback.print_exc()
            time.sleep(10)

# ════════════════════════════════════════════════════════════════════
#  SESSION STATE — in-memory only, never written to disk or logs
#
#  Flow:
#    1. Webapp POSTs { metaapi_token, metaapi_account } to /session/start
#       along with X-Bot-Secret header matching BOT_SECRET env var.
#    2. Bot loads MetaAPI broker into ACTIVE_SESSION, begins trading.
#    3. Webapp POSTs to /session/end → credentials wiped, broker
#       reverts to dormant PAPER mode, trading stops immediately.
#
#  Security model:
#    • BOT_SECRET is a shared secret set as an env var on Render.
#      Your webapp reads the same secret from its own env and sends
#      it as the X-Bot-Secret header. Never hardcode it here.
#    • Credentials live ONLY in ACTIVE_SESSION dict in RAM.
#      They are never logged, never written to any file.
#    • On session end, the dict is zeroed before deletion so the
#      token string doesn't linger in memory longer than needed.
# ════════════════════════════════════════════════════════════════════

import os as _os

# Set BOT_SECRET as an environment variable on Render.
# Your webapp must send this in the X-Bot-Secret header.
# If not set, session endpoints are disabled (safe default).
BOT_SECRET: str = _os.environ.get("BOT_SECRET", "")
API_KEY:    str = _os.environ.get("API_KEY", "your-secret
# Live session state — held in memory only
SESSION_ACTIVE = False
ACTIVE_SESSION: Dict = {
    "active":           False,
    "broker":           None,   # live MetaAPIBroker instance
    "metaapi_token":    "",     # wiped on session end
    "metaapi_account":  "",     # wiped on session end
    "started_at":       "",
    "user_id":          "",     # webapp user identifier (non-sensitive)
}

# Thread lock — session state is read by the bot loop thread
# and written by Flask threads; always acquire before touching it
_SESSION_LOCK = threading.Lock()


def _check_secret(request) -> bool:
    """
    Verify the X-Bot-Secret header matches BOT_SECRET.
    Returns False (reject) if BOT_SECRET is not configured —
    this prevents accidental open access on a misconfigured deploy.
    """
    if not BOT_SECRET:
        log_warn("BOT_SECRET env var not set — session endpoints disabled.")
        return False
    return request.headers.get("X-Bot-Secret", "") == BOT_SECRET


def _get_active_broker() -> Optional[BrokerBase]:
    """
    Thread-safe read of the active broker from session state.
    Returns None if no session is active.
    """
    with _SESSION_LOCK:
        if ACTIVE_SESSION["active"] and ACTIVE_SESSION["broker"] is not None:
            return ACTIVE_SESSION["broker"]
    return None


def _wipe_session() -> None:
    """
    Zero out credentials and reset session to dormant state.
    Called on /session/end and on any fatal broker error.
    """
    with _SESSION_LOCK:
        # Overwrite token strings before clearing — reduces RAM residency
        ACTIVE_SESSION["metaapi_token"]   = "0" * len(ACTIVE_SESSION.get("metaapi_token", ""))
        ACTIVE_SESSION["metaapi_account"] = "0" * len(ACTIVE_SESSION.get("metaapi_account", ""))
        ACTIVE_SESSION["active"]          = False
        ACTIVE_SESSION["broker"]          = None
        ACTIVE_SESSION["metaapi_token"]   = ""
        ACTIVE_SESSION["metaapi_account"] = ""
        ACTIVE_SESSION["started_at"]      = ""
        ACTIVE_SESSION["user_id"]         = ""
    log_info("Session ended — credentials wiped from memory.")


# ════════════════════════════════════════════════════════════════════
#  SESSION-AWARE BOT LOOP
#  Replaces run_bot_loop() when running on Render.
#  Idles (PAPER mode) until a session is started, then uses the
#  live MetaAPI broker injected by /session/start.
# ════════════════════════════════════════════════════════════════════

def run_session_bot_loop() -> None:
    """
    Continuous loop that:
      - Idles in PAPER mode when no session is active
      - Switches to live MetaAPI trading when session is started
      - Automatically reverts to idle on session end or broker error
    The Flask thread writes ACTIVE_SESSION; this thread reads it.
    """
        log_info("Session bot loop started — scanning signals always...")
    signal_broker = YFinanceBroker()   # always-on broker for signal scanning

    all_signals: List[Dict] = _load_json(CONFIG["signals_file"], [])
    scan_count = 0

    while True:
        try:
            # ── Determine mode for this cycle ─────────────────────────
            live_broker = _get_active_broker()

            if live_broker is not None and SESSION_ACTIVE:
                broker = live_broker   # live trading with MT5
                log_info("[LIVE] Session active — using MetaAPI broker")
            else:
                broker = signal_broker   # signal-only using Yahoo Finance
                log_info("[SIGNAL_ONLY] No active session — scanning signals only")

            # ── Always scan ───────────────────────────────────────────
            require_internet()
            if SESSION_ACTIVE:
                update_equity(broker)

            # ── Sync lifecycle state with broker ──────────────────────
            sync_lifecycle_with_broker(broker)

            # ── Update all open trades (trailing, SL/TP check) ────────
            for tid in list(_ACTIVE_TRADES.keys()):
                update_trade_record(tid, broker)

            open_pos  = broker.get_open_positions()
            n_open    = len(open_pos) if isinstance(open_pos, list) else 0
            log_info(f"[SESSION] Open positions: {n_open} / {CONFIG['max_trades']}")
            can_trade = n_open < CONFIG["max_trades"]

            cycle_data: Dict = {
                "signals":  [],
                "analysis": [],
                "trades":   list(open_pos) if isinstance(open_pos, list) else [],
                "metrics":  {},
            }

            for symbol in CONFIG["symbols"]:
                # Re-check session on every symbol — end can happen mid-scan
                if _get_active_broker() is None:
                    log_info("[SESSION] Session ended mid-scan — stopping cycle.")
                    break

                log_info(f"[SESSION] Scanning {symbol}...")
                try:
                    df_w  = broker.get_candles(symbol, CONFIG["tf_weekly"],  50)
                    df_d  = broker.get_candles(symbol, CONFIG["tf_daily"],   200)
                    df_h4 = broker.get_candles(symbol, CONFIG["tf_h4"],      200)
                    df_h1 = broker.get_candles(symbol, CONFIG["tf_h1"],      100)

                    if df_d is None or df_h4 is None or df_w is None:
                        log_warn(f"  {symbol}: No data, skipping")
                        continue

                    # LAYER 1
                    h4_report = run_chart_analysis(symbol, df_h4, "H4")
                    h1_report = run_chart_analysis(symbol, df_h1, "H1") \
                                if df_h1 is not None else {}
                    print_chart_analysis(h4_report)
                    print_chart_analysis(h1_report)

                    # LAYER 2
                    bias_dir, bias_tf, bias_score, bias_reasons = \
                        find_bias_cascade(symbol, broker)
                    log_info(f"  [{symbol}] Bias: {bias_dir} "
                             f"(TF={bias_tf}, votes={bias_score})")

                    # LAYER 3
                    conf = analyse_confluence(symbol, df_h4, bias_dir)
                    print_confluence(conf)

                    # LAYER 4
                    entry_analysis = analyse_entry(
                        symbol, broker, bias_dir, conf.get("poi", 0.0)
                    )
                    print_entry(entry_analysis)

                    # LAYER 5
                    signal = score_signal(
                        symbol, df_w, df_d, df_h4, df_h1,
                        bias_dir=bias_dir,
                        bias_tf=bias_tf,
                        bias_score=bias_score,
                    )

                    analysis_entry = {
                        "symbol":    symbol,
                        "timestamp": datetime.now().isoformat(),
                        "bias":      {"dir": bias_dir, "tf": bias_tf,
                                      "score": bias_score},
                        "confluence": conf,
                        "entry":      entry_analysis,
                        "H4":         h4_report,
                        "H1":         h1_report,
                        "signal":     signal.to_dict() if signal else None,
                    }
                    cycle_data["analysis"].append(analysis_entry)
                    prev_analysis = _load_json(CONFIG["analysis_file"], [])
                    prev_analysis.append(analysis_entry)
                    _save_json(CONFIG["analysis_file"], prev_analysis[-50:])

                    if signal:
                        log_signal(
                            f"{signal.direction.value} {symbol} | "
                            f"Score:{signal.score} | "
                            f"Pattern:{signal.pattern.value} | "
                            f"Bias:{bias_dir}@{bias_tf}"
                        )
                        sig_dict = signal.to_dict()
                        all_signals.append(sig_dict)
                        cycle_data["signals"].append(sig_dict)
                        _save_json(CONFIG["signals_file"], all_signals[-100:])

                        # Always push signal to webapp regardless of mode
                        sig_dict = signal.to_dict()
                        push_all_data({
                            "signals":  [sig_dict],
                            "analysis": [analysis_entry],
                            "trades":   [],
                            "metrics":  {},
                            "event":    {"type": "NEW_SIGNAL", "signal": sig_dict},
                        })

                        if not SESSION_ACTIVE:
                            # Signal-only mode — no trade execution
                            log_info(f"  [{symbol}] SIGNAL_ONLY — pushed to webapp, no trade placed.")
                        elif not can_trade:
                            log_info(f"  [{symbol}] Max trades reached — queued.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Confluence weak — skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] No entry trigger yet — waiting.")
                        else:
                            execute_trade_pipeline(broker, signal)
                            can_trade = (
                                len(broker.get_open_positions() or [])
                                < CONFIG["max_trades"]
                            )
                    else:
                        log_info(f"  [{symbol}]: No valid signal "
                                 f"(score < {CONFIG['combined_min']})")

                except ConnectionError as e:
                    log_error(f"  [{symbol}] Connection lost: {e}")
                    require_internet(retry_secs=30)
                    break
                except Exception as e:
                    log_error(f"  Error scanning {symbol}: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()

            cycle_data["metrics"] = compute_metrics(cycle_data["trades"])
            push_all_data(cycle_data)

            scan_count += 1
            log_info(f"[SESSION] Scan #{scan_count} done. "
                     f"Next in {CONFIG['scan_secs']}s...")
            responsive_sleep(CONFIG["scan_secs"])

        except KeyboardInterrupt:
            log_info("Bot stopped by user.")
            break
        except Exception as e:
            log_error(f"Session loop error: {e}")
            if "--debug" in sys.argv:
                traceback.print_exc()
            time.sleep(10)


# ════════════════════════════════════════════════════════════════════
#  WEB SERVER — Flask endpoints
# ════════════════════════════════════════════════════════════════════

if FLASK_OK:
    from flask import Flask, jsonify, request as flask_request
    app = Flask(__name__)

    # ── Public endpoints ─────────────────────────────────────────────

    @app.route("/")
    def home():
        with _SESSION_LOCK:
            active = ACTIVE_SESSION["active"]
            user   = ACTIVE_SESSION["user_id"]
        return jsonify({
            "status":       "BOT_RUNNING",
            "version":      "WyckoffSNR-2.0",
            "session":      "ACTIVE" if active else "IDLE",
            "session_user": user if active else None,
        })

    @app.route("/health")
    def health():
        return jsonify({"status": "healthy"})

    @app.route("/signals")
    def signals():
        data = _load_json(CONFIG["signals_file"], [])
        return jsonify({"signals": data[-20:]})

    @app.route("/analysis")
    def analysis():
        data = _load_json(CONFIG["analysis_file"], [])
        return jsonify({"analysis": data[-10:]})

    @app.route("/trades")
    def trades():
        """Return currently open trades from the lifecycle manager."""
        with _TRADE_LOCK:
            data = list(_ACTIVE_TRADES.values())
        return jsonify({"trades": data, "count": len(data)})

    @app.route("/trades/history")
    def trades_history():
        """Return last 50 closed trades from the lifecycle manager."""
        return jsonify({"history": _TRADE_HISTORY[-50:],
                        "count":   len(_TRADE_HISTORY)})

    # ── Session endpoints (protected by X-Bot-Secret header) ─────────
    @app.route("/start-session", methods=["POST"])
    def start_session():
        global SESSION_ACTIVE

        if flask_request.json.get("api_key") != API_KEY:
            return jsonify({"error": "unauthorized"}), 401

        SESSION_ACTIVE = True

        body    = flask_request.get_json(silent=True) or {}
        token   = body.get("metaapi_token",   "").strip()
        account = body.get("metaapi_account", "").strip()
        user_id = body.get("user_id",         "unknown")

        Required header:
          X-Bot-Secret: <BOT_SECRET env var value>

        The bot immediately constructs a MetaAPIBroker with these
        credentials and begins live scanning on the next cycle.
        Credentials are held in RAM only — never written to disk.
        """
        if not _check_secret(flask_request):
            log_warn("Session start rejected — invalid or missing X-Bot-Secret.")
            return jsonify({"error": "Unauthorized"}), 401

        body = flask_request.get_json(silent=True) or {}
        token   = body.get("metaapi_token",   "").strip()
        account = body.get("metaapi_account", "").strip()
        user_id = body.get("user_id",         "unknown")

        if not token or not account:
            return jsonify({"error": "metaapi_token and metaapi_account are required"}), 400

        # Build broker and do a quick connection test before accepting
        try:
            new_broker = MetaAPIBroker(
                token      = token,
                account_id = account,
                base_url   = CONFIG.get(
                    "metaapi_url",
                    "https://mt-client-api-v1.london.agiliumtrade.ai"
                ),
            )
            info = new_broker.get_account_info()
            if info is None:
                # Credentials rejected by MetaAPI
                # Zero local vars before returning
                token   = "0" * len(token)
                account = "0" * len(account)
                log_warn(f"Session start failed — MetaAPI rejected credentials "
                         f"for user_id={user_id}")
                return jsonify({
                    "error": "Could not connect to MetaAPI. "
                             "Check token and account ID."
                }), 403
        except Exception as e:
            log_error(f"Session start error: {e}")
            return jsonify({"error": "Broker connection error"}), 500

        # Credentials verified — store session
        with _SESSION_LOCK:
            if ACTIVE_SESSION["active"]:
                # Existing session: wipe old credentials first
                _wipe_session()

            ACTIVE_SESSION["active"]          = True
            ACTIVE_SESSION["broker"]          = new_broker
            ACTIVE_SESSION["metaapi_token"]   = token
            ACTIVE_SESSION["metaapi_account"] = account
            ACTIVE_SESSION["started_at"]      = datetime.now().isoformat()
            ACTIVE_SESSION["user_id"]         = user_id

        balance = info.get("balance", "N/A")
        log_info(f"Session started — user_id={user_id} | balance={balance}")
        # NOTE: token and account are intentionally omitted from this log

        return jsonify({
            "status":     "SESSION_STARTED",
            "user_id":    user_id,
            "started_at": ACTIVE_SESSION["started_at"],
            "balance":    balance,
        }), 200

        @app.route("/stop-session", methods=["POST"])
        def stop_session():
        global SESSION_ACTIVE
        SESSION_ACTIVE = False
        _wipe_session()
        return jsonify({"status": "session stopped"}), 200

    @app.route("/session/status", methods=["GET"])
    def session_status():
        """
        Lightweight status check — safe to poll from webapp.
        Returns session state WITHOUT any credential details.

        Required header:
          X-Bot-Secret: <BOT_SECRET env var value>
        """
        if not _check_secret(flask_request):
            return jsonify({"error": "Unauthorized"}), 401

        with _SESSION_LOCK:
            return jsonify({
                "active":     ACTIVE_SESSION["active"],
                "user_id":    ACTIVE_SESSION["user_id"],
                "started_at": ACTIVE_SESSION["started_at"],
            }), 200


# ════════════════════════════════════════════════════════════════════
#  ENTRY POINT
#  • Local / Pydroid:  python wyckoff_snr_bot_pydroid-4-1.py
#    → runs original run() — no Flask, no session logic
#  • Render server:    env vars RENDER or PORT are set automatically
#    → session-aware bot loop in background thread
#    → Flask serves on port 10000
#
#  Required Render env vars:
#    BOT_SECRET    — shared secret between this bot and your webapp
#    WEBAPP_API_URL — (optional) your webapp's push endpoint
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    render_env = _os.environ.get("RENDER") or _os.environ.get("PORT")

    if render_env and FLASK_OK:
        log_info("Render environment detected — starting hybrid session mode.")
        log_info("Bot loop     → background thread (idle until session start)")
        log_info("Flask server → port 10000")

        if not BOT_SECRET:
            log_warn("=" * 60)
            log_warn("  WARNING: BOT_SECRET env var is not set.")
            log_warn("  Session endpoints (/session/start, /session/end)")
            log_warn("  are DISABLED until BOT_SECRET is configured.")
            log_warn("  Set it in Render dashboard → Environment → Add var.")
            log_warn("=" * 60)

        bot_thread = threading.Thread(
            target=run_session_bot_loop, name="BotLoop"
        )
        bot_thread.daemon = True
        bot_thread.start()

        port = int(_os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port)

    else:
        # Local / Pydroid — original behaviour, no session logic
        run()
