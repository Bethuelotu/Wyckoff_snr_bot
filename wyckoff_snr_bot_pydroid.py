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
║  │               update_trade_record() - trailing stop     │       ║
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
import threading as _dthread
import queue     as _dqueue
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
#  LOGGING - Android/Pydroid safe (no colorama, plain text)
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
#  INTERNET CONNECTIVITY  - hard gate, no silent fallback
# ════════════════════════════════════════════════════════════════════

# Tracks live internet state - checked at startup and every scan
_INTERNET_OK: bool = False

# Lightweight hosts to ping - tries each in order
_PING_HOSTS = [
    ("8.8.8.8",         53),   # Google DNS
    ("1.1.1.1",         53),   # Cloudflare DNS
    ("query1.finance.yahoo.com", 443),  # Yahoo Finance directly
]

# ════════════════════════════════════════════════════════════════════
#  GLOBAL STATES - Equity, Daily Loss, Cooldown
# ════════════════════════════════════════════════════════════════════
EQUITY_STATE: Dict = {"equity": [], "last_balance": 10000.0}
DAILY_STATE:  Dict = {"date": datetime.now().date(), "loss": 0.0}
LAST_TRADE_TIME: Dict = {}

def check_internet(timeout: int = 5) -> bool:
    """
    Try to open a TCP socket to known public hosts.
    Returns True only if at least one host is reachable.
    Works on Android/Pydroid - no ICMP ping needed.
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



# ════════════════════════════════════════════════════════════════════
#  MARKET HOURS & SESSION FILTER
#  Forex is closed Saturday 22:00 UTC to Sunday 22:00 UTC.
#  Synthetic indices (Deriv V10/V25 etc) trade 24/7.
# ════════════════════════════════════════════════════════════════════

FOREX_SYMBOLS = {
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "AUD_USD",
    "USD_CAD", "NZD_USD", "EUR_GBP", "EUR_JPY", "GBP_JPY",
    "XAU_USD",
}

SYNTHETIC_SYMBOLS = {
    "VOLATILITY_10", "VOLATILITY_25", "VOLATILITY_50",
    "VOLATILITY_75", "VOLATILITY_100",
}

# Major Forex sessions (UTC hours)
FOREX_SESSIONS = {
    "SYDNEY":   {"open": 22, "close": 7},
    "TOKYO":    {"open": 0,  "close": 9},
    "LONDON":   {"open": 8,  "close": 17},
    "NEW_YORK": {"open": 13, "close": 22},
}


def is_forex_market_open() -> bool:
    """
    Returns True if Forex market is currently open.
    Market closes Friday 22:00 UTC, reopens Sunday 22:00 UTC.
    Also checks if at least one major session is active.
    """
    now     = datetime.utcnow()
    weekday = now.weekday()   # 0=Mon ... 4=Fri ... 5=Sat ... 6=Sun
    hour    = now.hour

    # Weekend close: Saturday after 22:00 UTC through Sunday before 22:00 UTC
    if weekday == 5 and hour >= 22:
        return False
    if weekday == 6 and hour < 22:
        return False

    # Check at least one session is active
    for name, sess in FOREX_SESSIONS.items():
        start, end = sess["open"], sess["close"]
        if start < end:
            if start <= hour < end:
                return True
        else:   # wraps midnight (Sydney 22:00-07:00)
            if hour >= start or hour < end:
                return True

    # Between sessions - market still technically open Mon-Fri
    # just no major session active (lower liquidity)
    if 0 <= weekday <= 4:
        return True

    return False


def is_synthetic_market_open() -> bool:
    """Synthetic indices on Deriv trade 24/7."""
    return True


def get_active_sessions() -> List[str]:
    """Return list of currently active Forex sessions."""
    now   = datetime.utcnow()
    hour  = now.hour
    active = []
    for name, sess in FOREX_SESSIONS.items():
        start, end = sess["open"], sess["close"]
        if start < end:
            if start <= hour < end:
                active.append(name)
        else:
            if hour >= start or hour < end:
                active.append(name)
    return active


def should_scan_symbol(symbol: str) -> bool:
    """
    Returns True if symbol should be scanned right now.
    Forex: only during market hours.
    Synthetics: always.
    """
    if symbol in SYNTHETIC_SYMBOLS:
        return True
    if symbol in FOREX_SYMBOLS:
        return is_forex_market_open()
    # Unknown symbol - allow by default
    return True


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
            print(f"  Retrying every {retry_secs}s - connect to WiFi/mobile data.")
            print("=" * 60)
            first_check = False
        log_warn(f"No internet - waiting {retry_secs}s before retry...")
        time.sleep(retry_secs)

# ════════════════════════════════════════════════════════════════════
#  CONFIGURATION  - Edit these values directly (no .env needed)
# ════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Broker Connection ─────────────────────────────────────────
    # RECOMMENDED SETUP:
    #   broker_mode = "YFINANCE_MT5"
    #   → Price data:   Yahoo Finance (free, real live prices, no account)
    #   → Trade execution: MT5 EA on your PC reads signal_queue.json
    #
    # Other modes:
    #   METAAPI  - MT5 via REST (metaapi.cloud)
    #   OANDA    - OANDA REST API
    #   PAPER    - synthetic data, no real trades
    #   YFINANCE - Yahoo Finance data, signal-only (no MT5)

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
    # Use a shared folder path - e.g. Google Drive, Dropbox, or LAN share.
    # On Android Pydroid, use a path your phone can write to.
    "mt5_signal_file": "signal_queue.json",   # bot writes here
    "mt5_magic":       20250422,              # EA magic number (any integer)

    # MT5 Bridge HTTP server (optional - EA runs a local HTTP server on PC)
    # When running: bot fetches live prices + sends orders via REST instead of file.
    # Leave as default if you are not running the bridge HTTP server.
    "mt5_bridge_url":  "http://localhost:8000",

    # ── Instruments ───────────────────────────────────────────────
    # Yahoo Finance symbol map - bot uses these internally.
    # MT5 symbols use standard broker names (set in EA below).
    "symbols": [
        "EUR_USD", "GBP_USD", "XAU_USD",
        "USD_JPY", "GBP_JPY", "VOLATILITY_10",
        "VOLATILITY_25", "AUD_USD", "NZD_USD", 
        "USD_CAD", "EUR_GBP", "USD_CHF"
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
    "combined_min":   7,

    # ── Timeframes ────────────────────────────────────────────────
    "tf_weekly":  "W",
    "tf_daily":   "D",
    "tf_h4":      "H4",
    "tf_h2":      "H2",
    "tf_h1":      "H1",
    "tf_m45":     "M45",
    "tf_m30":     "M30",
    "tf_m15":     "M15",

    # ── Bias cascade (BIAS only - not confluence/entry) ───────────
    "bias_cascade":   ["H4", "H2", "H1", "M45", "M30", "M15"],
    "bias_min_score": 2,

    # ── Scan & timing ─────────────────────────────────────────────
    "scan_secs":  300,

    # ── Telegram alerts (optional) ────────────────────────────────
    "tg_token": os.environ.get("TELEGRAM_TOKEN", ""),
    "tg_chat":  os.environ.get("TELEGRAM_CHAT_ID", ""),

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
    """HTTP GET - uses requests if available, else urllib."""
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
#  BROKER LAYER  - MetaAPI / OANDA / Paper
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

# ── Paper Broker (no real connection - for testing on Pydroid) ────────
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
                log_trade(f"[PAPER] CLOSED {direction} {symbol} - {hit}")
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
    Pulls live candles from Yahoo Finance - completely free, no account.
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
        "USD_CAD": "CAD=X",      "USD_CHF": "CHF=X", 
        "AUD_USD": "AUDUSD=X",  "NZD_USD": "NZDUSD=X",  
        "XAU_USD": "GC=F",      "USD_JPY": "JPY=X",
        "GBP_JPY": "GBPJPY=X",  "EUR_JPY": "EURJPY=X", 
        "EUR_GBP": "EURGBP=X",
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
        # True M45 requires M15 data - this is close enough for bias
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

        # ConnectionError propagates up - caller (main loop) handles it
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
    RECOMMENDED MODE - YFINANCE_MT5

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
        EA marks them EXECUTED - bot counts those as open positions.
        """
        try:
            data = _load_json(CONFIG["mt5_signal_file"], [])
            self._open_signals = data
            return [s for s in data if s.get("status") == "EXECUTED"]
        except Exception:
            return []



# ════════════════════════════════════════════════════════════════════
#  BINANCE DATA LAYER  - real volume, order book, footprint
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
    Volume here is REAL traded base-asset volume - not tick count.
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
    True footprint requires tick-level data - this bins recent
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
        return analysis   # forex/gold - no Binance data available

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
#  INDICATORS  (pure Python / numpy - no TA-Lib)
# ════════════════════════════════════════════════════════════════════
def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Average True Range - pure numpy, no TA-Lib."""
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
    """Exponential Moving Average - pure numpy."""
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
    """Find local minima or maxima - uses scipy if available, else pure numpy."""
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
    """Failed/Flipped engulfing - broken engulfing zone flips role."""
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
    """GAP / Hidden SNR - open-close gap between consecutive candles."""
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
    """SNR Congestion Zone - 3+ SNR levels at same price area."""
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
#  CHART ANALYSIS ENGINE  - real-time pattern recognition
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
            result["swings"].append("BOS BULLISH - broke last swing high")
        elif cur < lows[-1][2] and result["trend"] in ("UPTREND", "RANGING"):
            result["bos"]     = True
            result["bos_dir"] = "BEARISH_BOS"
            result["swings"].append("BOS BEARISH - broke last swing low")

        # Change of Character: trend reversal signal
        if len(highs) >= 3 and len(lows) >= 3:
            prev_trend_up = highs[-2][2] > highs[-3][2] and lows[-2][2] > lows[-3][2]
            prev_trend_dn = highs[-2][2] < highs[-3][2] and lows[-2][2] < lows[-3][2]
            if prev_trend_up and lh:
                result["choch"] = True
                result["swings"].append("CHoCH BEARISH - trend flipping")
            elif prev_trend_dn and hh:
                result["choch"] = True
                result["swings"].append("CHoCH BULLISH - trend flipping")

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
    # Forex/gold symbols are skipped - no public order book exists.
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
        log_info(f"  [CHART] *** CHoCH detected - possible reversal ***")

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
#  ADAPTIVE BIAS CASCADE  - H4 → H2 → H1 → M45 → M30 → M15
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
            votes_s += 1; reasons.append("CHoCH warning - potential reversal down")
        else:
            votes_b += 1; reasons.append("CHoCH warning - potential reversal up")

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
    # Checked from the df's symbol attribute if available - otherwise
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
            s_sc += 2; s_r.append("+2 LIQUIDITY SWEEP: institutional stop hunt detected")
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
            s_sc += 2; s_r.append("+2 LIQUIDITY SWEEP: institutional stop hunt detected")
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

    # ── Supply/Demand Zone Filter (Fear & Greed) ─────────────────
    # Applied to best signal before building - boosts/penalises
    # signals based on where price is in its 20-day range
    if best is not None and df_d is not None and len(df_d) >= 20:
        try:
            d_hi_20  = df_d["high"].values[-20:].max()
            d_lo_20  = df_d["low"].values[-20:].min()
            d_range  = d_hi_20 - d_lo_20
            if d_range > 0:
                pos = (cur - d_lo_20) / d_range   # 0=at low, 1=at high
                bdir = best["direction"]
                if pos > 0.85 and bdir == "SELL":
                    best["s_sc"] += 2
                    best_total   += 2
                    best["reasons"].append(
                        "+2 SUPPLY ZONE: price at 20D high (greed) - SELL favoured")
                elif pos < 0.15 and bdir == "BUY":
                    best["s_sc"] += 2
                    best_total   += 2
                    best["reasons"].append(
                        "+2 DEMAND ZONE: price at 20D low (fear) - BUY favoured")
                elif pos > 0.85 and bdir == "BUY":
                    best["s_sc"] -= 1
                    best_total   -= 1
                    best["reasons"].append(
                        "-1 BUY in supply/greed zone - reduce confidence")
                elif pos < 0.15 and bdir == "SELL":
                    best["s_sc"] -= 1
                    best_total   -= 1
                    best["reasons"].append(
                        "-1 SELL in demand/fear zone - reduce confidence")
        except Exception:
            pass

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
# ── Telegram configuration ────────────────────────────────────────────
_TELEGRAM_TOKEN:    str  = CONFIG.get("tg_token", "")
_TELEGRAM_CHAT_ID:  str  = CONFIG.get("tg_chat",  "")
_TELEGRAM_PAUSED:   bool = False
_TG_SIGNALS_ONLY:   bool = False
_TG_LAST_UPDATE_ID: int  = 0


def tg_send(text: str, chat_id: str = "") -> bool:
    """Send message to Telegram."""
    token = _TELEGRAM_TOKEN
    cid   = chat_id or _TELEGRAM_CHAT_ID
    if not token or not cid:
        return False
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": cid, "text": text, "parse_mode": "HTML"}
    try:
        if REQUESTS_OK:
            req_lib.post(url, json=data, timeout=5)
        else:
            body = json.dumps(data).encode()
            req  = Request(url, data=body,
                           headers={"Content-Type": "application/json"},
                           method="POST")
            urlopen(req, timeout=5)
        return True
    except Exception as e:
        log_error(f"[Telegram] Send error: {e}")
        return False


# Keep old name working
def send_telegram(msg: str) -> None:
    tg_send(msg)


def tg_notify_trade_opened(symbol, direction, entry, stake, multiplier, sl, tp):
    emoji = "📈" if direction == "BUY" else "📉"
    tg_send(
        f"{emoji} <b>TRADE OPENED</b>\n"
        f"Symbol : <b>{symbol}</b>\n"
        f"Dir    : <b>{direction}</b>\n"
        f"Entry  : {entry:.5f}\n"
        f"Stake  : ${stake} x{multiplier}\n"
        f"TP     : +${tp} | SL: -${sl}"
    )


def tg_notify_trade_closed(symbol, direction, pnl, reason=""):
    emoji = "✅" if pnl >= 0 else "🔴"
    tg_send(
        f"{emoji} <b>TRADE CLOSED</b>\n"
        f"Symbol : {symbol}\n"
        f"Dir    : {direction}\n"
        f"PnL    : {'+'if pnl>=0 else ''}{pnl:.2f}\n"
        f"Reason : {reason}"
    )


def tg_notify_signal(symbol, direction, score, pattern,
                     entry=0.0, sl=0.0, tp=0.0):
    emoji = "📈" if direction == "BUY" else "📉"
    tg_send(
        f"{emoji} <b>SIGNAL</b>\n"
        f"Symbol  : <b>{symbol}</b>\n"
        f"Dir     : <b>{direction}</b>\n"
        f"Score   : {score:.1f} (min:{CONFIG.get('combined_min', 9)})\n"
        f"Pattern : {pattern}\n"
        f"Entry   : {entry:.5f}\n"
        f"SL      : {sl:.5f}\n"
        f"TP      : {tp:.5f}"
    )


def tg_notify_signal_detail(signal) -> None:
    direction = signal.direction.value if hasattr(signal.direction, "value") else str(signal.direction)
    emoji     = "📈" if direction == "BUY" else "📉"
    tg_send(
        f"{emoji} <b>SIGNAL DETECTED</b>\n"
        f"Symbol    : <b>{getattr(signal,'symbol','?')}</b>\n"
        f"Direction : <b>{direction}</b>\n"
        f"Timeframe : {getattr(signal,'timeframe','H1')}\n"
        f"Entry     : {getattr(signal,'snr_price',0.0):.5f}\n"
        f"Stop Loss : {getattr(signal,'stop_loss',0.0):.5f}\n"
        f"Take Profit: {getattr(signal,'take_profit',0.0):.5f}\n"
        f"Score     : {getattr(signal,'score',0.0):.1f} (min:{CONFIG.get('combined_min',9)})\n"
        f"Session   : {'LIVE' if SESSION_ACTIVE else 'SIGNAL ONLY'}"
    )


def tg_notify_reversal_close(symbol, reason, pnl):
    tg_send(
        f"⚠️ <b>REVERSAL CLOSE</b>\n"
        f"Symbol : {symbol}\n"
        f"Reason : {reason}\n"
        f"PnL est: {pnl:.2f}"
    )


def tg_notify_emergency_stop(balance):
    tg_send(
        f"🚨 <b>EMERGENCY STOP</b>\n"
        f"Balance ${balance:.2f} too low.\n"
        f"All trading halted."
    )


def tg_notify_daily_limit(balance, loss):
    tg_send(
        f"⚠️ <b>DAILY LIMIT HIT</b>\n"
        f"Lost ${loss:.2f} today.\n"
        f"Balance: ${balance:.2f}\n"
        f"Trading paused until tomorrow."
    )


# ── Command handlers ──────────────────────────────────────────────────

def _tg_cmd_status(chat_id: str) -> None:
    try:
        deriv_state = "🟢 CONNECTED" if CONFIG.get("deriv_token") or \
                      _os.environ.get("DERIV_API_TOKEN") else "⚫ NO TOKEN"
        paused      = "⏸ PAUSED" if _TELEGRAM_PAUSED else "▶ RUNNING"
        tg_send(
            f"📊 <b>BOT STATUS</b>\n"
            f"Session  : {'🟢 ACTIVE' if SESSION_ACTIVE else '⚫ IDLE'}\n"
            f"Scanner  : {paused}\n"
            f"Deriv    : {deriv_state}\n"
            f"Signals  : {'📡 AUTO' if _TG_SIGNALS_ONLY else '🔕 MANUAL'}",
            chat_id
        )
    except Exception as e:
        tg_send(f"❌ Error: {e}", chat_id)


def _tg_cmd_signals_only_on(chat_id: str) -> None:
    global _TG_SIGNALS_ONLY
    _TG_SIGNALS_ONLY = True
    tg_send(
        "📡 <b>Signals-Only Mode ON</b>\n"
        "Every signal will be pushed to Telegram.\n"
        "Use /signals_only_off to disable.",
        chat_id,
    )


def _tg_cmd_signals_only_off(chat_id: str) -> None:
    global _TG_SIGNALS_ONLY
    _TG_SIGNALS_ONLY = False
    tg_send(
        "🔕 <b>Signals-Only Mode OFF</b>\n"
        "Auto signal push disabled.",
        chat_id,
    )


def _tg_cmd_pause(chat_id: str) -> None:
    global _TELEGRAM_PAUSED
    _TELEGRAM_PAUSED = True
    tg_send("⏸ <b>Scanning paused.</b>\nUse /resume to continue.", chat_id)


def _tg_cmd_resume(chat_id: str) -> None:
    global _TELEGRAM_PAUSED
    _TELEGRAM_PAUSED = False
    tg_send("▶ <b>Scanning resumed.</b>", chat_id)


def _tg_cmd_help(chat_id: str) -> None:
    tg_send(
        "🤖 <b>BOT COMMANDS</b>\n\n"
        "/status          - Bot status\n"
        "/signals         - Last signals\n"
        "/signals_only    - Auto-push signals ON\n"
        "/signals_only_off - Auto-push signals OFF\n"
        "/pause           - Pause scanning\n"
        "/resume          - Resume scanning\n"
        "/help            - This message",
        "/trades          - open deriv positions\n",
        "/balance         - deriv account balance\n",
        chat_id,
    )


def _tg_process_update(update: Dict) -> None:
    msg     = update.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id", ""))
    text    = msg.get("text", "").strip().lower()
    if not text or not chat_id:
        return
    cmd = text.split()[0].lstrip("/")
    if cmd == "status":
        _tg_cmd_status(chat_id)
    elif cmd == "signals_only_off":
        _tg_cmd_signals_only_off(chat_id)
    elif cmd in ("signals_only", "signals_only_on"):
        _tg_cmd_signals_only_on(chat_id)
    elif cmd == "pause":
        _tg_cmd_pause(chat_id)
    elif cmd == "resume":
        _tg_cmd_resume(chat_id)
    elif cmd in ("help", "start"):
        _tg_cmd_help(chat_id)
    elif cmd == "balance":
        try:
            from_broker = globals().get("_get_deriv_broker") and globals()["_get_deriv_broker"]()
            bal = from_broker.get_balance() if from_broker else 0.0
            tg_send(f"💰 <b>Balance</b>\n${bal:.2f}", chat_id)
        except Exception as e:
            tg_send(f"❌ Balance error: {e}", chat_id)
    elif cmd == "trades":
        try:
            from_broker = globals().get("_get_deriv_broker") and globals()["_get_deriv_broker"]()
            positions = from_broker.get_open_positions() if from_broker else []
            if not positions:
                tg_send("📭 No open trades.", chat_id)
            else:
                msg = "📊 <b>Open Trades</b>\n"
                for p in positions:
                    msg += (f"\n{p.get('symbol')} {p.get('direction')}\n"
                            f"Entry: {p.get('buy_price', 0):.5f}\n"
                            f"Stake: ${p.get('stake', 0)}\n")
                tg_send(msg, chat_id)
        except Exception as e:
            tg_send(f"❌ Trades error: {e}", chat_id)    
    else:
        tg_send(f"❓ Unknown: {text}\nUse /help", chat_id)


def is_telegram_paused() -> bool:
    return _TELEGRAM_PAUSED


def run_telegram_polling() -> None:
    global _TG_LAST_UPDATE_ID
    if not _TELEGRAM_TOKEN:
        log_info("[Telegram] No token - disabled")
        return
    log_info("[Telegram] Polling started")
    tg_send("🤖 <b>Bot started</b>\nWyckoff SNR Bot is online.\nUse /help for commands.")
    while True:
        try:
            url  = f"https://api.telegram.org/bot{_TELEGRAM_TOKEN}/getUpdates"
            if REQUESTS_OK:
                resp = req_lib.get(url, params={
                    "offset": _TG_LAST_UPDATE_ID + 1,
                    "timeout": 30, "limit": 10
                }, timeout=35)
                data = resp.json()
            else:
                from urllib.request import urlopen as _uo
                from urllib.parse import urlencode as _ue
                params = _ue({"offset": _TG_LAST_UPDATE_ID+1, "timeout": 30, "limit": 10})
                data   = json.loads(_uo(f"{url}?{params}", timeout=35).read())
            for update in data.get("result", []):
                try:
                    _tg_process_update(update)
                    _TG_LAST_UPDATE_ID = update.get("update_id", _TG_LAST_UPDATE_ID)
                except Exception as e:
                    log_error(f"[Telegram] Update error: {e}")
        except Exception as e:
            log_error(f"[Telegram] Poll error: {e}")
            time.sleep(10)

# ════════════════════════════════════════════════════════════════════
#  MAIN BOT LOOP
# ════════════════════════════════════════════════════════════════════

# ── Confluence Analysis (H4 - pattern confirmation layer) ─────────────
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


# ── Entry Analysis (M15/M30 - precision trigger layer) ────────────────
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
                # Too far from POI - not a valid entry TF yet
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



# ════════════════════════════════════════════════════════════════════
#  EMA 9/15 CROSSOVER ENTRY CONFIRMATION
#  Added as an additional entry confirmation layer on top of existing
#  Wyckoff + SNR analysis. Does NOT replace existing logic.
#  Based on: EMA9/EMA15 crossover + Fibonacci confluence + volume.
# ════════════════════════════════════════════════════════════════════

def ema_9_15_entry_check(df_ltf: pd.DataFrame,
                          direction: str,
                          snr_price: float = 0.0) -> Dict:
    """
    EMA 9/15 crossover entry confirmation layer.

    Checks:
    1. EMA9 crossed above/below EMA15 on last 3 candles
    2. Price near Fibonacci retracement level (0.382/0.500/0.618)
    3. Confirmation candle closes in signal direction
    4. Volume surge at entry point
    5. Price within 0.3% of SNR level

    Returns dict: valid, score, pattern, entry, sl, tp
    """
    result = {
        "valid":   False,
        "score":   0,
        "pattern": "EMA_CROSS_NONE",
        "entry":   0.0,
        "sl":      0.0,
        "tp":      0.0,
        "reasons": [],
    }

    if df_ltf is None or len(df_ltf) < 20:
        result["pattern"] = "INSUFFICIENT_DATA"
        return result

    closes = df_ltf["close"].values.astype(float)
    highs  = df_ltf["high"].values.astype(float)
    lows   = df_ltf["low"].values.astype(float)

    ema9  = calc_ema(closes, 9)
    ema15 = calc_ema(closes, 15)

    score   = 0
    reasons = []

    # ── 1. EMA crossover check (last 3 candles) ───────────────────
    crossed_up   = ema9[-3] < ema15[-3] and ema9[-1] > ema15[-1]
    crossed_down = ema9[-3] > ema15[-3] and ema9[-1] < ema15[-1]

    if direction == "BUY" and crossed_up:
        score += 3
        reasons.append("+3 EMA9 crossed ABOVE EMA15 (bullish crossover)")
        result["pattern"] = "EMA9_CROSS_UP"
    elif direction == "SELL" and crossed_down:
        score += 3
        reasons.append("+3 EMA9 crossed BELOW EMA15 (bearish crossover)")
        result["pattern"] = "EMA9_CROSS_DOWN"
    else:
        # No crossover - still check if EMA9 is aligned with direction
        ema_aligned_bull = ema9[-1] > ema15[-1] and direction == "BUY"
        ema_aligned_bear = ema9[-1] < ema15[-1] and direction == "SELL"
        if ema_aligned_bull or ema_aligned_bear:
            score += 1
            reasons.append("+1 EMA9/15 aligned with direction (no fresh cross)")
            result["pattern"] = "EMA_ALIGNED"
        else:
            result["pattern"] = "EMA_AGAINST_DIRECTION"
            result["reasons"] = reasons
            return result   # EMAs against signal - skip

    # ── 2. Fibonacci confluence ───────────────────────────────────
    lookback  = min(50, len(df_ltf))
    swing_hi  = highs[-lookback:].max()
    swing_lo  = lows[-lookback:].min()
    fib_range = swing_hi - swing_lo
    price     = closes[-1]

    if fib_range > 0:
        fib_levels = {
            "0.236": swing_hi - fib_range * 0.236,
            "0.382": swing_hi - fib_range * 0.382,
            "0.500": swing_hi - fib_range * 0.500,
            "0.618": swing_hi - fib_range * 0.618,
            "0.786": swing_hi - fib_range * 0.786,
        }
        for name, level in fib_levels.items():
            dist_pct = abs(price - level) / fib_range
            if dist_pct < 0.03:
                score += 2
                reasons.append(f"+2 Price at Fibonacci {name} level ({level:.5f})")
                result["pattern"] += f"+FIB_{name}"
                break

    # ── 3. Confirmation candle ────────────────────────────────────
    candle_bullish = closes[-1] > closes[-2]
    candle_bearish = closes[-1] < closes[-2]
    candle_body    = abs(closes[-1] - closes[-2])
    candle_range   = highs[-1] - lows[-1]
    strong_candle  = candle_range > 0 and candle_body / candle_range > 0.5

    if direction == "BUY" and candle_bullish:
        score += 2 if strong_candle else 1
        reasons.append(f"+{2 if strong_candle else 1} Bullish confirmation candle"
                        f"{' (strong body)' if strong_candle else ''}")
    elif direction == "SELL" and candle_bearish:
        score += 2 if strong_candle else 1
        reasons.append(f"+{2 if strong_candle else 1} Bearish confirmation candle"
                        f"{' (strong body)' if strong_candle else ''}")

    # ── 4. Volume surge ───────────────────────────────────────────
    if "volume" in df_ltf.columns:
        vols    = df_ltf["volume"].values.astype(float)
        avg_vol = vols[-20:].mean()
        if avg_vol > 0 and vols[-1] > avg_vol * 1.5:
            score += 2
            reasons.append(f"+2 Volume surge ({vols[-1]/avg_vol:.1f}x avg)")

    # ── 5. SNR proximity ─────────────────────────────────────────
    if snr_price > 0:
        snr_dist = abs(price - snr_price) / price
        if snr_dist < 0.003:
            score += 2
            reasons.append(f"+2 Price within 0.3% of SNR @ {snr_price:.5f}")
        elif snr_dist < 0.006:
            score += 1
            reasons.append(f"+1 Price within 0.6% of SNR @ {snr_price:.5f}")

    # ── 6. EMA9 slope confirmation ────────────────────────────────
    ema9_slope = (ema9[-1] - ema9[-3]) / ema9[-3] if ema9[-3] != 0 else 0
    if direction == "BUY" and ema9_slope > 0:
        score += 1
        reasons.append(f"+1 EMA9 sloping upward ({ema9_slope*100:.3f}%)")
    elif direction == "SELL" and ema9_slope < 0:
        score += 1
        reasons.append(f"+1 EMA9 sloping downward ({ema9_slope*100:.3f}%)")

    # ── Entry, SL, TP calculation (1:2 RR minimum) ────────────────
    atr = calc_atr(df_ltf, period=14)
    if atr == 0:
        atr = (highs[-1] - lows[-1]) * 0.5

    if direction == "BUY":
        entry = float(lows[-1])
        sl    = float(lows[-1]) - atr
        tp    = entry + (entry - sl) * 2.0
    else:
        entry = float(highs[-1])
        sl    = float(highs[-1]) + atr
        tp    = entry - (sl - entry) * 2.0

    result.update({
        "valid":   score >= 5,
        "score":   score,
        "entry":   round(entry, 5),
        "sl":      round(sl, 5),
        "tp":      round(tp, 5),
        "reasons": reasons,
    })

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
        log_warn("Daily loss limit reached - no new trades today.")
        return False
    return True


def cooldown_ok(symbol: str, cooldown: int = 1800) -> bool:
    """Returns False if a trade was placed on this symbol within cooldown seconds."""
    global LAST_TRADE_TIME
    now  = time.time()
    last = LAST_TRADE_TIME.get(symbol, 0)
    if now - last < cooldown:
        log_info(f"  [{symbol}] Cooldown active - "
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
#  PRICE FEED - live price from active broker (MT5 bridge or YFinance)
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
      1. MT5 bridge (localhost:8000/price/<symbol>) - most accurate,
         uses your real broker's live tick when PC bridge is running.
      2. broker.get_tick() - MetaAPI REST, YFinance M15 close, or
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

# Small-account threshold - trailing logic tightens below this balance
SMALL_ACCOUNT_LIMIT: float = 50.0


def _push_trade_event(event_type: str, payload: Dict) -> None:
    """
    Send a trade event to the webapp's push endpoint.
    Non-blocking - failures are logged but don't interrupt the bot.
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
      * Fetch live price via get_live_price() (bridge → broker fallback)
      * Calculate floating P&L
      * Apply trailing stop on small accounts (balance < SMALL_ACCOUNT_LIMIT)
      * Detect SL/TP hit and call close_trade_record() automatically
      * Push TRAIL_UPDATE event to webapp if SL moved

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
        log_warn(f"[LIFECYCLE] Trade {tid} no longer at broker - closing.")
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
            log_warn(f"  [{symbol}] No tick data - skipping.")
            return

        entry  = tick["ask"] if direction.value == "BUY" else tick["bid"]
        spread = abs(tick["ask"] - tick["bid"])

        # 3. Spread filter
        if spread > CONFIG["max_spread_pips"] * 0.0001:
            log_info(f"  [{symbol}] Spread too wide "
                     f"({spread:.5f}) - skipping.")
            return

        # 4. Max-trades guard
        open_positions = broker.get_open_positions() or []
        if len(open_positions) >= CONFIG["max_trades"]:
            log_info(f"  [{symbol}] Max trades reached - skipping.")
            return

        # 5. Duplicate-position guard
        if any(p.get("symbol") == symbol for p in open_positions):
            log_info(f"  [{symbol}] Position already open - skipping.")
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
            log_info(f"  [{symbol}] RR {rr:.2f} < 1.5 - skipping.")
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
#  TRADE LIFECYCLE  - open / update (trailing) / close
#
#  These functions wrap the existing execute_trade_pipeline with a
#  clean lifecycle layer that:
#    * Maintains an in-memory active_trades dict (also pushed to webapp)
#    * Applies small-account trailing stop logic on every scan tick
#    * Checks the MT5 bridge health before attempting direct execution
#    * Routes price feed through MT5 bridge → Yahoo Finance (fallback)
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
    Fast 2-second timeout - called before every bridge price fetch.
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
    # 1. MT5 bridge - fastest, real-time if EA is running locally
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

    log_warn(f"  [{symbol}] All price sources failed - cannot get current price.")
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
    Wraps execute_trade_pipeline - do NOT call both for the same signal.
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
            log_warn(f"  [{symbol}] Bridge execution failed: {e} - "
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


# ── Trade update - trailing stop ──────────────────────────────────────

def update_trade(trade: Dict, broker: Optional[BrokerBase] = None) -> None:
    """
    Called every scan tick for each open trade.

    Applies small-account trailing stop:
      * Triggers after profit_move > 0.5 pips (adjustable)
      * Trail distance = 0.5× ATR of the symbol on M15
        (falls back to 5-pip fixed trail if ATR unavailable)
      * Trailing only moves SL in the profit direction (never back)

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
    Does NOT send a close order to the broker - MetaAPI/MT5 manages
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


# ── SL/TP hit detection - called in the scan loop ─────────────────────

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
        log_info("PAPER mode - internet not required.")

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
    """Main scan loop - separated for clean restart and crash recovery."""
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
            # even when max_trades is reached - we just skip the order placement.
            can_trade = n_open < CONFIG["max_trades"]
            if not can_trade:
                log_info("Max trades active - scanning for analysis only (no new orders).")

            # ── Log market session status once per cycle ──────────────
            _active_sess = get_active_sessions()
            _mkt_open    = is_forex_market_open()
            log_info(f"[MARKET] Forex={'OPEN' if _mkt_open else 'CLOSED'} | "
                     f"Sessions: {_active_sess or ['NONE/BETWEEN']}")

            for symbol in CONFIG["symbols"]:
                # ── Market hours gate ─────────────────────────────────
                if not should_scan_symbol(symbol):
                    log_info(f"  [{symbol}] Market CLOSED (weekend/off-hours) - skipping")
                    continue
                # ─────────────────────────────────────────────────────
                log_info(f"Scanning {symbol}... Sessions:{_active_sess or ['BETWEEN']}")
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
                                     f"reached - queued for next open slot.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Signal valid but confluence "
                                     f"too weak (score={conf['score']}) - skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] Signal valid, confluence OK, "
                                     f"but no entry trigger yet - waiting...")
                        else:
                            # ── EMA 9/15 Crossover Final Confirmation ──────
                            df_m30_ema = broker.get_candles(symbol, "M30", 60)
                            ema_check  = ema_9_15_entry_check(
                                df_ltf    = df_m30_ema,
                                direction = signal.direction.value,
                                snr_price = signal.snr_price,
                            )
                            log_info(f"  [{symbol}] EMA9/15: "
                                     f"valid={ema_check['valid']} "
                                     f"score={ema_check['score']} "
                                     f"pattern={ema_check['pattern']}")
                            for _r in ema_check.get("reasons", []):
                                log_info(f"    {_r}")
                            if ema_check["valid"]:
                                signal.sl = ema_check["sl"]
                                signal.tp = ema_check["tp"]
                                log_info(f"  [{symbol}] EMA crossover CONFIRMED - executing")
                            else:
                                log_info(f"  [{symbol}] EMA crossover not confirmed "
                                         f"({ema_check['pattern']}) - skipping")
                            # ── Route through pipeline (EMA used as score boost) ─
                            if ema_check["valid"]:
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
                    log_info("  Internet restored - continuing scan cycle.")
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
#  HYBRID BOT REFACTOR - RENDER READY
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
#  DATA PIPELINE - push snapshot to external webapp API
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
#  ANALYTICS ENGINE - win-rate and PnL from trades list
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
#  BOT LOOP - background engine (wraps original run_main_loop)
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
        log_info("PAPER mode - internet not required.")

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
                log_info("Max trades active - scanning for analysis only.")

            # ── Snapshot accumulator for this scan cycle ─────────────
            cycle_data: Dict = {
                "signals":  [],
                "analysis": [],
                "trades":   list(open_pos) if isinstance(open_pos, list) else [],
                "metrics":  {},
            }

            # ── Log market session status once per cycle ──────────────
            _active_sess = get_active_sessions()
            _mkt_open    = is_forex_market_open()
            log_info(f"[MARKET] Forex={'OPEN' if _mkt_open else 'CLOSED'} | "
                     f"Sessions: {_active_sess or ['NONE/BETWEEN']}")

            for symbol in CONFIG["symbols"]:
                # ── Market hours gate ─────────────────────────────────
                if not should_scan_symbol(symbol):
                    log_info(f"  [{symbol}] Market CLOSED (weekend/off-hours) - skipping")
                    continue
                # ─────────────────────────────────────────────────────
                log_info(f"Scanning {symbol}... Sessions:{_active_sess or ['BETWEEN']}")
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
                        cycle_data["signals"].append(signal)
                        _save_json(CONFIG["signals_file"], all_signals[-100:])

                        if not can_trade:
                            log_info(f"  [{symbol}] Signal valid but max trades "
                                     f"reached - queued for next open slot.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Signal valid but confluence "
                                     f"too weak (score={conf['score']}) - skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] Signal valid, confluence OK, "
                                     f"but no entry trigger yet - waiting...")
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
                    log_info("  Internet restored - continuing scan cycle.")
                    break
                except Exception as e:
                    log_error(f"  Error scanning {symbol}: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()

            # ── End of cycle: compute metrics + push data ─────────────
            cycle_data["metrics"] = compute_metrics(cycle_data["trades"])
            push_data = dict(cycle_data)
            push_data["signals"] = [
                s.to_dict() if hasattr(s, "to_dict") else s
                for s in cycle_data.get("signals", [])
            ]
            push_all_data(push_data)
            # ── Deriv trade execution ─────────────────────────────────
            log_info(f"[Deriv] Reached execution block. Token set: {bool(os.environ.get('DERIV_API_TOKEN'))}")
            if os.environ.get("DERIV_API_TOKEN", ""):
                try:
                    _gdb = globals().get("_get_deriv_broker")
                    _deriv_b = _gdb() if callable(_gdb) else None
                except Exception as _dbe:
                    log_error(f"[Deriv] broker init error: {_dbe}")
                    _deriv_b = None
                if not _deriv_b:
                    log_warn("[Deriv] Broker not ready yet - skip this cycle")
                else:
                    _d_balance = _deriv_b.get_balance()
                    _d_maps    = (globals().get("DERIV_FOREX_MAP", {}),
                                  globals().get("DERIV_SYNTHETIC_MAP", {}))
                    _d_cfg     = globals().get("DERIV_CONFIG", {})
                    _d_rsk_cls = globals().get("DerivRiskManager")
                    for sig in cycle_data.get("signals", []):
                        try:
                            # Handle both Signal objects and dicts
                            sym   = getattr(sig, "symbol", None) or                                     (sig.get("symbol", "") if isinstance(sig, dict) else "")
                            dirn  = getattr(sig, "direction", None)
                            if dirn is None and isinstance(sig, dict):
                                dirn = sig.get("direction", "BUY")
                            dirn  = dirn.value if hasattr(dirn, "value") else str(dirn)
                            score = float(getattr(sig, "score", 0) or
                                          (sig.get("score", 0) if isinstance(sig, dict) else 0))

                            if score < CONFIG.get("combined_min", 9):
                                log_info(f"[Deriv] {sym} score={score:.0f} "
                                         f"< min={CONFIG.get('combined_min',9)} - skip")
                                continue

                            deriv_sym = _d_maps[0].get(sym) or _d_maps[1].get(sym)
                            if not deriv_sym:
                                log_info(f"[Deriv] {sym} not in Deriv maps - skip")
                                continue

                            if _d_rsk_cls:
                                stage = _d_rsk_cls(_d_balance).current_stage(_d_balance)
                                stake = stage.get("stake", 1.00)
                                mult  = stage.get("multiplier", 10)
                            else:
                                stake, mult = 1.00, 10

                            sl_usd = _d_cfg.get("stop_loss_usd", 0.50)
                            tp_usd = _d_cfg.get("take_profit_usd", 1.00)

                            log_info(f"[Deriv] >>> Placing {dirn} on {deriv_sym} "
                                     f"stake=${stake} x{mult} score={score:.0f}")

                            result = _deriv_b.place_multiplier(
                                symbol=deriv_sym,
                                direction=dirn,
                                stake=stake,
                                multiplier=mult,
                                sl_usd=sl_usd,
                                tp_usd=tp_usd,
                            )
                            if result:
                                log_trade(f"[Deriv] Trade placed: {result}")
                                if callable(globals().get("tg_notify_trade_opened")):
                                    tg_notify_trade_opened(
                                        deriv_sym, dirn,
                                        float(result.get("buy_price", 0)),
                                        stake, mult, sl_usd, tp_usd,
                                    )
                            else:
                                log_warn(f"[Deriv] Order failed for {deriv_sym}")
                        except Exception as _de:
                            log_error(f"[Deriv] Trade error {sym}: {_de}")
            # ─────────────────────────────────────────────────────────

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
#  SESSION STATE - in-memory only, never written to disk or logs
#
#  Flow:
#    1. Webapp POSTs { metaapi_token, metaapi_account } to /session/start
#       along with X-Bot-Secret header matching BOT_SECRET env var.
#    2. Bot loads MetaAPI broker into ACTIVE_SESSION, begins trading.
#    3. Webapp POSTs to /session/end → credentials wiped, broker
#       reverts to dormant PAPER mode, trading stops immediately.
#
#  Security model:
#    * BOT_SECRET is a shared secret set as an env var on Render.
#      Your webapp reads the same secret from its own env and sends
#      it as the X-Bot-Secret header. Never hardcode it here.
#    * Credentials live ONLY in ACTIVE_SESSION dict in RAM.
#      They are never logged, never written to any file.
#    * On session end, the dict is zeroed before deletion so the
#      token string doesn't linger in memory longer than needed.
# ════════════════════════════════════════════════════════════════════

import os as _os

# Set BOT_SECRET as an environment variable on Render.
# Your webapp must send this in the X-Bot-Secret header.
# If not set, session endpoints are disabled (safe default).
BOT_SECRET: str = _os.environ.get("BOT_SECRET", "")
API_KEY: str = _os.environ.get("API_KEY", "your-secret-key")
# Live session state - held in memory only
SESSION_ACTIVE = False
ACTIVE_SESSION: Dict = {
    "active":           False,
    "broker":           None,   # live MetaAPIBroker instance
    "metaapi_token":    "",     # wiped on session end
    "metaapi_account":  "",     # wiped on session end
    "started_at":       "",
    "user_id":          "",     # webapp user identifier (non-sensitive)
}

# Thread lock - session state is read by the bot loop thread
# and written by Flask threads; always acquire before touching it
_SESSION_LOCK = threading.Lock()


def _check_secret(request) -> bool:
    """
    Verify the X-Bot-Secret header matches BOT_SECRET.
    Returns False (reject) if BOT_SECRET is not configured -
    this prevents accidental open access on a misconfigured deploy.
    """
    if not BOT_SECRET:
        log_warn("BOT_SECRET env var not set - session endpoints disabled.")
        return False
    body = request.get_json(silent=True) or {}
    key = request.headers.get("X-Bot-Secret", "") or request.headers.get("X-Bot-Api-Key", "")
    return key == BOT_SECRET


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
        # Overwrite token strings before clearing - reduces RAM residency
        ACTIVE_SESSION["metaapi_token"]   = "0" * len(ACTIVE_SESSION.get("metaapi_token", ""))
        ACTIVE_SESSION["metaapi_account"] = "0" * len(ACTIVE_SESSION.get("metaapi_account", ""))
        ACTIVE_SESSION["active"]          = False
        ACTIVE_SESSION["broker"]          = None
        ACTIVE_SESSION["metaapi_token"]   = ""
        ACTIVE_SESSION["metaapi_account"] = ""
        ACTIVE_SESSION["started_at"]      = ""
        ACTIVE_SESSION["user_id"]         = ""
    log_info("Session ended - credentials wiped from memory.")


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
    log_info("Session bot loop started - scanning signals always...")
    _dt = os.environ.get("DERIV_API_TOKEN", "")
    _da = os.environ.get("DERIV_APP_ID", "1089")
    if _dt:
        log_info(f"[Deriv] Token found in loop (len={len(_dt)}) app_id={_da}")
    else:
        log_warn("[Deriv] DERIV_API_TOKEN not found in environment - Deriv disabled")
    signal_broker = YFinanceBroker()   # always-on broker for signal scanning

    all_signals: List[Dict] = _load_json(CONFIG["signals_file"], [])
    scan_count = 0
    
                
    while True:
        try:
            # Keep-alive ping to prevent Render sleep
            scan_count += 1
            if scan_count % 10 == 0:
                log_info("[KeepAlive] Bot active")
                
            # ── Determine mode for this cycle ─────────────────────────
            live_broker = _get_active_broker()

            if live_broker is not None and SESSION_ACTIVE:
                broker = live_broker   # live trading with MT5
                log_info("[LIVE] Session active - using MetaAPI broker")
            else:
                broker = signal_broker   # signal-only using Yahoo Finance
                log_info("[SIGNAL_ONLY] No active session - scanning signals only")

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
                # Re-check session on every symbol - end can happen mid-scan
                if _get_active_broker() is None:
                    if os.environ.get("DERIV_API_TOKEN", ""):
                        log_info("[Deriv] No MT5 session - continuing for Deriv only")
                    else:
                        log_info("[SESSION] Session ended mid-scan - stopping cycle.")
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
                        cycle_data["signals"].append(signal)   # Signal object for Deriv
                        cycle_data["_signals_raw"] = cycle_data.get("_signals_raw", [])
                        cycle_data["_signals_raw"].append(signal)
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
                        
                        # Always push signal to Telegram
                        if signal and float(signal.score) >= CONFIG.get("combined_min", 9):
                            tg_notify_signal(
                                symbol,
                                signal.direction.value,
                                float(signal.score),
                                entry_analysis.get("trigger", "N/A"),
                                entry=getattr(signal, "snr_price", 0.0),
                                sl=getattr(signal, "sl", 0.0),
                                tp=getattr(signal, "tp", 0.0),
                            )
                            if globals().get("_TG_SIGNALS_ONLY"):
                                tg_notify_signal_detail(signal)
                            
                        if not SESSION_ACTIVE:
                            # Signal-only mode - no trade execution
                            log_info(f"  [{symbol}] SIGNAL_ONLY - pushed to webapp, no trade placed.")
                        elif not can_trade:
                            log_info(f"  [{symbol}] Max trades reached - queued.")
                        elif not conf["valid"]:
                            log_info(f"  [{symbol}] Confluence weak - skipping.")
                        elif not entry_analysis["valid"]:
                            log_info(f"  [{symbol}] No entry trigger yet - waiting.")
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
            push_data = dict(cycle_data)
            push_data["signals"] = [
                s.to_dict() if hasattr(s, "to_dict") else s
                for s in cycle_data.get("signals", [])
            ]
            push_all_data(push_data)
            # ── Deriv v2 scan (run_deriv_scan_v2 with all safety gates) ──
            if os.environ.get("DERIV_API_TOKEN", ""):
                try:
                    if callable(globals().get("run_deriv_scan_v2")):
                        run_deriv_scan_v2(cycle_data.get("signals", []))
                    elif callable(globals().get("run_deriv_scan")):
                        run_deriv_scan(cycle_data.get("signals", []))
                    if callable(globals().get("deriv_sync_positions")):
                        deriv_sync_positions()
                except Exception as _de:
                    log_error(f"[Deriv] Scan error: {_de}")
                    log_info("[Deriv] Past scan_v2 block - proceeding to execution block")
            # ── Deriv trade execution ─────────────────────────────────
            log_info(f"[Deriv] Reached execution block. Token={bool(os.environ.get('DERIV_API_TOKEN'))}")
            if os.environ.get("DERIV_API_TOKEN", ""):
                try:
                    import sys as _sys
                    _mod = _sys.modules[__name__] if __name__ in _sys.modules else None
                    _gdb = globals().get("_get_deriv_broker")
                    log_info(f"[Deriv DEBUG] _gdb={_gdb} globals_keys_with_deriv={[k for k in globals() if 'deriv' in k.lower()]}")
                    _deriv_b = _gdb() if callable(_gdb) else None
    
                except Exception as _dbe:
                    log_error(f"[Deriv] broker error: {_dbe}")
                    _deriv_b = None
                if not _deriv_b:
                    log_warn("[Deriv] Broker not ready - skip")
                else:
                    _d_balance = _deriv_b.get_balance()
                    _d_maps = (DERIV_FOREX_MAP, DERIV_SYNTHETIC_MAP)
                    _d_cfg = DERIV_CONFIG
                    for sig in cycle_data.get("signals", []):
                        try:
                            sym = getattr(sig, "symbol", None) or (sig.get("symbol", "") if isinstance(sig, dict) else "")
                            dirn = getattr(sig, "direction", None)
                            if dirn is None and isinstance(sig, dict):
                                dirn = sig.get("direction", "BUY")
                            dirn = dirn.value if hasattr(dirn, "value") else str(dirn)
                            score = float(getattr(sig, "score", 0) or (sig.get("score", 0) if isinstance(sig, dict) else 0))
                            if score < CONFIG.get("combined_min", 7):
                                log_info(f"[Deriv] {sym} score={score:.0f} too low - skip")
                                continue
                            deriv_sym = _d_maps[0].get(sym) or _d_maps[1].get(sym)
                            if not deriv_sym:
                                log_info(f"[Deriv] {sym} not in Deriv maps - skip")
                                continue
                            if _d_rsk_cls := globals().get("DerivRiskManager"):
                                stage = _d_rsk_cls(_d_balance).current_stage(_d_balance)
                                stake = stage.get("stake", 1.00)
                                mult = stage.get("multiplier", 10)
                            else:
                                stake, mult = 1.00, 10
                            sl_usd = _d_cfg.get("stop_loss_usd", 0.50)
                            tp_usd = _d_cfg.get("take_profit_usd", 1.00)
                            log_info(f"[Deriv] >>> Placing {dirn} on {deriv_sym} stake=${stake} x{mult} score={score:.0f}")
                            result = _deriv_b.place_multiplier(
                                symbol=deriv_sym,
                                direction=dirn,
                                stake=stake,
                                multiplier=mult,
                                sl_usd=sl_usd,
                                tp_usd=tp_usd,
                            )
                            if result:
                                log_trade(f"[Deriv] Trade placed: {result}")
                                tg_notify_trade_opened(deriv_sym, dirn, float(result.get("buy_price", 0)), stake, mult, sl_usd, tp_usd)
                            else:
                                log_warn(f"[Deriv] Order failed for {deriv_sym}")
                        except Exception as _de:
                            log_error(f"[Deriv] Trade error {sym}: {_de}")
            # ─────────────────────────────────────────────────────────

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
#  WEB SERVER - Flask endpoints
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
                log_warn(f"Session start failed - MetaAPI rejected credentials "
                         f"for user_id={user_id}")
                return jsonify({
                    "error": "Could not connect to MetaAPI. "
                             "Check token and account ID."
                }), 403
        except Exception as e:
            log_error(f"Session start error: {e}")
            return jsonify({"error": "Broker connection error"}), 500

        # Credentials verified - store session
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
        log_info(f"Session started - user_id={user_id} | balance={balance}")
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
        Lightweight status check - safe to poll from webapp.
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
#  * Local / Pydroid:  python wyckoff_snr_bot_pydroid-4-1.py
#    → runs original run() - no Flask, no session logic
#  * Render server:    env vars RENDER or PORT are set automatically
#    → session-aware bot loop in background thread
#    → Flask serves on port 10000
#
#  Required Render env vars:
#    BOT_SECRET    - shared secret between this bot and your webapp
#    WEBAPP_API_URL - (optional) your webapp's push endpoint
# ════════════════════════════════════════════════════════════════════


# ════════════════════════════════════════════════════════════════════
#  SECTION 21: DERIV MULTIPLIERS LAYER
#  - DerivBroker     : WebSocket connection + auth + orders
#  - DerivCandles    : M1/M5 OHLC data from Deriv
#  - OHLCPatterns    : Engulfing, Pin Bar, Inside Bar detector
#  - AntiManipulation: Spike/spread filter
#  - DerivRiskManager: $3 account protection + growth stages
#  - MultiEntryManager: Multiple small entries on same signal
#  - DerivBotLoop    : Wired into existing run_session_bot_loop
#
#  DOES NOT TOUCH any existing sections 1-20.
#  Runs alongside MetaAPIBroker - Deriv handles synthetics always,
#  MetaAPI handles MT5 when bridge is online.
#
#  ENV VARS REQUIRED (add to Render dashboard):
#    DERIV_API_TOKEN  - your Deriv API token (Read + Trade scope)
#    DERIV_DEMO       - set to "1" for demo account, "0" for real
# ════════════════════════════════════════════════════════════════════

# ── Deriv env vars (read at startup, never hard-coded) ───────────────
_DERIV_TOKEN: str  = os.environ.get("DERIV_API_TOKEN", "")
_DERIV_DEMO:  bool = os.environ.get("DERIV_DEMO", "1") == "1"

# ── Deriv API endpoints (new api.derivws.com platform) ───────────────
_DERIV_APP_ID   = os.environ.get("DERIV_APP_ID", "")
_DERIV_REST_URL = "https://api.derivws.com"

# ── Symbol maps ───────────────────────────────────────────────────────
# Bot internal symbol → Deriv API symbol
DERIV_FOREX_MAP: Dict[str, str] = {
    "EUR_USD": "frxEURUSD",
    "GBP_USD": "frxGBPUSD",
    "USD_JPY": "frxUSDJPY",
    "USD_CHF": "frxUSDCHF",
    "AUD_USD": "frxAUDUSD",
    "USD_CAD": "frxUSDCAD",
    "NZD_USD": "frxNZDUSD",
    "EUR_GBP": "frxEURGBP",
    "EUR_JPY": "frxEURJPY",
    "GBP_JPY": "frxGBPJPY",
    "XAU_USD": "frxXAUUSD",
}

DERIV_SYNTHETIC_MAP: Dict[str, str] = {
    "VOLATILITY_10":  "R_10",
    "VOLATILITY_25":  "R_25",
    "VOLATILITY_50":  "R_50",
    "VOLATILITY_75":  "R_75",
    "VOLATILITY_100": "R_100",
}

# Reverse map: Deriv symbol → bot internal
_DERIV_REV_MAP: Dict[str, str] = {
    **{v: k for k, v in DERIV_FOREX_MAP.items()},
    **{v: k for k, v in DERIV_SYNTHETIC_MAP.items()},
}

# All Deriv symbols the bot will scan
DERIV_ALL_SYMBOLS: List[str] = list(DERIV_FOREX_MAP.keys()) + list(DERIV_SYNTHETIC_MAP.keys())

# ── Deriv CONFIG block (merged into main CONFIG at runtime) ───────────
DERIV_CONFIG: Dict = {
    # Account building - scales with balance
    "stages": [
        # (min_balance, max_balance, stake, multiplier, max_trades)
        (0.00,   10.00,  1.00,  10,  2),
        (10.00,  50.00,  1.00,  20,  3),
        (50.00,  200.0,  2.00,  20,  4),
        (200.0,  9999.,  5.00,  50,  5),
    ],

    # Risk per trade
    "stop_loss_usd":      0.50,   # max loss per trade in USD
    "take_profit_usd":    1.00,   # target profit per trade in USD
    "daily_loss_limit":   1.00,   # stop trading day if down this much

    # Multi-entry (same signal, multiple small trades)
    "max_entries_per_signal":  3,
    "entry_recheck_secs":     30,
    "price_tolerance_pct":  0.002,   # 0.2% - price must stay near original

    # Anti-manipulation filters
    "max_wick_atr_ratio":   3.0,   # skip if wick > 3x ATR (spike)
    "max_spread_pips":      5,     # skip if spread too wide
    "require_candle_close": True,  # never enter on open candle

    # Lower timeframe scan
    "ltf_timeframes": ["M1", "M5"],

    # Symbols safe for $3 account (low min stake)
    "safe_symbols_under_10": [
        "VOLATILITY_10", "VOLATILITY_25",
        "EUR_USD", "GBP_USD",
    ],
}

# ════════════════════════════════════════════════════════════════════
#  DERIV HTTP HELPER  (REST calls for OTP + account lookup)
# ════════════════════════════════════════════════════════════════════

def _deriv_rest(method: str, path: str, token: str,
                app_id: str, body: Optional[Dict] = None) -> Optional[Dict]:
    """
    Make a REST call to api.derivws.com.
    Returns parsed JSON dict or None on error.
    """
    import urllib.request as _ureq
    url = f"{_DERIV_REST_URL}{path}"
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}",
        "Deriv-App-ID":  app_id,
    }
    data = json.dumps(body).encode() if body else None
    req  = _ureq.Request(url, data=data, headers=headers, method=method)
    try:
        with _ureq.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        log_error(f"[Deriv REST] {method} {path} error: {e}")
        return None


def _deriv_get_account_id(token: str, app_id: str, demo: bool) -> Optional[str]:
    """
    Fetch account list and return the ID matching demo/real preference.
    """
    resp = _deriv_rest("GET", "/trading/v1/options/accounts", token, app_id)
    if not resp:
        return None
    accounts = resp.get("data", [])
    if not accounts:
        log_error("[Deriv] No Options accounts found. "
                  "Create one at home.deriv.com first.")
        return None
    target_type = "demo" if demo else "real"
    for acc in accounts:
        # try both field name formats
        acc_id = acc.get("accountId") or acc.get("account_id") or acc.get("id", "")
        if acc.get("account_type", "").lower() == target_type:
            log_info(f"[Deriv] Found {target_type} account: {acc_id} full={acc}")
            return acc_id
    # fallback: return first account
    log_warn(f"[Deriv] No {target_type} account found, using first: {accounts[0]}")
    acc0 = accounts[0]
    return acc0.get("accountId") or acc0.get("account_id") or acc0.get("id", "")


def _deriv_get_otp_url(token: str, app_id: str,
                       account_id: str) -> Optional[str]:
    """
    Call the OTP endpoint and return the ready-to-use WebSocket URL.
    """
    path = f"/trading/v1/options/accounts/{account_id}/otp"
    resp = _deriv_rest("POST", path, token, app_id)
    if not resp:
        return None
    url = (resp.get("data", {}) or {}).get("url")
    if not url:
        log_error(f"[Deriv] OTP response missing url: {resp}")
        return None
    log_info(f"[Deriv] Got OTP WebSocket URL (len={len(url)})")
    return url


# ════════════════════════════════════════════════════════════════════
#  DERIV WEBSOCKET CLIENT  (pure stdlib - no websockets library)
#  Uses urllib + ssl for handshake, then raw socket for frames.
#  Compatible with Pydroid 3 Python 3.8.
# ════════════════════════════════════════════════════════════════════

import ssl      as _ssl
import socket   as _socket
import hashlib  as _hashlib
import base64   as _base64
import struct   as _struct

class DerivWSClient:
    """
    Minimal WebSocket client that works on Pydroid 3.
    Handles connect, send JSON, receive JSON, auto-reconnect.
    Thread-safe: uses a lock for send operations.
    """

    def __init__(self, url: str):
        self._url      = url
        self._sock     = None
        self._lock     = _dthread.Lock()
        self._connected = False

    # ── Connection ───────────────────────────────────────────────────

    def connect(self) -> bool:
        """Open WebSocket connection. Returns True on success."""
        try:
            import re
            m = re.match(r"wss://([^/]+)(.*)", self._url)
            if not m:
                log_error("[Deriv WS] Invalid URL")
                return False
            host = m.group(1)
            path = m.group(2) or "/"

            ctx = _ssl.create_default_context()
            raw = _socket.create_connection((host, 443), timeout=10)
            self._sock = ctx.wrap_socket(raw, server_hostname=host.split("?")[0])

            # WebSocket handshake
            key = _base64.b64encode(os.urandom(16)).decode()
            handshake = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"Upgrade: websocket\r\n"
                f"Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                f"Sec-WebSocket-Version: 13\r\n\r\n"
            )
            self._sock.sendall(handshake.encode())

            # Read response headers
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = self._sock.recv(1024)
                if not chunk:
                    break
                resp += chunk

            if b"101" not in resp:
                log_error(f"[Deriv WS] Handshake failed: {resp[:200]}")
                return False

            self._connected = True
            log_info("[Deriv WS] Connected")
            return True

        except Exception as e:
            log_error(f"[Deriv WS] Connect error: {e}")
            return False

    def disconnect(self):
        """Close the WebSocket connection."""
        self._connected = False
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass
        self._sock = None

    # ── Frame encoding/decoding ──────────────────────────────────────

    def _encode_frame(self, payload: str) -> bytes:
        """Encode a text frame with masking (client must mask)."""
        data   = payload.encode("utf-8")
        length = len(data)
        mask   = os.urandom(4)

        # Build header
        if length < 126:
            header = _struct.pack("BB", 0x81, 0x80 | length)
        elif length < 65536:
            header = _struct.pack("BBH", 0x81, 0x80 | 126, length)
        else:
            header = _struct.pack("BBQ", 0x81, 0x80 | 127, length)

        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        return header + mask + masked

    def _recv_frame(self) -> Optional[str]:
        """Receive one WebSocket frame. Returns payload string or None."""
        if not self._sock:
            return None
        try:
            self._sock.settimeout(0.5)   # short timeout - non-blocking
            # Read first 2 bytes (FIN + opcode + length)
            hdr = self._recv_exact(2)
            if not hdr:
                return None
            opcode = hdr[0] & 0x0F
            length = hdr[1] & 0x7F

            if opcode == 8:   # close frame
                self._connected = False
                return None
            if opcode == 9:   # ping
                self._send_pong()
                return None

            if length == 126:
                length = _struct.unpack("!H", self._recv_exact(2))[0]
            elif length == 127:
                length = _struct.unpack("!Q", self._recv_exact(8))[0]

            payload = self._recv_exact(length)
            return payload.decode("utf-8") if payload else None

        except _socket.timeout:
            return None
        except Exception as e:
            log_error(f"[Deriv WS] recv error: {e}")
            self._connected = False
            return None

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                return buf
            buf += chunk
        return buf

    def _send_pong(self):
        """Respond to ping with pong."""
        try:
            self._sock.sendall(bytes([0x8A, 0x00]))
        except Exception:
            pass

    # ── Public API ───────────────────────────────────────────────────

    def send(self, payload: Dict) -> bool:
        """Send a JSON payload. Thread-safe."""
        if not self._connected or not self._sock:
            return False
        try:
            with self._lock:
                frame = self._encode_frame(json.dumps(payload))
                self._sock.sendall(frame)
            return True
        except Exception as e:
            log_error(f"[Deriv WS] send error: {e}")
            self._connected = False
            return False

    def recv(self) -> Optional[Dict]:
        """Receive one JSON response. Non-blocking with short timeout."""
        raw = self._recv_frame()
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def send_recv(self, payload: Dict, timeout: float = 10.0) -> Optional[Dict]:
        """Send and wait for matching response (by req_id or first reply)."""
        req_id = payload.get("req_id", 1)
        if not self.send(payload):
            return None
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = self.recv()
            if resp is None:
                time.sleep(0.05)   # small sleep to avoid spinning
                continue
            if resp.get("req_id") == req_id or "error" in resp:
                return resp
            time.sleep(0.05)
        log_warn(f"[Deriv WS] send_recv timeout after {timeout}s")
        return None

    @property
    def connected(self) -> bool:
        return self._connected


# ════════════════════════════════════════════════════════════════════
#  DERIV BROKER CLASS
# ════════════════════════════════════════════════════════════════════

class DerivBroker(BrokerBase):
    """
    Deriv Multipliers broker via WebSocket API.
    Handles: authorize, price ticks, OHLC candles,
             place_order (MULTUP/MULTDOWN), get_positions, close_position.

    Runs alongside MetaAPIBroker:
    - Deriv handles synthetic indices (24/7, always on)
    - Deriv handles forex when MetaAPI bridge is offline
    - MetaAPI handles MT5 symbols when bridge is online
    """

    # Deriv timeframe map: bot internal -> Deriv granularity (seconds)
    TF_MAP: Dict[str, int] = {
        "M1":  60,
        "M5":  300,
        "M15": 900,
        "M30": 1800,
        "H1":  3600,
        "H4":  14400,
        "D":   86400,
    }

    def __init__(self, token: str = "", demo: bool = True):
        self._token     = token or _DERIV_TOKEN
        self._demo      = demo if token else _DERIV_DEMO
        self._app_id    = _DERIV_APP_ID
        self._ws        = None          # created after OTP fetch
        self._authed    = False
        self._balance   = 0.0
        self._currency  = "USD"
        self._lock      = _dthread.Lock()
        self._req_id    = 1
        self._positions: Dict[str, Dict] = {}

        if not self._token:
            log_warn("[Deriv] No DERIV_API_TOKEN set - DerivBroker disabled.")
            log_warn("[Deriv] Add DERIV_API_TOKEN to Render environment vars.")
        if not self._app_id:
            log_warn("[Deriv] No DERIV_APP_ID set - DerivBroker disabled.")
            log_warn("[Deriv] Add DERIV_APP_ID to Render environment vars.")

    # ── Internal helpers ─────────────────────────────────────────────

    def _next_id(self) -> int:
        with self._lock:
            self._req_id += 1
            return self._req_id

    def _ensure_connected(self) -> bool:
        """
        New OTP flow:
        1. GET /accounts  → find demo or real account ID
        2. POST /accounts/{id}/otp → get WebSocket URL
        3. Connect WebSocket to that URL (no further auth needed)
        """
        if self._ws and self._ws.connected and self._authed:
            return True
        if not self._token or not self._app_id:
            return False

        # Step 1: get account ID
        log_info("[Deriv] Fetching account list...")
        account_id = _deriv_get_account_id(
            self._token, self._app_id, self._demo)
        if not account_id:
            log_error("[Deriv] Could not find account ID")
            return False

        # Step 2: get OTP WebSocket URL
        log_info(f"[Deriv] Fetching OTP for account {account_id}...")
        ws_url = _deriv_get_otp_url(self._token, self._app_id, account_id)
        if not ws_url:
            log_error("[Deriv] Could not get OTP WebSocket URL")
            return False

        # Step 3: connect WebSocket
        self._ws = DerivWSClient(ws_url)
        if not self._ws.connect():
            log_error("[Deriv] WebSocket connection failed")
            self._ws = None
            return False

        # OTP URL authenticates automatically — no authorize call needed
        self._authed = True
        mode = "DEMO" if self._demo else "REAL"
        log_info(f"[Deriv] Connected ({mode}) via OTP WebSocket")

        # Fetch balance to confirm connection works
        try:
            resp = self._ws.send_recv({
                "balance": 1,
                "req_id":  self._next_id(),
            }, timeout=8)
            if resp and "balance" in resp:
                self._balance  = float(resp["balance"].get("balance", 0))
                self._currency = resp["balance"].get("currency", "USD")
                log_info(f"[Deriv] Balance: {self._balance} {self._currency}")
        except Exception as _be:
            log_warn(f"[Deriv] Could not fetch balance: {_be}")

        return True

    # ── BrokerBase interface ─────────────────────────────────────────

    def get_account_info(self) -> Optional[Dict]:
        """Return balance and currency."""
        if not self._ensure_connected():
            return None
        resp = self._ws.send_recv({
            "balance": 1,
            "req_id": self._next_id(),
        }, timeout=8)
        if resp and "balance" in resp:
            self._balance = float(resp["balance"]["balance"])
            return {
                "balance":  self._balance,
                "currency": resp["balance"].get("currency", "USD"),
                "equity":   self._balance,
            }
        return {"balance": self._balance, "currency": self._currency}

    def get_tick(self, symbol: str) -> Optional[Dict]:
        """
        Get current bid/ask for a symbol.
        symbol: bot internal format (e.g. EUR_USD, VOLATILITY_10)
        """
        if not self._ensure_connected():
            return None
        deriv_sym = (DERIV_FOREX_MAP.get(symbol)
                     or DERIV_SYNTHETIC_MAP.get(symbol)
                     or symbol)
        resp = self._ws.send_recv({
            "ticks": deriv_sym,
            "subscribe": 0,
            "req_id": self._next_id(),
        }, timeout=8)
        if resp and "tick" in resp:
            tick = resp["tick"]
            mid  = float(tick.get("quote", 0))
            # Deriv ticks give mid price; estimate bid/ask from spread
            spread = 0.00010   # default 1 pip
            return {
                "bid":    round(mid - spread / 2, 5),
                "ask":    round(mid + spread / 2, 5),
                "mid":    mid,
                "symbol": symbol,
                "time":   tick.get("epoch", int(time.time())),
            }
        return None

    def get_candles(self, symbol: str, timeframe: str,
                    count: int = 200) -> Optional[pd.DataFrame]:
        """
        Fetch OHLC candles from Deriv.
        timeframe: M1, M5, M15, M30, H1, H4, D
        """
        if not self._ensure_connected():
            return None
        deriv_sym   = (DERIV_FOREX_MAP.get(symbol)
                       or DERIV_SYNTHETIC_MAP.get(symbol)
                       or symbol)
        granularity = self.TF_MAP.get(timeframe, 3600)

        resp = self._ws.send_recv({
            "ticks_history": deriv_sym,
            "style":         "candles",
            "granularity":   granularity,
            "count":         min(count, 1000),
            "end":           "latest",
            "req_id":        self._next_id(),
        }, timeout=15)

        if not resp or "candles" not in resp:
            err = (resp.get("error", {}).get("message", "no candles")
                   if resp else "timeout")
            log_warn(f"[Deriv] get_candles {symbol} {timeframe}: {err}")
            return None

        rows = []
        for c in resp["candles"]:
            rows.append({
                "time":        str(datetime.fromtimestamp(int(c["epoch"]))),
                "open":        float(c["open"]),
                "high":        float(c["high"]),
                "low":         float(c["low"]),
                "close":       float(c["close"]),
                "tick_volume": 1.0,   # Deriv doesn't expose real volume
            })
        if not rows:
            return None
        df = pd.DataFrame(rows)
        return _enrich_df(df)

    def place_order(self, symbol: str, direction, lot: float,
                    sl: float, tp: float) -> Optional[Dict]:
        """
        Place a Multipliers trade.
        direction: Direction.BUY or Direction.SELL
        lot: ignored (stake comes from DerivRiskManager)
        sl/tp: in price units (converted to USD internally)
        """
        return self.place_multiplier(
            symbol    = symbol,
            direction = direction.value if hasattr(direction, "value") else direction,
        )

    def place_multiplier(self, symbol: str, direction: str,
                         stake: Optional[float]  = None,
                         multiplier: Optional[int] = None,
                         sl_usd: Optional[float]   = None,
                         tp_usd: Optional[float]   = None) -> Optional[Dict]:
        """
        Place a Deriv Multipliers trade.

        Args:
            symbol    : bot internal symbol (e.g. EUR_USD, VOLATILITY_10)
            direction : BUY or SELL
            stake     : USD stake (from DerivRiskManager if not provided)
            multiplier: x10, x20, x50, x100
            sl_usd    : stop loss in USD (from DERIV_CONFIG if not provided)
            tp_usd    : take profit in USD (from DERIV_CONFIG if not provided)
        """
        if not self._ensure_connected():
            log_error("[Deriv] Cannot place order - not connected")
            return None

        deriv_sym  = (DERIV_FOREX_MAP.get(symbol)
                      or DERIV_SYNTHETIC_MAP.get(symbol)
                      or symbol)

        # Get risk params from manager if not provided
        rm         = DerivRiskManager(self._balance)
        stage      = rm.current_stage()
        stake      = stake      or stage["stake"]
        multiplier = multiplier or stage["multiplier"]
        sl_usd     = sl_usd     or DERIV_CONFIG["stop_loss_usd"]
        tp_usd     = tp_usd     or DERIV_CONFIG["take_profit_usd"]

        contract_type = "MULTUP" if direction == "BUY" else "MULTDOWN"

        # Step 1: Get proposal
        proposal_payload = {
            "proposal":       1,
            "amount":         stake,
            "basis":          "stake",
            "contract_type":  contract_type,
            "currency":       self._currency,
            "duration_unit":  "s",
            "multiplier":     multiplier,
            "underlying_symbol": deriv_sym,
            "limit_order": {
                "stop_loss":   {"order_type": "stop",        "order_amount": sl_usd},
                "take_profit": {"order_type": "take_profit",  "order_amount": tp_usd},
            },
            "req_id": self._next_id(),
    }
    prop_resp = self._ws.send_recv(proposal_payload, timeout=15)
    if not prop_resp or "error" in prop_resp or "proposal" not in prop_resp:
            err = (prop_resp.get("error", {}).get("message", "no proposal") if prop_resp else "timeout")
            log_error(f"[Deriv] proposal failed ({symbol}): {err}")
            return None

    proposal_id = prop_resp["proposal"]["id"]

    # Step 2: Buy using proposal ID
    payload = {
            "buy":    proposal_id,
            "price":  stake,
            "req_id": self._next_id(),
    }

    resp = self._ws.send_recv(payload, timeout=15)

    if resp and "error" not in resp and "buy" in resp:
            buy_info    = resp["buy"]
            contract_id = str(buy_info.get("contract_id", ""))
            self._positions[contract_id] = {
                "contract_id":  contract_id,
                "symbol":       symbol,
                "deriv_symbol": deriv_sym,
                "direction":    direction,
                "stake":        stake,
                "multiplier":   multiplier,
                "sl_usd":       sl_usd,
                "tp_usd":       tp_usd,
                "buy_price":    buy_info.get("start_spot", 0),
                "opened_at":    datetime.now().isoformat(),
            }
            log_trade(f"[Deriv] {direction} {symbol} "
                      f"stake=${stake} x{multiplier} "
                      f"SL=${sl_usd} TP=${tp_usd} "
                      f"id={contract_id}")
            return self._positions[contract_id]

        err_resp = resp.get("error", {}) if resp else {}
        err = err_resp.get("message", "timeout")
        log_error(f"[Deriv] place_multiplier failed ({symbol}): {err}")
        return None

    def close_position(self, contract_id: str) -> bool:
        """Sell (close) an open Multipliers contract."""
        if not self._ensure_connected():
            return False
        resp = self._ws.send_recv({
            "sell":       contract_id,
            "price":      0,
            "req_id":     self._next_id(),
        }, timeout=10)
        if resp and "sell" in resp and "error" not in resp:
            profit = resp["sell"].get("sold_for", 0)
            log_trade(f"[Deriv] Closed {contract_id} profit=${profit}")
            self._positions.pop(contract_id, None)
            return True
        err = resp.get("error", {}).get("message", "unknown") if resp else "timeout"
        log_error(f"[Deriv] close_position failed ({contract_id}): {err}")
        return False

    def get_open_positions(self) -> List[Dict]:
        """Return list of open Multipliers positions from Deriv."""
        if not self._ensure_connected():
            return list(self._positions.values())
        resp = self._ws.send_recv({
            "portfolio": 1,
            "req_id":    self._next_id(),
        }, timeout=10)
        if resp and "portfolio" in resp:
            contracts = resp["portfolio"].get("contracts", [])
            # Sync local cache
            self._positions = {
                str(c["contract_id"]): {
                    "contract_id": str(c["contract_id"]),
                    "symbol":      _DERIV_REV_MAP.get(c.get("symbol", ""), c.get("symbol", "")),
                    "direction":   "BUY" if "MULTUP" in c.get("contract_type", "") else "SELL",
                    "buy_price":   float(c.get("buy_price", 0)),
                    "pnl":         float(c.get("bid", 0)) - float(c.get("buy_price", 0)),
                }
                for c in contracts
            }
        return list(self._positions.values())

    def check_and_close_exits(self) -> None:
        """Check open positions for SL/TP breach and close them."""
        for cid, pos in list(self._positions.items()):
            # Deriv handles SL/TP server-side - just sync portfolio
            pass
        self.get_open_positions()

    def get_balance(self) -> float:
        """Return current account balance."""
        info = self.get_account_info()
        return info["balance"] if info else self._balance

    def disconnect(self):
        """Close WebSocket connection."""
        if self._ws:
            self._ws.disconnect()
        self._authed = False
        self._ws = None
        log_info("[Deriv] Disconnected")


# ════════════════════════════════════════════════════════════════════
#  DERIV RISK MANAGER
#  Enforces account building stages, daily loss limit, per-trade risk
# ════════════════════════════════════════════════════════════════════

class DerivRiskManager:
    """
    Protects account through staged risk rules.
    Stage auto-advances as balance grows.
    """

    def __init__(self, balance: float):
        self._balance    = balance
        self._day_start  = balance
        self._day_loss   = 0.0
        self._day_date   = datetime.now().date()

    def _reset_day_if_needed(self, current_balance: float):
        today = datetime.now().date()
        if today != self._day_date:
            self._day_date   = today
            self._day_start  = current_balance
            self._day_loss   = 0.0

    def current_stage(self, balance: Optional[float] = None) -> Dict:
        """Return current risk stage based on balance."""
        bal = balance or self._balance
        for min_b, max_b, stake, mult, max_trades in DERIV_CONFIG["stages"]:
            if min_b <= bal < max_b:
                return {
                    "stake":      stake,
                    "multiplier": mult,
                    "max_trades": max_trades,
                    "min_balance": min_b,
                    "max_balance": max_b,
                }
        # Fallback - safest stage
        return {"stake": 1.00, "multiplier": 10, "max_trades": 2,
                "min_balance": 0, "max_balance": 10}

    def can_trade(self, current_balance: float,
                  open_trade_count: int) -> Tuple[bool, str]:
        """
        Check if a new trade is allowed.
        Returns (allowed: bool, reason: str)
        """
        self._balance = current_balance
        self._reset_day_if_needed(current_balance)

        stage = self.current_stage(current_balance)

        # Daily loss limit
        day_loss = self._day_start - current_balance
        if day_loss >= DERIV_CONFIG["daily_loss_limit"]:
            return False, (f"Daily loss limit reached "
                           f"(lost ${day_loss:.2f} today, "
                           f"limit=${DERIV_CONFIG['daily_loss_limit']})")

        # Max open trades
        if open_trade_count >= stage["max_trades"]:
            return False, (f"Max open trades reached "
                           f"({open_trade_count}/{stage['max_trades']})")

        # Minimum balance check (need enough for at least one stake)
        if current_balance < stage["stake"]:
            return False, (f"Balance ${current_balance:.2f} too low "
                           f"for min stake ${stage['stake']:.2f}")

        return True, "OK"

    def safe_symbols(self, balance: float) -> List[str]:
        """Return symbols safe for current balance level."""
        if balance < 10.0:
            return DERIV_CONFIG["safe_symbols_under_10"]
        return DERIV_ALL_SYMBOLS

    def log_stage(self, balance: float) -> None:
        """Log current stage info."""
        stage = self.current_stage(balance)
        log_info(f"[DerivRisk] Balance=${balance:.2f} "
                 f"Stage: stake=${stage['stake']} "
                 f"x{stage['multiplier']} "
                 f"max_trades={stage['max_trades']}")


# ════════════════════════════════════════════════════════════════════
#  OHLC PATTERN DETECTOR  (Lower Timeframe Entry Engine)
# ════════════════════════════════════════════════════════════════════

class OHLCPatterns:
    """
    Detects entry patterns on lower timeframes (M1, M5).
    Called AFTER higher TF gives a valid Wyckoff + SNR signal.
    Patterns: Engulfing, Pin Bar, Inside Bar Breakout.
    """

    @staticmethod
    def engulfing(df: pd.DataFrame, direction: str) -> bool:
        """
        Bullish engulfing (for BUY) or Bearish engulfing (for SELL).
        Last candle body must fully engulf previous candle body.
        """
        if df is None or len(df) < 2:
            return False
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        if direction == "BUY":
            return (curr["close"] > curr["open"] and
                    curr["open"]  < prev["close"] and
                    curr["close"] > prev["open"])
        else:  # SELL
            return (curr["close"] < curr["open"] and
                    curr["open"]  > prev["close"] and
                    curr["close"] < prev["open"])

    @staticmethod
    def pin_bar(df: pd.DataFrame, direction: str,
                wick_ratio: float = 2.0) -> bool:
        """
        Pin bar / hammer / shooting star.
        Wick must be at least wick_ratio times the body size.
        """
        if df is None or len(df) < 1:
            return False
        c         = df.iloc[-1]
        body      = abs(c["close"] - c["open"])
        if body == 0:
            return False
        upper_wick = c["high"] - max(c["open"], c["close"])
        lower_wick = min(c["open"], c["close"]) - c["low"]
        if direction == "BUY":
            # Long lower wick = rejection of lower prices
            return lower_wick >= body * wick_ratio and upper_wick < body * 0.5
        else:
            # Long upper wick = rejection of higher prices
            return upper_wick >= body * wick_ratio and lower_wick < body * 0.5

    @staticmethod
    def inside_bar_breakout(df: pd.DataFrame, direction: str) -> bool:
        """
        Inside bar: current bar high/low inside previous bar.
        Breakout: last closed bar broke out in signal direction.
        """
        if df is None or len(df) < 3:
            return False
        mother  = df.iloc[-3]
        inside  = df.iloc[-2]
        current = df.iloc[-1]
        is_inside = (inside["high"] < mother["high"] and
                     inside["low"]  > mother["low"])
        if not is_inside:
            return False
        if direction == "BUY":
            return current["close"] > mother["high"]
        else:
            return current["close"] < mother["low"]

    @staticmethod
    def detect(df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
        """
        Run all pattern checks. Returns (confirmed, pattern_name).
        Priority: Inside Bar > Engulfing > Pin Bar
        """
        if OHLCPatterns.inside_bar_breakout(df, direction):
            return True, "INSIDE_BAR_BREAKOUT"
        if OHLCPatterns.engulfing(df, direction):
            return True, "ENGULFING"
        if OHLCPatterns.pin_bar(df, direction):
            return True, "PIN_BAR"
        return False, "NO_PATTERN"


# ════════════════════════════════════════════════════════════════════
#  ANTI-MANIPULATION FILTER
# ════════════════════════════════════════════════════════════════════

class AntiManipFilter:
    """
    Filters out spike candles, wide spreads, and open-candle entries.
    Extra important for synthetic indices where Deriv controls price.
    """

    @staticmethod
    def is_spike(df: pd.DataFrame, atr_ratio: float = 3.0) -> bool:
        """True if last candle wick is abnormally large (spike)."""
        if df is None or len(df) < 10:
            return False
        atr     = float(df["range"].rolling(14).mean().iloc[-1])
        last    = df.iloc[-1]
        wick_up   = last["high"] - max(last["open"], last["close"])
        wick_down = min(last["open"], last["close"]) - last["low"]
        max_wick  = max(wick_up, wick_down)
        return atr > 0 and max_wick > atr * atr_ratio

    @staticmethod
    def spread_ok(tick: Optional[Dict],
                  max_pips: int = 5,
                  pip_size: float = 0.0001) -> bool:
        """True if spread is within acceptable range."""
        if tick is None:
            return True   # no tick data - allow (will fail at order time)
        spread = tick.get("ask", 0) - tick.get("bid", 0)
        return spread <= max_pips * pip_size

    @staticmethod
    def candle_closed(df: pd.DataFrame) -> bool:
        """
        True if the last candle appears to be closed
        (not the currently forming candle).
        We use a simple heuristic: check there are at least 2 candles.
        """
        return df is not None and len(df) >= 2

    @staticmethod
    def check(df: pd.DataFrame, tick: Optional[Dict],
              direction: str) -> Tuple[bool, str]:
        """
        Full filter check. Returns (ok: bool, reason: str).
        """
        if not AntiManipFilter.candle_closed(df):
            return False, "CANDLE_NOT_CLOSED"
        if AntiManipFilter.is_spike(df, DERIV_CONFIG["max_wick_atr_ratio"]):
            return False, "SPIKE_DETECTED"
        if not AntiManipFilter.spread_ok(tick, DERIV_CONFIG["max_spread_pips"]):
            return False, "SPREAD_TOO_WIDE"
        return True, "OK"


# ════════════════════════════════════════════════════════════════════
#  MULTI-ENTRY MANAGER
#  Opens up to N small trades on the same signal if price holds
# ════════════════════════════════════════════════════════════════════

class MultiEntryManager:
    """
    After a signal fires, checks if price is still near the entry zone
    every 30 seconds, and places additional trades up to the maximum.
    This grows exposure gradually on confirmed moves.
    """

    def __init__(self, broker: DerivBroker, risk: DerivRiskManager):
        self._broker     = broker
        self._risk       = risk
        self._entries:   Dict[str, List[Dict]] = {}   # signal_id -> trades

    def execute(self, signal: "Signal", balance: float) -> List[Dict]:
        """
        Execute up to max_entries trades for a signal.
        Waits between entries, checks price stays near original.
        Returns list of opened trade dicts.
        """
        max_e    = DERIV_CONFIG["max_entries_per_signal"]
        wait_s   = DERIV_CONFIG["entry_recheck_secs"]
        tol      = DERIV_CONFIG["price_tolerance_pct"]
        opened   = []
        signal_id = f"{signal.symbol}_{signal.timestamp}"

        for entry_num in range(1, max_e + 1):
            # Check risk allows another trade
            open_count = len(self._broker.get_open_positions())
            ok, reason = self._risk.can_trade(balance, open_count)
            if not ok:
                log_info(f"[MultiEntry] Stop at entry {entry_num}: {reason}")
                break

            # Get current price
            tick = self._broker.get_tick(signal.symbol)
            if tick is None:
                log_warn(f"[MultiEntry] No tick for {signal.symbol}, stopping")
                break

            # Check price still near original entry
            if entry_num > 1:
                mid     = tick.get("mid", 0)
                orig    = signal.snr_price
                drift   = abs(mid - orig) / orig if orig != 0 else 1.0
                if drift > tol:
                    log_info(f"[MultiEntry] Price drifted {drift:.3%} "
                             f"from entry - stopping additional entries")
                    break

            # Place trade
            stage = self._risk.current_stage(balance)
            trade = self._broker.place_multiplier(
                symbol     = signal.symbol,
                direction  = signal.direction.value,
                stake      = stage["stake"],
                multiplier = stage["multiplier"],
                sl_usd     = DERIV_CONFIG["stop_loss_usd"],
                tp_usd     = DERIV_CONFIG["take_profit_usd"],
            )

            if trade:
                opened.append(trade)
                log_info(f"[MultiEntry] Entry {entry_num}/{max_e} opened "
                         f"for {signal.symbol}")
            else:
                log_warn(f"[MultiEntry] Entry {entry_num} failed - stopping")
                break

            # Wait before next entry (except last one)
            if entry_num < max_e:
                log_info(f"[MultiEntry] Waiting {wait_s}s before next entry...")
                time.sleep(wait_s)

        self._entries[signal_id] = opened
        return opened


# ════════════════════════════════════════════════════════════════════
#  DERIV SCAN LOOP  - integrates with existing run_session_bot_loop
# ════════════════════════════════════════════════════════════════════

# Global DerivBroker instance (created once, reused)
_deriv_broker:  Optional[DerivBroker]     = None
_deriv_risk:    Optional[DerivRiskManager] = None
_deriv_entries: Optional[MultiEntryManager] = None


def _get_deriv_broker() -> Optional[DerivBroker]:
    """Return singleton DerivBroker, creating if needed."""
    global _deriv_broker
    _token = os.environ.get("DERIV_API_TOKEN", "")
    log_info(f"[Deriv DEBUG] token_len={len(_token)} broker_exists={_deriv_broker is not None}")
    if not _token:
        return None
    if _deriv_broker is None:
        _deriv_broker = DerivBroker(token=_token, demo=os.environ.get("DERIV_DEMO","1")=="1")

    # Retry connection every call if not yet authorized
    if not _deriv_broker._authed:
        try:
            log_info("[Deriv] Attempting connection via OTP flow...")
            connected = _deriv_broker._ensure_connected()
            log_info(f"[Deriv] Connection result: {connected}")
            if not connected:
                log_error("[Deriv] Connection failed - check DERIV_API_TOKEN and DERIV_APP_ID")
                _deriv_broker = None
                return None
        except Exception as _ce:
            log_error(f"[Deriv] Connection error: {_ce}")
            _deriv_broker = None
            return None

    return _deriv_broker


def deriv_ltf_entry_check(signal: "Signal",
                           broker: DerivBroker) -> Tuple[bool, str]:
    """
    Run lower-timeframe OHLC + anti-manipulation check on a signal.
    Called after higher TF Wyckoff + SNR confirms the signal.

    Returns (entry_ok: bool, reason: str)
    """
    direction = signal.direction.value
    confirmed = False
    pattern   = "NO_PATTERN"

    for tf in DERIV_CONFIG["ltf_timeframes"]:
        df = broker.get_candles(signal.symbol, tf, count=50)
        if df is None or len(df) < 5:
            log_warn(f"[DerivLTF] No {tf} candles for {signal.symbol}")
            continue

        # Anti-manipulation check
        tick       = broker.get_tick(signal.symbol)
        manip_ok, manip_reason = AntiManipFilter.check(df, tick, direction)
        if not manip_ok:
            log_info(f"[DerivLTF] {signal.symbol} {tf} "
                     f"FILTER: {manip_reason}")
            continue

        # OHLC pattern check
        pat_ok, pat_name = OHLCPatterns.detect(df, direction)
        if pat_ok:
            log_info(f"[DerivLTF] {signal.symbol} {tf} "
                     f"PATTERN: {pat_name} - ENTRY CONFIRMED")
            confirmed = True
            pattern   = f"{tf}:{pat_name}"
            break
        else:
            log_info(f"[DerivLTF] {signal.symbol} {tf} "
                     f"No pattern ({pat_name})")

    return confirmed, pattern


def run_deriv_scan(signals: List["Signal"]) -> None:
    """
    Process a list of signals through the Deriv entry pipeline.
    Called from run_session_bot_loop after existing analysis runs.

    Flow:
      1. Check DerivBroker available + token set
      2. For each signal:
         a. Check symbol is in Deriv symbol list
         b. DerivRiskManager.can_trade()
         c. deriv_ltf_entry_check() - LTF OHLC + anti-manip
         d. MultiEntryManager.execute() - place up to N trades
         e. Push trade event to webapp
    """
    global _deriv_risk, _deriv_entries

    broker = _get_deriv_broker()
    if broker is None:
        log_info("[Deriv] Skipping Deriv scan - no token configured")
        return

    # Get current balance for risk checks
    balance = broker.get_balance()
    if balance <= 0:
        log_warn("[Deriv] Cannot get balance - skipping scan")
        return

    # Init risk manager and multi-entry manager
    if _deriv_risk is None:
        _deriv_risk = DerivRiskManager(balance)
    if _deriv_entries is None:
        _deriv_entries = MultiEntryManager(broker, _deriv_risk)

    _deriv_risk.log_stage(balance)

    # Filter signals to Deriv-supported symbols
    safe_syms    = _deriv_risk.safe_symbols(balance)
    deriv_signals = [
        s for s in signals
        if s.symbol in safe_syms and
           (s.symbol in DERIV_FOREX_MAP or s.symbol in DERIV_SYNTHETIC_MAP)
    ]

    if not deriv_signals:
        log_info("[Deriv] No Deriv-compatible signals this cycle")
        return

    log_info(f"[Deriv] Processing {len(deriv_signals)} "
             f"signal(s) through Deriv pipeline")

    open_positions = broker.get_open_positions()
    open_count     = len(open_positions)

    for signal in deriv_signals:
        log_info(f"[Deriv] Evaluating {signal.symbol} "
                 f"{signal.direction.value} score={signal.score}")

        # Risk check
        ok, reason = _deriv_risk.can_trade(balance, open_count)
        if not ok:
            log_info(f"[Deriv] {signal.symbol} BLOCKED: {reason}")
            continue

        # Lower TF entry confirmation
        entry_ok, pattern = deriv_ltf_entry_check(signal, broker)
        if not entry_ok:
            log_info(f"[Deriv] {signal.symbol} "
                     f"No LTF entry confirmation - skipping")
            continue

        log_info(f"[Deriv] {signal.symbol} ENTRY CONFIRMED "
                 f"pattern={pattern} - executing...")

        # Multi-entry execution
        trades = _deriv_entries.execute(signal, balance)

        if trades:
            open_count += len(trades)
            # Update balance estimate (subtract stakes placed)
            balance -= sum(t.get("stake", 1.0) for t in trades)

            # Push to webapp
            push_all_data({
                "signals": [signal.to_dict()],
                "trades":  trades,
                "analysis": [],
                "metrics": {
                    "deriv_balance":  balance,
                    "entries_placed": len(trades),
                    "pattern":        pattern,
                },
                "event": {
                    "type":    "DERIV_TRADE_OPENED",
                    "symbol":  signal.symbol,
                    "pattern": pattern,
                    "entries": len(trades),
                },
            })
        else:
            log_warn(f"[Deriv] {signal.symbol} - no trades executed")


def deriv_sync_positions() -> None:
    """
    Sync open Deriv positions with webapp.
    Call this periodically (every scan cycle) to keep webapp updated.
    """
    broker = _get_deriv_broker()
    if broker is None:
        return
    positions = broker.get_open_positions()
    if positions:
        push_all_data({
            "signals":  [],
            "trades":   positions,
            "analysis": [],
            "metrics":  {"deriv_open_trades": len(positions)},
            "event":    {"type": "DERIV_POSITIONS_SYNC"},
        })


# ── Add Deriv Flask endpoints ─────────────────────────────────────────
# These are registered when Flask is available (same guard as other routes)

def _register_deriv_routes(app_instance):
    """Register Deriv-specific Flask routes on the existing Flask app."""

    from flask import request as flask_req, jsonify

    @app_instance.route("/deriv/status", methods=["GET"])
    def deriv_status():
        """Deriv connection status and balance."""
        broker = _get_deriv_broker()
        if broker is None:
            return jsonify({
                "connected": False,
                "reason":    "No DERIV_API_TOKEN configured",
            }), 200
        info = broker.get_account_info()
        return jsonify({
            "connected": broker._ws.connected and broker._authed,
            "demo":      broker._demo,
            "balance":   info["balance"] if info else 0,
            "currency":  info["currency"] if info else "USD",
        }), 200

    @app_instance.route("/deriv/positions", methods=["GET"])
    def deriv_positions():
        """List open Deriv Multiplier positions."""
        broker = _get_deriv_broker()
        if broker is None:
            return jsonify({"positions": [], "error": "No token"}), 200
        return jsonify({"positions": broker.get_open_positions()}), 200

    @app_instance.route("/deriv/close/<contract_id>", methods=["POST"])
    def deriv_close(contract_id):
        """Manually close a Deriv position from the webapp."""
        if not _check_secret(flask_req):
            return jsonify({"error": "Unauthorized"}), 401
        broker = _get_deriv_broker()
        if broker is None:
            return jsonify({"error": "Deriv not configured"}), 400
        ok = broker.close_position(contract_id)
        return jsonify({"closed": ok, "contract_id": contract_id}), 200

    @app_instance.route("/deriv/balance", methods=["GET"])
    def deriv_balance():
        """Quick balance check endpoint."""
        broker = _get_deriv_broker()
        if broker is None:
            return jsonify({"balance": 0, "error": "No token"}), 200
        return jsonify({"balance": broker.get_balance()}), 200

    log_info("[Deriv] Flask routes registered: "
             "/deriv/status /deriv/positions /deriv/close /deriv/balance")


# ════════════════════════════════════════════════════════════════════
#  SECTION 22: SYNTHETIC INDEX ALGORITHMIC ANALYSIS
#  Extra analysis layer for Deriv-generated indices (V10/V25/V50 etc)
#  These are NOT real markets - Deriv's algorithm controls price.
#  We detect their patterns algorithmically to trade WITH the algo,
#  not against it.
# ════════════════════════════════════════════════════════════════════

class SyntheticAnalyser:
    """
    Algorithmic analysis layer specifically for Deriv synthetic indices.

    Why synthetics need special treatment:
    - Price is generated by Deriv's algorithm, not real supply/demand
    - No news events, no bank manipulation - but Deriv CAN spike
    - Volatility % is fixed (V10=10%, V25=25% annualised etc)
    - Trends form and break algorithmically - detectable with stats
    - Boom/Crash indices have programmed spike events every N ticks

    Strategy: detect algorithmic micro-trends using momentum,
    mean-reversion signals, and tick velocity - then confirm with
    existing Wyckoff S/R from higher timeframe.
    """

    # Synthetic symbols and their expected volatility %
    VOLATILITY_PROFILE: Dict[str, float] = {
        "VOLATILITY_10":  0.10,
        "VOLATILITY_25":  0.25,
        "VOLATILITY_50":  0.50,
        "VOLATILITY_75":  0.75,
        "VOLATILITY_100": 1.00,
    }

    # Safe synthetics for small accounts (lower volatility = slower moves)
    SAFE_FOR_SMALL_ACCOUNT = {"VOLATILITY_10", "VOLATILITY_25"}

    @staticmethod
    def is_synthetic(symbol: str) -> bool:
        """True if the symbol is a Deriv synthetic index."""
        return symbol in SyntheticAnalyser.VOLATILITY_PROFILE

    @staticmethod
    def expected_atr(symbol: str, price: float) -> float:
        """
        Estimate expected ATR for a synthetic based on its volatility %.
        Used to calibrate spike detection thresholds per symbol.
        """
        vol_pct = SyntheticAnalyser.VOLATILITY_PROFILE.get(symbol, 0.25)
        # Daily ATR approx = price * vol_pct / sqrt(252)
        return price * vol_pct / (252 ** 0.5)

    @staticmethod
    def momentum_score(df: pd.DataFrame, period: int = 10) -> float:
        """
        Algorithmic momentum score for synthetic price series.
        Returns value between -1.0 (strong down) and +1.0 (strong up).

        Uses:
        - Rate of change over last N candles
        - EMA slope direction
        - Close relative to N-period high/low range
        """
        if df is None or len(df) < period + 2:
            return 0.0

        closes  = df["close"].values
        roc     = (closes[-1] - closes[-period]) / closes[-period] if closes[-period] != 0 else 0.0

        # EMA slope: positive = uptrend
        ema     = calc_ema(closes, period)
        slope   = (ema[-1] - ema[-3]) / ema[-3] if ema[-3] != 0 else 0.0

        # Position in range (0 = at low, 1 = at high)
        hi      = df["high"].values[-period:].max()
        lo      = df["low"].values[-period:].min()
        pos     = (closes[-1] - lo) / (hi - lo) if hi != lo else 0.5

        # Combine: normalise each component
        score   = (
            0.4 * max(-1.0, min(1.0, roc   * 100)) +
            0.3 * max(-1.0, min(1.0, slope * 100)) +
            0.3 * (pos * 2 - 1)            # convert 0-1 range to -1 to +1
        )
        return round(max(-1.0, min(1.0, score)), 4)

    @staticmethod
    def mean_reversion_zone(df: pd.DataFrame,
                            atr_mult: float = 1.5) -> Optional[str]:
        """
        Detect if price is stretched beyond ATR bands (mean-reversion setup).
        Returns 'LONG' if oversold, 'SHORT' if overbought, None if neutral.

        Synthetics revert to mean more reliably than real markets
        because there is no fundamental driver pushing price away.
        """
        if df is None or len(df) < 20:
            return None
        atr     = calc_atr(df)
        mid     = calc_ema(df["close"].values, 20)[-1]
        price   = df["close"].iloc[-1]

        upper   = mid + atr * atr_mult
        lower   = mid - atr * atr_mult

        if price < lower:
            return "LONG"   # oversold - expect bounce
        if price > upper:
            return "SHORT"  # overbought - expect pullback
        return None

    @staticmethod
    def tick_velocity(df_m1: pd.DataFrame, lookback: int = 5) -> float:
        """
        Measure how fast price is moving on M1.
        High velocity in signal direction = enter now.
        High velocity AGAINST signal = wait.

        Returns pip/candle velocity (positive = up, negative = down).
        """
        if df_m1 is None or len(df_m1) < lookback + 1:
            return 0.0
        closes  = df_m1["close"].values
        delta   = closes[-1] - closes[-lookback]
        return round(delta / lookback, 6)

    @staticmethod
    def algo_spike_risk(df: pd.DataFrame, symbol: str) -> bool:
        """
        Detect if the synthetic is in a spike/surge that is likely
        algorithmic rather than tradeable momentum.

        Uses symbol-calibrated ATR thresholds - V75 needs wider bands
        than V10 because it is designed to be more volatile.
        Returns True if spike risk is HIGH (skip trade).
        """
        if df is None or len(df) < 5:
            return False
        price       = float(df["close"].iloc[-1])
        exp_atr     = SyntheticAnalyser.expected_atr(symbol, price)
        actual_atr  = calc_atr(df, period=5)   # short ATR to catch recent spike
        if exp_atr == 0:
            return False
        # If recent ATR is more than 3x the expected ATR for this symbol,
        # the algo is in a programmed surge - too risky to enter
        return actual_atr > exp_atr * 3.0

    @staticmethod
    def analyse(symbol: str, df_htf: pd.DataFrame,
                df_m5: Optional[pd.DataFrame],
                df_m1: Optional[pd.DataFrame],
                signal_direction: str) -> Dict:
        """
        Full synthetic analysis pipeline.
        Returns analysis dict with recommendation and confidence score.

        Args:
            symbol           : bot internal symbol (e.g. VOLATILITY_10)
            df_htf           : higher timeframe candles (H1/H4 from existing analysis)
            df_m5            : M5 candles from Deriv (lower TF confirmation)
            df_m1            : M1 candles from Deriv (entry timing)
            signal_direction : BUY or SELL from existing Wyckoff analysis

        Returns dict:
            ok          : bool - proceed with trade?
            confidence  : 0-100
            reason      : str explanation
            entry_timing: IMMEDIATE | WAIT | SKIP
        """
        result = {
            "ok":           False,
            "confidence":   0,
            "reason":       "",
            "entry_timing": "SKIP",
            "momentum":     0.0,
            "mean_rev":     None,
            "velocity":     0.0,
            "spike_risk":   False,
        }

        if not SyntheticAnalyser.is_synthetic(symbol):
            # Not a synthetic - return OK so regular analysis proceeds
            result["ok"]           = True
            result["entry_timing"] = "IMMEDIATE"
            result["reason"]       = "Forex pair - skip synthetic analysis"
            result["confidence"]   = 70
            return result

        # 1. Check symbol safe for small account
        if symbol not in SyntheticAnalyser.SAFE_FOR_SMALL_ACCOUNT:
            result["reason"] = (f"{symbol} too volatile for small account. "
                                f"Use VOLATILITY_10 or VOLATILITY_25.")
            return result

        # 2. Spike risk check on HTF
        if SyntheticAnalyser.algo_spike_risk(df_htf, symbol):
            result["reason"]       = "ALGO_SPIKE: ATR 3x expected - skip"
            result["spike_risk"]   = True
            return result

        score = 0

        # 3. Momentum check on M5
        mom_m5 = SyntheticAnalyser.momentum_score(df_m5, period=10) if df_m5 is not None else 0.0
        result["momentum"] = mom_m5
        if signal_direction == "BUY"  and mom_m5 > 0.2:
            score += 30
        elif signal_direction == "SELL" and mom_m5 < -0.2:
            score += 30
        elif abs(mom_m5) < 0.1:
            score += 10   # neutral momentum - caution but not blocked

        # 4. Mean reversion zone check on HTF
        rev_zone = SyntheticAnalyser.mean_reversion_zone(df_htf)
        result["mean_rev"] = rev_zone
        if rev_zone == signal_direction[:4] if signal_direction == "LONG" else rev_zone == signal_direction:
            score += 20
        if rev_zone == signal_direction:
            score += 20
        elif rev_zone is None:
            score += 10   # neutral

        # 5. Tick velocity on M1
        velocity = SyntheticAnalyser.tick_velocity(df_m1) if df_m1 is not None else 0.0
        result["velocity"] = velocity
        if signal_direction == "BUY"  and velocity > 0:
            score += 25
        elif signal_direction == "SELL" and velocity < 0:
            score += 25
        elif (signal_direction == "BUY"  and velocity < 0) or              (signal_direction == "SELL" and velocity > 0):
            score -= 15   # velocity against signal - caution

        # 6. Spike risk on M5
        if df_m5 is not None and SyntheticAnalyser.algo_spike_risk(df_m5, symbol):
            score -= 20
            result["spike_risk"] = True

        score = max(0, min(100, score))
        result["confidence"] = score

        if score >= 55:
            result["ok"]           = True
            result["entry_timing"] = "IMMEDIATE"
            result["reason"]       = (f"Synthetic OK: score={score} "
                                      f"mom={mom_m5:.2f} vel={velocity:.6f}")
        elif score >= 35:
            result["ok"]           = True
            result["entry_timing"] = "WAIT"
            result["reason"]       = (f"Synthetic MARGINAL: score={score} "
                                      f"- wait for next candle")
        else:
            result["ok"]           = False
            result["entry_timing"] = "SKIP"
            result["reason"]       = (f"Synthetic LOW CONFIDENCE: score={score} "
                                      f"- skipping")

        return result


# ════════════════════════════════════════════════════════════════════
#  SECTION 23: $3 ACCOUNT MANAGER
#  Full drawdown guard, emergency stop, balance-based auto-scaling.
#  Wraps DerivRiskManager with additional hard safety rules.
# ════════════════════════════════════════════════════════════════════

class AccountManager:
    """
    Hard safety layer for small accounts ($3 starting balance).

    Rules enforced:
    1. Never risk more than 30% of balance on open trades combined
    2. Daily loss limit: $1.00 or 33% of opening balance (whichever lower)
    3. Emergency stop: if balance drops below $1.50, STOP ALL TRADING
    4. Winning streak bonus: after 3 consecutive wins, allow 1 extra trade
    5. Losing streak pause: after 2 consecutive losses, pause 30 minutes
    6. Auto-scale stakes as balance grows (inherits from DerivRiskManager)
    7. Never open a trade within 60 seconds of previous trade on same symbol
    """

    EMERGENCY_STOP_BALANCE  = 1.50   # hard stop below this
    MAX_EXPOSURE_PCT        = 0.30   # max 30% of balance in open stakes
    WIN_STREAK_BONUS        = 3      # wins before allowing extra trade
    LOSS_STREAK_PAUSE_SECS  = 1800   # 30 min pause after 2 losses
    SAME_SYMBOL_COOLDOWN    = 60     # seconds between trades on same symbol

    def __init__(self, opening_balance: float):
        self._opening_balance  = opening_balance
        self._day_start        = opening_balance
        self._day_date         = datetime.now().date()
        self._win_streak       = 0
        self._loss_streak      = 0
        self._last_loss_time:  Optional[float] = None
        self._last_trade_time: Dict[str, float] = {}   # symbol -> timestamp
        self._total_wins       = 0
        self._total_losses     = 0
        self._risk             = DerivRiskManager(opening_balance)

    def _reset_day_if_needed(self, balance: float) -> None:
        today = datetime.now().date()
        if today != self._day_date:
            self._day_date  = today
            self._day_start = balance
            log_info(f"[AccountMgr] New trading day. Start balance=${balance:.2f}")

    def record_trade_result(self, won: bool, pnl: float) -> None:
        """Call after each trade closes to update streak tracking."""
        if won:
            self._win_streak  += 1
            self._loss_streak  = 0
            self._total_wins  += 1
            log_info(f"[AccountMgr] WIN #{self._total_wins} "
                     f"pnl=+${pnl:.2f} streak={self._win_streak}")
        else:
            self._loss_streak     += 1
            self._win_streak       = 0
            self._total_losses    += 1
            self._last_loss_time   = time.time()
            log_info(f"[AccountMgr] LOSS #{self._total_losses} "
                     f"pnl=-${abs(pnl):.2f} streak={self._loss_streak}")

    def can_trade(self, balance: float, open_positions: List[Dict],
                  symbol: str) -> Tuple[bool, str]:
        """
        Master gate: checks ALL safety rules before allowing a trade.
        Returns (allowed: bool, reason: str).
        """
        self._reset_day_if_needed(balance)

        # Rule 1: Emergency stop
        if balance < self.EMERGENCY_STOP_BALANCE:
            return False, (f"EMERGENCY STOP: balance ${balance:.2f} < "
                           f"${self.EMERGENCY_STOP_BALANCE:.2f} minimum. "
                           f"Trading halted to protect account.")

        # Rule 2: Daily loss limit
        day_loss   = self._day_start - balance
        daily_limit = min(1.00, self._day_start * 0.33)
        if day_loss >= daily_limit:
            return False, (f"DAILY LIMIT HIT: lost ${day_loss:.2f} today "
                           f"(limit=${daily_limit:.2f}). Resume tomorrow.")

        # Rule 3: Max exposure
        stage           = self._risk.current_stage(balance)
        stake_per_trade = stage["stake"]
        open_exposure   = len(open_positions) * stake_per_trade
        max_exposure    = balance * self.MAX_EXPOSURE_PCT
        if open_exposure >= max_exposure:
            return False, (f"MAX EXPOSURE: ${open_exposure:.2f} already "
                           f"at risk (limit=${max_exposure:.2f})")

        # Rule 4: Stage max trades
        max_trades = stage["max_trades"]
        if self._win_streak >= self.WIN_STREAK_BONUS:
            max_trades += 1   # bonus trade after win streak
        if len(open_positions) >= max_trades:
            return False, (f"MAX TRADES: {len(open_positions)}/{max_trades} "
                           f"open positions")

        # Rule 5: Loss streak pause
        if self._loss_streak >= 2 and self._last_loss_time:
            elapsed = time.time() - self._last_loss_time
            if elapsed < self.LOSS_STREAK_PAUSE_SECS:
                wait = int(self.LOSS_STREAK_PAUSE_SECS - elapsed)
                return False, (f"LOSS STREAK PAUSE: {self._loss_streak} "
                               f"consecutive losses. Pausing {wait}s more.")
            else:
                self._loss_streak = 0   # pause served - reset

        # Rule 6: Same symbol cooldown
        last = self._last_trade_time.get(symbol, 0)
        if time.time() - last < self.SAME_SYMBOL_COOLDOWN:
            wait = int(self.SAME_SYMBOL_COOLDOWN - (time.time() - last))
            return False, (f"SYMBOL COOLDOWN: {symbol} "
                           f"traded recently. Wait {wait}s.")

        # Rule 7: Min balance for stake
        if balance < stake_per_trade:
            return False, (f"INSUFFICIENT BALANCE: ${balance:.2f} < "
                           f"stake ${stake_per_trade:.2f}")

        return True, "OK"

    def record_trade_opened(self, symbol: str) -> None:
        """Call when a trade is opened."""
        self._last_trade_time[symbol] = time.time()

    def get_stage_info(self, balance: float) -> Dict:
        """Return current stage info for logging/webapp."""
        stage = self._risk.current_stage(balance)
        return {
            "balance":      round(balance, 2),
            "stage_stake":  stage["stake"],
            "multiplier":   stage["multiplier"],
            "max_trades":   stage["max_trades"],
            "win_streak":   self._win_streak,
            "loss_streak":  self._loss_streak,
            "total_wins":   self._total_wins,
            "total_losses": self._total_losses,
            "day_start":    round(self._day_start, 2),
            "day_pnl":      round(balance - self._day_start, 2),
        }


# ════════════════════════════════════════════════════════════════════
#  SECTION 24: DERIV TRADE WATCHER
#  Monitors open trades in a background thread.
#  Closes trades immediately if price reversal is detected.
#  This is separate from Deriv's server-side SL - it catches
#  reversals BEFORE they hit SL, preserving more of your balance.
# ════════════════════════════════════════════════════════════════════

class DerivTradeWatcher:
    """
    Background thread that monitors every open Deriv position.

    Reversal detection logic:
    1. Price moves against trade by > 0.5 ATR on M1 = WARNING
    2. Two consecutive M1 candles closing against trade = CLOSE NOW
    3. Momentum flips against trade direction on M1 = CLOSE NOW
    4. Price breaks back through entry S/R level = CLOSE NOW

    This runs every 15 seconds to catch reversals fast.
    Deriv SL is the hard backstop; this watcher is the soft early exit.
    """

    CHECK_INTERVAL_SECS = 15    # check every 15 seconds
    REVERSAL_CANDLES    = 2     # consecutive adverse candles before closing
    MOMENTUM_FLIP_THRESH = -0.3  # momentum score below this = flip detected

    def __init__(self, broker: DerivBroker, account_mgr: AccountManager):
        self._broker      = broker
        self._acct        = account_mgr
        self._running     = False
        self._thread: Optional[_dthread.Thread] = None
        self._watch_list: Dict[str, Dict] = {}  # contract_id -> position info

    def start(self) -> None:
        """Start the watcher background thread."""
        if self._running:
            return
        self._running = True
        self._thread  = _dthread.Thread(
            target=self._watch_loop,
            name="DerivTradeWatcher",
            daemon=True,
        )
        self._thread.start()
        log_info("[TradeWatcher] Started - monitoring positions every "
                 f"{self.CHECK_INTERVAL_SECS}s")

    def stop(self) -> None:
        """Stop the watcher thread."""
        self._running = False
        log_info("[TradeWatcher] Stopped")

    def add_position(self, position: Dict) -> None:
        """Register a position to be watched."""
        cid = str(position.get("contract_id", ""))
        if cid:
            self._watch_list[cid] = {
                **position,
                "entry_time":     time.time(),
                "adverse_candles": 0,
                "last_m1_close":   None,
            }
            log_info(f"[TradeWatcher] Watching {position.get('symbol')} "
                     f"id={cid}")

    def remove_position(self, contract_id: str) -> None:
        """Remove a position from watch list (after close)."""
        self._watch_list.pop(str(contract_id), None)

    def _detect_reversal(self, position: Dict,
                          df_m1: Optional[pd.DataFrame],
                          current_price: float) -> Tuple[bool, str]:
        """
        Check if a position should be closed due to reversal.
        Returns (close_now: bool, reason: str).
        """
        direction = position.get("direction", "BUY")
        entry     = float(position.get("buy_price", current_price))
        symbol    = position.get("symbol", "")

        if df_m1 is None or len(df_m1) < 5:
            return False, "NO_DATA"

        # ── Check 1: Momentum flip ───────────────────────────────────
        mom = SyntheticAnalyser.momentum_score(df_m1, period=5)
        if direction == "BUY"  and mom < self.MOMENTUM_FLIP_THRESH:
            return True, f"MOMENTUM_FLIP: score={mom:.2f} (BUY->bearish)"
        if direction == "SELL" and mom > abs(self.MOMENTUM_FLIP_THRESH):
            return True, f"MOMENTUM_FLIP: score={mom:.2f} (SELL->bullish)"

        # ── Check 2: Consecutive adverse candles ────────────────────
        last_candles = df_m1.iloc[-self.REVERSAL_CANDLES:]
        if direction == "BUY":
            # All last N candles closed bearish
            all_bearish = all(
                row["close"] < row["open"]
                for _, row in last_candles.iterrows()
            )
            if all_bearish:
                return True, (f"ADVERSE_CANDLES: {self.REVERSAL_CANDLES} "
                              f"bearish candles against BUY")
        else:  # SELL
            all_bullish = all(
                row["close"] > row["open"]
                for _, row in last_candles.iterrows()
            )
            if all_bullish:
                return True, (f"ADVERSE_CANDLES: {self.REVERSAL_CANDLES} "
                              f"bullish candles against SELL")

        # ── Check 3: Price moved back through entry level ───────────
        atr  = calc_atr(df_m1, period=5)
        if direction == "BUY" and current_price < entry - atr * 0.5:
            return True, (f"ENTRY_BREACH: price {current_price:.5f} < "
                          f"entry {entry:.5f} - 0.5ATR")
        if direction == "SELL" and current_price > entry + atr * 0.5:
            return True, (f"ENTRY_BREACH: price {current_price:.5f} > "
                          f"entry {entry:.5f} + 0.5ATR")

        return False, "OK"

    def _watch_loop(self) -> None:
        """Main watcher loop - runs in background thread."""
        log_info("[TradeWatcher] Watch loop running...")
        while self._running:
            try:
                if not self._watch_list:
                    time.sleep(self.CHECK_INTERVAL_SECS)
                    continue

                # Sync positions from Deriv
                live_positions = self._broker.get_open_positions()
                live_ids       = {str(p["contract_id"]) for p in live_positions}

                # Remove positions closed server-side (SL/TP hit)
                for cid in list(self._watch_list.keys()):
                    if cid not in live_ids:
                        log_info(f"[TradeWatcher] {cid} closed server-side")
                        pos    = self._watch_list.pop(cid)
                        symbol = pos.get("symbol", "")
                        # Try to determine win/loss
                        self._acct.record_trade_result(
                            won=False,   # conservative - assume SL hit
                            pnl=-DERIV_CONFIG["stop_loss_usd"]
                        )

                # Check each watched position for reversal
                for cid, pos in list(self._watch_list.items()):
                    symbol = pos.get("symbol", "")
                    try:
                        # Get M1 candles for reversal check
                        df_m1 = self._broker.get_candles(symbol, "M1", count=20)
                        tick  = self._broker.get_tick(symbol)
                        price = tick["mid"] if tick else 0.0

                        close_now, reason = self._detect_reversal(
                            pos, df_m1, price
                        )

                        if close_now:
                            log_warn(f"[TradeWatcher] REVERSAL DETECTED "
                                     f"{symbol} {cid}: {reason}")
                            log_warn(f"[TradeWatcher] Closing {cid} NOW "
                                     f"to protect account...")
                            ok = self._broker.close_position(cid)
                            if ok:
                                self._watch_list.pop(cid, None)
                                log_trade(f"[TradeWatcher] CLOSED {cid} "
                                          f"on reversal: {reason}")
                                # Record as loss (closed early, below TP)
                                entry_price = float(pos.get("buy_price", price))
                                pnl_est     = (
                                    (price - entry_price)
                                    * pos.get("stake", 1.0)
                                    * pos.get("multiplier", 10)
                                )
                                won = pnl_est > 0
                                self._acct.record_trade_result(won, pnl_est)

                                # Push close event to webapp
                                push_all_data({
                                    "signals":  [],
                                    "trades":   [],
                                    "analysis": [],
                                    "metrics":  {},
                                    "event": {
                                        "type":        "DERIV_REVERSAL_CLOSE",
                                        "contract_id": cid,
                                        "symbol":      symbol,
                                        "reason":      reason,
                                        "pnl_est":     round(pnl_est, 4),
                                    },
                                })
                            else:
                                log_error(f"[TradeWatcher] Failed to close "
                                          f"{cid} - Deriv SL will handle it")
                        else:
                            log_info(f"[TradeWatcher] {symbol} {cid} OK "
                                     f"({reason})")

                    except Exception as e:
                        log_error(f"[TradeWatcher] Error checking {cid}: {e}")

            except Exception as e:
                log_error(f"[TradeWatcher] Loop error: {e}")

            time.sleep(self.CHECK_INTERVAL_SECS)


# ════════════════════════════════════════════════════════════════════
#  SECTION 25: UPDATED DERIV SCAN WITH ALL LAYERS INTEGRATED
#  Replaces run_deriv_scan - adds synthetic analysis + account manager
#  + trade watcher to the existing pipeline.
# ════════════════════════════════════════════════════════════════════

# ── Register Deriv Flask routes (defined above, safe to call now) ────
if FLASK_OK:
    try:
        _register_deriv_routes(app)
        log_info("[Deriv] Flask routes registered successfully")
    except Exception as _dre:
        log_warn(f"[Deriv] Could not register Flask routes: {_dre}")

# ── Log Deriv token status ────────────────────────────────────────────
if _DERIV_TOKEN:
    _dmode = "DEMO" if _DERIV_DEMO else "REAL"
    log_info(f"[Deriv] Token configured - {_dmode} account")
    log_info("[Deriv] DerivBroker will connect on first scan cycle")
else:
    log_warn("[Deriv] DERIV_API_TOKEN not set - Deriv trading disabled")
    log_warn("[Deriv] Add DERIV_API_TOKEN to Render environment vars")

# Global singletons for new components
_account_mgr:   Optional[AccountManager]    = None
_trade_watcher: Optional[DerivTradeWatcher] = None


def run_deriv_scan_v2(signals: List["Signal"]) -> None:
    """
    Full Deriv pipeline with all safety layers.
    Replaces run_deriv_scan in the main bot loop.

    Pipeline per signal:
    1. AccountManager.can_trade() - hard safety gate
    2. SyntheticAnalyser.analyse() - algo analysis for indices
    3. deriv_ltf_entry_check() - LTF OHLC + anti-manipulation
    4. MultiEntryManager.execute() - place up to N trades
    5. DerivTradeWatcher.add_position() - monitor each trade
    6. Push to webapp
    """
    global _account_mgr, _trade_watcher

    broker = _get_deriv_broker()
    if broker is None:
        log_info("[Deriv] Skipping - no DERIV_API_TOKEN")
        return

    balance = broker.get_balance()
    if balance <= 0:
        log_warn("[Deriv] Cannot get balance - skipping")
        return

    # Init account manager
    if _account_mgr is None:
        _account_mgr = AccountManager(balance)
        log_info(f"[AccountMgr] Initialised with ${balance:.2f}")

    # Init and start trade watcher
    if _trade_watcher is None:
        _trade_watcher = DerivTradeWatcher(broker, _account_mgr)
        _trade_watcher.start()

    # Log account stage
    stage_info = _account_mgr.get_stage_info(balance)
    log_info(f"[AccountMgr] {stage_info}")

    # Emergency stop check before doing anything
    if balance < AccountManager.EMERGENCY_STOP_BALANCE:
        log_warn(f"[AccountMgr] EMERGENCY STOP - balance ${balance:.2f}. "
                 f"No trading until reloaded.")
        push_all_data({
            "signals": [], "trades": [], "analysis": [],
            "metrics": stage_info,
            "event":   {"type": "EMERGENCY_STOP", "balance": balance},
        })
        return

    open_positions = broker.get_open_positions()

    # Filter to Deriv symbols safe for current balance
    safe_syms = _account_mgr._risk.safe_symbols(balance)
    deriv_signals = [
        s for s in signals
        if s.symbol in safe_syms and
           (s.symbol in DERIV_FOREX_MAP or s.symbol in DERIV_SYNTHETIC_MAP)
    ]

    if not deriv_signals:
        log_info("[Deriv] No compatible signals this cycle")
        return

    log_info(f"[Deriv] Evaluating {len(deriv_signals)} signal(s)...")

    risk_mgr    = DerivRiskManager(balance)
    entry_mgr   = MultiEntryManager(broker, risk_mgr)

    for signal in deriv_signals:
        symbol    = signal.symbol
        direction = signal.direction.value

        log_info(f"[Deriv] ── {symbol} {direction} score={signal.score} ──")

        # ── Gate 1: AccountManager hard safety check ─────────────────
        ok, reason = _account_mgr.can_trade(balance, open_positions, symbol)
        if not ok:
            log_info(f"[AccountMgr] BLOCKED {symbol}: {reason}")
            continue

        # ── Gate 2: Synthetic algorithmic analysis ───────────────────
        if SyntheticAnalyser.is_synthetic(symbol):
            df_htf = broker.get_candles(symbol, "H1", count=100)
            df_m5  = broker.get_candles(symbol, "M5", count=50)
            df_m1  = broker.get_candles(symbol, "M1", count=30)

            syn    = SyntheticAnalyser.analyse(
                symbol, df_htf, df_m5, df_m1, direction
            )
            log_info(f"[SyntheticAnalyser] {symbol}: "
                     f"ok={syn['ok']} conf={syn['confidence']} "
                     f"timing={syn['entry_timing']} "
                     f"reason={syn['reason']}")

            if not syn["ok"]:
                log_info(f"[Deriv] {symbol} BLOCKED by synthetic analysis")
                continue

            if syn["entry_timing"] == "WAIT":
                log_info(f"[Deriv] {symbol} - waiting for next candle "
                         f"(marginal synthetic setup)")
                continue

        # ── Gate 3: LTF OHLC + anti-manipulation check ───────────────
        entry_ok, pattern = deriv_ltf_entry_check(signal, broker)
        if not entry_ok:
            log_info(f"[Deriv] {symbol} no LTF confirmation - skipping")
            continue

        log_info(f"[Deriv] {symbol} ALL GATES PASSED - "
                 f"pattern={pattern} - executing...")

        # ── Execute trades ────────────────────────────────────────────
        _account_mgr.record_trade_opened(symbol)
        trades = entry_mgr.execute(signal, balance)

        if trades:
            open_positions = list(open_positions) + trades
            balance        -= sum(t.get("stake", 1.0) for t in trades)

            # Register each trade with the watcher
            for trade in trades:
                _trade_watcher.add_position(trade)

            # Push to webapp
            push_all_data({
                "signals":  [signal.to_dict()],
                "trades":   trades,
                "analysis": [],
                "metrics":  {
                    **stage_info,
                    "entries_placed": len(trades),
                    "pattern":        pattern,
                },
                "event": {
                    "type":      "DERIV_TRADE_OPENED",
                    "symbol":    symbol,
                    "direction": direction,
                    "pattern":   pattern,
                    "entries":   len(trades),
                    "balance":   round(balance, 2),
                },
            })
        else:
            log_warn(f"[Deriv] {symbol} - no trades executed")

if __name__ == "__main__":
    render_env = _os.environ.get("RENDER") or _os.environ.get("PORT")

    if render_env and FLASK_OK:
        log_info("Render environment detected - starting hybrid session mode.")
        log_info("Bot loop     → background thread (idle until session start)")
        log_info("Flask server → port 10000")
        if not BOT_SECRET:
            log_warn("=" * 60)
            log_warn("  WARNING: BOT_SECRET env var is not set.")
            log_warn("  Session endpoints (/session/start, /session/end)")
            log_warn("  are DISABLED until BOT_SECRET is configured.")
            log_warn("  Set it in Render dashboard → Environment → Add var.")
            log_warn("=" * 60)
            
        if _TELEGRAM_TOKEN:
            _tg_thread = threading.Thread(
                target=run_telegram_polling, name="TelegramPoller", daemon=True
            )
            _tg_thread.start()
            log_info("[Telegram] Polling thread started")
        
        bot_thread = threading.Thread(
            target=run_session_bot_loop, name="BotLoop"
        )
        bot_thread.daemon = True
        bot_thread.start()

        port = int(_os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port)

    else:
        # Local / Pydroid - original behaviour, no session logic
        run()
