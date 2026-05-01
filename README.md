# Wyckoff 2.0 + Malaysian SNR Trading Bot

A modular, multi-environment algorithmic trading bot based on Wyckoff Phase Analysis and Malaysian SNR (Support & Resistance) methodology.

Runs on **Android (Pydroid 3)**, **PC**, and **Render (cloud server)** from a single file.

---

## Architecture

```
wyckoff_snr_bot_pydroid-4-1.py
│
├── Section 1  – Imports & Logging
├── Section 2  – CONFIG (broker credentials, symbols, risk)
├── Section 3  – Enums & Dataclasses
├── Section 4  – HTTP Helpers (requests + urllib fallback)
├── Section 5  – Broker Layer
│                 MetaAPIBroker | OANDABroker
│                 YFinanceBroker | YFinanceMT5Broker | PaperBroker
├── Section 6  – Binance Data Layer (crypto OB, volume, footprint)
├── Section 7  – Data Helpers & Indicators (ATR, EMA, VWAP, VPOC)
├── Section 8  – Malaysian SNR Detection (A/V shape, gap, congestion)
├── Section 9  – Wyckoff Phase Analysis
├── Section 10 – Bias Cascade & Confluence
├── Section 11 – 5-Layer Signal Scoring
├── Section 12 – Risk & Control (lot sizing, daily loss guard, cooldown)
├── Section 13 – PnL & Equity Tracking
├── Section 14 – Price Feed (MT5 bridge → broker fallback)
├── Section 15 – Trade Lifecycle Manager
│                 open_trade_record()
│                 update_trade_record()  ← trailing stop
│                 close_trade_record()
│                 sync_lifecycle_with_broker()
├── Section 16 – Execution Pipeline (pre-trade gate)
├── Section 17 – Main Scan Loop
├── Section 18 – Render Hybrid (Flask + background bot thread)
├── Section 19 – Session Manager (/session/start, /session/end)
└── Section 20 – Entry Point (auto-detects environment)
```

---

## Price Feed Priority

| Priority | Source | When active |
|---|---|---|
| 1st | MT5 bridge (`localhost:8000`) | When your PC bridge server is running |
| 2nd | `broker.get_tick()` | MetaAPI REST / YFinance / PaperBroker |

---

## Trade Lifecycle Flow

```
signal scored
    └─▶ execute_trade_pipeline()   (pre-trade gate: spread, RR, daily loss…)
            └─▶ broker.place_order()
                    └─▶ open_trade_record()    registers trade in memory
                              │
                    [every scan cycle]
                              ▼
                        update_trade_record()  trailing stop, PnL, SL/TP check
                              │
                    [SL/TP hit or broker closed]
                              ▼
                        close_trade_record()   moves to history, pushes to webapp
```

---

## Broker Modes

| Mode | Price data | Execution |
|---|---|---|
| `YFINANCE_MT5` | Yahoo Finance (free) | MT5 EA reads `signal_queue.json` |
| `METAAPI` | MetaAPI REST | Direct MT5 via MetaAPI cloud |
| `OANDA` | OANDA REST | OANDA v20 REST API |
| `YFINANCE` | Yahoo Finance | Signal log only (no execution) |
| `PAPER` | Synthetic | Simulated trades locally |

---

## Installation

```bash
# Clone
git clone https://github.com/yourusername/wyckoff-snr-bot.git
cd wyckoff-snr-bot

# Install dependencies
pip install numpy pandas scipy requests flask

# Run locally
python wyckoff_snr_bot_pydroid-4-1.py
```

### Android (Pydroid 3)
Install from the Pydroid pip menu: `numpy`, `pandas`, `scipy`, `requests`  
Flask is not required on Android — the bot runs in local mode automatically.

---

## Render Deployment

1. Push this repo to GitHub
2. Create a new **Web Service** on [render.com](https://render.com)
3. Set the start command: `python wyckoff_snr_bot_pydroid-4-1.py`
4. Add environment variables:

| Variable | Value |
|---|---|
| `BOT_SECRET` | A long random string (shared with your webapp) |
| `WEBAPP_API_URL` | Your webapp's push endpoint (optional) |

---

## Flask API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Bot status + session state |
| GET | `/health` | Render health check |
| GET | `/signals` | Last 20 signals |
| GET | `/analysis` | Last 10 chart analysis snapshots |
| GET | `/trades` | Live open trades |
| GET | `/trades/history` | Last 50 closed trades |
| POST | `/session/start` | Inject MetaAPI credentials (secured) |
| POST | `/session/end` | Wipe credentials, stop trading |
| GET | `/session/status` | Session active/idle check |

All `/session/*` endpoints require the `X-Bot-Secret` header.

### Session Start Payload
```json
{
  "metaapi_token":   "<user's MetaAPI token>",
  "metaapi_account": "<user's MetaAPI account ID>",
  "user_id":         "<your webapp user ID>"
}
```

---

## Security Notes

- `BOT_SECRET` is never hardcoded — set it as an environment variable
- MetaAPI credentials are held **in RAM only** — never written to disk or logs
- On `/session/end`, the token string is overwritten with zeros before deletion
- If `BOT_SECRET` is not set, all session endpoints return 401 automatically

---

## Configuration

Edit the `CONFIG` dict at the top of the bot file:

```python
CONFIG = {
    "broker_mode":    "METAAPI",       # METAAPI | YFINANCE_MT5 | PAPER | OANDA
    "metaapi_token":  "YOUR_TOKEN",    # MetaAPI token (or use session injection)
    "metaapi_account":"YOUR_ACCT_ID",
    "symbols":        ["EUR_USD", "GBP_USD", "XAU_USD"],
    "risk_pct":       1.0,             # % of balance risked per trade
    "max_trades":     3,
    "combined_min":   9,               # minimum score to take a signal
    "scan_secs":      300,             # scan every 5 minutes
}
```

---

## Files Written at Runtime

| File | Contents |
|---|---|
| `signals_log.json` | Last 100 scored signals |
| `chart_analysis.json` | Last 50 chart analysis snapshots |
| `open_trades.json` | Currently open trades (lifecycle manager) |
| `trade_history.json` | Last 200 closed trades |
| `signal_queue.json` | MT5 EA signal queue (YFINANCE_MT5 mode) |
| `equity_curve.json` | Equity history (last 500 points) |
| `bot_log.txt` | Full rotating log |

---

## License

MIT — use freely, trade at your own risk.
