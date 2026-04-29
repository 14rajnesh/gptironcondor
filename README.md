# Weekly NIFTY Iron Condor Paper Trading Bot

This is a **paper-only** weekly NIFTY iron condor bot following this flow:

1. Start system and load config
2. Telegram control: `/start`, `/stop`, `/status`, `/pnl`
3. Circuit breaker: NORMAL / DEGRADED / OPEN / HALF_OPEN
4. Fetch market data
5. Validate data
6. Liquidity filter
7. Greeks engine: Angel One Greeks first, Black-Scholes fallback
8. VIX processing
9. Existing trade management or new trade entry
10. CSV logging
11. Streamlit dashboard support
12. Error handling and circuit recovery

## Important

- This bot does **not** place real orders.
- FYERS is used for NIFTY spot, VIX, and option chain.
- Angel One is optional for Greeks only. If Angel credentials are not present, Black-Scholes Greeks are used.
- CSV state is stored in the `data/` folder.
- For serious multi-day paper trading, run it on a VPS/local machine. GitHub Actions can run it, but long-running loops and CSV persistence are less reliable than a VPS.

## Files

```text
weekly_iron_condor_bot.py     Main bot loop
config.py                     All thresholds and credentials
market_data.py                FYERS + optional Angel Greeks
strategy.py                   Iron condor entry, exit, adjustment logic
greeks_engine.py              Angel Greeks + Black-Scholes fallback
liquidity.py                  Bid/ask, volume, OI filter
pricing.py                    Mid price → LTP → BS fallback
circuit_breaker.py            NORMAL / DEGRADED / OPEN / HALF_OPEN state
telegram_control.py           Telegram commands
storage_csv.py                trades.csv, capital.csv, vix_log.csv, snapshots.csv
dashboard.py                  Streamlit dashboard
requirements.txt              Python dependencies
.github/workflows/weekly_iron_condor_loop.yml
```

## Required GitHub Secrets

```text
FYERS_CLIENT_ID
FYERS_ACCESS_TOKEN
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
```

## Optional Angel One Greeks Secrets

```text
ANGEL_API_KEY
ANGEL_JWT_TOKEN
ANGEL_CLIENT_LOCAL_IP
ANGEL_CLIENT_PUBLIC_IP
ANGEL_MAC_ADDRESS
```

If these are missing, the bot still works using Black-Scholes Greeks from VIX.

## Local Run

```bash
pip install -r requirements.txt
export FYERS_CLIENT_ID="your_fyers_client_id"
export FYERS_ACCESS_TOKEN="your_fyers_access_token"
export TELEGRAM_BOT_TOKEN="your_telegram_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export RUN_MODE="loop"
export LOOP_SECONDS="60"
python weekly_iron_condor_bot.py
```

## Telegram Commands

```text
/start   Resume new entries
/stop    Pause new entries; management continues
/status  Send system state
/pnl     Send current paper PnL
```

## Main Defaults

```text
Capital: ₹10,00,000
Lot size: 75
Short strikes: delta around ±0.15
Long hedges: delta around ±0.05
VIX filter: 10 to 20
Liquidity: bid > 0, ask > 0, spread <= 30%, volume/OI threshold
Normal adjustment: portfolio delta per lot > ±0.15
Degraded adjustment: portfolio delta per lot > ±0.40
Profit exit: 50% of premium
Time exit: disabled by default, because you asked to ride expiry unless risk triggers
```

## Dashboard

```bash
streamlit run dashboard.py
```

## CSV Outputs

```text
data/trades.csv
data/capital.csv
data/vix_log.csv
data/snapshots.csv
data/system_logs.csv
data/circuit_breaker.json
data/control.json
```
