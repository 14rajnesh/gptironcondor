"""
Angel One SmartAPI weekly NIFTY Iron Condor PAPER trading bot.

- Angel One only. No FYERS imports, no FYERS secrets.
- Paper trading only. No real order placement.
- CSV state/logs in ./data
- Telegram commands: /start, /stop, /status, /pnl

Required env:
  ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PIN, ANGEL_TOTP_SECRET
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyotp
import pytz
import requests

try:
    from SmartApi import SmartConnect
except Exception:  # pragma: no cover
    from SmartApi.smartConnect import SmartConnect

# ============================================================
# CONFIG
# ============================================================
IST = pytz.timezone("Asia/Kolkata")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

APP_NAME = "ANGEL_WEEKLY_IRON_CONDOR_PAPER"
PAPER_TRADING_ONLY = os.getenv("PAPER_TRADING_ONLY", "true").lower() == "true"

INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "1000000"))
LOT_SIZE = int(os.getenv("LOT_SIZE", "75"))
UNDERLYING = os.getenv("UNDERLYING", "NIFTY").upper()

BOT_MODE = os.getenv("BOT_MODE", "ONCE").upper()  # ONCE or LOOP
LOOP_SLEEP_SECONDS = int(os.getenv("LOOP_SLEEP_SECONDS", "60"))
MARKET_START_TIME = os.getenv("MARKET_START_TIME", "09:15")
MARKET_END_TIME = os.getenv("MARKET_END_TIME", "15:30")

ENTRY_MIN_DTE = int(os.getenv("ENTRY_MIN_DTE", "2"))
ENTRY_MAX_DTE = int(os.getenv("ENTRY_MAX_DTE", "6"))
TIME_EXIT_DTE = int(os.getenv("TIME_EXIT_DTE", "3"))
ENABLE_TIME_EXIT = os.getenv("ENABLE_TIME_EXIT", "true").lower() == "true"

MIN_VIX = float(os.getenv("MIN_VIX", "10"))
MAX_VIX = float(os.getenv("MAX_VIX", "20"))
DEFAULT_VIX = float(os.getenv("DEFAULT_VIX", "15"))

SHORT_DELTA_TARGET = float(os.getenv("SHORT_DELTA_TARGET", "0.15"))
LONG_DELTA_TARGET = float(os.getenv("LONG_DELTA_TARGET", "0.05"))

MIN_VOLUME = int(os.getenv("MIN_VOLUME", "1000"))
MIN_OI = int(os.getenv("MIN_OI", "10000"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "30"))
OPTION_CHAIN_STRIKES_EACH_SIDE = int(os.getenv("OPTION_CHAIN_STRIKES_EACH_SIDE", "25"))

PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", "0.50"))
STOP_LOSS_MULTIPLIER = float(os.getenv("STOP_LOSS_MULTIPLIER", "2.0"))
NORMAL_DELTA_ADJUST = float(os.getenv("NORMAL_DELTA_ADJUST", "0.15"))
DEGRADED_DELTA_ADJUST = float(os.getenv("DEGRADED_DELTA_ADJUST", "0.40"))
MAX_ADJUSTMENTS_PER_DAY = int(os.getenv("MAX_ADJUSTMENTS_PER_DAY", "2"))

FAILURE_THRESHOLD = int(os.getenv("FAILURE_THRESHOLD", "3"))
BREAKER_COOLDOWN_SECONDS = int(os.getenv("BREAKER_COOLDOWN_SECONDS", "300"))

# Angel index tokens. These are commonly used SmartAPI index tokens.
NIFTY_SPOT_TOKEN = os.getenv("NIFTY_SPOT_TOKEN", "99926000")
INDIA_VIX_TOKEN = os.getenv("INDIA_VIX_TOKEN", "99926017")

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE")
ANGEL_PIN = os.getenv("ANGEL_PIN")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SCRIP_MASTER_URL = os.getenv(
    "SCRIP_MASTER_URL",
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
)

# ============================================================
# FILES
# ============================================================
STATE_FILE = DATA_DIR / "state.json"
TRADES_CSV = DATA_DIR / "trades.csv"
CAPITAL_CSV = DATA_DIR / "capital.csv"
VIX_CSV = DATA_DIR / "vix_log.csv"
SNAPSHOTS_CSV = DATA_DIR / "snapshots.csv"
SYSTEM_LOGS_CSV = DATA_DIR / "system_logs.csv"
TELEGRAM_OFFSET_FILE = DATA_DIR / "telegram_offset.txt"
SCRIP_MASTER_CACHE = DATA_DIR / "OpenAPIScripMaster.json"

# ============================================================
# UTILS
# ============================================================
def now_ist() -> datetime:
    return datetime.now(IST)


def parse_hhmm(value: str) -> dtime:
    h, m = value.split(":")
    return dtime(int(h), int(m))


def in_market_hours() -> bool:
    n = now_ist()
    if n.weekday() >= 5:
        return False
    return parse_hhmm(MARKET_START_TIME) <= n.time() <= parse_hhmm(MARKET_END_TIME)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        if isinstance(x, str):
            x = x.replace(",", "")
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        if isinstance(x, str):
            x = x.replace(",", "")
        return int(float(x))
    except Exception:
        return default


def round_to_50(x: float) -> int:
    return int(round(x / 50.0) * 50)


def append_csv(path: Path, row: Dict[str, Any], fieldnames: Optional[List[str]] = None) -> None:
    if fieldnames is None:
        fieldnames = list(row.keys())
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def rewrite_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def log_system(level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
    print(f"[{level}] {message}")
    append_csv(
        SYSTEM_LOGS_CSV,
        {
            "timestamp": now_ist().isoformat(),
            "level": level,
            "message": message,
            "details_json": json.dumps(details or {}, default=str),
        },
    )


def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chunk in [message[i : i + 3900] for i in range(0, len(message), 3900)]:
        try:
            r = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "text": chunk},
                timeout=10,
            )
            if not r.ok:
                print("Telegram error:", r.text)
        except Exception as e:
            print("Telegram send failed:", repr(e))

# ============================================================
# STATE + TELEGRAM CONTROL
# ============================================================
def default_state() -> Dict[str, Any]:
    return {
        "paused": False,
        "breaker_state": "NORMAL",  # NORMAL, DEGRADED, OPEN, HALF_OPEN
        "failure_count": 0,
        "cooldown_until": None,
        "last_error": None,
    }


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return default_state()
    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        base = default_state()
        base.update(state)
        return base
    except Exception:
        return default_state()


def save_state(state: Dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def telegram_control_check(bot: "IronCondorBot") -> None:
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        offset = 0
        if TELEGRAM_OFFSET_FILE.exists():
            offset = safe_int(TELEGRAM_OFFSET_FILE.read_text().strip(), 0)
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        resp = requests.get(url, params={"timeout": 1, "offset": offset + 1}, timeout=5)
        data = resp.json()
        if not data.get("ok"):
            return
        max_update = offset
        for upd in data.get("result", []):
            max_update = max(max_update, upd.get("update_id", 0))
            msg = upd.get("message", {}) or {}
            chat_id = str((msg.get("chat") or {}).get("id", ""))
            text = str(msg.get("text", "")).strip().lower()
            if TELEGRAM_CHAT_ID and chat_id != str(TELEGRAM_CHAT_ID):
                continue
            if text == "/start":
                bot.state["paused"] = False
                save_state(bot.state)
                send_telegram("✅ Bot resumed. New entries allowed.")
            elif text == "/stop":
                bot.state["paused"] = True
                save_state(bot.state)
                send_telegram("⏸️ Bot paused. New entries blocked. Existing trade management continues.")
            elif text == "/status":
                send_telegram(bot.status_message())
            elif text == "/pnl":
                send_telegram(bot.pnl_message())
        if max_update > offset:
            TELEGRAM_OFFSET_FILE.write_text(str(max_update), encoding="utf-8")
    except Exception as e:
        log_system("WARN", "Telegram command check failed", {"error": repr(e)})

# ============================================================
# ANGEL MARKET DATA
# ============================================================
class MarketDataError(Exception):
    pass


class AngelProvider:
    def __init__(self):
        missing = [
            k
            for k, v in {
                "ANGEL_API_KEY": ANGEL_API_KEY,
                "ANGEL_CLIENT_CODE": ANGEL_CLIENT_CODE,
                "ANGEL_PIN": ANGEL_PIN,
                "ANGEL_TOTP_SECRET": ANGEL_TOTP_SECRET,
            }.items()
            if not v
        ]
        if missing:
            raise MarketDataError(f"Missing Angel One secrets: {', '.join(missing)}")
        self.obj = SmartConnect(api_key=ANGEL_API_KEY)
        self.login()

    def login(self) -> None:
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        data = self.obj.generateSession(ANGEL_CLIENT_CODE, ANGEL_PIN, totp)
        if not data or data.get("status") is False:
            raise MarketDataError(f"Angel login failed: {data}")
        log_system("INFO", "Angel login successful")

    def get_market_data(self, mode: str, exchange_tokens: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        # SmartAPI Python uses getMarketData(mode, exchangeTokens)
        resp = self.obj.getMarketData(mode, exchange_tokens)
        if not resp or resp.get("status") is False:
            raise MarketDataError(f"getMarketData failed: {resp}")
        data = resp.get("data") or {}
        fetched = data.get("fetched") or []
        return fetched

    def get_spot_and_vix(self) -> Tuple[float, float]:
        fetched = self.get_market_data("LTP", {"NSE": [NIFTY_SPOT_TOKEN, INDIA_VIX_TOKEN]})
        spot = 0.0
        vix = 0.0
        for item in fetched:
            token = str(item.get("symbolToken") or item.get("symboltoken") or item.get("token") or "")
            tsym = str(item.get("tradingSymbol") or item.get("tradingsymbol") or item.get("symbol") or "").upper()
            ltp = safe_float(item.get("ltp") or item.get("lastPrice") or item.get("last_price"), 0)
            if token == NIFTY_SPOT_TOKEN or "NIFTY" in tsym:
                spot = ltp or spot
            if token == INDIA_VIX_TOKEN or "VIX" in tsym:
                vix = ltp or vix
        if spot <= 0:
            raise MarketDataError(f"Could not fetch NIFTY spot from Angel response: {fetched}")
        if vix <= 0:
            vix = DEFAULT_VIX
            log_system("WARN", f"India VIX unavailable; using default {DEFAULT_VIX}")
        return spot, vix

    def get_full_quotes_nfo(self, tokens: List[str], chunk_size: int = 50) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        unique = list(dict.fromkeys([str(t) for t in tokens if t]))
        for i in range(0, len(unique), chunk_size):
            batch = unique[i : i + chunk_size]
            fetched = self.get_market_data("FULL", {"NFO": batch})
            for item in fetched:
                token = str(item.get("symbolToken") or item.get("symboltoken") or item.get("token") or "")
                if token:
                    out[token] = item
        return out

    def option_greeks(self, underlying: str, expiry: date) -> List[Dict[str, Any]]:
        # If SDK endpoint differs or fails, caller will fallback to Black-Scholes.
        payloads = [
            {"name": underlying, "expirydate": expiry.strftime("%d%b%Y").upper()},
            {"name": underlying, "expirydate": expiry.strftime("%d%b%Y").title()},
            {"name": underlying, "expirydate": expiry.strftime("%d-%b-%Y")},
        ]
        for p in payloads:
            try:
                if hasattr(self.obj, "optionGreek"):
                    resp = self.obj.optionGreek(p)
                elif hasattr(self.obj, "optionGreekData"):
                    resp = self.obj.optionGreekData(p)
                else:
                    return []
                if resp and resp.get("status") is not False:
                    data = resp.get("data") or []
                    if isinstance(data, list):
                        return data
            except Exception:
                continue
        return []

# ============================================================
# INSTRUMENT MASTER + NORMALIZATION
# ============================================================
def download_scrip_master(force: bool = False) -> pd.DataFrame:
    if force or not SCRIP_MASTER_CACHE.exists() or SCRIP_MASTER_CACHE.stat().st_mtime < time.time() - 86400:
        log_system("INFO", "Downloading Angel OpenAPI scrip master")
        r = requests.get(SCRIP_MASTER_URL, timeout=30)
        r.raise_for_status()
        SCRIP_MASTER_CACHE.write_text(r.text, encoding="utf-8")
    raw = json.loads(SCRIP_MASTER_CACHE.read_text(encoding="utf-8"))
    return pd.DataFrame(raw)


def parse_expiry(value: Any) -> Optional[date]:
    if value is None:
        return None
    s = str(value).strip()
    for fmt in ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d %b %Y"]:
        try:
            return datetime.strptime(s.upper(), fmt).date()
        except Exception:
            pass
    return None


def normalize_strike(value: Any) -> float:
    strike = safe_float(value, 0)
    # Angel master often stores options strikes multiplied by 100.
    if strike > 100000:
        strike = strike / 100.0
    return strike


def build_option_universe(spot: float) -> Tuple[date, pd.DataFrame]:
    df = download_scrip_master()
    # Robust column defaults
    for col in ["exch_seg", "name", "instrumenttype", "symbol", "token", "expiry", "strike", "lotsize"]:
        if col not in df.columns:
            df[col] = ""

    opt = df[
        (df["exch_seg"].astype(str).str.upper() == "NFO")
        & (df["name"].astype(str).str.upper() == UNDERLYING)
        & (df["instrumenttype"].astype(str).str.upper().str.contains("OPT"))
        & (df["symbol"].astype(str).str.upper().str.endswith(("CE", "PE")))
    ].copy()

    if opt.empty:
        raise MarketDataError("No NIFTY option rows found in Angel scrip master")

    opt["expiry_date"] = opt["expiry"].apply(parse_expiry)
    opt = opt[opt["expiry_date"].notna()].copy()
    opt["strike_norm"] = opt["strike"].apply(normalize_strike)
    opt["option_type"] = opt["symbol"].astype(str).str[-2:].str.upper()

    today = now_ist().date()
    expiries = sorted([e for e in opt["expiry_date"].unique() if e >= today])
    if not expiries:
        raise MarketDataError("No future NIFTY expiries found in Angel scrip master")

    selected_expiry = None
    for e in expiries:
        dte = (e - today).days
        if ENTRY_MIN_DTE <= dte <= ENTRY_MAX_DTE:
            selected_expiry = e
            break
    if selected_expiry is None:
        selected_expiry = expiries[0]

    expiry_df = opt[opt["expiry_date"] == selected_expiry].copy()
    atm = round_to_50(spot)
    low = atm - OPTION_CHAIN_STRIKES_EACH_SIDE * 50
    high = atm + OPTION_CHAIN_STRIKES_EACH_SIDE * 50
    expiry_df = expiry_df[(expiry_df["strike_norm"] >= low) & (expiry_df["strike_norm"] <= high)].copy()

    if expiry_df.empty:
        raise MarketDataError("No option rows around ATM after strike filtering")

    return selected_expiry, expiry_df


def best_bid_ask(item: Dict[str, Any]) -> Tuple[float, float]:
    depth = item.get("depth") or {}
    buy = depth.get("buy") or depth.get("bids") or []
    sell = depth.get("sell") or depth.get("asks") or []
    bid = 0.0
    ask = 0.0
    if isinstance(buy, list) and buy:
        bid = safe_float(buy[0].get("price") or buy[0].get("bidprice") or buy[0].get("bidPrice"), 0)
    if isinstance(sell, list) and sell:
        ask = safe_float(sell[0].get("price") or sell[0].get("askprice") or sell[0].get("askPrice"), 0)
    # Some variants have direct fields.
    bid = bid or safe_float(item.get("bestBidPrice") or item.get("bidprice") or item.get("bid_price"), 0)
    ask = ask or safe_float(item.get("bestAskPrice") or item.get("askprice") or item.get("ask_price"), 0)
    return bid, ask


def enrich_options_with_quotes(provider: AngelProvider, opt_df: pd.DataFrame) -> pd.DataFrame:
    tokens = opt_df["token"].astype(str).tolist()
    quotes = provider.get_full_quotes_nfo(tokens)
    rows = []
    for _, r in opt_df.iterrows():
        token = str(r["token"])
        q = quotes.get(token, {})
        bid, ask = best_bid_ask(q)
        ltp = safe_float(q.get("ltp") or q.get("lastPrice") or q.get("last_price"), 0)
        volume = safe_int(q.get("tradeVolume") or q.get("volume") or q.get("totalTradedVolume"), 0)
        oi = safe_int(q.get("opnInterest") or q.get("openInterest") or q.get("oi"), 0)
        rows.append(
            {
                "symbol": str(r["symbol"]),
                "token": token,
                "expiry": r["expiry_date"],
                "strike": float(r["strike_norm"]),
                "type": str(r["option_type"]),
                "lot_size": safe_int(r.get("lotsize"), LOT_SIZE),
                "ltp": ltp,
                "bid": bid,
                "ask": ask,
                "volume": volume,
                "oi": oi,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        raise MarketDataError("No enriched option rows")
    return out

# ============================================================
# GREEKS + LIQUIDITY + PRICING
# ============================================================
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black_scholes_greeks(spot: float, strike: float, dte: int, sigma: float, option_type: str, r: float = 0.06) -> Tuple[float, float]:
    T = max(dte, 1) / 365.0
    sigma = max(float(sigma), 0.01)
    if spot <= 0 or strike <= 0:
        return 0.0, 0.0
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    gamma = norm_pdf(d1) / (spot * sigma * math.sqrt(T))
    if option_type.upper() == "CE":
        delta = norm_cdf(d1)
    else:
        delta = norm_cdf(d1) - 1.0
    return delta, gamma


def black_scholes_price(spot: float, strike: float, dte: int, sigma: float, option_type: str, r: float = 0.06) -> float:
    T = max(dte, 1) / 365.0
    sigma = max(float(sigma), 0.01)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type.upper() == "CE":
        return spot * norm_cdf(d1) - strike * math.exp(-r * T) * norm_cdf(d2)
    return strike * math.exp(-r * T) * norm_cdf(-d2) - spot * norm_cdf(-d1)


def merge_angel_greeks(provider: AngelProvider, options: pd.DataFrame, spot: float, vix: float, expiry: date) -> pd.DataFrame:
    dte = max((expiry - now_ist().date()).days, 1)
    sigma = max(vix / 100.0, 0.01)
    options = options.copy()
    options["delta_source"] = "BS"
    options["delta"] = 0.0
    options["gamma"] = 0.0

    for idx, row in options.iterrows():
        delta, gamma = black_scholes_greeks(spot, row["strike"], dte, sigma, row["type"])
        options.at[idx, "delta"] = delta
        options.at[idx, "gamma"] = gamma

    # Try Angel Greeks and override by symbol/strike/type when possible.
    greek_rows = provider.option_greeks(UNDERLYING, expiry)
    if not greek_rows:
        return options

    gmap: Dict[Tuple[int, str], Tuple[float, float]] = {}
    for g in greek_rows:
        strike = round_to_50(safe_float(g.get("strikePrice") or g.get("strike") or g.get("strikeprice"), 0))
        opt_type = str(g.get("optionType") or g.get("option_type") or g.get("type") or "").upper()
        if opt_type not in ["CE", "PE"]:
            tsym = str(g.get("tradingSymbol") or g.get("symbol") or "").upper()
            opt_type = "CE" if tsym.endswith("CE") else "PE" if tsym.endswith("PE") else ""
        delta = safe_float(g.get("delta"), None)
        gamma = safe_float(g.get("gamma"), None)
        if strike and opt_type in ["CE", "PE"] and delta is not None:
            gmap[(strike, opt_type)] = (delta, gamma or 0.0)

    for idx, row in options.iterrows():
        key = (int(row["strike"]), str(row["type"]).upper())
        if key in gmap:
            options.at[idx, "delta"] = gmap[key][0]
            options.at[idx, "gamma"] = gmap[key][1]
            options.at[idx, "delta_source"] = "ANGEL"

    return options


def add_liquidity(options: pd.DataFrame) -> pd.DataFrame:
    df = options.copy()
    df["mid"] = df.apply(lambda r: (r["bid"] + r["ask"]) / 2 if r["bid"] > 0 and r["ask"] > 0 else r["ltp"], axis=1)
    df["spread"] = df["ask"] - df["bid"]
    df["spread_pct"] = df.apply(lambda r: (r["spread"] / r["mid"] * 100) if r["mid"] > 0 and r["spread"] >= 0 else 999, axis=1)
    liquid_mask = (
        (df["bid"] > 0)
        & (df["ask"] > 0)
        & (df["ask"] >= df["bid"])
        & (df["mid"] > 0)
        & (df["spread_pct"] <= MAX_SPREAD_PCT)
        & (df["volume"] >= MIN_VOLUME)
        & (df["oi"] >= MIN_OI)
    )
    df["is_liquid"] = liquid_mask
    if len(df) > 0:
        vol_rank = df["volume"].rank(pct=True).fillna(0)
        oi_rank = df["oi"].rank(pct=True).fillna(0)
        spread_score = (1 - (df["spread_pct"].clip(0, MAX_SPREAD_PCT) / MAX_SPREAD_PCT)).fillna(0)
        df["liquidity_score"] = (0.4 * vol_rank + 0.4 * oi_rank + 0.2 * spread_score).clip(0, 1)
    else:
        df["liquidity_score"] = 0.0
    return df


def price_option(row: pd.Series, spot: float, vix: float, expiry: date) -> float:
    if row.get("bid", 0) > 0 and row.get("ask", 0) > 0:
        return float((row["bid"] + row["ask"]) / 2.0)
    if row.get("ltp", 0) > 0:
        return float(row["ltp"])
    dte = max((expiry - now_ist().date()).days, 1)
    return float(black_scholes_price(spot, row["strike"], dte, max(vix / 100, 0.01), row["type"]))

# ============================================================
# STRATEGY
# ============================================================
@dataclass
class Leg:
    action: str  # SELL or BUY
    option_type: str  # CE or PE
    strike: float
    symbol: str
    token: str
    entry_price: float
    current_price: float
    delta: float
    gamma: float


def select_leg(df: pd.DataFrame, option_type: str, target_abs_delta: float, side: str, short_strike: Optional[float] = None) -> Optional[pd.Series]:
    d = df[(df["type"] == option_type) & (df["is_liquid"])].copy()
    if short_strike is not None:
        if option_type == "CE":
            d = d[d["strike"] > short_strike]
        else:
            d = d[d["strike"] < short_strike]
    if d.empty:
        return None
    d["score"] = (d["delta"].abs() - target_abs_delta).abs() + (1 - d["liquidity_score"])
    d = d.sort_values(["score", "spread_pct"], ascending=True)
    return d.iloc[0]


def build_iron_condor(options: pd.DataFrame, spot: float, vix: float, expiry: date) -> Optional[List[Leg]]:
    short_ce = select_leg(options, "CE", SHORT_DELTA_TARGET, "SELL")
    short_pe = select_leg(options, "PE", SHORT_DELTA_TARGET, "SELL")
    if short_ce is None or short_pe is None:
        return None
    long_ce = select_leg(options, "CE", LONG_DELTA_TARGET, "BUY", short_strike=float(short_ce["strike"]))
    long_pe = select_leg(options, "PE", LONG_DELTA_TARGET, "BUY", short_strike=float(short_pe["strike"]))
    if long_ce is None or long_pe is None:
        return None

    legs = []
    for action, row in [("SELL", short_ce), ("BUY", long_ce), ("SELL", short_pe), ("BUY", long_pe)]:
        p = price_option(row, spot, vix, expiry)
        legs.append(
            Leg(
                action=action,
                option_type=str(row["type"]),
                strike=float(row["strike"]),
                symbol=str(row["symbol"]),
                token=str(row["token"]),
                entry_price=p,
                current_price=p,
                delta=float(row["delta"]),
                gamma=float(row["gamma"]),
            )
        )
    return legs


def credit_per_unit(legs: List[Leg], use_current: bool = False) -> float:
    val = 0.0
    for leg in legs:
        price = leg.current_price if use_current else leg.entry_price
        if leg.action == "SELL":
            val += price
        else:
            val -= price
    return val


def position_greeks(legs: List[Leg]) -> Tuple[float, float]:
    delta = 0.0
    gamma = 0.0
    for leg in legs:
        sign = -1 if leg.action == "SELL" else 1
        delta += sign * leg.delta
        gamma += sign * leg.gamma
    return delta, gamma

# ============================================================
# BOT
# ============================================================
class IronCondorBot:
    trade_fields = [
        "trade_id", "created_at", "updated_at", "status", "expiry", "entry_spot", "entry_vix",
        "lot_size", "credit_per_unit", "credit_total", "wing_width", "max_loss_total",
        "breakeven_upper", "breakeven_lower", "current_pnl_total", "exit_reason",
        "adjustment_count", "last_adjustment_date", "adjustments_today", "legs_json", "history_json",
    ]

    def __init__(self):
        self.state = load_state()
        self.provider = AngelProvider()

    def status_message(self) -> str:
        active = self.get_active_trade()
        return (
            f"🤖 {APP_NAME}\n"
            f"Paused: {self.state.get('paused')}\n"
            f"Circuit: {self.state.get('breaker_state')}\n"
            f"Failures: {self.state.get('failure_count')}\n"
            f"Active trade: {'YES' if active else 'NO'}"
        )

    def pnl_message(self) -> str:
        active = self.get_active_trade()
        if not active:
            return "📭 No active paper trade."
        return f"📊 Current paper P&L: ₹{safe_float(active.get('current_pnl_total')):,.2f}\nExpiry: {active.get('expiry')}"

    def breaker_failure(self, error: str) -> None:
        self.state["failure_count"] = safe_int(self.state.get("failure_count")) + 1
        self.state["last_error"] = error
        if self.state["failure_count"] >= FAILURE_THRESHOLD:
            self.state["breaker_state"] = "OPEN"
            self.state["cooldown_until"] = (now_ist() + timedelta(seconds=BREAKER_COOLDOWN_SECONDS)).isoformat()
            send_telegram(f"⚡ Circuit breaker OPEN. Entries blocked. Error: {error[:180]}")
        save_state(self.state)

    def breaker_success(self) -> None:
        self.state["failure_count"] = 0
        self.state["last_error"] = None
        self.state["breaker_state"] = "NORMAL"
        self.state["cooldown_until"] = None
        save_state(self.state)

    def breaker_check(self) -> str:
        state = self.state.get("breaker_state", "NORMAL")
        if state == "OPEN":
            cooldown = self.state.get("cooldown_until")
            if cooldown:
                try:
                    if now_ist() >= datetime.fromisoformat(cooldown):
                        self.state["breaker_state"] = "HALF_OPEN"
                        save_state(self.state)
                        return "HALF_OPEN"
                except Exception:
                    pass
            return "OPEN"
        return state

    def get_active_trade(self) -> Optional[Dict[str, str]]:
        rows = read_csv_rows(TRADES_CSV)
        for row in reversed(rows):
            if row.get("status") == "ACTIVE":
                return row
        return None

    def upsert_trade(self, trade: Dict[str, Any]) -> None:
        rows = read_csv_rows(TRADES_CSV)
        found = False
        for i, row in enumerate(rows):
            if row.get("trade_id") == trade.get("trade_id"):
                rows[i] = {**row, **{k: str(v) for k, v in trade.items()}}
                found = True
                break
        if not found:
            rows.append({k: str(v) for k, v in trade.items()})
        rewrite_csv(TRADES_CSV, rows, self.trade_fields)

    def fetch_validated_market(self) -> Tuple[float, float, date, pd.DataFrame]:
        spot, vix = self.provider.get_spot_and_vix()
        if spot <= 0:
            raise MarketDataError("Invalid spot")
        expiry, base_options = build_option_universe(spot)
        options = enrich_options_with_quotes(self.provider, base_options)
        if options.empty or (options["ltp"].fillna(0) <= 0).all():
            raise MarketDataError("Invalid option market data: empty or all prices zero")
        options = merge_angel_greeks(self.provider, options, spot, vix, expiry)
        options = add_liquidity(options)
        liquid_count = int(options["is_liquid"].sum())
        if liquid_count < 4:
            raise MarketDataError(f"Not enough liquid options after filter: {liquid_count}")
        append_csv(VIX_CSV, {"timestamp": now_ist().isoformat(), "spot": spot, "vix": vix, "expiry": expiry.isoformat()})
        return spot, vix, expiry, options

    def entry_filter(self, spot: float, vix: float, expiry: date, options: pd.DataFrame) -> Optional[str]:
        if self.state.get("paused"):
            return "Bot paused by Telegram /stop"
        if self.breaker_check() in ["OPEN", "DEGRADED"]:
            return f"Circuit {self.state.get('breaker_state')} blocks new entry"
        dte = (expiry - now_ist().date()).days
        if dte < ENTRY_MIN_DTE or dte > ENTRY_MAX_DTE:
            return f"DTE {dte} outside entry range {ENTRY_MIN_DTE}-{ENTRY_MAX_DTE}"
        if ENABLE_TIME_EXIT and dte <= TIME_EXIT_DTE:
            return f"DTE {dte} <= TIME_EXIT_DTE {TIME_EXIT_DTE}; entry blocked to avoid immediate time exit"
        if not (MIN_VIX <= vix <= MAX_VIX):
            return f"VIX {vix:.2f} outside range {MIN_VIX}-{MAX_VIX}"
        if int(options["is_liquid"].sum()) < 4:
            return "Liquidity filter failed"
        return None

    def enter_trade(self, spot: float, vix: float, expiry: date, options: pd.DataFrame) -> None:
        reason = self.entry_filter(spot, vix, expiry, options)
        if reason:
            log_system("INFO", f"Entry skipped: {reason}")
            return
        legs = build_iron_condor(options, spot, vix, expiry)
        if not legs:
            raise MarketDataError("Could not build iron condor with liquid delta strikes")
        credit = credit_per_unit(legs)
        if credit <= 0:
            raise MarketDataError(f"Iron condor credit <= 0: {credit}")
        sell_ce = [l for l in legs if l.action == "SELL" and l.option_type == "CE"][0]
        buy_ce = [l for l in legs if l.action == "BUY" and l.option_type == "CE"][0]
        sell_pe = [l for l in legs if l.action == "SELL" and l.option_type == "PE"][0]
        buy_pe = [l for l in legs if l.action == "BUY" and l.option_type == "PE"][0]
        wing_width = max(buy_ce.strike - sell_ce.strike, sell_pe.strike - buy_pe.strike)
        credit_total = credit * LOT_SIZE
        max_loss_total = max(0.0, (wing_width - credit) * LOT_SIZE)
        trade_id = f"IC-{now_ist().strftime('%Y%m%d-%H%M%S')}"
        history = [{"ts": now_ist().isoformat(), "event": "ENTRY", "spot": spot, "vix": vix}]
        trade = {
            "trade_id": trade_id,
            "created_at": now_ist().isoformat(),
            "updated_at": now_ist().isoformat(),
            "status": "ACTIVE",
            "expiry": expiry.isoformat(),
            "entry_spot": spot,
            "entry_vix": vix,
            "lot_size": LOT_SIZE,
            "credit_per_unit": round(credit, 2),
            "credit_total": round(credit_total, 2),
            "wing_width": round(wing_width, 2),
            "max_loss_total": round(max_loss_total, 2),
            "breakeven_upper": round(sell_ce.strike + credit, 2),
            "breakeven_lower": round(sell_pe.strike - credit, 2),
            "current_pnl_total": 0,
            "exit_reason": "",
            "adjustment_count": 0,
            "last_adjustment_date": "",
            "adjustments_today": 0,
            "legs_json": json.dumps([asdict(l) for l in legs]),
            "history_json": json.dumps(history),
        }
        self.upsert_trade(trade)
        append_csv(CAPITAL_CSV, {"timestamp": now_ist().isoformat(), "event": "ENTRY", "capital": INITIAL_CAPITAL, "pnl": 0})
        send_telegram(
            f"📥 NEW PAPER IRON CONDOR\n"
            f"Spot: {spot:.2f} | VIX: {vix:.2f}\n"
            f"Expiry: {expiry.strftime('%d %b %Y')} | Credit: ₹{credit:.2f}/unit | ₹{credit_total:,.0f} total\n\n"
            f"SELL CE {sell_ce.strike:.0f} @ {sell_ce.entry_price:.2f}\n"
            f"BUY  CE {buy_ce.strike:.0f} @ {buy_ce.entry_price:.2f}\n"
            f"SELL PE {sell_pe.strike:.0f} @ {sell_pe.entry_price:.2f}\n"
            f"BUY  PE {buy_pe.strike:.0f} @ {buy_pe.entry_price:.2f}\n\n"
            f"BE: {trade['breakeven_lower']} - {trade['breakeven_upper']}\n"
            f"Max loss approx: ₹{max_loss_total:,.0f}\n"
            f"✅ Paper trade only. No real orders placed."
        )

    def reprice_legs(self, legs: List[Leg], spot: float, vix: float, expiry: date) -> List[Leg]:
        quote_map = self.provider.get_full_quotes_nfo([l.token for l in legs])
        dte = max((expiry - now_ist().date()).days, 1)
        sigma = max(vix / 100, 0.01)
        out = []
        for leg in legs:
            q = quote_map.get(leg.token, {})
            bid, ask = best_bid_ask(q)
            ltp = safe_float(q.get("ltp") or q.get("lastPrice") or q.get("last_price"), 0)
            if bid > 0 and ask > 0:
                curr = (bid + ask) / 2
            elif ltp > 0:
                curr = ltp
            else:
                curr = black_scholes_price(spot, leg.strike, dte, sigma, leg.option_type)
            delta, gamma = black_scholes_greeks(spot, leg.strike, dte, sigma, leg.option_type)
            out.append(
                Leg(
                    action=leg.action,
                    option_type=leg.option_type,
                    strike=leg.strike,
                    symbol=leg.symbol,
                    token=leg.token,
                    entry_price=leg.entry_price,
                    current_price=curr,
                    delta=delta,
                    gamma=gamma,
                )
            )
        return out

    def close_trade(self, trade: Dict[str, str], reason: str, pnl_total: float) -> None:
        trade["status"] = "CLOSED"
        trade["updated_at"] = now_ist().isoformat()
        trade["exit_reason"] = reason
        trade["current_pnl_total"] = round(pnl_total, 2)
        history = json.loads(trade.get("history_json") or "[]")
        history.append({"ts": now_ist().isoformat(), "event": "EXIT", "reason": reason, "pnl_total": pnl_total})
        trade["history_json"] = json.dumps(history)
        self.upsert_trade(trade)
        append_csv(CAPITAL_CSV, {"timestamp": now_ist().isoformat(), "event": "EXIT", "capital": INITIAL_CAPITAL + pnl_total, "pnl": pnl_total})
        send_telegram(f"🏁 PAPER EXIT: {reason}\nFinal P&L: ₹{pnl_total:,.2f}")

    def adjust_trade(self, trade: Dict[str, str], spot: float, vix: float, expiry: date, options: pd.DataFrame, old_delta: float, old_gamma: float) -> None:
        today = now_ist().date().isoformat()
        if trade.get("last_adjustment_date") != today:
            trade["adjustments_today"] = "0"
        if safe_int(trade.get("adjustments_today")) >= MAX_ADJUSTMENTS_PER_DAY:
            log_system("INFO", "Adjustment skipped: daily limit reached")
            return
        new_legs = build_iron_condor(options, spot, vix, expiry)
        if not new_legs:
            log_system("WARN", "Adjustment failed: could not build replacement condor")
            return
        new_credit = credit_per_unit(new_legs)
        if new_credit <= 0:
            log_system("WARN", "Adjustment skipped: replacement credit <= 0")
            return
        history = json.loads(trade.get("history_json") or "[]")
        history.append(
            {
                "ts": now_ist().isoformat(),
                "event": "ADJUSTMENT_RECENTER",
                "spot": spot,
                "vix": vix,
                "old_delta": old_delta,
                "old_gamma": old_gamma,
                "new_credit": new_credit,
            }
        )
        trade["legs_json"] = json.dumps([asdict(l) for l in new_legs])
        trade["credit_per_unit"] = round(new_credit, 2)
        trade["credit_total"] = round(new_credit * LOT_SIZE, 2)
        trade["updated_at"] = now_ist().isoformat()
        trade["adjustment_count"] = safe_int(trade.get("adjustment_count")) + 1
        trade["adjustments_today"] = safe_int(trade.get("adjustments_today")) + 1
        trade["last_adjustment_date"] = today
        trade["history_json"] = json.dumps(history)
        self.upsert_trade(trade)
        send_telegram(
            f"⚠️ PAPER ADJUSTMENT\n"
            f"Action: Recentered iron condor\n"
            f"Spot: {spot:.2f} | VIX: {vix:.2f}\n"
            f"Old Δ: {old_delta:+.3f} | Γ: {old_gamma:+.5f}\n"
            f"New credit: ₹{new_credit:.2f}/unit\n"
            f"Adjustments today: {trade['adjustments_today']}"
        )

    def manage_trade(self, trade: Dict[str, str], spot: float, vix: float, expiry: date, options: pd.DataFrame) -> None:
        stored_expiry = datetime.fromisoformat(trade["expiry"]).date()
        legs = [Leg(**x) for x in json.loads(trade.get("legs_json") or "[]")]
        legs = self.reprice_legs(legs, spot, vix, stored_expiry)
        entry_credit = safe_float(trade.get("credit_per_unit"), credit_per_unit(legs))
        current_debit = credit_per_unit(legs, use_current=True)
        pnl_per_unit = entry_credit - current_debit
        pnl_total = pnl_per_unit * LOT_SIZE
        delta, gamma = position_greeks(legs)
        dte = (stored_expiry - now_ist().date()).days

        trade["current_pnl_total"] = round(pnl_total, 2)
        trade["updated_at"] = now_ist().isoformat()
        trade["legs_json"] = json.dumps([asdict(l) for l in legs])
        self.upsert_trade(trade)
        append_csv(
            SNAPSHOTS_CSV,
            {
                "timestamp": now_ist().isoformat(),
                "trade_id": trade["trade_id"],
                "spot": spot,
                "vix": vix,
                "dte": dte,
                "pnl_total": round(pnl_total, 2),
                "portfolio_delta": round(delta, 4),
                "portfolio_gamma": round(gamma, 6),
            },
        )

        credit_total = safe_float(trade.get("credit_total"), entry_credit * LOT_SIZE)
        target = credit_total * PROFIT_TARGET_PCT
        stop = -credit_total * STOP_LOSS_MULTIPLIER

        send_telegram(
            f"📊 PAPER IC UPDATE\n"
            f"Spot: {spot:.2f} | VIX: {vix:.2f} | DTE: {dte}\n"
            f"P&L: ₹{pnl_total:,.2f}\n"
            f"Portfolio Δ: {delta:+.3f} | Γ: {gamma:+.5f}"
        )

        if pnl_total >= target:
            self.close_trade(trade, f"PROFIT_TARGET_{PROFIT_TARGET_PCT:.0%}", pnl_total)
            return
        if pnl_total <= stop:
            self.close_trade(trade, "STOP_LOSS", pnl_total)
            return
        if ENABLE_TIME_EXIT and dte <= TIME_EXIT_DTE:
            self.close_trade(trade, f"TIME_EXIT_DTE_{dte}", pnl_total)
            return
        if dte <= 0:
            self.close_trade(trade, "EXPIRY_DAY_EXIT", pnl_total)
            return

        breaker = self.breaker_check()
        threshold = DEGRADED_DELTA_ADJUST if breaker in ["DEGRADED", "OPEN", "HALF_OPEN"] else NORMAL_DELTA_ADJUST
        if abs(delta) > threshold:
            self.adjust_trade(trade, spot, vix, stored_expiry, options, delta, gamma)

    def full_cycle(self) -> None:
        telegram_control_check(self)
        breaker = self.breaker_check()
        if breaker == "OPEN":
            active = self.get_active_trade()
            if not active:
                log_system("WARN", "Circuit OPEN: entries blocked")
                return

        try:
            spot, vix, expiry, options = self.fetch_validated_market()
            self.breaker_success()
        except Exception as e:
            self.breaker_failure(str(e))
            log_system("ERROR", "Market data failure", {"error": repr(e), "trace": traceback.format_exc()})
            return

        active = self.get_active_trade()
        if active:
            self.manage_trade(active, spot, vix, expiry, options)
        else:
            self.enter_trade(spot, vix, expiry, options)

    def run(self) -> None:
        send_telegram(f"🚀 {APP_NAME} started. Paper mode: {PAPER_TRADING_ONLY}")
        if BOT_MODE == "LOOP":
            while True:
                if in_market_hours():
                    self.full_cycle()
                else:
                    log_system("INFO", "Outside market hours")
                time.sleep(LOOP_SLEEP_SECONDS)
        else:
            self.full_cycle()
            send_telegram("✅ Session complete.")


if __name__ == "__main__":
    if not PAPER_TRADING_ONLY:
        print("This file is paper-trading only. Set PAPER_TRADING_ONLY=true.")
        sys.exit(1)
    IronCondorBot().run()
