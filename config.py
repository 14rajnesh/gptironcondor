"""Configuration for the weekly NIFTY Iron Condor paper bot.

All values can be overridden with environment variables.
The defaults are deliberately conservative and paper-trading only.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Config:
    # Core mode
    PAPER_TRADING: bool = _bool("PAPER_TRADING", True)
    RUN_MODE: str = os.getenv("RUN_MODE", "once")  # once or loop
    LOOP_SECONDS: int = _int("LOOP_SECONDS", 60)
    MAX_RUNTIME_MINUTES: int = _int("MAX_RUNTIME_MINUTES", 0)  # 0 = no cap
    DATA_DIR: str = os.getenv("DATA_DIR", "data")

    # Capital and contract assumptions
    INITIAL_CAPITAL: float = _float("INITIAL_CAPITAL", 1_000_000)
    LOT_SIZE: int = _int("LOT_SIZE", 75)
    STRIKE_STEP: int = _int("STRIKE_STEP", 50)
    STRIKECOUNT: int = _int("STRIKECOUNT", 80)

    # Entry filters
    MIN_DTE: int = _int("MIN_DTE", 2)
    MAX_DTE: int = _int("MAX_DTE", 6)
    VIX_MIN: float = _float("VIX_MIN", 10.0)
    VIX_MAX: float = _float("VIX_MAX", 20.0)
    ENTRY_START_TIME: str = os.getenv("ENTRY_START_TIME", "09:25")
    ENTRY_END_TIME: str = os.getenv("ENTRY_END_TIME", "14:45")

    # Liquidity filter
    MIN_VOLUME: int = _int("MIN_VOLUME", 1000)
    MIN_OI: int = _int("MIN_OI", 1000)
    MAX_SPREAD_PCT: float = _float("MAX_SPREAD_PCT", 30.0)
    MIN_LIQUIDITY_SCORE: float = _float("MIN_LIQUIDITY_SCORE", 0.20)

    # Greeks and selection
    RISK_FREE_RATE: float = _float("RISK_FREE_RATE", 0.065)
    SHORT_DELTA_TARGET: float = _float("SHORT_DELTA_TARGET", 0.15)
    LONG_DELTA_TARGET: float = _float("LONG_DELTA_TARGET", 0.05)
    NORMAL_DELTA_ADJUST: float = _float("NORMAL_DELTA_ADJUST", 0.15)
    DEGRADED_DELTA_ADJUST: float = _float("DEGRADED_DELTA_ADJUST", 0.40)
    DELTA_MATCH_WEIGHT: float = _float("DELTA_MATCH_WEIGHT", 0.65)
    LIQUIDITY_WEIGHT: float = _float("LIQUIDITY_WEIGHT", 0.35)

    # Risk management
    PROFIT_TARGET_PCT: float = _float("PROFIT_TARGET_PCT", 0.50)  # 50% of credit
    STOP_LOSS_MULTIPLE: float = _float("STOP_LOSS_MULTIPLE", 2.0)  # debit to close vs credit
    TIME_EXIT_DTE: int = _int("TIME_EXIT_DTE", 3)
    ENABLE_TIME_EXIT: bool = _bool("ENABLE_TIME_EXIT", False)  # False = ride expiry by default
    MAX_ADJUSTMENTS_PER_DAY: int = _int("MAX_ADJUSTMENTS_PER_DAY", 2)
    ADJUSTMENT_ROLL_POINTS: int = _int("ADJUSTMENT_ROLL_POINTS", 50)
    RECENTER_MOVE_POINTS: float = _float("RECENTER_MOVE_POINTS", 250.0)

    # Circuit breaker
    FAILURE_THRESHOLD: int = _int("FAILURE_THRESHOLD", 3)
    COOLDOWN_SECONDS: int = _int("COOLDOWN_SECONDS", 300)

    # Credentials
    FYERS_CLIENT_ID: str | None = os.getenv("FYERS_CLIENT_ID")
    FYERS_ACCESS_TOKEN: str | None = os.getenv("FYERS_ACCESS_TOKEN")
    TELEGRAM_BOT_TOKEN: str | None = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: str | None = os.getenv("TELEGRAM_CHAT_ID")

    # Optional Angel One Greeks API. If not provided, bot falls back to Black-Scholes.
    ANGEL_API_KEY: str | None = os.getenv("ANGEL_API_KEY")
    ANGEL_JWT_TOKEN: str | None = os.getenv("ANGEL_JWT_TOKEN")
    ANGEL_CLIENT_LOCAL_IP: str = os.getenv("ANGEL_CLIENT_LOCAL_IP", "127.0.0.1")
    ANGEL_CLIENT_PUBLIC_IP: str = os.getenv("ANGEL_CLIENT_PUBLIC_IP", "127.0.0.1")
    ANGEL_MAC_ADDRESS: str = os.getenv("ANGEL_MAC_ADDRESS", "00:00:00:00:00:00")


CFG = Config()
