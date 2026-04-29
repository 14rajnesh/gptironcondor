from __future__ import annotations

import math
from datetime import datetime, date, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except Exception:
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.replace(",", "")
        return int(float(value))
    except Exception:
        return default


def round_to_step(value: float, step: int) -> int:
    return int(round(float(value) / step) * step)


def parse_hhmm(value: str) -> time:
    hour, minute = value.split(":")
    return time(int(hour), int(minute), tzinfo=IST)


def in_time_window(start_hhmm: str, end_hhmm: str) -> bool:
    current = now_ist().time()
    start = parse_hhmm(start_hhmm)
    end = parse_hhmm(end_hhmm)
    return start <= current <= end


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)
