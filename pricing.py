from __future__ import annotations

from greeks_engine import black_scholes_greeks
from models import OptionRow
from config import CFG


def option_price(row: OptionRow, spot: float, vix: float, dte: int) -> float:
    """Bid/ask mid → LTP → Black-Scholes fallback."""
    mid = row.mid()
    if mid > 0:
        return mid
    if row.ltp > 0:
        return row.ltp
    sigma = max(vix / 100.0, 0.01)
    return black_scholes_greeks(spot, row.strike, dte, sigma, row.option_type, CFG.RISK_FREE_RATE)["price"]
