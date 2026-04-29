from __future__ import annotations

import math
from datetime import datetime

from config import CFG
from market_data import AngelGreeksProvider
from models import OptionRow, MarketSnapshot
from utils import IST, norm_cdf, norm_pdf, safe_float


def black_scholes_greeks(spot: float, strike: float, dte: int, sigma: float, option_type: str, r: float = None) -> dict:
    """Return delta/gamma and theoretical price. Sigma must be annualized decimal."""
    r = CFG.RISK_FREE_RATE if r is None else r
    t = max(dte, 1) / 365.0
    sigma = max(float(sigma), 0.01)
    if spot <= 0 or strike <= 0:
        return {"delta": 0.0, "gamma": 0.0, "price": 0.0}

    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)

    if option_type == "CE":
        delta = norm_cdf(d1)
        price = spot * norm_cdf(d1) - strike * math.exp(-r * t) * norm_cdf(d2)
    else:
        delta = norm_cdf(d1) - 1.0
        price = strike * math.exp(-r * t) * norm_cdf(-d2) - spot * norm_cdf(-d1)

    gamma = norm_pdf(d1) / (spot * sigma * math.sqrt(t))
    return {"delta": delta, "gamma": gamma, "price": max(0.0, price)}


class GreeksEngine:
    def __init__(self):
        self.angel = AngelGreeksProvider()

    @staticmethod
    def expiry_for_angel(expiry_text: str) -> str:
        # Angel expects examples like 25JAN2024.
        for fmt in ["%d %b %Y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y"]:
            try:
                dt = datetime.strptime(expiry_text, fmt)
                return dt.strftime("%d%b%Y").upper()
            except Exception:
                continue
        return expiry_text.replace(" ", "").replace("-", "").upper()

    def apply_greeks(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        sigma = max(snapshot.vix / 100.0, 0.01)
        angel_key = self.expiry_for_angel(snapshot.expiry)
        angel_map = self.angel.fetch_greeks(angel_key)

        for row in snapshot.chain:
            primary = angel_map.get((float(row.strike), row.option_type))
            if primary:
                row.delta = safe_float(primary.get("delta"), row.delta or 0.0)
                row.gamma = safe_float(primary.get("gamma"), row.gamma or 0.0)
                row.theta = safe_float(primary.get("theta"), row.theta or 0.0)
                row.vega = safe_float(primary.get("vega"), row.vega or 0.0)
                row.iv = safe_float(primary.get("iv"), row.iv or 0.0)
                if primary.get("volume", 0) and row.volume == 0:
                    row.volume = int(primary["volume"])
                row.source = "angel_greeks+fyers_chain"

            if row.delta is None or row.gamma is None or row.delta == 0:
                bs = black_scholes_greeks(snapshot.spot, row.strike, snapshot.dte, sigma, row.option_type)
                row.delta = bs["delta"]
                row.gamma = bs["gamma"]
                if row.ltp <= 0 and row.bid <= 0 and row.ask <= 0:
                    row.ltp = bs["price"]
                if row.source == "fyers":
                    row.source = "black_scholes_fallback"
        return snapshot

    @staticmethod
    def position_greeks(legs: list[dict], chain_by_symbol: dict[str, OptionRow], lot_size: int) -> tuple[float, float]:
        total_delta = 0.0
        total_gamma = 0.0
        for leg in legs:
            row = chain_by_symbol.get(leg["symbol"])
            if not row:
                continue
            qty_sign = -1 if leg["side"] == "SELL" else 1
            lots = int(leg.get("lots", 1))
            total_delta += qty_sign * lots * lot_size * safe_float(row.delta, 0)
            total_gamma += qty_sign * lots * lot_size * safe_float(row.gamma, 0)
        return total_delta, total_gamma
