from __future__ import annotations

from config import CFG
from models import OptionRow


def compute_liquidity(rows: list[OptionRow]) -> list[OptionRow]:
    max_vol = max([r.volume for r in rows] + [1])
    max_oi = max([r.oi for r in rows] + [1])

    for r in rows:
        mid = r.mid()
        if r.bid > 0 and r.ask > 0 and r.ask >= r.bid and mid > 0:
            spread_pct = (r.ask - r.bid) / mid * 100.0
        else:
            spread_pct = 999.0
        r.spread_pct = spread_pct

        spread_score = max(0.0, 1.0 - min(spread_pct, CFG.MAX_SPREAD_PCT) / max(CFG.MAX_SPREAD_PCT, 1))
        volume_score = min(1.0, r.volume / max(max_vol, 1))
        oi_score = min(1.0, r.oi / max(max_oi, 1))
        price_score = 1.0 if r.bid > 0 and r.ask > 0 and mid > 0 else 0.0

        r.liquidity_score = 0.45 * spread_score + 0.25 * volume_score + 0.25 * oi_score + 0.05 * price_score
    return rows


def filter_liquid(rows: list[OptionRow]) -> list[OptionRow]:
    compute_liquidity(rows)
    return [
        r for r in rows
        if r.bid > 0
        and r.ask > 0
        and r.ask >= r.bid
        and r.spread_pct <= CFG.MAX_SPREAD_PCT
        and r.volume >= CFG.MIN_VOLUME
        and r.oi >= CFG.MIN_OI
        and r.liquidity_score >= CFG.MIN_LIQUIDITY_SCORE
    ]
