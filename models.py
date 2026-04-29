from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class OptionRow:
    symbol: str
    strike: float
    option_type: str  # CE or PE
    expiry: str
    expiry_timestamp: str = ""
    bid: float = 0.0
    ask: float = 0.0
    ltp: float = 0.0
    volume: int = 0
    oi: int = 0
    iv: float = 0.0
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    liquidity_score: float = 0.0
    spread_pct: float = 999.0
    source: str = "fyers"

    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0 and self.ask >= self.bid:
            return (self.bid + self.ask) / 2.0
        if self.ltp > 0:
            return self.ltp
        return 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MarketSnapshot:
    spot: float
    vix: float
    expiry: str
    expiry_timestamp: str
    dte: int
    chain: list[OptionRow]
