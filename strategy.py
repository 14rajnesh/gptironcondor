from __future__ import annotations

import uuid
from typing import Optional

from config import CFG
from greeks_engine import GreeksEngine
from liquidity import filter_liquid
from models import MarketSnapshot, OptionRow
from pricing import option_price
from utils import now_ist, safe_float, safe_int, today_ist


def by_symbol(rows: list[OptionRow]) -> dict[str, OptionRow]:
    return {r.symbol: r for r in rows}


def _score(row: OptionRow, target_delta: float) -> float:
    delta_diff = abs(safe_float(row.delta, 0) - target_delta)
    return CFG.DELTA_MATCH_WEIGHT * delta_diff + CFG.LIQUIDITY_WEIGHT * (1.0 - row.liquidity_score)


def _pick_short(rows: list[OptionRow], option_type: str, target_abs_delta: float) -> Optional[OptionRow]:
    target = target_abs_delta if option_type == "CE" else -target_abs_delta
    candidates = [r for r in rows if r.option_type == option_type and r.mid() > 0]
    candidates.sort(key=lambda r: _score(r, target))
    return candidates[0] if candidates else None


def _pick_long(rows: list[OptionRow], option_type: str, short_strike: float, target_abs_delta: float) -> Optional[OptionRow]:
    target = target_abs_delta if option_type == "CE" else -target_abs_delta
    if option_type == "CE":
        candidates = [r for r in rows if r.option_type == "CE" and r.strike > short_strike and r.mid() > 0]
    else:
        candidates = [r for r in rows if r.option_type == "PE" and r.strike < short_strike and r.mid() > 0]
    candidates.sort(key=lambda r: _score(r, target))
    return candidates[0] if candidates else None


def build_iron_condor(snapshot: MarketSnapshot) -> Optional[dict]:
    liquid = filter_liquid(snapshot.chain)
    if not liquid:
        return None

    short_ce = _pick_short(liquid, "CE", CFG.SHORT_DELTA_TARGET)
    short_pe = _pick_short(liquid, "PE", CFG.SHORT_DELTA_TARGET)
    if not short_ce or not short_pe:
        return None

    long_ce = _pick_long(liquid, "CE", short_ce.strike, CFG.LONG_DELTA_TARGET)
    long_pe = _pick_long(liquid, "PE", short_pe.strike, CFG.LONG_DELTA_TARGET)
    if not long_ce or not long_pe:
        return None

    legs = []
    for side, row in [("SELL", short_ce), ("SELL", short_pe), ("BUY", long_ce), ("BUY", long_pe)]:
        price = option_price(row, snapshot.spot, snapshot.vix, snapshot.dte)
        legs.append({
            "symbol": row.symbol,
            "side": side,
            "option_type": row.option_type,
            "strike": row.strike,
            "entry_price": price,
            "current_price": price,
            "delta": safe_float(row.delta, 0),
            "gamma": safe_float(row.gamma, 0),
            "liquidity_score": row.liquidity_score,
            "spread_pct": row.spread_pct,
            "volume": row.volume,
            "oi": row.oi,
            "lots": 1,
        })

    credit_per_unit = sum(l["entry_price"] for l in legs if l["side"] == "SELL") - sum(l["entry_price"] for l in legs if l["side"] == "BUY")
    if credit_per_unit <= 0:
        return None

    call_width = long_ce.strike - short_ce.strike
    put_width = short_pe.strike - long_pe.strike
    wing_width = max(call_width, put_width)
    credit_total = credit_per_unit * CFG.LOT_SIZE
    max_loss_total = max(0.0, (wing_width - credit_per_unit) * CFG.LOT_SIZE)
    breakeven_upper = short_ce.strike + credit_per_unit
    breakeven_lower = short_pe.strike - credit_per_unit
    avg_liquidity = sum(l["liquidity_score"] for l in legs) / len(legs)

    return {
        "trade_id": str(uuid.uuid4()),
        "created_at": now_ist().isoformat(),
        "updated_at": now_ist().isoformat(),
        "status": "OPEN",
        "expiry": snapshot.expiry,
        "expiry_timestamp": snapshot.expiry_timestamp,
        "dte_entry": snapshot.dte,
        "entry_spot": snapshot.spot,
        "entry_vix": snapshot.vix,
        "lot_size": CFG.LOT_SIZE,
        "legs": legs,
        "credit_per_unit": credit_per_unit,
        "credit_total": credit_total,
        "debit_to_close": credit_per_unit,
        "pnl_total": 0.0,
        "realized_pnl_total": 0.0,
        "portfolio_delta": 0.0,
        "portfolio_gamma": 0.0,
        "wing_width": wing_width,
        "max_loss_total": max_loss_total,
        "breakeven_lower": breakeven_lower,
        "breakeven_upper": breakeven_upper,
        "adjustment_count": 0,
        "adjustments_today": 0,
        "last_adjustment_date": "",
        "exit_time": "",
        "exit_reason": "",
        "history": [{
            "timestamp": now_ist().isoformat(),
            "event": "ENTRY",
            "spot": snapshot.spot,
            "vix": snapshot.vix,
            "avg_liquidity": avg_liquidity,
            "legs": legs,
        }],
    }


def evaluate_trade(trade: dict, snapshot: MarketSnapshot) -> dict:
    chain_map = by_symbol(snapshot.chain)
    updated_legs = []
    debit_to_close = 0.0

    for leg in trade.get("legs", []):
        leg = dict(leg)
        row = chain_map.get(leg["symbol"])
        if row:
            price = option_price(row, snapshot.spot, snapshot.vix, snapshot.dte)
            leg["current_price"] = price
            leg["delta"] = safe_float(row.delta, leg.get("delta", 0))
            leg["gamma"] = safe_float(row.gamma, leg.get("gamma", 0))
            leg["liquidity_score"] = row.liquidity_score
            leg["spread_pct"] = row.spread_pct
        price = safe_float(leg.get("current_price"), 0)
        if leg["side"] == "SELL":
            debit_to_close += price
        else:
            debit_to_close -= price
        updated_legs.append(leg)

    initial_credit = safe_float(trade.get("credit_per_unit"), 0)
    realized = safe_float(trade.get("realized_pnl_total"), 0)
    pnl_total = realized + (initial_credit - debit_to_close) * safe_int(trade.get("lot_size"), CFG.LOT_SIZE)

    # position delta/gamma from leg dicts; sell = - contract greek, buy = + contract greek
    total_delta = 0.0
    total_gamma = 0.0
    for leg in updated_legs:
        sign = -1 if leg["side"] == "SELL" else 1
        total_delta += sign * safe_float(leg.get("delta"), 0) * safe_int(leg.get("lots"), 1) * CFG.LOT_SIZE
        total_gamma += sign * safe_float(leg.get("gamma"), 0) * safe_int(leg.get("lots"), 1) * CFG.LOT_SIZE

    trade.update({
        "legs": updated_legs,
        "debit_to_close": debit_to_close,
        "pnl_total": pnl_total,
        "portfolio_delta": total_delta,
        "portfolio_gamma": total_gamma,
        "updated_at": now_ist().isoformat(),
    })
    return trade


def exit_signal(trade: dict, snapshot: MarketSnapshot) -> Optional[str]:
    credit_total = safe_float(trade.get("credit_total"), 0)
    pnl_total = safe_float(trade.get("pnl_total"), 0)
    debit = safe_float(trade.get("debit_to_close"), 0)
    credit_per_unit = safe_float(trade.get("credit_per_unit"), 0)

    if credit_total > 0 and pnl_total >= credit_total * CFG.PROFIT_TARGET_PCT:
        return f"PROFIT_TARGET_{int(CFG.PROFIT_TARGET_PCT * 100)}pct"
    if CFG.ENABLE_TIME_EXIT and snapshot.dte <= CFG.TIME_EXIT_DTE:
        return f"TIME_EXIT_DTE_{snapshot.dte}"
    if credit_per_unit > 0 and debit >= credit_per_unit * CFG.STOP_LOSS_MULTIPLE:
        return f"STOP_LOSS_{CFG.STOP_LOSS_MULTIPLE}x_CREDIT"
    if snapshot.dte <= 0:
        return "EXPIRY_SETTLEMENT"
    return None


def _find_roll_pair(snapshot: MarketSnapshot, side_to_roll: str, current_short: float) -> Optional[tuple[OptionRow, OptionRow]]:
    liquid = filter_liquid(snapshot.chain)
    opt_type = "CE" if side_to_roll == "CALL" else "PE"
    short = _pick_short(liquid, opt_type, CFG.SHORT_DELTA_TARGET)
    if not short:
        return None
    if opt_type == "CE":
        long = _pick_long(liquid, "CE", short.strike, CFG.LONG_DELTA_TARGET)
    else:
        long = _pick_long(liquid, "PE", short.strike, CFG.LONG_DELTA_TARGET)
    if not long:
        return None
    return short, long


def adjustment_needed(trade: dict, degraded: bool = False) -> Optional[str]:
    threshold = CFG.DEGRADED_DELTA_ADJUST if degraded else CFG.NORMAL_DELTA_ADJUST
    # Convert absolute portfolio delta to per-lot delta approximation.
    per_lot_delta = safe_float(trade.get("portfolio_delta"), 0) / max(CFG.LOT_SIZE, 1)
    if per_lot_delta < -threshold:
        # Too much negative delta: upside risk, roll put side up to add positive delta.
        return "PUT"
    if per_lot_delta > threshold:
        # Too much positive delta: downside risk, roll call side down to add negative delta.
        return "CALL"
    return None


def apply_roll_adjustment(trade: dict, snapshot: MarketSnapshot, side_to_roll: str) -> Optional[dict]:
    if safe_int(trade.get("adjustments_today"), 0) >= CFG.MAX_ADJUSTMENTS_PER_DAY:
        return None

    roll_pair = _find_roll_pair(snapshot, side_to_roll, 0)
    if not roll_pair:
        return None
    new_short, new_long = roll_pair
    chain_map = by_symbol(snapshot.chain)

    legs = list(trade.get("legs", []))
    kept = []
    closed = []
    realized_change = 0.0

    for leg in legs:
        is_roll_side = (side_to_roll == "CALL" and leg["option_type"] == "CE") or (side_to_roll == "PUT" and leg["option_type"] == "PE")
        if is_roll_side:
            row = chain_map.get(leg["symbol"])
            current = option_price(row, snapshot.spot, snapshot.vix, snapshot.dte) if row else safe_float(leg.get("current_price"), 0)
            entry = safe_float(leg.get("entry_price"), 0)
            if leg["side"] == "SELL":
                realized_change += (entry - current) * CFG.LOT_SIZE
            else:
                realized_change += (current - entry) * CFG.LOT_SIZE
            closed.append({**leg, "exit_price": current})
        else:
            kept.append(leg)

    for side, row in [("SELL", new_short), ("BUY", new_long)]:
        price = option_price(row, snapshot.spot, snapshot.vix, snapshot.dte)
        kept.append({
            "symbol": row.symbol,
            "side": side,
            "option_type": row.option_type,
            "strike": row.strike,
            "entry_price": price,
            "current_price": price,
            "delta": safe_float(row.delta, 0),
            "gamma": safe_float(row.gamma, 0),
            "liquidity_score": row.liquidity_score,
            "spread_pct": row.spread_pct,
            "volume": row.volume,
            "oi": row.oi,
            "lots": 1,
        })

    trade["legs"] = kept
    trade["realized_pnl_total"] = safe_float(trade.get("realized_pnl_total"), 0) + realized_change
    trade["adjustment_count"] = safe_int(trade.get("adjustment_count"), 0) + 1
    if str(trade.get("last_adjustment_date")) == str(today_ist()):
        trade["adjustments_today"] = safe_int(trade.get("adjustments_today"), 0) + 1
    else:
        trade["adjustments_today"] = 1
        trade["last_adjustment_date"] = str(today_ist())
    history = list(trade.get("history", []))
    history.append({
        "timestamp": now_ist().isoformat(),
        "event": "ADJUSTMENT",
        "side": side_to_roll,
        "spot": snapshot.spot,
        "vix": snapshot.vix,
        "closed": closed,
        "new_short": new_short.to_dict(),
        "new_long": new_long.to_dict(),
        "realized_change": realized_change,
    })
    trade["history"] = history
    return evaluate_trade(trade, snapshot)
