from __future__ import annotations

import time
import traceback
from typing import Optional

from circuit_breaker import CircuitBreaker
from config import CFG
from greeks_engine import GreeksEngine
from liquidity import compute_liquidity, filter_liquid
from market_data import FyersProvider, MarketDataError
from models import MarketSnapshot
from storage_csv import CsvStore
from strategy import (
    adjustment_needed,
    apply_roll_adjustment,
    build_iron_condor,
    evaluate_trade,
    exit_signal,
)
from telegram_control import TelegramControl
from utils import in_time_window, now_ist, safe_float, safe_int


VERSION = "weekly-iron-condor-paper-v3-flow"


class IronCondorBot:
    def __init__(self):
        self.store = CsvStore(CFG.DATA_DIR)
        self.tg = TelegramControl(CFG.DATA_DIR)
        self.breaker = CircuitBreaker(CFG.DATA_DIR)
        self.fyers = FyersProvider()
        self.greeks = GreeksEngine()

    # =========================================================
    # Telegram status / pnl providers
    # =========================================================
    def status_message(self) -> str:
        active = self.store.get_active_trade()
        paused = self.tg.is_paused()
        capital = self.store.get_capital()
        return (
            f"📟 SYSTEM STATUS\n"
            f"Version: {VERSION}\n"
            f"Paused: {paused}\n"
            f"Capital: ₹{capital:,.2f}\n"
            f"Active trade: {'YES' if active else 'NO'}\n"
            f"{self.breaker.summary()}"
        )

    def pnl_message(self) -> str:
        active = self.store.get_active_trade()
        if not active:
            return "📊 PnL: No active paper trade."
        return (
            f"📊 CURRENT PAPER PnL\n"
            f"Trade: {active.get('trade_id')}\n"
            f"Expiry: {active.get('expiry')}\n"
            f"PnL: ₹{safe_float(active.get('pnl_total')):,.2f}\n"
            f"Debit to close: ₹{safe_float(active.get('debit_to_close')):.2f}\n"
            f"Portfolio Delta: {safe_float(active.get('portfolio_delta')):.2f}\n"
            f"Portfolio Gamma: {safe_float(active.get('portfolio_gamma')):.4f}\n"
            f"Adjustments: {safe_int(active.get('adjustment_count'))}"
        )

    # =========================================================
    # Data validation
    # =========================================================
    def validate_snapshot(self, snapshot: MarketSnapshot) -> None:
        if snapshot.spot <= 0:
            raise MarketDataError("Invalid data: NIFTY spot <= 0")
        if not snapshot.chain:
            raise MarketDataError("Invalid data: empty option chain")
        prices = [r.mid() or r.ltp for r in snapshot.chain]
        if not any(p > 0 for p in prices):
            raise MarketDataError("Invalid data: all option prices are zero")
        missing_symbol = [r for r in snapshot.chain if not r.symbol or r.strike <= 0 or r.option_type not in {"CE", "PE"}]
        if len(missing_symbol) == len(snapshot.chain):
            raise MarketDataError("Invalid data: missing required option columns")

    def fetch_and_prepare_market(self) -> MarketSnapshot:
        snapshot = self.fyers.get_market_snapshot()
        self.validate_snapshot(snapshot)
        snapshot = self.greeks.apply_greeks(snapshot)
        compute_liquidity(snapshot.chain)
        self.store.append_vix(snapshot.spot, snapshot.vix, snapshot.expiry, snapshot.dte)
        return snapshot

    # =========================================================
    # Trade handling
    # =========================================================
    def send_entry_message(self, trade: dict) -> None:
        legs = trade.get("legs", [])
        lines = []
        for leg in legs:
            arrow = "SELL" if leg["side"] == "SELL" else "BUY "
            lines.append(
                f"{arrow} {leg['option_type']} {int(float(leg['strike']))} @ ₹{safe_float(leg['entry_price']):.2f} "
                f"| Δ {safe_float(leg.get('delta')):+.2f} | Liq {safe_float(leg.get('liquidity_score')):.2f}"
            )
        avg_liq = sum(safe_float(l.get("liquidity_score")) for l in legs) / max(len(legs), 1)
        self.tg.send(
            "📥 NEW PAPER IRON CONDOR\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Spot: {safe_float(trade.get('entry_spot')):.2f}\n"
            f"VIX: {safe_float(trade.get('entry_vix')):.2f}\n"
            f"Expiry: {trade.get('expiry')} | DTE: {trade.get('dte_entry')}\n\n"
            + "\n".join(lines) + "\n\n"
            f"Credit: ₹{safe_float(trade.get('credit_per_unit')):.2f} per unit | ₹{safe_float(trade.get('credit_total')):,.2f} total\n"
            f"Max loss estimate: ₹{safe_float(trade.get('max_loss_total')):,.2f}\n"
            f"BE: {safe_float(trade.get('breakeven_lower')):.2f} - {safe_float(trade.get('breakeven_upper')):.2f}\n"
            f"Liquidity score avg: {avg_liq:.2f}\n"
            "Mode: PAPER only, no real order placed"
        )

    def send_mtm_message(self, trade: dict, snapshot: MarketSnapshot) -> None:
        self.tg.send(
            "📊 IRON CONDOR MTM\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Spot: {snapshot.spot:.2f} | VIX: {snapshot.vix:.2f} | DTE: {snapshot.dte}\n"
            f"PnL: ₹{safe_float(trade.get('pnl_total')):,.2f}\n"
            f"Debit to close: ₹{safe_float(trade.get('debit_to_close')):.2f}\n"
            f"Initial credit: ₹{safe_float(trade.get('credit_per_unit')):.2f}\n"
            f"Portfolio Delta: {safe_float(trade.get('portfolio_delta')):+.2f}\n"
            f"Portfolio Gamma: {safe_float(trade.get('portfolio_gamma')):+.4f}\n"
            f"Circuit: {self.breaker.state.mode}\n"
            f"Adjustments: {safe_int(trade.get('adjustment_count'))}"
        )

    def close_trade(self, trade: dict, snapshot: MarketSnapshot, reason: str) -> None:
        trade = evaluate_trade(trade, snapshot)
        final_pnl = safe_float(trade.get("pnl_total"), 0)
        self.store.update_trade(trade["trade_id"], {
            **trade,
            "status": "CLOSED",
            "exit_time": now_ist().isoformat(),
            "exit_reason": reason,
        })
        new_capital = self.store.update_capital(final_pnl, f"TRADE_CLOSE_{reason}")
        self.tg.send(
            "🏁 PAPER TRADE CLOSED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Reason: {reason}\n"
            f"Final PnL: ₹{final_pnl:,.2f}\n"
            f"Updated paper capital: ₹{new_capital:,.2f}\n"
            "All legs closed in simulation only."
        )

    def manage_existing_trade(self, active: dict, snapshot: MarketSnapshot) -> None:
        trade = evaluate_trade(active, snapshot)
        self.store.append_snapshot({
            "trade_id": trade.get("trade_id"),
            "spot": snapshot.spot,
            "vix": snapshot.vix,
            "dte": snapshot.dte,
            "portfolio_delta": safe_float(trade.get("portfolio_delta")),
            "portfolio_gamma": safe_float(trade.get("portfolio_gamma")),
            "debit_to_close": safe_float(trade.get("debit_to_close")),
            "initial_credit": safe_float(trade.get("credit_per_unit")),
            "pnl_total": safe_float(trade.get("pnl_total")),
            "status": trade.get("status", "OPEN"),
        })
        self.store.update_trade(trade["trade_id"], trade)
        self.send_mtm_message(trade, snapshot)

        reason = exit_signal(trade, snapshot)
        if reason and self.breaker.can_exit():
            self.close_trade(trade, snapshot, reason)
            return

        if not self.breaker.can_adjust():
            return

        side = adjustment_needed(trade, degraded=self.breaker.is_degraded())
        if side:
            adjusted = apply_roll_adjustment(trade, snapshot, side)
            if adjusted:
                self.store.update_trade(adjusted["trade_id"], adjusted)
                self.tg.send(
                    "⚠️ ADJUSTMENT APPLIED\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"Rolled side: {side}\n"
                    f"Spot: {snapshot.spot:.2f} | VIX: {snapshot.vix:.2f}\n"
                    f"Portfolio Delta: {safe_float(adjusted.get('portfolio_delta')):+.2f}\n"
                    f"Portfolio Gamma: {safe_float(adjusted.get('portfolio_gamma')):+.4f}\n"
                    f"Realized after roll: ₹{safe_float(adjusted.get('realized_pnl_total')):,.2f}\n"
                    f"Total adjustments: {safe_int(adjusted.get('adjustment_count'))}"
                )
            else:
                self.tg.send("⚠️ Adjustment signal generated, but no liquid roll pair found or daily adjustment limit reached.")

    def try_entry(self, snapshot: MarketSnapshot) -> None:
        if self.tg.is_paused():
            self.store.log("INFO", "Entry skipped: Telegram paused")
            return
        if not self.breaker.can_enter():
            self.store.log("INFO", "Entry skipped: circuit not normal", self.breaker.summary())
            return
        if not in_time_window(CFG.ENTRY_START_TIME, CFG.ENTRY_END_TIME):
            self.store.log("INFO", "Entry skipped: outside entry time window")
            return
        if snapshot.dte < CFG.MIN_DTE or snapshot.dte > CFG.MAX_DTE:
            self.store.log("INFO", "Entry skipped: DTE outside range", {"dte": snapshot.dte})
            return
        if snapshot.vix and not (CFG.VIX_MIN <= snapshot.vix <= CFG.VIX_MAX):
            self.tg.send(f"🚫 Entry skipped: VIX {snapshot.vix:.2f} outside {CFG.VIX_MIN}-{CFG.VIX_MAX}.")
            return
        liquid = filter_liquid(snapshot.chain)
        if len(liquid) < 8:
            self.tg.send(f"🚫 Entry skipped: only {len(liquid)} liquid option rows after filter.")
            return

        trade = build_iron_condor(snapshot)
        if not trade:
            self.tg.send("🚫 Entry skipped: could not build liquid delta-based iron condor.")
            return
        self.store.save_new_trade(trade)
        self.send_entry_message(trade)

    # =========================================================
    # Full cycle / loop
    # =========================================================
    def cycle(self) -> None:
        self.tg.check_commands(status_provider=self.status_message, pnl_provider=self.pnl_message)
        self.breaker.before_cycle()

        if self.tg.is_paused():
            self.store.log("INFO", "Telegram STOP active: new trades paused")

        snapshot = self.fetch_and_prepare_market()
        self.breaker.record_success()

        active = self.store.get_active_trade()
        if active:
            self.manage_existing_trade(active, snapshot)
        else:
            self.try_entry(snapshot)

    def run(self) -> None:
        self.tg.send(f"🚀 Bot starting: {VERSION}\n{self.breaker.summary()}")
        started = time.time()
        while True:
            try:
                self.cycle()
            except Exception as e:
                tb = traceback.format_exc()
                self.breaker.record_failure(str(e))
                self.store.log("ERROR", str(e), tb)
                self.tg.send(
                    "⚡ ERROR HANDLING\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"Error: {repr(e)[:300]}\n"
                    f"{self.breaker.summary()}\n"
                    "Entries blocked if circuit is degraded/open; exits remain allowed."
                )

            if CFG.RUN_MODE.lower() != "loop":
                break
            if CFG.MAX_RUNTIME_MINUTES > 0 and (time.time() - started) >= CFG.MAX_RUNTIME_MINUTES * 60:
                self.tg.send("✅ Max runtime reached. Session complete.")
                break
            time.sleep(CFG.LOOP_SECONDS)

        self.tg.send("✅ Session complete.")


if __name__ == "__main__":
    IronCondorBot().run()
