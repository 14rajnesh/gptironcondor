from __future__ import annotations

import csv
import json
import os
from typing import Any, Optional

from config import CFG
from utils import now_ist, safe_float


class CsvStore:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.trades_path = os.path.join(data_dir, "trades.csv")
        self.capital_path = os.path.join(data_dir, "capital.csv")
        self.vix_path = os.path.join(data_dir, "vix_log.csv")
        self.snapshots_path = os.path.join(data_dir, "snapshots.csv")
        self.system_logs_path = os.path.join(data_dir, "system_logs.csv")
        self._init_files()

    def _init_files(self) -> None:
        if not os.path.exists(self.trades_path):
            self._write_rows(self.trades_path, [], self.trade_fields())
        if not os.path.exists(self.capital_path):
            self._write_rows(self.capital_path, [{
                "timestamp": now_ist().isoformat(),
                "capital": CFG.INITIAL_CAPITAL,
                "change": 0,
                "reason": "INITIAL",
            }], ["timestamp", "capital", "change", "reason"])
        if not os.path.exists(self.vix_path):
            self._write_rows(self.vix_path, [], ["timestamp", "spot", "vix", "expiry", "dte"])
        if not os.path.exists(self.snapshots_path):
            self._write_rows(self.snapshots_path, [], [
                "timestamp", "trade_id", "spot", "vix", "dte", "portfolio_delta", "portfolio_gamma",
                "debit_to_close", "initial_credit", "pnl_total", "status",
            ])
        if not os.path.exists(self.system_logs_path):
            self._write_rows(self.system_logs_path, [], ["timestamp", "level", "message", "details"])

    @staticmethod
    def trade_fields() -> list[str]:
        return [
            "trade_id", "created_at", "updated_at", "status", "expiry", "expiry_timestamp", "dte_entry",
            "entry_spot", "entry_vix", "lot_size", "legs_json", "credit_per_unit", "credit_total",
            "debit_to_close", "pnl_total", "realized_pnl_total", "portfolio_delta", "portfolio_gamma", "wing_width",
            "max_loss_total", "breakeven_lower", "breakeven_upper", "adjustment_count", "adjustments_today",
            "last_adjustment_date", "exit_time", "exit_reason", "history_json",
        ]

    def _read_rows(self, path: str) -> list[dict]:
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def _write_rows(self, path: str, rows: list[dict], fields: list[str]) -> None:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(tmp, path)

    def _append_row(self, path: str, row: dict, fields: list[str]) -> None:
        exists = os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    def get_capital(self) -> float:
        rows = self._read_rows(self.capital_path)
        if not rows:
            return CFG.INITIAL_CAPITAL
        return safe_float(rows[-1].get("capital"), CFG.INITIAL_CAPITAL)

    def update_capital(self, change: float, reason: str) -> float:
        new_capital = self.get_capital() + change
        self._append_row(self.capital_path, {
            "timestamp": now_ist().isoformat(),
            "capital": new_capital,
            "change": change,
            "reason": reason,
        }, ["timestamp", "capital", "change", "reason"])
        return new_capital

    def get_active_trade(self) -> Optional[dict]:
        rows = self._read_rows(self.trades_path)
        for row in reversed(rows):
            if row.get("status") == "OPEN":
                return self._decode_trade(row)
        return None

    def get_last_trade(self) -> Optional[dict]:
        rows = self._read_rows(self.trades_path)
        if not rows:
            return None
        return self._decode_trade(rows[-1])

    def save_new_trade(self, trade: dict) -> None:
        self._append_row(self.trades_path, self._encode_trade(trade), self.trade_fields())

    def update_trade(self, trade_id: str, updates: dict) -> None:
        rows = self._read_rows(self.trades_path)
        out = []
        for row in rows:
            if row.get("trade_id") == trade_id:
                decoded = self._decode_trade(row)
                decoded.update(updates)
                decoded["updated_at"] = now_ist().isoformat()
                out.append(self._encode_trade(decoded))
            else:
                out.append(row)
        self._write_rows(self.trades_path, out, self.trade_fields())

    def append_vix(self, spot: float, vix: float, expiry: str, dte: int) -> None:
        self._append_row(self.vix_path, {
            "timestamp": now_ist().isoformat(),
            "spot": spot,
            "vix": vix,
            "expiry": expiry,
            "dte": dte,
        }, ["timestamp", "spot", "vix", "expiry", "dte"])

    def append_snapshot(self, row: dict) -> None:
        fields = ["timestamp", "trade_id", "spot", "vix", "dte", "portfolio_delta", "portfolio_gamma", "debit_to_close", "initial_credit", "pnl_total", "status"]
        row = {**{k: "" for k in fields}, **row, "timestamp": now_ist().isoformat()}
        self._append_row(self.snapshots_path, row, fields)

    def log(self, level: str, message: str, details: Any = None) -> None:
        self._append_row(self.system_logs_path, {
            "timestamp": now_ist().isoformat(),
            "level": level,
            "message": message,
            "details": json.dumps(details, default=str) if details is not None else "",
        }, ["timestamp", "level", "message", "details"])

    def _encode_trade(self, trade: dict) -> dict:
        out = dict(trade)
        out["legs_json"] = json.dumps(out.get("legs", []), default=str)
        out["history_json"] = json.dumps(out.get("history", []), default=str)
        out.pop("legs", None)
        out.pop("history", None)
        return out

    def _decode_trade(self, row: dict) -> dict:
        out = dict(row)
        for key in ["legs_json", "history_json"]:
            try:
                decoded = json.loads(out.get(key) or "[]")
            except Exception:
                decoded = []
            out["legs" if key == "legs_json" else "history"] = decoded
        return out
