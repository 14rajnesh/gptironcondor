from __future__ import annotations

import json
import os
import requests
from typing import Any

from config import CFG
from utils import now_ist


class TelegramControl:
    def __init__(self, data_dir: str):
        self.token = CFG.TELEGRAM_BOT_TOKEN
        self.chat_id = CFG.TELEGRAM_CHAT_ID
        self.state_file = os.path.join(data_dir, "telegram_state.json")
        self.control_file = os.path.join(data_dir, "control.json")
        os.makedirs(data_dir, exist_ok=True)
        self._ensure_control_file()

    def _ensure_control_file(self) -> None:
        if not os.path.exists(self.control_file):
            self._write_control({"paused": False, "last_command": None, "updated_at": now_ist().isoformat()})

    def _read_json(self, path: str, default: Any) -> Any:
        try:
            if not os.path.exists(path):
                return default
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default

    def _write_json(self, path: str, data: Any) -> None:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp, path)

    def _read_control(self) -> dict:
        return self._read_json(self.control_file, {"paused": False})

    def _write_control(self, data: dict) -> None:
        self._write_json(self.control_file, data)

    def send(self, message: str) -> None:
        if not self.token or not self.chat_id:
            print(message)
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        for chunk in [message[i:i + 3900] for i in range(0, len(message), 3900)]:
            try:
                r = requests.post(url, data={"chat_id": self.chat_id, "text": chunk}, timeout=10)
                if not r.ok:
                    print("Telegram error:", r.text)
            except Exception as e:
                print("Telegram send failed:", repr(e))
                print(chunk)

    def is_paused(self) -> bool:
        return bool(self._read_control().get("paused", False))

    def set_paused(self, paused: bool, command: str) -> None:
        self._write_control({
            "paused": paused,
            "last_command": command,
            "updated_at": now_ist().isoformat(),
        })

    def _get_offset(self) -> int:
        return int(self._read_json(self.state_file, {"offset": 0}).get("offset", 0))

    def _set_offset(self, offset: int) -> None:
        self._write_json(self.state_file, {"offset": offset})

    def check_commands(self, status_provider=None, pnl_provider=None) -> None:
        """Read Telegram commands: /start, /stop, /status, /pnl."""
        if not self.token:
            return

        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params = {"offset": self._get_offset() + 1, "timeout": 1}
        try:
            r = requests.get(url, params=params, timeout=5)
            if not r.ok:
                return
            payload = r.json()
        except Exception:
            return

        for upd in payload.get("result", []):
            update_id = upd.get("update_id", 0)
            self._set_offset(max(self._get_offset(), update_id))
            msg = upd.get("message", {}) or upd.get("edited_message", {}) or {}
            chat = msg.get("chat", {})
            text = str(msg.get("text", "")).strip().lower()
            if self.chat_id and str(chat.get("id")) != str(self.chat_id):
                continue

            if text.startswith("/stop"):
                self.set_paused(True, "/stop")
                self.send("⏸️ Bot paused. New entries are blocked. Existing trade management continues.")
            elif text.startswith("/start"):
                self.set_paused(False, "/start")
                self.send("▶️ Bot resumed. New entries are allowed when filters pass.")
            elif text.startswith("/status"):
                if status_provider:
                    self.send(status_provider())
                else:
                    self.send(f"📟 Status: paused={self.is_paused()}")
            elif text.startswith("/pnl"):
                if pnl_provider:
                    self.send(pnl_provider())
                else:
                    self.send("📊 PnL provider not attached.")
