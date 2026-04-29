from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict

from config import CFG


@dataclass
class CircuitState:
    mode: str = "NORMAL"  # NORMAL, OPEN, HALF_OPEN, DEGRADED
    failure_count: int = 0
    last_failure_ts: float = 0.0
    last_error: str = ""


class CircuitBreaker:
    def __init__(self, data_dir: str):
        self.path = os.path.join(data_dir, "circuit_breaker.json")
        os.makedirs(data_dir, exist_ok=True)
        self.state = self._load()

    def _load(self) -> CircuitState:
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    return CircuitState(**json.load(f))
        except Exception:
            pass
        return CircuitState()

    def _save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(self.state), f, indent=2)
        os.replace(tmp, self.path)

    def before_cycle(self) -> None:
        if self.state.mode == "OPEN":
            elapsed = time.time() - self.state.last_failure_ts
            if elapsed >= CFG.COOLDOWN_SECONDS:
                self.state.mode = "HALF_OPEN"
                self._save()

    def record_success(self) -> None:
        if self.state.mode in {"HALF_OPEN", "DEGRADED", "OPEN"}:
            self.state.mode = "NORMAL"
        self.state.failure_count = 0
        self.state.last_error = ""
        self._save()

    def record_failure(self, error: str) -> None:
        self.state.failure_count += 1
        self.state.last_failure_ts = time.time()
        self.state.last_error = str(error)[:500]
        if self.state.failure_count >= CFG.FAILURE_THRESHOLD:
            self.state.mode = "OPEN"
        elif self.state.failure_count > 0:
            self.state.mode = "DEGRADED"
        self._save()

    def can_enter(self) -> bool:
        return self.state.mode == "NORMAL"

    def can_adjust(self) -> bool:
        return self.state.mode in {"NORMAL", "DEGRADED", "HALF_OPEN"}

    def can_exit(self) -> bool:
        return True

    def is_degraded(self) -> bool:
        return self.state.mode in {"DEGRADED", "OPEN", "HALF_OPEN"}

    def delta_threshold(self) -> float:
        return CFG.DEGRADED_DELTA_ADJUST if self.is_degraded() else CFG.NORMAL_DELTA_ADJUST

    def summary(self) -> str:
        return (
            f"Circuit: {self.state.mode}\n"
            f"Failures: {self.state.failure_count}/{CFG.FAILURE_THRESHOLD}\n"
            f"Last error: {self.state.last_error or 'None'}"
        )
