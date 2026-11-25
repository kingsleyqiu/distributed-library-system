"""Lamport logical clock and bounded event log for dashboard ordering."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any

LAMPORT_HEADER = "x-lamport-clock"


class LamportClock:
    """Thread-safe Lamport logical clock (Lamport 1978)."""

    def __init__(self, initial: int = 0) -> None:
        self._value = initial
        self._lock = Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        """Increment before a local event or sending a message."""
        with self._lock:
            self._value += 1
            return self._value

    def receive(self, remote_time: int | None) -> int:
        """Update on receiving a message: L = max(L, remote) + 1."""
        with self._lock:
            if remote_time is not None and remote_time > self._value:
                self._value = remote_time
            self._value += 1
            return self._value


def parse_lamport_header(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except ValueError:
        return None


class EventLog:
    """Bounded in-memory ring buffer of Lamport-stamped node events."""

    def __init__(self, node_id: str, clock: LamportClock, maxlen: int = 200) -> None:
        self.node_id = node_id
        self.clock = clock
        self._events: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = Lock()

    def record(
        self,
        event_type: str,
        summary: str,
        *,
        lamport_time: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ts = lamport_time if lamport_time is not None else self.clock.tick()
        entry: dict[str, Any] = {
            "lamport_time": ts,
            "node_id": self.node_id,
            "event_type": event_type,
            "summary": summary,
            "wall_time": datetime.now(timezone.utc).isoformat(),
            "details": details or {},
        }
        with self._lock:
            self._events.append(entry)
        return entry

    def snapshot(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self._events)
        return items[-limit:]