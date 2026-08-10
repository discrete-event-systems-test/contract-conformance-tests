from __future__ import annotations

import heapq
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Event:
    event_id: str
    at: int
    payload: str


@dataclass(frozen=True)
class ExecutedEvent:
    event_id: str
    at: int
    payload: str
    sequence: int


class Scheduler:
    """Dependency-free future-event-list oracle for DES conformance tests.

    Policy:
    - simulation time is an integer tick and never regresses;
    - scheduling in the past or with a negative timestamp fails closed;
    - event identifiers are unique while pending;
    - equal-time events execute in insertion order;
    - cancellation is idempotent and cancelled events never execute;
    - rescheduling creates a fresh insertion sequence, so equal-time peers already
      queued at the destination timestamp remain ahead of the rescheduled event;
    - stale heap entries are ignored using a generation token.
    """

    def __init__(self) -> None:
        self.current_time = 0
        self._sequence = 0
        self._heap: list[tuple[int, int, str, int]] = []
        self._active: dict[str, tuple[int, int, int, str]] = {}

    @property
    def pending_count(self) -> int:
        return len(self._active)

    @property
    def storage_count(self) -> int:
        return len(self._heap)

    def schedule(self, event: Event) -> None:
        self._validate_time(event.at)
        if not event.event_id or not event.payload:
            raise ValueError("event_id and payload must be non-empty")
        if event.event_id in self._active:
            raise ValueError("event_id is already pending")
        self._sequence += 1
        generation = 1
        self._active[event.event_id] = (
            event.at,
            self._sequence,
            generation,
            event.payload,
        )
        heapq.heappush(
            self._heap,
            (event.at, self._sequence, event.event_id, generation),
        )
        self._compact_if_needed()

    def cancel(self, event_id: str) -> bool:
        removed = self._active.pop(event_id, None)
        self._compact_if_needed()
        return removed is not None

    def reschedule(self, event_id: str, at: int) -> None:
        self._validate_time(at)
        active = self._active.get(event_id)
        if active is None:
            raise KeyError("event_id is not pending")
        _, _, generation, payload = active
        self._sequence += 1
        next_generation = generation + 1
        self._active[event_id] = (at, self._sequence, next_generation, payload)
        heapq.heappush(
            self._heap,
            (at, self._sequence, event_id, next_generation),
        )
        self._compact_if_needed()

    def pop_next(self) -> ExecutedEvent | None:
        while self._heap:
            at, sequence, event_id, generation = heapq.heappop(self._heap)
            active = self._active.get(event_id)
            if active is None:
                continue
            active_at, active_sequence, active_generation, payload = active
            if (at, sequence, generation) != (
                active_at,
                active_sequence,
                active_generation,
            ):
                continue
            if at < self.current_time:
                raise AssertionError("future-event list attempted time regression")
            self.current_time = at
            del self._active[event_id]
            self._compact_if_needed()
            return ExecutedEvent(event_id, at, payload, sequence)
        return None

    def drain(self, limit: int = 100_000) -> tuple[ExecutedEvent, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        trace: list[ExecutedEvent] = []
        while self.pending_count:
            if len(trace) >= limit:
                raise RuntimeError("execution limit exceeded")
            event = self.pop_next()
            if event is None:
                raise AssertionError("pending events became unreachable")
            trace.append(event)
        return tuple(trace)

    def snapshot(self) -> str:
        pending = [
            {
                "event_id": event_id,
                "at": at,
                "sequence": sequence,
                "generation": generation,
                "payload": payload,
            }
            for event_id, (at, sequence, generation, payload) in self._active.items()
        ]
        pending.sort(key=lambda item: (item["at"], item["sequence"], item["event_id"]))
        return json.dumps(
            {
                "current_time": self.current_time,
                "next_sequence": self._sequence,
                "pending": pending,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def _validate_time(self, at: int) -> None:
        if isinstance(at, bool) or not isinstance(at, int):
            raise TypeError("event time must be an integer tick")
        if at < 0 or at < self.current_time:
            raise ValueError("event time must be non-negative and not precede current time")

    def _compact_if_needed(self) -> None:
        # Rescheduling/cancellation intentionally leaves stale heap nodes. Keep the
        # backing heap bounded relative to live events so adversarial churn does not
        # create unbounded hidden state.
        threshold = max(64, self.pending_count * 3 + 16)
        if self.storage_count <= threshold:
            return
        self._heap = [
            (at, sequence, event_id, generation)
            for event_id, (at, sequence, generation, _payload) in self._active.items()
        ]
        heapq.heapify(self._heap)
