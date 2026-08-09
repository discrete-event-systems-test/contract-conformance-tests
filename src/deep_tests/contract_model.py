from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from typing import Iterable


class IdempotencyConflict(ValueError):
    pass


@dataclass(frozen=True)
class Command:
    kind: str
    entity_id: str
    value: str | None
    idempotency_key: str

    def fingerprint(self) -> str:
        body = json.dumps(
            {"entity_id": self.entity_id, "kind": self.kind, "value": self.value},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class Outcome:
    revision: int
    entity_id: str
    value: str | None
    deleted: bool


class ReferenceStore:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._tombstones: set[str] = set()
        self._revision = 0
        self._dedupe: dict[str, tuple[str, Outcome]] = {}
        self._history: list[Outcome] = []

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def history(self) -> tuple[Outcome, ...]:
        return tuple(self._history)

    def apply(self, command: Command) -> Outcome:
        fingerprint = command.fingerprint()
        prior = self._dedupe.get(command.idempotency_key)
        if prior is not None:
            prior_fingerprint, prior_outcome = prior
            if prior_fingerprint != fingerprint:
                raise IdempotencyConflict("idempotency key was reused for different intent")
            return prior_outcome

        if command.kind == "create":
            if command.value is None or command.entity_id in self._values:
                raise ValueError("create requires a value and a missing entity")
            self._values[command.entity_id] = command.value
            self._tombstones.discard(command.entity_id)
            deleted = False
        elif command.kind == "update":
            if command.value is None or command.entity_id not in self._values:
                raise ValueError("update requires an existing entity and a value")
            self._values[command.entity_id] = command.value
            deleted = False
        elif command.kind == "delete":
            if command.entity_id not in self._values:
                raise ValueError("delete requires an existing entity")
            del self._values[command.entity_id]
            self._tombstones.add(command.entity_id)
            deleted = True
        else:
            raise ValueError(f"unknown command kind: {command.kind}")

        self._revision += 1
        outcome = Outcome(
            revision=self._revision,
            entity_id=command.entity_id,
            value=None if deleted else self._values[command.entity_id],
            deleted=deleted,
        )
        self._dedupe[command.idempotency_key] = (fingerprint, outcome)
        self._history.append(outcome)
        return outcome

    def snapshot(self) -> str:
        return json.dumps(
            {
                "revision": self._revision,
                "tombstones": sorted(self._tombstones),
                "values": dict(sorted(self._values.items())),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def generate_valid_trace(seed: int, steps: int = 250) -> tuple[Command, ...]:
    randomizer = random.Random(seed)
    live: set[str] = set()
    next_id = 0
    commands: list[Command] = []
    for index in range(steps):
        decision = randomizer.random()
        if not live or decision < 0.36:
            entity_id = f"entity-{seed}-{next_id}"
            next_id += 1
            live.add(entity_id)
            kind = "create"
            value = f"value-{randomizer.randrange(1_000_000)}"
        elif decision < 0.79:
            entity_id = randomizer.choice(sorted(live))
            kind = "update"
            value = f"value-{randomizer.randrange(1_000_000)}"
        else:
            entity_id = randomizer.choice(sorted(live))
            live.remove(entity_id)
            kind = "delete"
            value = None
        commands.append(
            Command(
                kind=kind,
                entity_id=entity_id,
                value=value,
                idempotency_key=f"seed-{seed}-step-{index}",
            )
        )
    return tuple(commands)


def replay(commands: Iterable[Command], duplicate_every: int = 0) -> ReferenceStore:
    store = ReferenceStore()
    for index, command in enumerate(commands):
        first = store.apply(command)
        if duplicate_every and index % duplicate_every == 0:
            assert store.apply(command) == first
    return store
