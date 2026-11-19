"""Deterministic catalog state machine driven by committed log entries.

Business operations (borrow, return, renew, hold, ...) are *not* implemented
here. Their owners register a handler per operation::

    def borrow(snapshot, command, context) -> CommandOutcome: ...
    machine.register("borrow", borrow)

Handlers run on every replica, in log order, and must be deterministic:
no wall clock (use ``context.timestamp``, fixed by the leader), no randomness,
no I/O. To refuse a command, raise ``CommandRejected``; the entry is still
committed, the catalog is left unchanged, and the client receives the error.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any

from pydantic import ValidationError

from library_system.models import Book, CatalogSnapshot, CommandEnvelope, CommandResult, LogEntry


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntryContext:
    index: int
    term: int
    timestamp: datetime


@dataclass(frozen=True)
class CommandOutcome:
    """``books=None`` leaves the catalog unchanged."""

    books: tuple[Book, ...] | None = None
    result: dict[str, Any] = field(default_factory=dict)


class CommandRejected(Exception):
    """A deterministic business refusal, e.g. a copy version mismatch."""


CommandHandler = Callable[[CatalogSnapshot, CommandEnvelope, EntryContext], CommandOutcome]


class CatalogStateMachine:
    def __init__(self, handlers: Mapping[str, CommandHandler] | None = None):
        self._handlers: dict[str, CommandHandler] = dict(handlers or {})

    def register(self, operation: str, handler: CommandHandler) -> None:
        self._handlers[operation] = handler

    def supports(self, operation: str) -> bool:
        return operation in self._handlers

    def apply(self, snapshot: CatalogSnapshot, entry: LogEntry) -> tuple[CatalogSnapshot, CommandResult]:
        """Return the catalog after ``entry`` and the result for its client.

        The revision advances by one only when the books actually change, so
        all replicas that apply the same log report the same revision.
        """
        books: tuple[Book, ...] | None = None
        result: dict[str, Any] = {}
        error: str | None = None
        if entry.kind == "replace_catalog":
            books = entry.books
        elif entry.kind == "command":
            assert entry.command is not None
            handler = self._handlers.get(entry.command.operation)
            context = EntryContext(entry.index, entry.term, entry.timestamp)
            if handler is None:
                error = f"No handler registered for operation {entry.command.operation!r}"
            else:
                try:
                    outcome = handler(snapshot, entry.command, context)
                    books, result = outcome.books, dict(outcome.result)
                except CommandRejected as rejection:
                    error = str(rejection) or "Command rejected"
                except Exception as failure:  # A handler bug must not wedge the apply loop.
                    logger.exception("Handler for %s failed at index %d", entry.command.operation, entry.index)
                    error = f"Handler error: {failure}"

        updated = snapshot
        if error is None and books is not None and tuple(books) != snapshot.books:
            try:
                updated = CatalogSnapshot(revision=snapshot.revision + 1, books=tuple(books))
            except ValidationError as invalid:
                error = f"Resulting catalog is invalid: {invalid.errors()[0]['msg']}"
                result = {}
        return updated, CommandResult(
            index=entry.index,
            term=entry.term,
            revision=updated.revision,
            ok=error is None,
            result=result if error is None else {},
            error=error,
        )
