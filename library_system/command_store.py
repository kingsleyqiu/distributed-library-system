"""Request deduplication inside the same transaction as the catalog change."""

import sqlite3
from typing import TYPE_CHECKING

from library_system.models import CatalogSnapshot, CommandResult, LogEntry

if TYPE_CHECKING:
    from library_system.consensus.state_machine import CatalogStateMachine


def initialize_requests(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS command_results ("
        "request_id TEXT PRIMARY KEY, payload TEXT NOT NULL, response TEXT NOT NULL)"
    )


def apply_entry(
    connection: sqlite3.Connection,
    machine: "CatalogStateMachine",
    snapshot: CatalogSnapshot,
    entry: LogEntry,
) -> tuple[CatalogSnapshot, CommandResult]:
    """Caller owns the transaction and persists the returned catalog in it.

    Both successful and rejected commands are remembered. Reusing an ID for
    another payload is refused without replacing the original result.
    """
    command = entry.command
    if command is not None:
        row = connection.execute(
            "SELECT payload, response FROM command_results WHERE request_id = ?",
            (command.request_id,),
        ).fetchone()
        if row is not None:
            if row[0] == command.model_dump_json():
                return snapshot, CommandResult.model_validate_json(row[1])
            return snapshot, CommandResult(
                index=entry.index, term=entry.term, revision=snapshot.revision,
                request_id=command.request_id, ok=False, error="request_id_conflict",
            )

    updated, result = machine.apply(snapshot, entry)
    if command is not None:
        result = result.model_copy(update={"request_id": command.request_id})
        connection.execute(
            "INSERT INTO command_results VALUES (?, ?, ?)",
            (command.request_id, command.model_dump_json(), result.model_dump_json()),
        )
    return updated, result
