"""Durable Raft state for one storage replica, kept in an SQLite file.

Everything a replica must keep across a crash lives here:

* ``meta``  - the current term and the vote cast in it (Raft's persistent state);
* ``log``   - every log entry, keyed by index;
* ``state`` - the applied catalog, its revision, and ``last_applied``;
* ``command_results`` - original command payloads and outcomes for safe retries.

Every method is one SQLite transaction with ``synchronous=FULL`` in WAL mode,
so a change is on disk before the caller acknowledges it to anyone else.
``commit_index`` is not stored: after a restart it is safe to resume from
``last_applied``, because only committed entries are ever applied.
"""

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3

from library_system.command_store import apply_entry, initialize_requests
from library_system.consensus.state_machine import CatalogStateMachine
from library_system.models import Book, CatalogSnapshot, CommandResult, LogEntry
from library_system.storage import DEFAULT_BOOKS_DIR, load_books


@dataclass(frozen=True)
class PersistentState:
    current_term: int
    voted_for: str | None
    log: tuple[LogEntry, ...]
    last_applied: int
    snapshot: CatalogSnapshot


def _books_json(books: Iterable[Book]) -> str:
    return json.dumps([book.model_dump(mode="json") for book in books])


class RaftStore:
    def __init__(self, db_path: str | Path, books_dir: str | Path = DEFAULT_BOOKS_DIR):
        self.db_path = Path(db_path)
        self.books_dir = Path(books_dir)

    @contextmanager
    def _transaction(self, write: bool = True) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, isolation_level=None)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create the schema; seed the catalog only for a brand-new database.

        Seed copy IDs are deterministic, so every fresh replica starts from an
        identical state at revision 1 with an empty log.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing_connection(self.db_path) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
        with self._transaction() as connection:
            initialize_requests(connection)
            connection.execute(
                "CREATE TABLE IF NOT EXISTS meta ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), "
                "current_term INTEGER NOT NULL CHECK (current_term >= 0), "
                "voted_for TEXT)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS log ("
                "idx INTEGER PRIMARY KEY CHECK (idx >= 1), "
                "term INTEGER NOT NULL CHECK (term >= 1), "
                "entry TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS state ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), "
                "last_applied INTEGER NOT NULL CHECK (last_applied >= 0), "
                "revision INTEGER NOT NULL CHECK (revision >= 1), "
                "books TEXT NOT NULL)"
            )
            connection.execute("INSERT OR IGNORE INTO meta VALUES (1, 0, NULL)")
            if connection.execute("SELECT 1 FROM state WHERE id = 1").fetchone() is None:
                books = load_books(self.books_dir)
                connection.execute("INSERT INTO state VALUES (1, 0, 1, ?)", (_books_json(books),))

    def load(self) -> PersistentState:
        with self._transaction(write=False) as connection:
            term, voted_for = connection.execute(
                "SELECT current_term, voted_for FROM meta WHERE id = 1"
            ).fetchone()
            rows = connection.execute("SELECT idx, entry FROM log ORDER BY idx").fetchall()
            last_applied, revision, books = connection.execute(
                "SELECT last_applied, revision, books FROM state WHERE id = 1"
            ).fetchone()
        log = tuple(LogEntry.model_validate_json(entry) for _, entry in rows)
        if any(entry.index != position for position, entry in enumerate(log, start=1)):
            raise RuntimeError("Persisted log has a gap or a mismatched index")
        if last_applied > len(log):
            raise RuntimeError("Persisted last_applied is beyond the end of the log")
        return PersistentState(
            current_term=term,
            voted_for=voted_for,
            log=log,
            last_applied=last_applied,
            snapshot=CatalogSnapshot(revision=revision, books=json.loads(books)),
        )

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        with self._transaction() as connection:
            (current,) = connection.execute("SELECT current_term FROM meta WHERE id = 1").fetchone()
            if term < current:
                raise RuntimeError(f"Refusing to move the term backwards ({current} -> {term})")
            connection.execute(
                "UPDATE meta SET current_term = ?, voted_for = ? WHERE id = 1", (term, voted_for)
            )

    def append(self, entries: Iterable[LogEntry]) -> None:
        with self._transaction() as connection:
            self._insert(connection, entries)

    def replace_suffix(self, from_index: int, entries: Iterable[LogEntry]) -> None:
        """Delete entries at ``from_index`` and after, then append ``entries``,
        atomically. Applied (hence committed) entries can never be removed."""
        with self._transaction() as connection:
            (last_applied,) = connection.execute(
                "SELECT last_applied FROM state WHERE id = 1"
            ).fetchone()
            if from_index <= last_applied:
                raise RuntimeError(
                    f"Refusing to truncate applied entry {from_index} (last_applied={last_applied})"
                )
            connection.execute("DELETE FROM log WHERE idx >= ?", (from_index,))
            self._insert(connection, entries)

    @staticmethod
    def _insert(connection: sqlite3.Connection, entries: Iterable[LogEntry]) -> None:
        (last,) = connection.execute("SELECT COALESCE(MAX(idx), 0) FROM log").fetchone()
        for entry in entries:
            if entry.index != last + 1:
                raise RuntimeError(f"Log append must be contiguous: expected {last + 1}, got {entry.index}")
            connection.execute(
                "INSERT INTO log VALUES (?, ?, ?)",
                (entry.index, entry.term, entry.model_dump_json()),
            )
            last = entry.index

    def save_applied(self, index: int, snapshot: CatalogSnapshot) -> None:
        """Record that entry ``index`` was applied, with the resulting catalog."""
        with self._transaction() as connection:
            updated = connection.execute(
                "UPDATE state SET last_applied = ?, revision = ?, books = ? "
                "WHERE id = 1 AND last_applied = ?",
                (index, snapshot.revision, _books_json(snapshot.books), index - 1),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"Entries must be applied in order; cannot apply {index}")

    def apply_committed(
        self, entry: LogEntry, snapshot: CatalogSnapshot, machine: CatalogStateMachine,
    ) -> tuple[CatalogSnapshot, CommandResult]:
        """Persist the catalog, request result, and apply index atomically."""
        with self._transaction() as connection:
            updated, result = apply_entry(connection, machine, snapshot, entry)
            changed = connection.execute(
                "UPDATE state SET last_applied = ?, revision = ?, books = ? "
                "WHERE id = 1 AND last_applied = ?",
                (entry.index, updated.revision, _books_json(updated.books), entry.index - 1),
            ).rowcount
            if changed != 1:
                raise RuntimeError(f"Entries must be applied in order; cannot apply {entry.index}")
        return updated, result

    def entries(self, low: int, high: int | None = None) -> tuple[LogEntry, ...]:
        """Entries with ``low <= index <= high`` (``high`` defaults to the end)."""
        with self._transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT entry FROM log WHERE idx >= ? AND idx <= ? ORDER BY idx",
                (low, high if high is not None else 2**62),
            ).fetchall()
        return tuple(LogEntry.model_validate_json(entry) for (entry,) in rows)

    def last_index_term(self) -> tuple[int, int]:
        with self._transaction(write=False) as connection:
            row = connection.execute("SELECT idx, term FROM log ORDER BY idx DESC LIMIT 1").fetchone()
        return (row[0], row[1]) if row else (0, 0)


@contextmanager
def closing_connection(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        yield connection
    finally:
        connection.close()
