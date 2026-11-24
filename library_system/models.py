"""HTTP contracts shared by branch nodes and the future Raft storage cluster."""

from datetime import date, datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


BranchId = Literal["branch-1", "branch-2", "branch-3"]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BookCopy(Contract):
    copy_id: str = Field(min_length=1, max_length=128)
    branch_id: BranchId
    available: bool
    due_date: date | None = None
    version: int = Field(ge=0)

    @model_validator(mode="after")
    def available_copy_has_no_due_date(self) -> Self:
        if self.available and self.due_date is not None:
            raise ValueError("An available copy cannot have a due date")
        return self


class Book(Contract):
    book_id: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=300)
    author: str = Field(min_length=1, max_length=300)
    copies: tuple[BookCopy, ...]


class CatalogVersion(Contract):
    revision: int = Field(ge=0)


class CatalogSnapshot(CatalogVersion):
    books: tuple[Book, ...]

    @model_validator(mode="after")
    def unique_identifiers(self) -> Self:
        book_ids = [book.book_id for book in self.books]
        copy_ids = [copy.copy_id for book in self.books for copy in book.copies]
        if len(set(book_ids)) != len(book_ids):
            raise ValueError("Catalog book IDs must be unique")
        if len(set(copy_ids)) != len(copy_ids):
            raise ValueError("Catalog copy IDs must be globally unique")
        return self


class CacheMetadata(Contract):
    branch_id: BranchId
    revision: int
    last_synced_at: datetime
    stale: bool


class BookSearchResult(Contract):
    books: tuple[Book, ...]
    total: int
    cache: CacheMetadata


class BookDetailResult(Contract):
    book: Book
    cache: CacheMetadata


class CommandEnvelope(Contract):
    """Future mutation request; this phase does not execute commands."""

    request_id: str = Field(min_length=1, max_length=128)
    operation: Literal["borrow", "return", "renew", "hold", "add_book", "remove_book"]
    actor_id: str = Field(min_length=1, max_length=128)
    book_id: str = Field(min_length=1, max_length=128)
    copy_id: str | None = Field(default=None, min_length=1, max_length=128)
    expected_version: int | None = Field(default=None, ge=0)


# --- Replicated storage (log replication, majority commit) -------------------

EntryKind = Literal["noop", "command", "replace_catalog"]
RaftRole = Literal["follower", "candidate", "leader"]


class LogEntry(Contract):
    """One replicated log entry. `timestamp` is assigned once by the leader so
    every replica applies the entry with the same notion of time."""

    index: int = Field(ge=1)
    term: int = Field(ge=1)
    kind: EntryKind
    timestamp: datetime
    command: CommandEnvelope | None = None
    books: tuple[Book, ...] | None = None

    @model_validator(mode="after")
    def payload_matches_kind(self) -> Self:
        if (self.kind == "command") != (self.command is not None):
            raise ValueError("A command entry needs a command, and only command entries may have one")
        if (self.kind == "replace_catalog") != (self.books is not None):
            raise ValueError("A replace_catalog entry needs books, and only it may have them")
        if self.books is not None:
            CatalogSnapshot(revision=0, books=self.books)
        return self


class AppendEntriesRequest(Contract):
    term: int = Field(ge=1)
    leader_id: str = Field(min_length=1, max_length=64)
    prev_log_index: int = Field(ge=0)
    prev_log_term: int = Field(ge=0)
    entries: tuple[LogEntry, ...] = ()
    leader_commit: int = Field(ge=0)

    @model_validator(mode="after")
    def entries_follow_prev(self) -> Self:
        for offset, entry in enumerate(self.entries, start=1):
            if entry.index != self.prev_log_index + offset:
                raise ValueError("Entries must be contiguous and start after prev_log_index")
            if entry.term > self.term:
                raise ValueError("An entry cannot be newer than the leader's term")
        return self


class AppendEntriesResponse(Contract):
    term: int = Field(ge=0)
    success: bool
    match_index: int = Field(default=0, ge=0)
    # Hints for fast backtracking when the consistency check fails.
    conflict_index: int | None = Field(default=None, ge=1)
    conflict_term: int | None = Field(default=None, ge=1)


class RequestVoteRequest(Contract):
    """A candidate asks for a vote in `term` (Raft section 5.2). The last log
    index and term let the voter refuse a candidate whose log is behind."""

    term: int = Field(ge=1)
    candidate_id: str = Field(min_length=1, max_length=64)
    last_log_index: int = Field(ge=0)
    last_log_term: int = Field(ge=0)


class RequestVoteResponse(Contract):
    term: int = Field(ge=0)
    vote_granted: bool


class CommandResult(Contract):
    """Outcome of a committed and applied entry. `ok` is false when the state
    machine deterministically rejected it; the entry is still committed."""

    index: int
    term: int
    revision: int
    ok: bool
    result: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class ReplaceCatalogRequest(Contract):
    """Development/test write: replace every book in one replicated entry."""

    books: tuple[Book, ...]

    @model_validator(mode="after")
    def valid_catalog(self) -> Self:
        CatalogSnapshot(revision=0, books=self.books)
        return self


class PeerProgress(Contract):
    next_index: int
    match_index: int
    lag: int
    last_contact_seconds: float | None


class NodeStatus(Contract):
    node_id: str
    role: RaftRole
    term: int
    voted_for: str | None
    leader_id: str | None
    commit_index: int
    last_applied: int
    last_log_index: int
    last_log_term: int
    revision: int
    ready_for_reads: bool
    peers: dict[str, PeerProgress] = Field(default_factory=dict)
