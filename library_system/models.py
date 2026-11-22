"""HTTP contracts shared by branch nodes and the future Raft storage cluster."""

from datetime import date, datetime
from typing import Literal, Self

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
