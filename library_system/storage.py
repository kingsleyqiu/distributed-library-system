"""Single-node catalog stand-in. This service does not implement Raft."""

import asyncio
import json
import os
import sqlite3
from contextlib import asynccontextmanager, closing
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import Field

from library_system.models import (
    Book,
    BookCopy,
    BranchId,
    CatalogSnapshot,
    CatalogVersion,
    CommandEnvelope,
    Contract,
)


DEFAULT_BOOKS_DIR = Path(__file__).resolve().parent.parent / "data" / "books"


class SeedBook(Contract):
    book_id: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=300)
    author: str = Field(min_length=1, max_length=300)
    branch_id: BranchId
    copies: int = Field(strict=True, ge=1)


def load_books(books_dir: Path) -> tuple[Book, ...]:
    files = sorted(books_dir.glob("*.txt"))
    if not files:
        raise ValueError(f"No sample book files found in {books_dir}")
    books = []
    for path in files:
        header = path.read_text(encoding="utf-8").split("\n", 1)[0]
        seed = SeedBook.model_validate_json(header)
        books.append(
            Book(
                book_id=seed.book_id,
                title=seed.title,
                author=seed.author,
                copies=tuple(
                    BookCopy(
                        copy_id=f"{seed.book_id}-copy-{number}",
                        branch_id=seed.branch_id,
                        available=True,
                        version=0,
                    )
                    for number in range(1, seed.copies + 1)
                ),
            )
        )
    return CatalogSnapshot(revision=1, books=tuple(books)).books


class CatalogRepository:
    def __init__(self, db_path: str | Path, books_dir: str | Path = DEFAULT_BOOKS_DIR):
        self.db_path = Path(db_path)
        self.books_dir = Path(books_dir)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.db_path)) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS catalog ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), "
                "revision INTEGER NOT NULL CHECK (revision >= 1), "
                "books TEXT NOT NULL)"
            )
            row = connection.execute("SELECT revision, books FROM catalog WHERE id = 1").fetchone()
            if row is None:
                books = load_books(self.books_dir)
                connection.execute(
                    "INSERT INTO catalog VALUES (1, 1, ?)",
                    (json.dumps([book.model_dump(mode="json") for book in books]),),
                )
            else:
                self._snapshot(row)

    @staticmethod
    def _snapshot(row: tuple) -> CatalogSnapshot:
        if row is None:
            raise RuntimeError("Catalog has not been initialized")
        return CatalogSnapshot(revision=row[0], books=json.loads(row[1]))

    def snapshot(self) -> CatalogSnapshot:
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute("SELECT revision, books FROM catalog WHERE id = 1").fetchone()
        return self._snapshot(row)

    def version(self) -> CatalogVersion:
        with closing(sqlite3.connect(self.db_path)) as connection:
            row = connection.execute("SELECT revision FROM catalog WHERE id = 1").fetchone()
        if row is None:
            raise RuntimeError("Catalog has not been initialized")
        return CatalogVersion(revision=row[0])

    def replace_books(self, books: tuple[Book, ...]) -> CatalogSnapshot:
        """Atomically replace the stand-in catalog; not an HTTP mutation API."""
        validated = CatalogSnapshot(revision=0, books=books)
        with closing(sqlite3.connect(self.db_path)) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT revision FROM catalog WHERE id = 1").fetchone()
            if row is None:
                raise RuntimeError("Catalog has not been initialized")
            snapshot = CatalogSnapshot(revision=row[0] + 1, books=validated.books)
            connection.execute(
                "UPDATE catalog SET revision = ?, books = ? WHERE id = 1",
                (snapshot.revision, json.dumps([book.model_dump(mode="json") for book in books])),
            )
        return snapshot


def create_app(
    db_path: str | Path | None = None,
    books_dir: str | Path = DEFAULT_BOOKS_DIR,
) -> FastAPI:
    repository = CatalogRepository(db_path or os.getenv("CATALOG_DB", "var/catalog.sqlite3"), books_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(repository.initialize)
        yield

    app = FastAPI(title="Library catalog single-node stand-in", lifespan=lifespan)
    app.state.catalog = repository

    @app.get("/health")
    def health():
        return {"status": "ok", "mode": "single-node-stand-in"}

    @app.get("/internal/catalog/version", response_model=CatalogVersion)
    def catalog_version():
        return repository.version()

    @app.get("/internal/catalog", response_model=CatalogSnapshot)
    def catalog_snapshot():
        return repository.snapshot()

    @app.post("/internal/commands", status_code=501)
    def commands(command: CommandEnvelope):
        raise HTTPException(501, "Commands are not implemented by the storage stand-in")

    return app


app = create_app()
