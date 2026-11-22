"""Stateless branch API with a version-polled, in-memory catalog cache."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
import logging
import math
import os
from time import monotonic
from typing import get_args

from fastapi import FastAPI, HTTPException, Query, Response
import httpx

from library_system.models import (
    BookDetailResult,
    BookSearchResult,
    BranchId,
    CacheMetadata,
    CatalogSnapshot,
    CatalogVersion,
    CommandEnvelope,
    CommandResult,
)


logger = logging.getLogger(__name__)


class CatalogCache:
    def __init__(self, branch_id: BranchId, client: httpx.AsyncClient, max_age: float):
        self.branch_id = branch_id
        self.client = client
        self.max_age = max_age
        self.snapshot: CatalogSnapshot | None = None
        self.last_synced_at: datetime | None = None
        self.last_success: float | None = None
        self.last_error: str | None = "Catalog has not synchronized yet"

    @property
    def stale(self) -> bool:
        return (
            self.last_error is not None
            or self.last_success is None
            or monotonic() - self.last_success > self.max_age
        )

    async def refresh(self) -> None:
        try:
            announced_revision = None
            if self.snapshot is not None:
                response = await self.client.get("/internal/catalog/version")
                response.raise_for_status()
                announced_revision = CatalogVersion.model_validate(response.json()).revision
                if announced_revision < self.snapshot.revision:
                    raise ValueError("Storage catalog revision moved backwards")

            if self.snapshot is None or announced_revision != self.snapshot.revision:
                response = await self.client.get("/internal/catalog")
                response.raise_for_status()
                updated = CatalogSnapshot.model_validate(response.json())
                if announced_revision is not None and updated.revision < announced_revision:
                    raise ValueError("Snapshot is older than the announced catalog revision")
                # Recheck after the HTTP await so concurrent refreshes cannot roll back state.
                if self.snapshot is not None:
                    if updated.revision < self.snapshot.revision:
                        raise ValueError("Storage catalog revision moved backwards")
                    if updated.revision == self.snapshot.revision and updated != self.snapshot:
                        raise ValueError("Catalog changed without advancing its revision")
                self.snapshot = updated

            self.last_synced_at = datetime.now(timezone.utc)
            self.last_success = monotonic()
            self.last_error = None
        except (httpx.HTTPError, ValueError) as error:
            message = str(error)
            if message != self.last_error:
                logger.warning("Catalog synchronization failed: %s", message)
            self.last_error = message

    def metadata(self) -> CacheMetadata:
        if self.snapshot is None or self.last_synced_at is None:
            raise HTTPException(503, "Catalog unavailable: no cached snapshot")
        return CacheMetadata(
            branch_id=self.branch_id,
            revision=self.snapshot.revision,
            last_synced_at=self.last_synced_at,
            stale=self.stale,
        )

    async def poll(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            await self.refresh()


def create_app(
    branch_id: BranchId = "branch-1",
    storage_url: str = "http://127.0.0.1:8100",
    sync_interval: float = 1.0,
    storage_timeout: float = 1.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    if branch_id not in get_args(BranchId):
        raise ValueError("BRANCH_ID must be branch-1, branch-2, or branch-3")
    for name, value in (("sync_interval", sync_interval), ("storage_timeout", storage_timeout)):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be a positive finite number")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with httpx.AsyncClient(
            base_url=storage_url,
            timeout=storage_timeout,
            transport=transport,
            trust_env=False,
        ) as client:
            cache = CatalogCache(branch_id, client, 2 * sync_interval + storage_timeout)
            app.state.catalog_cache = cache
            await cache.refresh()
            polling = asyncio.create_task(cache.poll(sync_interval))
            try:
                yield
            finally:
                polling.cancel()
                with suppress(asyncio.CancelledError):
                    await polling

    app = FastAPI(title=f"Library {branch_id}", lifespan=lifespan)

    @app.get("/health")
    async def health():
        cache = app.state.catalog_cache
        status = "unavailable" if cache.snapshot is None else "degraded" if cache.stale else "ready"
        return {
            "mode": "branch",
            "branch_id": branch_id,
            "status": status,
            "revision": cache.snapshot.revision if cache.snapshot is not None else None,
            "last_synced_at": cache.last_synced_at,
            "storage_available": not cache.stale,
        }

    @app.get("/books", response_model=BookSearchResult)
    async def search_books(
        q: str = Query(default="", max_length=200),
        branch_id: BranchId | None = None,
        available: bool | None = None,
    ):
        cache = app.state.catalog_cache
        metadata = cache.metadata()
        query = q.strip().casefold()
        results = []
        for book in cache.snapshot.books:
            if query and query not in book.title.casefold() and query not in book.author.casefold():
                continue
            copies = tuple(
                copy for copy in book.copies
                if (branch_id is None or copy.branch_id == branch_id)
                and (available is None or copy.available == available)
            )
            if (branch_id is not None or available is not None) and not copies:
                continue
            results.append(book.model_copy(update={"copies": copies}))
        return BookSearchResult(books=tuple(results), total=len(results), cache=metadata)

    @app.get("/books/{book_id:path}", response_model=BookDetailResult)
    async def book_detail(book_id: str):
        cache = app.state.catalog_cache
        metadata = cache.metadata()
        for book in cache.snapshot.books:
            if book.book_id == book_id:
                return BookDetailResult(book=book, cache=metadata)
        raise HTTPException(404, "Book not found")

    @app.post("/commands", response_model=CommandResult)
    async def commands(command: CommandEnvelope):
        cache = app.state.catalog_cache
        if cache.stale:
            raise HTTPException(503, "Storage unavailable; mutations are not accepted")
        try:
            response = await cache.client.post(
                "/internal/commands", json=command.model_dump(mode="json"),
                timeout=max(storage_timeout, 10.0),
            )
        except httpx.HTTPError as error:
            cache.last_error = str(error) or "Command forwarding failed"
            raise HTTPException(
                503, "Outcome unknown; retry the same command with the same request_id",
            ) from error
        if response.status_code != 200:
            if response.status_code >= 500 and response.status_code != 501:
                cache.last_error = "Storage could not confirm the command"
            return Response(
                response.content, status_code=response.status_code,
                media_type=response.headers.get("content-type"),
            )
        try:
            result = CommandResult.model_validate(response.json())
            if result.request_id != command.request_id:
                raise ValueError("Storage returned a different request_id")
        except ValueError as error:
            cache.last_error = "Storage returned an invalid command result"
            raise HTTPException(
                503, "Outcome unknown; retry the same command with the same request_id",
            ) from error
        # Refresh authoritative state even after conflicts. A failed refresh
        # must never turn a committed success into a failed loan.
        await cache.refresh()
        return result

    return app


app = create_app(
    branch_id=os.getenv("BRANCH_ID", "branch-1"),
    storage_url=os.getenv("STORAGE_URL", "http://127.0.0.1:8100"),
    sync_interval=float(os.getenv("SYNC_INTERVAL_SECONDS", "1")),
    storage_timeout=float(os.getenv("STORAGE_TIMEOUT_SECONDS", "1")),
)
