import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from library_system.models import Book, BookCopy, CatalogSnapshot
from library_system.storage import CatalogRepository, DEFAULT_BOOKS_DIR, create_app, load_books


def sample_book(book_id="test-book", title="Updated Catalog"):
    return Book(
        book_id=book_id,
        title=title,
        author="Synthetic Author",
        copies=(BookCopy(copy_id=f"{book_id}-copy-1", branch_id="branch-2", available=True, version=0),),
    )


def write_seed(directory, **changes):
    data = {"book_id": "book-test", "title": "Test", "author": "Author", "branch_id": "branch-1", "copies": 1}
    data.update(changes)
    (directory / "sample.txt").write_text(json.dumps(data) + "\n\nOriginal test text.\n", encoding="utf-8")


def test_twenty_original_samples_have_valid_distinct_ids():
    books = load_books(DEFAULT_BOOKS_DIR)
    snapshot = CatalogSnapshot(revision=1, books=books)
    assert len(snapshot.books) == 20
    assert {copy.branch_id for book in books for copy in book.copies} == {"branch-1", "branch-2", "branch-3"}
    assert all(copy.available and copy.due_date is None for book in books for copy in book.copies)


def test_restart_preserves_catalog_and_revision_instead_of_reseeding(tmp_path):
    db_path = tmp_path / "catalog.sqlite3"
    repository = CatalogRepository(db_path)
    repository.initialize()
    assert repository.version().revision == 1
    updated = repository.replace_books((sample_book(),))
    assert updated.revision == 2

    restarted = CatalogRepository(db_path, tmp_path / "missing-seed-directory")
    restarted.initialize()
    assert restarted.snapshot() == updated
    assert restarted.version().revision == 2


def test_invalid_replacement_leaves_snapshot_and_revision_unchanged(tmp_path):
    repository = CatalogRepository(tmp_path / "catalog.sqlite3")
    repository.initialize()
    original = repository.snapshot()
    duplicate = sample_book()
    with pytest.raises(ValidationError, match="book IDs must be unique"):
        repository.replace_books((duplicate, duplicate))
    assert repository.snapshot() == original


def test_concurrent_replacements_allocate_distinct_revisions(tmp_path):
    repository = CatalogRepository(tmp_path / "catalog.sqlite3")
    repository.initialize()
    with ThreadPoolExecutor(max_workers=4) as pool:
        snapshots = list(pool.map(lambda number: repository.replace_books((sample_book(f"book-{number}"),)), range(4)))
    assert sorted(snapshot.revision for snapshot in snapshots) == [2, 3, 4, 5]
    final = repository.snapshot()
    assert final.revision == 5
    assert final in snapshots


@pytest.mark.parametrize("copies", [0, -1, True, 1.5, "2"])
def test_bad_copy_counts_do_not_initialize_catalog(tmp_path, copies):
    seeds = tmp_path / "seeds"
    seeds.mkdir()
    write_seed(seeds, copies=copies)
    repository = CatalogRepository(tmp_path / "catalog.sqlite3", seeds)
    with pytest.raises(ValidationError):
        repository.initialize()
    write_seed(seeds, copies=2)
    repository.initialize()
    snapshot = repository.snapshot()
    assert snapshot.revision == 1
    assert len(snapshot.books[0].copies) == 2


def test_duplicate_seed_book_ids_are_rejected(tmp_path):
    write_seed(tmp_path)
    (tmp_path / "duplicate.txt").write_text((tmp_path / "sample.txt").read_text(encoding="utf-8"), encoding="utf-8")
    with pytest.raises(ValidationError, match="book IDs must be unique"):
        load_books(tmp_path)


def test_empty_seed_directory_is_not_silently_used(tmp_path):
    with pytest.raises(ValueError, match="No sample book files"):
        load_books(tmp_path)


def test_http_contract_and_circulation(tmp_path):
    app = create_app(tmp_path / "catalog.sqlite3")
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok", "mode": "single-node-stand-in"}
        assert client.get("/internal/catalog/version").json() == {"revision": 1}
        snapshot = CatalogSnapshot.model_validate(client.get("/internal/catalog").json())
        assert len(snapshot.books) == 20
        command = {"request_id": "request-1", "operation": "borrow", "actor_id": "patron-1", "book_id": "book-001", "copy_id": "book-001-copy-1", "expected_version": 0}
        response = client.post("/internal/commands", json=command)
        assert response.status_code == 200 and response.json()["ok"]
        assert response.json()["request_id"] == command["request_id"]
        assert app.state.catalog.version().revision == 2
        added = client.post("/internal/commands", json={
            "request_id": "staff-add-http-1", "operation": "add_book", "actor_id": "staff-1",
            "book_id": "book-http-new", "title": "HTTP Catalog Sample", "author": "Test Author",
            "home_branch": "branch-2", "num_copies": 2,
        })
        assert added.status_code == 200 and added.json()["ok"]
        updated = CatalogSnapshot.model_validate(client.get("/internal/catalog").json())
        new_book = next(book for book in updated.books if book.book_id == "book-http-new")
        assert new_book.title == "HTTP Catalog Sample" and new_book.author == "Test Author"
        assert len(new_book.copies) == 2
        assert all(copy.branch_id == "branch-2" and copy.available for copy in new_book.copies)
        assert updated.revision == 3
        assert client.post("/internal/commands", json={**command, "expected_version": -1}).status_code == 422
        assert client.post("/internal/commands", json={**command, "unexpected": "field"}).status_code == 422
