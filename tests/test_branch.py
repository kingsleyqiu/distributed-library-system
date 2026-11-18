from collections import Counter
from datetime import datetime
from time import monotonic, sleep

import httpx
import pytest
from fastapi.testclient import TestClient

from library_system.branch import create_app


VERSION_PATH = "/internal/catalog/version"
SNAPSHOT_PATH = "/internal/catalog"


def sample_catalog():
    return {
        "revision": 3,
        "books": [
            {
                "book_id": "book-1",
                "title": "Distributed Systems",
                "author": "Tanenbaum",
                "copies": [
                    {"copy_id": "copy-1", "branch_id": "branch-1", "available": True,
                     "due_date": None, "version": 0},
                    {"copy_id": "copy-2", "branch_id": "branch-2", "available": False,
                     "due_date": "2026-12-01", "version": 1},
                ],
            },
            {
                "book_id": "book-2",
                "title": "Database Design",
                "author": "Ada Lovelace",
                "copies": [
                    {"copy_id": "copy-3", "branch_id": "branch-2", "available": True,
                     "due_date": None, "version": 0},
                ],
            },
            {
                "book_id": "book-3",
                "title": "Distributed Algorithms",
                "author": "Nancy Lynch",
                "copies": [
                    {"copy_id": "copy-4", "branch_id": "branch-3", "available": False,
                     "due_date": "2026-12-10", "version": 4},
                ],
            },
            {"book_id": "book-4", "title": "Reference Catalogue",
             "author": "Example Author", "copies": []},
        ],
    }


class FakeStorage:
    def __init__(self):
        self.catalog = sample_catalog()
        self.online = True
        self.responses = {}
        self.calls = Counter()

    def __call__(self, request):
        assert request.method == "GET"
        path = request.url.path
        self.calls[path] += 1
        if not self.online:
            raise httpx.ConnectError("Storage disconnected", request=request)
        if path in self.responses:
            return httpx.Response(200, **self.responses[path])
        if path == VERSION_PATH:
            return httpx.Response(200, json={"revision": self.catalog["revision"]})
        if path == SNAPSHOT_PATH:
            return httpx.Response(200, json=self.catalog)
        raise AssertionError(f"Unexpected storage request: {request.method} {path}")


@pytest.fixture
def storage():
    return FakeStorage()


def branch_client(storage, sync_interval=3600):
    app = create_app(
        branch_id="branch-1",
        storage_url="http://storage",
        sync_interval=sync_interval,
        storage_timeout=0.1,
        transport=httpx.MockTransport(storage),
    )
    return TestClient(app)


@pytest.fixture
def client(storage):
    with branch_client(storage) as connection:
        yield connection


def refresh(client):
    client.portal.call(client.app.state.catalog_cache.refresh)


def book_ids(response):
    assert response.status_code == 200
    return [book["book_id"] for book in response.json()["books"]]


def test_search_reads_all_library_branches_and_reports_cache_metadata(client):
    response = client.get("/books")
    assert book_ids(response) == ["book-1", "book-2", "book-3", "book-4"]
    payload = response.json()
    assert payload["total"] == 4
    assert {copy["branch_id"] for book in payload["books"] for copy in book["copies"]} == {
        "branch-1", "branch-2", "branch-3"
    }
    assert payload["cache"]["branch_id"] == "branch-1"
    assert payload["cache"]["revision"] == 3
    assert payload["cache"]["stale"] is False
    assert datetime.fromisoformat(payload["cache"]["last_synced_at"]).tzinfo is not None


@pytest.mark.parametrize(
    "query, expected",
    [(" distributed ", ["book-1", "book-3"]), ("tAnEnBaUm", ["book-1"]),
     ("absent", []), ("   ", ["book-1", "book-2", "book-3", "book-4"])],
)
def test_keyword_search_trims_and_matches_title_or_author(client, query, expected):
    assert book_ids(client.get("/books", params={"q": query})) == expected


def test_branch_and_availability_must_match_the_same_copy(client):
    assert book_ids(client.get("/books", params={"branch_id": "branch-1", "available": False})) == []
    response = client.get("/books", params={"branch_id": "branch-2", "available": False})
    assert book_ids(response) == ["book-1"]
    assert [copy["copy_id"] for copy in response.json()["books"][0]["copies"]] == ["copy-2"]


def test_available_filter_removes_unmatched_copies_and_empty_titles(client):
    response = client.get("/books", params={"available": True})
    assert book_ids(response) == ["book-1", "book-2"]
    assert [copy["copy_id"] for book in response.json()["books"] for copy in book["copies"]] == [
        "copy-1", "copy-3"
    ]


@pytest.mark.parametrize("params", [{"branch_id": "branch-4"}, {"available": "maybe"}])
def test_invalid_search_filters_are_rejected(client, params):
    assert client.get("/books", params=params).status_code == 422


def test_detail_retains_all_copies_and_unknown_book_is_404(client):
    response = client.get("/books/book-1")
    assert response.status_code == 200
    assert len(response.json()["book"]["copies"]) == 2
    assert client.get("/books/missing").status_code == 404


def test_unchanged_revision_does_not_download_another_snapshot(client, storage):
    initial_snapshots = storage.calls[SNAPSHOT_PATH]
    refresh(client)
    assert storage.calls[VERSION_PATH] > 0
    assert storage.calls[SNAPSHOT_PATH] == initial_snapshots


def test_new_revision_replaces_snapshot_and_recovers_after_disconnect(client, storage):
    original_sync = client.get("/books").json()["cache"]["last_synced_at"]
    storage.online = False
    refresh(client)
    cached = client.get("/books").json()
    assert cached["cache"]["stale"] is True
    assert cached["cache"]["last_synced_at"] == original_sync
    assert cached["cache"]["revision"] == 3
    health = client.get("/health").json()
    assert health["status"] == "degraded"
    assert health["storage_available"] is False

    storage.online = True
    storage.catalog["revision"] = 4
    storage.catalog["books"][0]["title"] = "Updated Catalogue"
    refresh(client)
    result = client.get("/books", params={"q": "Updated"}).json()
    assert result["total"] == 1
    assert result["cache"]["revision"] == 4
    assert result["cache"]["stale"] is False
    assert client.get("/health").json()["status"] == "ready"


def test_cold_disconnected_branch_is_unavailable_then_recovers(storage):
    storage.online = False
    with branch_client(storage) as client:
        assert client.get("/books").status_code == 503
        assert client.get("/books/missing").status_code == 503
        assert client.get("/health").json()["status"] == "unavailable"
        storage.online = True
        refresh(client)
        assert client.get("/books").status_code == 200


def test_restarted_branch_repopulates_its_cache_from_storage(storage):
    with branch_client(storage) as first:
        assert first.get("/books").json()["cache"]["revision"] == 3
    storage.catalog["revision"] = 4
    storage.catalog["books"] = []
    with branch_client(storage) as restarted:
        result = restarted.get("/books").json()
        assert result["cache"]["revision"] == 4
        assert result["total"] == 0


def test_reserved_commands_never_claim_to_execute_mutations(client, storage):
    command = {
        "request_id": "request-1", "operation": "borrow", "actor_id": "patron-1",
        "book_id": "book-1", "copy_id": "copy-1", "expected_version": 0,
    }
    assert client.post("/commands", json=command).status_code == 501
    assert client.get("/books/book-1").json()["book"]["copies"][0]["available"] is True
    storage.online = False
    refresh(client)
    assert client.post("/commands", json=command).status_code == 503


def test_invalid_reserved_command_payload_is_rejected(client):
    assert client.post("/commands", json={"operation": "borrow"}).status_code == 422


@pytest.mark.parametrize(
    "path, response",
    [
        (VERSION_PATH, {"content": b"{"}),
        (VERSION_PATH, {"json": {"revision": "invalid"}}),
        (SNAPSHOT_PATH, {"content": b"{"}),
        (SNAPSHOT_PATH, {"json": {"revision": 4, "books": [{"book_id": "broken"}]}}),
    ],
)
def test_bad_upstream_data_keeps_the_last_valid_snapshot(client, storage, path, response):
    storage.catalog["revision"] = 4
    storage.responses[path] = response
    refresh(client)
    result = client.get("/books").json()
    assert result["total"] == 4
    assert result["cache"]["revision"] == 3
    assert result["cache"]["stale"] is True


def test_lower_revision_cannot_overwrite_the_cache(client, storage):
    storage.catalog["revision"] = 2
    storage.catalog["books"] = []
    refresh(client)
    result = client.get("/books").json()
    assert result["total"] == 4
    assert result["cache"]["revision"] == 3
    assert result["cache"]["stale"] is True


def test_snapshot_behind_announced_version_is_rejected(client, storage):
    storage.responses[VERSION_PATH] = {"json": {"revision": 4}}
    refresh(client)
    result = client.get("/books").json()
    assert result["cache"]["revision"] == 3
    assert result["cache"]["stale"] is True


def test_snapshot_newer_than_announced_version_is_accepted(client, storage):
    storage.responses[VERSION_PATH] = {"json": {"revision": 4}}
    storage.catalog["revision"] = 5
    refresh(client)
    assert client.get("/books").json()["cache"]["revision"] == 5


def test_background_poller_observes_a_new_catalog_revision(storage):
    with branch_client(storage, sync_interval=0.02) as client:
        storage.catalog["revision"] = 4
        deadline = monotonic() + 2
        while monotonic() < deadline:
            if client.get("/books").json()["cache"]["revision"] == 4:
                break
            sleep(0.01)
        assert client.get("/books").json()["cache"]["revision"] == 4
