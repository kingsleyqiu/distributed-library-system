"""Circulation semantics and durable, atomic retries in both storage modes."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import sqlite3

from fastapi.testclient import TestClient
import pytest

from library_system.circulation import create_circulation_machine
from library_system.consensus import RaftStore
from library_system.models import CatalogSnapshot, CommandEnvelope, LogEntry
from library_system.replica import create_app as replica_app
from library_system.storage import CatalogRepository, create_app as storage_app, load_books, DEFAULT_BOOKS_DIR
from test_replica import SOLO, wait_ready


NOW = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)


def command(request_id="borrow-1", operation="borrow", actor_id="alice", **changes):
    payload = {
        "request_id": request_id, "operation": operation, "actor_id": actor_id,
        "book_id": "book-001",
    }
    if operation != "hold":
        payload.update(copy_id="book-001-copy-1", expected_version=0)
    return {**payload, **changes}


@contextmanager
def running(backend, path):
    app = storage_app(path) if backend == "single" else replica_app(cluster=SOLO, db_path=path)
    with TestClient(app) as client:
        if backend == "replica":
            wait_ready(client)
        yield client


@pytest.fixture(params=["single", "replica"])
def backend(request):
    return request.param


@pytest.fixture
def client(backend, tmp_path):
    with running(backend, tmp_path / "catalog.sqlite3") as client:
        yield client


def post(client, payload):
    response = client.post("/internal/commands", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["request_id"] == payload["request_id"]
    return response.json()


def catalog(client):
    return client.get("/internal/catalog").json()


def test_borrow_renew_return_and_retries(client):
    before = datetime.now(timezone.utc).date()
    borrowed = post(client, command())
    after = datetime.now(timezone.utc).date()
    assert borrowed["ok"] and borrowed["revision"] == 2
    copy = borrowed["result"]["copy"]
    assert copy["borrower_id"] == "alice" and not copy["available"]
    assert before + timedelta(days=14) <= date.fromisoformat(copy["due_date"]) <= after + timedelta(days=14)
    assert copy["version"] == 1 and copy["renewals"] == 0
    assert post(client, command()) == borrowed

    renewed = post(client, command("renew-1", "renew", expected_version=1))
    assert renewed["ok"] and renewed["revision"] == 3
    assert renewed["result"]["copy"]["due_date"] == (date.fromisoformat(copy["due_date"]) + timedelta(days=14)).isoformat()
    assert renewed["result"]["copy"]["renewals"] == 1
    assert post(client, command("renew-1", "renew", expected_version=1)) == renewed

    returned = post(client, command("return-1", "return", expected_version=2))
    assert returned["ok"] and returned["revision"] == 4
    copy = returned["result"]["copy"]
    assert copy["available"] and copy["version"] == 3
    assert copy["borrower_id"] is None and copy["due_date"] is None and copy["renewals"] == 0
    assert post(client, command("return-1", "return", expected_version=2)) == returned
    assert post(client, command()) == borrowed  # Original result even after later changes.
    assert catalog(client)["revision"] == 4
    assert catalog(client)["books"][0]["copies"][0]["available"]


def test_request_id_cannot_be_reused_for_a_different_payload(client):
    original = post(client, command())
    for changes in ({"actor_id": "bob"}, {"operation": "return", "expected_version": 1}, {"expected_version": 1}):
        result = post(client, command(**changes))
        assert not result["ok"] and result["error"] == "request_id_conflict"
    assert post(client, command()) == original
    assert catalog(client)["revision"] == 2


def test_business_rejections_do_not_change_state(client):
    borrowed = post(client, command())
    baseline = catalog(client)
    for payload, error in [
        (command("stale", actor_id="bob"), "version_conflict"),
        (command("busy", actor_id="bob", expected_version=1), "copy_unavailable"),
        (command("wrong-return", "return", "bob", expected_version=1), "not_borrower"),
        (command("wrong-renew", "renew", "bob", expected_version=1), "not_borrower"),
        (command("missing-book", book_id="missing"), "book_not_found"),
        (command("wrong-copy", book_id="book-002"), "copy_not_found"),
        (command("own-hold", "hold"), "already_borrowed"),
    ]:
        rejected = post(client, payload)
        assert not rejected["ok"] and rejected["error"] == error
        assert rejected["revision"] == borrowed["revision"]
        assert post(client, payload) == rejected
        assert catalog(client) == baseline


def test_renewal_limit(client):
    post(client, command())
    for version in (1, 2):
        assert post(client, command(f"renew-{version}", "renew", expected_version=version))["ok"]
    result = post(client, command("renew-3", "renew", expected_version=3))
    assert not result["ok"] and result["error"] == "renewal_limit"
    assert catalog(client)["revision"] == 4


def test_holds_are_fifo_block_renewals_and_are_consumed_on_borrow(client):
    post(client, command())
    first = post(client, command("hold-bob", "hold", "bob"))
    second = post(client, command("hold-carol", "hold", "carol"))
    assert first["ok"] and first["result"]["position"] == 1
    assert second["ok"] and second["result"]["position"] == 2
    assert post(client, command("hold-bob", "hold", "bob")) == first
    assert post(client, command("bob-again", "hold", "bob"))["error"] == "already_on_hold"
    assert post(client, command("renew", "renew", expected_version=1))["error"] == "holds_pending"
    assert post(client, command("return", "return", expected_version=1))["ok"]
    assert post(client, command("carol-early", actor_id="carol", expected_version=2))["error"] == "hold_queue_priority"
    assert post(client, command("outsider", actor_id="dave", expected_version=2))["error"] == "hold_queue_priority"
    assert post(client, command("borrow-bob", actor_id="bob", expected_version=2))["ok"]
    assert [hold["actor_id"] for hold in catalog(client)["books"][0]["holds"]] == ["carol"]
    assert post(client, command("bob-return", "return", "bob", expected_version=3))["ok"]
    assert post(client, command("borrow-carol", actor_id="carol", expected_version=4))["ok"]
    assert catalog(client)["books"][0]["holds"] == []


def test_hold_can_reserve_an_available_title(client):
    assert post(client, command("hold", "hold", "bob"))["ok"]
    assert post(client, command())["error"] == "hold_queue_priority"
    assert post(client, command("bob", actor_id="bob"))["ok"]


@pytest.mark.parametrize("operation", ["return", "renew"])
def test_available_copy_cannot_be_returned_or_renewed(client, operation):
    result = post(client, command(operation=operation))
    assert result["error"] == "copy_not_borrowed"
    assert catalog(client)["revision"] == 1


@pytest.mark.parametrize("payload", [
    command(copy_id=None), command(expected_version=None), command(expected_version=-1),
    command(expected_version=True), command(expected_version="0"), command(request_id=" "),
    command(actor_id=" "), command("h", "hold", copy_id="book-001-copy-1"),
    command("h", "hold", expected_version=0),
])
def test_invalid_commands_are_rejected_before_execution(client, payload):
    assert client.post("/internal/commands", json=payload).status_code == 422
    assert catalog(client)["revision"] == 1


def test_concurrent_borrows_have_one_winner(client):
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda n: post(client, command(f"borrow-{n}", actor_id=f"patron-{n}")), range(6)))
    assert sum(result["ok"] for result in results) == 1
    assert all(result["error"] == "version_conflict" for result in results if not result["ok"])
    assert catalog(client)["revision"] == 2


def test_concurrent_identical_requests_apply_once(client):
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: post(client, command()), range(6)))
    assert all(result == results[0] for result in results)
    assert results[0]["ok"] and catalog(client)["revision"] == 2


def test_loans_holds_and_request_results_survive_restart(backend, tmp_path):
    path = tmp_path / "durable.sqlite3"
    with running(backend, path) as client:
        borrowed = post(client, command())
        rejected = post(client, command("stale", actor_id="bob"))
        held = post(client, command("hold", "hold", "bob"))
        baseline = catalog(client)
    with running(backend, path) as client:
        assert catalog(client) == baseline
        assert post(client, command()) == borrowed
        assert post(client, command("stale", actor_id="bob")) == rejected
        assert post(client, command("hold", "hold", "bob")) == held
        assert post(client, command("stale", actor_id="carol"))["error"] == "request_id_conflict"
        assert catalog(client) == baseline


def test_a_rejected_request_stays_rejected_after_state_changes(client):
    payload = command("early-return", "return", expected_version=1)
    rejected = post(client, payload)
    assert rejected["error"] == "version_conflict"
    post(client, command())
    assert post(client, payload) == rejected
    assert not catalog(client)["books"][0]["copies"][0]["available"]


def test_deterministic_dates_overdue_renewal_and_multiple_copy_rules():
    machine = create_circulation_machine()
    snapshot = CatalogSnapshot(revision=1, books=load_books(DEFAULT_BOOKS_DIR))
    entry = LogEntry(index=1, term=1, kind="command", timestamp=NOW, command=CommandEnvelope(**command()))
    borrowed, result = machine.apply(snapshot, entry)
    assert result.result["copy"]["due_date"] == "2026-12-08"
    assert machine.apply(snapshot, entry) == (borrowed, result)
    overdue = entry.model_copy(update={
        "index": 2, "timestamp": NOW + timedelta(days=15),
        "command": CommandEnvelope(**command("renew", "renew", expected_version=1)),
    })
    unchanged, rejected = machine.apply(borrowed, overdue)
    assert unchanged == borrowed and rejected.error == "loan_overdue"
    other_copy = borrowed.books[0].copies[0].model_copy(update={
        "copy_id": "extra", "available": True, "borrower_id": None, "due_date": None, "version": 0,
    })
    books = (borrowed.books[0].model_copy(update={"copies": (*borrowed.books[0].copies, other_copy)}), *borrowed.books[1:])
    two_copies = CatalogSnapshot(revision=borrowed.revision, books=books)
    _, rejected = machine.apply(two_copies, entry.model_copy(update={
        "command": CommandEnvelope(**command("second-copy", copy_id="extra")),
    }))
    assert rejected.error == "already_borrowed"


def test_single_storage_rolls_back_result_when_catalog_write_fails(tmp_path):
    repository = CatalogRepository(tmp_path / "rollback.sqlite3")
    repository.initialize()
    baseline = repository.snapshot()
    with sqlite3.connect(repository.db_path) as connection:
        connection.execute("CREATE TRIGGER fail_write BEFORE UPDATE ON catalog BEGIN SELECT RAISE(ABORT, 'test failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match="test failure"):
        repository.execute(CommandEnvelope(**command()))
    with sqlite3.connect(repository.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM command_results").fetchone()[0] == 0
        connection.execute("DROP TRIGGER fail_write")
    assert repository.snapshot() == baseline
    assert repository.execute(CommandEnvelope(**command())).ok


def test_raft_rolls_back_result_if_apply_index_cannot_advance(tmp_path):
    store = RaftStore(tmp_path / "rollback.sqlite3")
    store.initialize()
    snapshot = store.load().snapshot
    entry = LogEntry(index=2, term=1, kind="command", timestamp=NOW, command=CommandEnvelope(**command()))
    with pytest.raises(RuntimeError, match="in order"):
        store.apply_committed(entry, snapshot, create_circulation_machine())
    with sqlite3.connect(store.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM command_results").fetchone()[0] == 0
    assert store.load().snapshot == snapshot
