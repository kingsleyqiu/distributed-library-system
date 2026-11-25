"""Tests for stateless patron/staff auth, Lamport clocks, and live dashboard."""

from pathlib import Path

from fastapi.testclient import TestClient
import httpx
import pytest

from library_system.auth import authenticate_user, issue_token, verify_token
from library_system.branch import create_app as create_branch_app
from library_system.dashboard import create_app as create_dashboard_app
from library_system.lamport import LAMPORT_HEADER, LamportClock
from library_system.storage import create_app as create_storage_app


def _make_storage_and_branch(tmp_path: Path, branch_id: str = "branch-1", require_auth: bool = True):
    storage_app = create_storage_app(db_path=tmp_path / "catalog.sqlite3")
    storage_client = TestClient(storage_app)
    storage_client.__enter__()
    transport = httpx.ASGITransport(app=storage_app)
    branch_app = create_branch_app(
        branch_id=branch_id,
        storage_url="http://storage.test",
        transport=transport,
        require_auth=require_auth,
    )
    return storage_app, storage_client, branch_app


def test_lamport_clock_causality():
    clock_a = LamportClock()
    clock_b = LamportClock()

    t1 = clock_a.tick()
    assert t1 == 1
    # B receives message from A (t=1) -> L_b = max(0, 1) + 1 = 2
    t2 = clock_b.receive(t1)
    assert t2 == 2
    # B ticks and replies (t=3), A receives -> L_a = max(1, 3) + 1 = 4
    t3 = clock_b.tick()
    t4 = clock_a.receive(t3)
    assert t4 == 4


def test_token_issue_and_tamper_rejection():
    user = authenticate_user("patron-1", "password123")
    assert user is not None and user.role == "patron"
    token = issue_token(user, secret="test-secret")
    verified = verify_token(token, secret="test-secret")
    assert verified == user

    with pytest.raises(ValueError):
        verify_token(token + "tampered", secret="test-secret")
    with pytest.raises(ValueError):
        verify_token(token, secret="wrong-secret")


def test_branch_login_role_checks_and_cross_branch_token(tmp_path: Path):
    storage_app, storage_client, branch1_app = _make_storage_and_branch(tmp_path, "branch-1", require_auth=True)
    branch2_app = create_branch_app(
        branch_id="branch-2",
        storage_url="http://storage.test",
        transport=httpx.ASGITransport(app=storage_app),
        require_auth=True,
    )
    try:
        with TestClient(branch1_app) as b1, TestClient(branch2_app) as b2:
            # Invalid password -> 401
            bad = b1.post("/auth/login", json={"user_id": "patron-1", "password": "wrong"})
            assert bad.status_code == 401

            # Valid login on branch-1
            login = b1.post("/auth/login", json={"user_id": "patron-1", "password": "password123"})
            assert login.status_code == 200
            token = login.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}

            # Stateless verification: token issued by branch-1 works on branch-2
            me = b2.get("/auth/me", headers=headers)
            assert me.status_code == 200
            assert me.json()["user_id"] == "patron-1"

            # Unauthenticated command rejected when require_auth=True
            books = b1.get("/books").json()["books"]
            book = books[0]
            copy = book["copies"][0]
            cmd = {
                "request_id": "req-auth-1",
                "operation": "borrow",
                "actor_id": "patron-1",
                "book_id": book["book_id"],
                "copy_id": copy["copy_id"],
                "expected_version": copy["version"],
            }
            assert b1.post("/commands", json=cmd).status_code == 401

            # Patron cannot impersonate another patron -> 403
            impersonate = {**cmd, "request_id": "req-auth-2", "actor_id": "patron-2"}
            assert b1.post("/commands", json=impersonate, headers=headers).status_code == 403

            # Patron cannot run staff-only add_book -> 403
            staff_cmd = {
                "request_id": "req-auth-3",
                "operation": "add_book",
                "actor_id": "patron-1",
                "book_id": "book-999",
            }
            assert b1.post("/commands", json=staff_cmd, headers=headers).status_code == 403

            # Authenticated patron borrow succeeds on branch-2 and records Lamport events
            ok = b2.post("/commands", json=cmd, headers={**headers, LAMPORT_HEADER: "10"})
            assert ok.status_code == 200
            assert ok.json()["ok"] is True

            ev = b2.get("/internal/events").json()
            assert ev["lamport_clock"] > 10
            assert any(e["event_type"] == "command_forwarded" for e in ev["events"])
    finally:
        storage_client.__exit__(None, None, None)


def test_dashboard_aggregates_nodes_and_orders_lamport_events():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/internal/raft/status"):
            if "storage-1" in url:
                return httpx.Response(200, json={
                    "node_id": "storage-1", "role": "leader", "term": 2, "voted_for": "storage-1",
                    "leader_id": "storage-1", "commit_index": 5, "last_applied": 5,
                    "last_log_index": 5, "last_log_term": 2, "revision": 5, "ready_for_reads": True, "peers": {},
                })
            return httpx.Response(500)
        if url.endswith("/health"):
            return httpx.Response(200, json={
                "mode": "branch", "branch_id": "branch-1", "status": "ready",
                "revision": 5, "last_synced_at": "2026-11-24T00:00:00Z", "storage_available": True,
            })
        if url.endswith("/internal/events"):
            if "storage-1" in url:
                return httpx.Response(200, json={
                    "node_id": "storage-1", "lamport_clock": 4,
                    "events": [{"lamport_time": 4, "node_id": "storage-1", "event_type": "command_committed", "summary": "ok", "wall_time": "2026-11-24T00:00:02Z", "details": {}}],
                })
            return httpx.Response(200, json={
                "node_id": "branch-1", "lamport_clock": 2,
                "events": [{"lamport_time": 2, "node_id": "branch-1", "event_type": "command_forwarded", "summary": "borrow", "wall_time": "2026-11-24T00:00:01Z", "details": {}}],
            })
        return httpx.Response(404)

    dash = create_dashboard_app(
        replicas={"storage-1": "http://storage-1", "storage-2": "http://storage-2"},
        branches={"branch-1": "http://branch-1"},
        transport=httpx.MockTransport(handler),
    )
    with TestClient(dash) as client:
        assert client.get("/").status_code == 200
        data = client.get("/api/cluster").json()
        assert data["leader_id"] == "storage-1"
        assert data["term"] == 2
        assert data["commit_index"] == 5
        # storage-2 failed -> marked offline without crashing dashboard
        by_id = {r["node_id"]: r for r in data["replicas"]}
        assert by_id["storage-1"]["online"] is True
        assert by_id["storage-2"]["online"] is False
        # Events are sorted by Lamport clock (L=2 before L=4)
        clocks = [e["lamport_time"] for e in data["events"]]
        assert clocks == [2, 4]