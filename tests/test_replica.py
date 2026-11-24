"""HTTP contract of a replica, using a single-node cluster (majority of one)."""

import time

import pytest
from fastapi.testclient import TestClient

from library_system.consensus import CatalogStateMachine, CommandOutcome
from library_system.models import CatalogSnapshot, CommandResult, NodeStatus
from library_system.replica import create_app, parse_cluster


SOLO = {"storage-1": "http://127.0.0.1:9"}
COMMAND = {
    "request_id": "request-1", "operation": "borrow", "actor_id": "patron-1",
    "book_id": "book-001", "copy_id": "book-001-copy-1", "expected_version": 0,
}


def wait_ready(client):
    deadline = time.monotonic() + 5
    while not client.get("/internal/raft/status").json()["ready_for_reads"]:
        assert time.monotonic() < deadline, "replica never became ready"
        time.sleep(0.02)


def test_parse_cluster():
    assert parse_cluster(" a=http://x:1/ , b=http://y:2 ") == {"a": "http://x:1", "b": "http://y:2"}
    with pytest.raises(ValueError):
        parse_cluster("a=http://x:1,broken")


def test_rejects_inconsistent_configuration(tmp_path):
    with pytest.raises(ValueError, match="RAFT_NODE_ID"):
        create_app(node_id="storage-9", cluster=SOLO, db_path=tmp_path / "r.sqlite3")
    with pytest.raises(ValueError, match="RAFT_INITIAL_LEADER"):
        create_app(cluster=SOLO, db_path=tmp_path / "r.sqlite3", initial_leader="storage-9")


def test_single_replica_serves_reads_and_replicated_writes(tmp_path):
    app = create_app(cluster=SOLO, db_path=tmp_path / "r.sqlite3", enable_test_writes=True)
    with TestClient(app) as client:
        wait_ready(client)
        health = client.get("/health").json()
        assert health["status"] == "ok" and health["mode"] == "raft-replica"
        assert (health["role"], health["leader_id"], health["term"]) == ("leader", "storage-1", 1)

        snapshot = CatalogSnapshot.model_validate(client.get("/internal/catalog").json())
        assert snapshot.revision == 1 and len(snapshot.books) == 20
        assert client.get("/internal/catalog/version").json() == {"revision": 1}

        books = [book.model_dump(mode="json") for book in snapshot.books]
        books[0]["title"] = "Replicated Title"
        written = CommandResult.model_validate(
            client.post("/internal/raft/test/replace_catalog", json={"books": books}).json()
        )
        assert written.ok and written.revision == 2
        assert client.get("/internal/catalog").json()["books"][0]["title"] == "Replicated Title"
        assert client.post("/internal/raft/test/replace_catalog", json={"books": books + books[:1]}).status_code == 422

        # Unimplemented operations are refused before anything is appended.
        before = client.get("/internal/raft/status").json()["last_log_index"]
        assert client.post("/internal/commands", json={**COMMAND, "operation": "add_book"}).status_code == 501
        assert client.get("/internal/raft/status").json()["last_log_index"] == before
        assert client.post("/internal/commands", json={**COMMAND, "expected_version": -1}).status_code == 422

        status = NodeStatus.model_validate(client.post("/internal/raft/promote").json())
        assert status.role == "leader" and status.term == 1  # Already leader: unchanged.

    # Restarting keeps the committed catalog, revision, and term.
    with TestClient(create_app(cluster=SOLO, db_path=tmp_path / "r.sqlite3")) as client:
        wait_ready(client)
        assert client.get("/internal/catalog/version").json() == {"revision": 2}
        assert client.get("/health").json()["term"] == 2


def test_registered_handler_runs_through_commands_endpoint(tmp_path):
    machine = CatalogStateMachine({"borrow": lambda snapshot, command, context: CommandOutcome(result={"seen": command.request_id})})
    app = create_app(cluster=SOLO, db_path=tmp_path / "r.sqlite3", state_machine=machine)
    with TestClient(app) as client:
        wait_ready(client)
        response = client.post("/internal/commands", json=COMMAND)
        assert response.status_code == 200
        assert response.json()["result"] == {"seen": "request-1"}
        assert client.post("/internal/raft/test/replace_catalog", json={"books": []}).status_code == 404


def test_follower_without_a_leader_refuses_rather_than_serving(tmp_path):
    cluster = {"storage-1": "http://127.0.0.1:9", "storage-2": "http://127.0.0.1:9"}
    app = create_app(node_id="storage-2", cluster=cluster, db_path=tmp_path / "r.sqlite3", initial_leader=None)
    with TestClient(app) as client:
        assert client.get("/health").json()["role"] == "follower"
        assert client.get("/internal/catalog").status_code == 503
        assert client.get("/internal/catalog/version").status_code == 503
        assert client.post("/internal/raft/promote").status_code == 409  # Peer unreachable: no majority.
        stale = {"term": 1, "leader_id": "storage-1", "prev_log_index": 0, "prev_log_term": 0, "entries": [], "leader_commit": 0}
        assert client.post("/internal/raft/append_entries", json=stale).json()["success"] is True
        assert client.get("/health").json()["leader_id"] == "storage-1"
