"""Tests for staff add_book and remove_book operations with role enforcement."""

from pathlib import Path

from fastapi.testclient import TestClient
import httpx

from library_system.branch import create_app as create_branch_app
from library_system.storage import create_app as create_storage_app


def test_staff_can_add_and_remove_books_and_patrons_cannot(tmp_path: Path):
    storage_app = create_storage_app(db_path=tmp_path / "catalog.sqlite3")
    with TestClient(storage_app):
        branch_app = create_branch_app(
            branch_id="branch-1",
            storage_url="http://storage.test",
            transport=httpx.ASGITransport(app=storage_app),
            require_auth=True,
        )
        with TestClient(branch_app) as client:
            staff_tok = client.post(
                "/auth/login", json={"user_id": "staff-1", "password": "staff123"}
            ).json()["access_token"]
            patron_tok = client.post(
                "/auth/login", json={"user_id": "patron-1", "password": "password123"}
            ).json()["access_token"]
            staff_hdr = {"Authorization": f"Bearer {staff_tok}"}
            patron_hdr = {"Authorization": f"Bearer {patron_tok}"}

            add_cmd = {
                "request_id": "staff-add-1",
                "operation": "add_book",
                "actor_id": "staff-1",
                "book_id": "book-900",
                "title": "Designing Data-Intensive Applications",
                "author": "Martin Kleppmann",
                "home_branch": "branch-2",
                "num_copies": 2,
            }

            # Patron blocked with 403
            assert client.post("/commands", json={**add_cmd, "actor_id": "patron-1"}, headers=patron_hdr).status_code == 403

            # Staff succeeds and book is immediately searchable
            res = client.post("/commands", json=add_cmd, headers=staff_hdr)
            assert res.status_code == 200 and res.json()["ok"] is True
            detail = client.get("/books/book-900")
            assert detail.status_code == 200
            assert len(detail.json()["book"]["copies"]) == 2

            # Duplicate add_book rejected deterministically
            dup = client.post("/commands", json={**add_cmd, "request_id": "staff-add-dup"}, headers=staff_hdr)
            assert dup.status_code == 200 and dup.json()["ok"] is False and dup.json()["error"] == "book_already_exists"

            # Borrow one copy -> remove_book must be rejected while checked out
            borrow = client.post("/commands", json={
                "request_id": "patron-borrow-900",
                "operation": "borrow",
                "actor_id": "patron-1",
                "book_id": "book-900",
                "copy_id": "book-900-c1",
                "expected_version": 0,
            }, headers=patron_hdr)
            assert borrow.status_code == 200 and borrow.json()["ok"] is True

            rm_blocked = client.post("/commands", json={
                "request_id": "staff-rm-blocked",
                "operation": "remove_book",
                "actor_id": "staff-1",
                "book_id": "book-900",
            }, headers=staff_hdr)
            assert rm_blocked.status_code == 200 and rm_blocked.json()["ok"] is False
            assert rm_blocked.json()["error"] == "copies_checked_out"

            # Return copy -> remove_book succeeds
            ret = client.post("/commands", json={
                "request_id": "patron-ret-900",
                "operation": "return",
                "actor_id": "patron-1",
                "book_id": "book-900",
                "copy_id": "book-900-c1",
                "expected_version": 1,
            }, headers=patron_hdr)
            assert ret.status_code == 200 and ret.json()["ok"] is True

            rm_ok = client.post("/commands", json={
                "request_id": "staff-rm-ok",
                "operation": "remove_book",
                "actor_id": "staff-1",
                "book_id": "book-900",
            }, headers=staff_hdr)
            assert rm_ok.status_code == 200 and rm_ok.json()["ok"] is True
            assert client.get("/books/book-900").status_code == 404