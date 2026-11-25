from copy import deepcopy
from datetime import date

import pytest
from pydantic import ValidationError

from library_system.models import BookCopy, CatalogSnapshot, CommandEnvelope


def copy_record(**changes):
    return {
        "copy_id": "copy-1",
        "branch_id": "branch-1",
        "available": True,
        "due_date": None,
        "version": 0,
        **changes,
    }


def snapshot_record():
    return {
        "revision": 0,
        "books": [
            {
                "book_id": "book-1",
                "title": "Distributed Systems",
                "author": "Example Author",
                "copies": [copy_record()],
            }
        ],
    }


def test_borrowed_copy_preserves_due_date():
    copy = BookCopy.model_validate(
        copy_record(available=False, due_date="2026-12-01", version=2)
    )
    assert copy.due_date == date(2026, 12, 1)
    assert copy.model_dump(mode="json")["due_date"] == "2026-12-01"


@pytest.mark.parametrize(
    "changes",
    [
        {"copy_id": ""},
        {"branch_id": "branch-4"},
        {"version": -1},
        {"due_date": "not-a-date"},
        {"due_date": "2026-12-01"},
        {"unknown_field": "unexpected"},
    ],
)
def test_invalid_copy_records_are_rejected(changes):
    with pytest.raises(ValidationError):
        BookCopy.model_validate(copy_record(**changes))


def test_snapshot_rejects_duplicate_book_ids():
    payload = snapshot_record()
    other = deepcopy(payload["books"][0])
    other["copies"][0]["copy_id"] = "copy-2"
    payload["books"].append(other)
    with pytest.raises(ValidationError, match="book IDs"):
        CatalogSnapshot.model_validate(payload)


def test_snapshot_rejects_copy_ids_reused_across_titles():
    payload = snapshot_record()
    other = deepcopy(payload["books"][0])
    other["book_id"] = "book-2"
    payload["books"].append(other)
    with pytest.raises(ValidationError, match="copy IDs"):
        CatalogSnapshot.model_validate(payload)


def test_snapshot_rejects_negative_revision():
    payload = snapshot_record()
    payload["revision"] = -1
    with pytest.raises(ValidationError):
        CatalogSnapshot.model_validate(payload)


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": ""},
        {"operation": "delete_everything"},
        {"expected_version": -1},
        {"unknown_field": "unexpected"},
    ],
)
def test_invalid_future_command_envelopes_are_rejected(changes):
    payload = {
        "request_id": "request-1",
        "operation": "borrow",
        "actor_id": "patron-1",
        "book_id": "book-1",
        "copy_id": "copy-1",
        "expected_version": 0,
        **changes,
    }
    with pytest.raises(ValidationError):
        CommandEnvelope.model_validate(payload)
