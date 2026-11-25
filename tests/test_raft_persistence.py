from datetime import datetime, timezone

import pytest

from library_system.consensus import RaftStore
from library_system.models import Book, BookCopy, CatalogSnapshot, LogEntry


NOW = datetime(2026, 11, 19, tzinfo=timezone.utc)


def entry(index, term, kind="noop"):
    return LogEntry(index=index, term=term, kind=kind, timestamp=NOW)


def store_at(tmp_path, name="replica.sqlite3"):
    store = RaftStore(tmp_path / name)
    store.initialize()
    return store


def test_fresh_replicas_seed_identical_state_at_revision_one(tmp_path):
    first = store_at(tmp_path, "a.sqlite3").load()
    second = store_at(tmp_path, "b.sqlite3").load()
    assert first == second
    assert (first.current_term, first.voted_for, first.log, first.last_applied) == (0, None, (), 0)
    assert first.snapshot.revision == 1
    assert len(first.snapshot.books) == 20


def test_term_and_vote_survive_reopen_and_never_move_backwards(tmp_path):
    store = store_at(tmp_path)
    store.save_term_vote(3, "storage-2")
    reopened = RaftStore(tmp_path / "replica.sqlite3", tmp_path / "no-seed-needed")
    reopened.initialize()
    state = reopened.load()
    assert (state.current_term, state.voted_for) == (3, "storage-2")
    with pytest.raises(RuntimeError, match="backwards"):
        reopened.save_term_vote(2, None)


def test_log_append_is_contiguous_and_durable(tmp_path):
    store = store_at(tmp_path)
    store.append([entry(1, 1), entry(2, 1)])
    with pytest.raises(RuntimeError, match="contiguous"):
        store.append([entry(4, 1)])
    assert store.load().log == (entry(1, 1), entry(2, 1))
    assert store.last_index_term() == (2, 1)
    assert store.entries(2) == (entry(2, 1),)


def test_replace_suffix_is_atomic(tmp_path):
    store = store_at(tmp_path)
    store.append([entry(1, 1), entry(2, 1), entry(3, 1)])
    store.replace_suffix(2, [entry(2, 2)])
    assert store.load().log == (entry(1, 1), entry(2, 2))
    # A failing replacement (gap in the new entries) must leave the old suffix intact.
    with pytest.raises(RuntimeError, match="contiguous"):
        store.replace_suffix(2, [entry(3, 3)])
    assert store.load().log == (entry(1, 1), entry(2, 2))


def test_applied_entries_can_never_be_truncated(tmp_path):
    store = store_at(tmp_path)
    store.append([entry(1, 1), entry(2, 1)])
    store.save_applied(1, store.load().snapshot)
    with pytest.raises(RuntimeError, match="applied"):
        store.replace_suffix(1, [entry(1, 2)])
    store.replace_suffix(2, [entry(2, 2)])


def test_applied_state_survives_reopen_and_must_be_in_order(tmp_path):
    store = store_at(tmp_path)
    books = (
        Book(
            book_id="new-book",
            title="New",
            author="Author",
            copies=(BookCopy(copy_id="new-copy", branch_id="branch-3", available=True, version=0),),
        ),
    )
    store.append([entry(1, 1), LogEntry(index=2, term=1, kind="replace_catalog", timestamp=NOW, books=books)])
    with pytest.raises(RuntimeError, match="in order"):
        store.save_applied(2, CatalogSnapshot(revision=2, books=books))
    store.save_applied(1, store.load().snapshot)
    store.save_applied(2, CatalogSnapshot(revision=2, books=books))

    reopened = RaftStore(tmp_path / "replica.sqlite3")
    reopened.initialize()
    state = reopened.load()
    assert state.last_applied == 2
    assert state.snapshot == CatalogSnapshot(revision=2, books=books)
