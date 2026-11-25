"""Replication, majority commit, and catch-up on an in-process 3-node cluster."""

import asyncio
from datetime import datetime, timezone

import pytest

from library_system.consensus import (
    CatalogStateMachine,
    CommandOutcome,
    CommandRejected,
    CommitUnknownError,
    InMemoryTransport,
    NotLeaderError,
    NotReadyError,
    RaftNode,
    RaftStore,
)
from library_system.models import AppendEntriesRequest, CatalogSnapshot, CommandEnvelope, LogEntry


NOW = datetime(2026, 11, 19, tzinfo=timezone.utc)
IDS = ("storage-1", "storage-2", "storage-3")


class Cluster:
    def __init__(self, tmp_path, **node_options):
        self.tmp_path = tmp_path
        self.transport = InMemoryTransport()
        self.nodes: dict[str, RaftNode] = {}
        self.options = {"heartbeat_interval": 0.01, "commit_timeout": 1.0, **node_options}

    def store(self, node_id):
        return RaftStore(self.tmp_path / f"{node_id}.sqlite3")

    async def start_node(self, node_id) -> RaftNode:
        node = RaftNode(node_id, IDS, self.store(node_id), self.transport, **self.options)
        await node.start()
        self.transport.register(node)
        self.nodes[node_id] = node
        return node

    async def crash(self, node_id):
        node = self.nodes.pop(node_id)
        self.transport.unregister(node_id)
        await node.stop()

    async def stop(self):
        for node in list(self.nodes.values()):
            await node.stop()

    def __getitem__(self, number) -> RaftNode:
        return self.nodes[f"storage-{number}"]


@pytest.fixture
def run_cluster(tmp_path):
    def run(scenario, **node_options):
        async def main():
            cluster = Cluster(tmp_path, **node_options)
            for node_id in IDS:
                await cluster.start_node(node_id)
            try:
                await scenario(cluster)
            finally:
                await cluster.stop()

        asyncio.run(main())

    return run


async def eventually(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("Condition not reached in time")
        await asyncio.sleep(0.01)


def retitled(snapshot: CatalogSnapshot, title: str):
    books = list(snapshot.books)
    books[0] = books[0].model_copy(update={"title": title})
    return tuple(books)


async def write(node: RaftNode, title: str):
    return await node.submit("replace_catalog", books=retitled(node.snapshot, title))


def titles_in_log(node: RaftNode):
    return [entry.books[0].title for entry in node.log if entry.kind == "replace_catalog"]


def test_majority_commits_with_one_follower_down_and_it_catches_up(run_cluster):
    async def scenario(c):
        c.transport.isolate("storage-3")
        await c[1].become_leader(1)
        result = await write(c[1], "Replicated")
        assert result.ok and result.revision == 2
        await eventually(lambda: c[2].last_applied == c[1].last_applied)
        assert c[2].snapshot == c[1].snapshot
        assert c[3].log == [] and c[3].snapshot.revision == 1

        c.transport.heal()
        await eventually(lambda: c[3].last_applied == c[1].last_applied)
        assert c[3].log == c[1].log
        assert c[3].snapshot == c[1].snapshot
        assert c[3].snapshot.books[0].title == "Replicated"

    run_cluster(scenario)


def test_no_commit_without_a_majority(run_cluster):
    async def scenario(c):
        await c[1].become_leader(1)
        await eventually(lambda: c[1].ready_for_reads)
        c.transport.isolate("storage-2", "storage-3")
        with pytest.raises(CommitUnknownError):
            await write(c[1], "Unconfirmed")
        assert c[1].commit_index == 1
        assert c[1].snapshot.revision == 1
        assert len(c[1].log) == 2  # Appended locally, but not committed.

        # The outcome was unknown, not failed: once a majority returns, it commits.
        c.transport.heal()
        await eventually(lambda: all(node.snapshot.revision == 2 for node in c.nodes.values()))

    run_cluster(scenario, commit_timeout=0.3)


def test_restarted_follower_catches_up_from_disk_in_batches(run_cluster, tmp_path):
    async def scenario(c):
        await c[1].become_leader(1)
        await write(c[1], "Before crash")
        await eventually(lambda: c[3].last_applied == c[1].last_applied)
        await c.crash("storage-3")

        for number in range(5):
            assert (await write(c[1], f"While down {number}")).ok
        restarted = await c.start_node("storage-3")
        assert restarted.snapshot.books[0].title == "Before crash"  # Reloaded from disk.

        await eventually(lambda: restarted.last_applied == c[1].last_applied)
        assert restarted.log == c[1].log
        assert restarted.snapshot == c[1].snapshot
        assert restarted.snapshot.revision == 7
        # The follower applies an entry before its acknowledgement reaches the leader.
        await eventually(lambda: c[1].status().peers["storage-3"].lag == 0)
        assert c[1].status().peers["storage-3"].lag == 0

        on_disk = RaftStore(tmp_path / "storage-3.sqlite3").load()
        assert on_disk.snapshot == c[1].snapshot
        assert list(on_disk.log) == c[1].log

    run_cluster(scenario, batch_size=2)


def test_divergent_uncommitted_suffix_is_replaced_on_rejoin(run_cluster):
    async def scenario(c):
        await c[1].become_leader(1)
        await eventually(lambda: c[1].ready_for_reads)
        c.transport.isolate("storage-2", "storage-3")
        with pytest.raises(CommitUnknownError):
            await write(c[1], "Never committed")
        await c.crash("storage-1")
        c.transport.heal()

        await c[2].become_leader(2)
        assert (await write(c[2], "Committed by new leader")).ok

        old_leader = await c.start_node("storage-1")
        assert titles_in_log(old_leader) == ["Never committed"]
        await eventually(lambda: old_leader.log == c[2].log and old_leader.last_applied == c[2].last_applied)
        assert titles_in_log(old_leader) == ["Committed by new leader"]
        assert old_leader.snapshot == c[2].snapshot
        assert old_leader.current_term == 2

    run_cluster(scenario, commit_timeout=0.3)


def test_previous_term_entry_is_not_committed_by_counting_replicas(tmp_path):
    """Raft figure 8: an old-term entry commits only with a current-term entry."""

    async def main():
        cluster = Cluster(tmp_path, batch_size=1)
        prepared = cluster.store("storage-1")
        prepared.initialize()
        old_books = retitled(prepared.load().snapshot, "Old term entry")
        prepared.append([
            LogEntry(index=1, term=1, kind="noop", timestamp=NOW),
            LogEntry(index=2, term=2, kind="replace_catalog", timestamp=NOW, books=old_books),
        ])
        prepared.save_term_vote(2, "storage-1")
        for node_id in IDS:
            await cluster.start_node(node_id)
        leader, follower = cluster[1], cluster[2]
        try:
            cluster.transport.isolate("storage-3")
            # Let storage-2 receive entries 1 and 2 but not the new term-3 no-op.
            cluster.transport.drop_rule = lambda source, target, request: (
                target == "storage-2"
                and len(follower.log) >= 2
                and any(entry.index == 3 for entry in request.entries)
            )
            await leader.become_leader(3)
            await eventually(lambda: leader.status().peers["storage-2"].match_index == 2)
            await asyncio.sleep(0.1)
            assert follower.log == leader.log[:2]  # Entry 2 is on a majority...
            assert leader.commit_index == 0  # ...but is from term 2, so not committed.
            assert leader.snapshot.revision == 1

            cluster.transport.drop_rule = None
            await eventually(lambda: leader.last_applied == 3)
            assert leader.commit_index == 3
            assert leader.snapshot.books[0].title == "Old term entry"
        finally:
            await cluster.stop()

    asyncio.run(main())


def test_duplicate_and_reordered_append_entries_are_idempotent(run_cluster):
    async def scenario(c):
        follower = c[2]
        entries = tuple(LogEntry(index=i, term=1, kind="noop", timestamp=NOW) for i in (1, 2, 3))

        def request(prev_index=0, prev_term=0, items=entries, term=1, commit=0):
            return AppendEntriesRequest(
                term=term, leader_id="storage-1", prev_log_index=prev_index,
                prev_log_term=prev_term, entries=items, leader_commit=commit,
            )

        first = await follower.handle_append_entries(request())
        assert first.success and first.match_index == 3
        delayed = await follower.handle_append_entries(request(items=entries[:2]))
        assert delayed.success and delayed.match_index == 2
        assert len(follower.log) == 3  # A delayed, shorter request does not truncate.
        assert (await follower.handle_append_entries(request())).match_index == 3
        assert follower.log == list(entries)

        gap = await follower.handle_append_entries(request(prev_index=5, prev_term=1, items=()))
        assert not gap.success and gap.conflict_index == 4
        mismatch = await follower.handle_append_entries(request(prev_index=3, prev_term=2, items=(), term=2))
        assert not mismatch.success and (mismatch.conflict_term, mismatch.conflict_index) == (1, 1)

        stale = await follower.handle_append_entries(request(items=(), term=1))
        assert not stale.success and stale.term == 2

        committed = await follower.handle_append_entries(request(prev_index=3, prev_term=1, items=(), term=2, commit=2))
        assert committed.success and follower.commit_index == 2 and follower.last_applied == 2

    run_cluster(scenario)


def test_leader_steps_down_on_higher_term_and_pending_writes_fail(run_cluster):
    async def scenario(c):
        await c[1].become_leader(1)
        await eventually(lambda: c[1].ready_for_reads)
        c.transport.isolate("storage-2", "storage-3")
        pending = asyncio.create_task(write(c[1], "Pending"))
        await asyncio.sleep(0.05)

        response = await c[1].handle_append_entries(
            AppendEntriesRequest(term=5, leader_id="storage-2", prev_log_index=0, prev_log_term=0, leader_commit=0)
        )
        assert response.success
        assert (c[1].role, c[1].current_term, c[1].leader_id) == ("follower", 5, "storage-2")
        with pytest.raises(CommitUnknownError):
            await pending
        with pytest.raises(NotLeaderError):
            await write(c[1], "Rejected")

    run_cluster(scenario, commit_timeout=5.0)


def test_leader_steps_down_when_a_follower_reports_a_higher_term(run_cluster):
    async def scenario(c):
        await c[2].set_term_vote(7, None)
        await c[1].become_leader(1)
        await eventually(lambda: c[1].role == "follower")
        assert c[1].current_term == 7
        assert RaftStore(c[1].store.db_path).load().current_term == 7

    run_cluster(scenario)


def test_reads_wait_for_the_new_leaders_commit_and_a_live_majority(run_cluster):
    async def scenario(c):
        c.transport.isolate("storage-2", "storage-3")
        await c[1].become_leader(1)
        with pytest.raises(NotReadyError):
            c[1].read_snapshot()
        with pytest.raises(NotLeaderError):
            c[2].read_snapshot()

        c.transport.heal()
        await eventually(lambda: c[1].ready_for_reads)
        assert c[1].read_snapshot().revision == 1

        # A leader cut off from the majority stops serving possibly stale reads.
        c.transport.isolate("storage-2", "storage-3")
        await eventually(lambda: not c[1].ready_for_reads)

    run_cluster(scenario)


def test_registered_command_handlers_are_replicated(run_cluster):
    def borrow(snapshot, command, context):
        books = []
        for book in snapshot.books:
            copies = []
            for copy in book.copies:
                if copy.copy_id == command.copy_id:
                    if copy.version != command.expected_version:
                        raise CommandRejected("Version mismatch")
                    copy = copy.model_copy(update={
                        "available": False, "due_date": context.timestamp.date(), "version": copy.version + 1,
                    })
                copies.append(copy)
            books.append(book.model_copy(update={"copies": tuple(copies)}))
        return CommandOutcome(books=tuple(books), result={"copy_id": command.copy_id})

    machine = CatalogStateMachine({"borrow": borrow})

    async def scenario(c):
        for node in c.nodes.values():
            node.state_machine = machine
        await c[1].become_leader(1)
        command = CommandEnvelope(
            request_id="r1", operation="borrow", actor_id="patron", book_id="book-001",
            copy_id="book-001-copy-1", expected_version=0,
        )
        result = await c[1].submit("command", command=command)
        assert result.ok and result.revision == 2 and result.result == {"copy_id": "book-001-copy-1"}

        assert await c[1].submit("command", command=command) == result
        rejected = await c[1].submit(
            "command", command=command.model_copy(update={"request_id": "r2"}),
        )  # New request with a stale expected_version.
        assert not rejected.ok and rejected.error == "Version mismatch" and rejected.revision == 2

        unknown = await c[1].submit("command", command=command.model_copy(update={"request_id": "r3", "operation": "renew"}))
        assert not unknown.ok and "No handler" in unknown.error

        await eventually(lambda: all(n.last_applied == c[1].last_applied for n in c.nodes.values()))
        assert all(n.snapshot == c[1].snapshot for n in c.nodes.values())

    run_cluster(scenario)
