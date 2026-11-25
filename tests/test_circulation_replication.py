"""Circulation remains deterministic and retry-safe through Raft failures."""

import asyncio

import pytest

from library_system.circulation import create_circulation_machine
from library_system.consensus import CommitUnknownError
from library_system.models import CommandEnvelope
from test_circulation import command
from test_replication import Cluster, IDS, eventually


def test_circulation_survives_leader_change_and_follower_restart(tmp_path):
    async def scenario():
        cluster = Cluster(tmp_path, state_machine=create_circulation_machine())
        for node_id in IDS:
            await cluster.start_node(node_id)
        try:
            await cluster[1].become_leader(1)
            borrow = CommandEnvelope(**command())
            results = await asyncio.gather(*(cluster[1].submit("command", command=borrow) for _ in range(4)))
            assert results[0].ok and all(result == results[0] for result in results)
            hold = CommandEnvelope(**command("hold", "hold", "bob"))
            held = await cluster[1].submit("command", command=hold)
            assert held.ok
            await eventually(lambda: all(node.last_applied == cluster[1].last_applied for node in cluster.nodes.values()))
            assert all(node.snapshot == cluster[1].snapshot for node in cluster.nodes.values())
            assert cluster[1].snapshot.revision == 3

            await cluster.crash("storage-1")
            await cluster[2].become_leader(2)
            assert await cluster[2].submit("command", command=borrow) == results[0]
            assert await cluster[2].submit("command", command=hold) == held
            assert cluster[2].snapshot.revision == 3
            returned = await cluster[2].submit("command", command=CommandEnvelope(**command("return", "return", expected_version=1)))
            assert returned.ok
            bob = await cluster[2].submit("command", command=CommandEnvelope(**command("bob-borrow", actor_id="bob", expected_version=2)))
            assert bob.ok
            restarted = await cluster.start_node("storage-1")
            await eventually(lambda: all(node.last_applied == cluster[2].last_applied for node in cluster.nodes.values()))
            assert all(node.snapshot == cluster[2].snapshot for node in cluster.nodes.values())
            assert restarted.snapshot.books[0].copies[0].borrower_id == "bob"
            assert restarted.snapshot.books[0].holds == ()

            # The rejoined node's persisted request table must also survive disk reload.
            await cluster.crash("storage-1")
            restarted = await cluster.start_node("storage-1")
            await cluster[2].become_follower(3, "storage-1")
            await restarted.become_leader(3)
            assert await restarted.submit("command", command=borrow) == results[0]
            assert restarted.snapshot.revision == 5
        finally:
            await cluster.stop()

    asyncio.run(scenario())


def test_unknown_commit_and_retry_do_not_create_a_second_loan(tmp_path):
    async def scenario():
        cluster = Cluster(tmp_path, state_machine=create_circulation_machine(), commit_timeout=0.1)
        for node_id in IDS:
            await cluster.start_node(node_id)
        try:
            await cluster[1].become_leader(1)
            await eventually(lambda: cluster[1].ready_for_reads)
            cluster.transport.isolate("storage-2", "storage-3")
            borrow = CommandEnvelope(**command())
            with pytest.raises(CommitUnknownError):
                await cluster[1].submit("command", command=borrow)
            assert all(node.snapshot.revision == 1 for node in cluster.nodes.values())
            cluster.transport.heal()
            retried = await cluster[1].submit("command", command=borrow)
            assert retried.ok and retried.index == 2  # Outcome of the original entry.
            await eventually(lambda: all(node.last_applied == cluster[1].last_applied for node in cluster.nodes.values()))
            assert all(node.snapshot.revision == 2 for node in cluster.nodes.values())
            assert all(node.snapshot.books[0].copies[0].version == 1 for node in cluster.nodes.values())
        finally:
            await cluster.stop()

    asyncio.run(scenario())
