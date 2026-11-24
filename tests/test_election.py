"""Automatic leader election, terms, and failure detection on an in-process
3-node cluster. Timeouts are scaled down (heartbeat 10 ms, election timeout
150-300 ms) so failures are detected quickly; the logic is the same as in the
real replicas (heartbeat 100 ms, election timeout 1.5-3.0 s)."""

import asyncio
from datetime import datetime, timezone
import random

import pytest

from library_system.consensus import (
    CommitUnknownError,
    InMemoryTransport,
    NotLeaderError,
    NotReadyError,
    RaftElection,
    RaftNode,
    RaftStore,
)
from library_system.models import AppendEntriesRequest, LogEntry, RequestVoteRequest


IDS = ("storage-1", "storage-2", "storage-3")
NOW = datetime(2026, 11, 24, tzinfo=timezone.utc)
ELECTION_TIMEOUT = (0.15, 0.3)
NODE_OPTIONS = {"heartbeat_interval": 0.01, "commit_timeout": 1.0, "read_lease": 0.05}


class ElectionCluster:
    def __init__(self, tmp_path, seed=0, election_timeout=ELECTION_TIMEOUT):
        self.tmp_path = tmp_path
        self.seed = seed
        self.election_timeout = election_timeout
        self.transport = InMemoryTransport()
        self.nodes: dict[str, RaftNode] = {}
        self.elections: dict[str, RaftElection] = {}
        self.wins: list[tuple[int, str]] = []  # Every (term, node) that became leader.
        self.restarts = 0

    async def start_node(self, node_id: str, rng=None) -> RaftNode:
        node = RaftNode(node_id, IDS, RaftStore(self.tmp_path / f"{node_id}.sqlite3"), self.transport, **NODE_OPTIONS)
        await node.start()
        become_leader = node.become_leader

        async def recorded_become_leader(term):
            await become_leader(term)
            self.wins.append((term, node_id))

        node.become_leader = recorded_become_leader
        self.restarts += 1
        election = RaftElection(
            node,
            self.transport,
            election_timeout=self.election_timeout,
            rng=rng or random.Random(self.seed * 1000 + self.restarts),
        )
        self.transport.register(node)
        self.transport.register_voter(node_id, election)
        self.nodes[node_id] = node
        self.elections[node_id] = election
        await election.start()
        return node

    async def crash(self, node_id: str) -> None:
        election = self.elections.pop(node_id)
        node = self.nodes.pop(node_id)
        self.transport.unregister(node_id)
        await election.stop()
        await node.stop()

    async def stop(self) -> None:
        for node_id in list(self.nodes):
            await self.crash(node_id)

    def leaders(self) -> list[RaftNode]:
        return [node for node in self.nodes.values() if node.role == "leader"]

    def leader(self) -> RaftNode | None:
        leaders = self.leaders()
        return leaders[0] if len(leaders) == 1 else None

    async def stable_leader(self, among=None, timeout=5.0) -> RaftNode:
        """Wait until exactly one leader among ``among`` is ready for reads and
        every other live node in that group follows it."""
        group = among or tuple(self.nodes)

        def settled():
            leaders = [self.nodes[n] for n in group if n in self.nodes and self.nodes[n].role == "leader"]
            if len(leaders) != 1 or not leaders[0].ready_for_reads:
                return None
            leader = leaders[0]
            followers_agree = all(
                self.nodes[n].leader_id == leader.node_id and self.nodes[n].current_term == leader.current_term
                for n in group
                if n in self.nodes and n != leader.node_id
            )
            return leader if followers_agree else None

        await eventually(lambda: settled() is not None, timeout)
        return settled()


@pytest.fixture
def run_cluster(tmp_path):
    def run(scenario, seed=0, start=IDS, **options):
        async def main():
            cluster = ElectionCluster(tmp_path, seed=seed, **options)
            for node_id in start:
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
        await asyncio.sleep(0.005)


async def write(node: RaftNode, title: str):
    books = list(node.snapshot.books)
    books[0] = books[0].model_copy(update={"title": title})
    return await node.submit("replace_catalog", books=tuple(books))


def titles(node: RaftNode, committed_only=False):
    log = node.log[: node.commit_index] if committed_only else node.log
    return [entry.books[0].title for entry in log if entry.kind == "replace_catalog"]


def one_winner_per_term(cluster: ElectionCluster) -> bool:
    terms = [term for term, _ in cluster.wins]
    return len(terms) == len(set(terms))


# --- 1. Fresh start ----------------------------------------------------------


def test_fresh_cluster_elects_exactly_one_leader_and_keeps_it(run_cluster):
    async def scenario(cluster):
        leader = await cluster.stable_leader()
        assert leader.current_term >= 1
        term = leader.current_term
        # Heartbeats keep every follower's timer from expiring: no new elections.
        await asyncio.sleep(1.0)
        assert cluster.leaders() == [leader] and leader.current_term == term
        assert one_winner_per_term(cluster)

    run_cluster(scenario)


# --- 2-3. Vote rules (RaftNode.handle_request_vote) ------------------------------


def vote(term, candidate, last_index=0, last_term=0):
    return RequestVoteRequest(term=term, candidate_id=candidate, last_log_index=last_index, last_log_term=last_term)


def test_vote_rules(tmp_path):
    async def main():
        node = RaftNode("storage-1", IDS, RaftStore(tmp_path / "n.sqlite3"), InMemoryTransport())
        await node.start()
        try:
            granted = await node.handle_request_vote(vote(1, "storage-2"))
            assert granted.vote_granted and (node.current_term, node.voted_for) == (1, "storage-2")
            # One vote per term: a second candidate is refused, the same one is answered again.
            assert not (await node.handle_request_vote(vote(1, "storage-3"))).vote_granted
            assert (await node.handle_request_vote(vote(1, "storage-2"))).vote_granted

            # Give this node one entry from term 2, as a leader would.
            entry = LogEntry(index=1, term=2, kind="noop", timestamp=NOW)
            await node.handle_append_entries(AppendEntriesRequest(
                term=2, leader_id="storage-2", prev_log_index=0, prev_log_term=0, entries=(entry,), leader_commit=0
            ))
            # A stale term is refused and told the current term.
            stale = await node.handle_request_vote(vote(1, "storage-3"))
            assert (stale.term, stale.vote_granted) == (2, False)
            # A newer term moves this node to it, but a log that is behind gets no vote:
            # an older last term loses even with a longer log...
            behind = await node.handle_request_vote(vote(3, "storage-3", last_index=5, last_term=1))
            assert (behind.term, behind.vote_granted) == (3, False) and node.voted_for is None
            # ...and the same last term needs at least our length.
            assert not (await node.handle_request_vote(vote(3, "storage-3", last_index=0, last_term=2))).vote_granted
            assert (await node.handle_request_vote(vote(3, "storage-3", last_index=1, last_term=2))).vote_granted
        finally:
            await node.stop()

    asyncio.run(main())


def test_vote_survives_a_restart(tmp_path):
    async def main():
        store = tmp_path / "n.sqlite3"
        node = RaftNode("storage-1", IDS, RaftStore(store), InMemoryTransport())
        await node.start()
        assert (await node.handle_request_vote(vote(4, "storage-2"))).vote_granted
        await node.stop()

        restarted = RaftNode("storage-1", IDS, RaftStore(store), InMemoryTransport())
        await restarted.start()
        try:
            assert (restarted.current_term, restarted.voted_for) == (4, "storage-2")
            assert not (await restarted.handle_request_vote(vote(4, "storage-3"))).vote_granted
        finally:
            await restarted.stop()

    asyncio.run(main())


# --- 4-6. Failures --------------------------------------------------------------


def test_leader_crash_elects_a_new_leader_with_every_committed_entry(run_cluster):
    async def scenario(cluster):
        old = await cluster.stable_leader()
        for title in ("A", "B", "C"):
            assert (await write(old, title)).ok
        old_id, old_term = old.node_id, old.current_term
        await cluster.crash(old_id)

        survivors = tuple(n for n in IDS if n != old_id)
        new = await cluster.stable_leader(among=survivors)
        assert new.node_id != old_id and new.current_term > old_term
        assert titles(new, committed_only=True)[-3:] == ["A", "B", "C"]
        assert (await write(new, "D")).ok

        # The old leader restarts as a follower and catches up instead of taking over.
        await cluster.start_node(old_id)
        assert await cluster.stable_leader() is new
        await eventually(lambda: titles(cluster.nodes[old_id], committed_only=True) == titles(new, committed_only=True))
        assert one_winner_per_term(cluster)

    run_cluster(scenario)


def test_partitioned_leader_steps_down_and_loses_its_uncommitted_write(run_cluster):
    async def scenario(cluster):
        old = await cluster.stable_leader()
        assert (await write(old, "Committed")).ok
        cluster.transport.isolate(old.node_id)
        lost = asyncio.create_task(write(old, "Never Committed"))

        # The cut-off leader stops serving reads after its lease, then steps down (check-quorum).
        await eventually(lambda: not old.ready_for_reads, 1.0)
        with pytest.raises((NotReadyError, NotLeaderError)):
            old.read_snapshot()
        await eventually(lambda: old.role != "leader", 2.0)

        majority = tuple(n for n in IDS if n != old.node_id)
        new = await cluster.stable_leader(among=majority)
        assert (await write(new, "After Partition")).ok
        with pytest.raises(CommitUnknownError):
            await lost

        cluster.transport.heal()
        leader = await cluster.stable_leader()
        # The old leader's uncommitted entry is replaced by the new leader's log.
        await eventually(lambda: titles(old) == titles(leader))
        assert "Never Committed" not in titles(old)
        assert titles(leader, committed_only=True)[-2:] == ["Committed", "After Partition"]
        assert one_winner_per_term(cluster)

    run_cluster(scenario)


def test_partitioned_follower_rejoins_without_losing_committed_entries(run_cluster):
    async def scenario(cluster):
        leader = await cluster.stable_leader()
        follower = next(n for n in IDS if n != leader.node_id)
        cluster.transport.isolate(follower)
        assert (await write(leader, "While Partitioned")).ok
        await asyncio.sleep(0.6)  # The follower times out and runs elections it cannot win.
        assert cluster.nodes[follower].role != "leader"

        cluster.transport.heal()
        # Its higher term may force one extra election (no pre-vote); the cluster settles again.
        settled = await cluster.stable_leader()
        await eventually(lambda: all(
            "While Partitioned" in titles(node, committed_only=True) for node in cluster.nodes.values()
        ))
        assert settled.node_id != follower or "While Partitioned" in titles(settled)
        assert one_winner_per_term(cluster)

    run_cluster(scenario)


# --- 7. Split vote ------------------------------------------------------------------


def test_a_split_vote_is_resolved_in_a_later_term(run_cluster):
    async def scenario(cluster):
        # Lose every vote request of the first term, so each candidate has only
        # its own vote: nobody can win term 1.
        cluster.transport.vote_drop_rule = lambda source, target, request: request.term == 1
        leader = await cluster.stable_leader()
        assert leader.current_term >= 2
        assert all(term != 1 for term, _ in cluster.wins)
        assert sum(e.elections_started for e in cluster.elections.values()) >= 2
        assert one_winner_per_term(cluster)

    run_cluster(scenario)


# --- 8. No majority --------------------------------------------------------------------


def test_no_leader_without_a_majority(run_cluster):
    async def scenario(cluster):
        leader = await cluster.stable_leader()
        follower = next(n for n in IDS if n != leader.node_id)
        survivor = next(n for n in IDS if n not in (leader.node_id, follower))
        await cluster.crash(leader.node_id)
        await cluster.crash(follower)

        lone = cluster.nodes[survivor]
        tries = cluster.elections[survivor].elections_started
        await asyncio.sleep(1.0)
        assert lone.role != "leader" and cluster.wins[-1][1] != survivor
        assert cluster.elections[survivor].elections_started > tries  # It keeps trying.
        with pytest.raises(NotLeaderError):
            await write(lone, "No Majority")

    run_cluster(scenario)


def test_leader_with_no_reachable_followers_steps_down(run_cluster):
    async def scenario(cluster):
        leader = await cluster.stable_leader()
        for node_id in [n for n in IDS if n != leader.node_id]:
            await cluster.crash(node_id)
        await eventually(lambda: leader.role != "leader", 1.0)  # check-quorum
        with pytest.raises(NotLeaderError):
            await write(leader, "No Majority")

    run_cluster(scenario)


# --- 9. Safety under churn -----------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_never_two_leaders_in_one_term_under_crashes_and_partitions(run_cluster, seed):
    async def scenario(cluster):
        chaos = random.Random(seed)
        await cluster.stable_leader()
        for step in range(30):
            action = chaos.choice(("isolate", "heal", "restart", "write"))
            target = chaos.choice(IDS)
            if action == "isolate":
                cluster.transport.isolate(target)
            elif action == "heal":
                cluster.transport.heal()
            elif action == "restart" and target in cluster.nodes:
                await cluster.crash(target)
                await asyncio.sleep(chaos.uniform(0.0, 0.2))
                await cluster.start_node(target)
            elif action == "write" and cluster.leader() is not None:
                asyncio.create_task(write(cluster.leader(), f"Step {step}"))
            await asyncio.sleep(chaos.uniform(0.02, 0.25))
            assert one_winner_per_term(cluster)

        cluster.transport.heal()
        leader = await cluster.stable_leader(timeout=10)
        # Every replica ends with the same committed log.
        await eventually(lambda: all(
            titles(node, committed_only=True) == titles(leader, committed_only=True)
            for node in cluster.nodes.values()
        ), 10)
        assert one_winner_per_term(cluster)

    run_cluster(scenario, seed=seed)


# --- Demo trigger ---------------------------------------------------------------------


def test_start_election_now_moves_leadership_to_an_up_to_date_follower(run_cluster):
    async def scenario(cluster):
        old = await cluster.stable_leader()
        assert (await write(old, "Before")).ok
        challenger = next(n for n in IDS if n != old.node_id)
        await eventually(lambda: "Before" in titles(cluster.nodes[challenger], committed_only=True))

        status = await cluster.elections[challenger].start_election_now()
        assert status.role == "leader" and status.term > old.current_term - 1
        new = await cluster.stable_leader()
        assert new.node_id == challenger and old.role == "follower"
        assert titles(new, committed_only=True)[-1] == "Before"

    run_cluster(scenario)


def test_election_timeout_must_exceed_the_read_lease(tmp_path):
    node = RaftNode("storage-1", IDS, RaftStore(tmp_path / "n.sqlite3"), InMemoryTransport(), read_lease=0.5)
    with pytest.raises(ValueError, match="read lease"):
        RaftElection(node, InMemoryTransport(), election_timeout=(0.4, 1.0))
    with pytest.raises(ValueError):
        RaftElection(node, InMemoryTransport(), election_timeout=(2.0, 1.0))
