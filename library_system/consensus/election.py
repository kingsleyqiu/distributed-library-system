"""Automatic leader election for the storage replicas (Raft section 5.2).

Failure detection uses the leader's heartbeats: ``RaftNode`` sends an
AppendEntries to every follower each ``heartbeat_interval`` and calls
``on_valid_append_entries`` on the follower when one arrives. Each follower
keeps a randomized election deadline; if no heartbeat arrives before it
passes, the follower assumes the leader has failed and runs for election:

1. ``node.begin_candidacy()`` moves to the next term and durably votes for
   itself before any request is sent.
2. RequestVote goes to every peer in parallel. A peer grants at most one vote
   per term, and only to a candidate whose log is at least as up to date as
   its own (``RaftNode.handle_request_vote``).
3. With a majority of votes, ``node.become_leader(term)`` writes the term's
   no-op entry and starts the heartbeats. A reply carrying a higher term makes
   the candidate a follower. Otherwise the next random deadline starts a new
   election in the following term.

A leader also checks it still has a quorum (check-quorum): if fewer than a
majority of replicas, counting itself, acknowledged it within one minimum
election timeout, it steps down instead of claiming leadership while cut off.

Timing: the minimum election timeout must exceed the leader's read lease plus
one RPC timeout. A follower starts its timer when it *receives* a heartbeat,
but the leader starts its lease when the acknowledgement arrives back, so the
margin ensures an old leader has stopped serving reads before anyone can be
elected in its place.
"""

import asyncio
from collections.abc import Callable
import logging
import random

from library_system.consensus.replication import RaftNode
from library_system.consensus.transport import PeerTransport, PeerUnavailable
from library_system.models import NodeStatus, RequestVoteRequest, RequestVoteResponse


logger = logging.getLogger(__name__)


class RaftElection:
    def __init__(
        self,
        node: RaftNode,
        transport: PeerTransport,
        *,
        election_timeout: tuple[float, float] = (1.5, 3.0),
        initial_leader: str | None = None,
        check_quorum: bool = True,
        rng: random.Random | None = None,
        clock: Callable[[], float] | None = None,
    ):
        low, high = election_timeout
        if not 0 < low <= high:
            raise ValueError("The election timeout range must satisfy 0 < min <= max")
        if low <= node.read_lease:
            raise ValueError(
                f"The minimum election timeout ({low}s) must exceed the read lease ({node.read_lease}s)"
            )
        self.node = node
        self.transport = transport
        self.election_timeout = (low, high)
        self.initial_leader = initial_leader
        self.check_quorum = check_quorum
        self.rng = rng or random.Random()
        self.clock = clock or node.clock
        self.elections_started = 0
        self._deadline = 0.0
        self._leader_since: float | None = None
        self._election_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._closed = False
        node.hooks = self

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        low, _ = self.election_timeout
        if self.node.node_id == self.initial_leader:
            # A short first timeout so demos start with a predictable leader.
            # It is still several heartbeats long, so a restarted replica
            # hears an existing leader before it runs.
            self._deadline = self.clock() + low / 3
        else:
            self.reset_election_timer()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # --- ElectionHooks (called by RaftNode) --------------------------------

    def on_valid_append_entries(self, term: int, leader_id: str) -> None:
        """A current leader's heartbeat: the leader is alive."""
        self.reset_election_timer()

    def on_higher_term(self, term: int) -> None:
        """The node saw a newer term and is now a follower."""
        self._leader_since = None
        self.reset_election_timer()

    # --- RPC handler ------------------------------------------------------

    async def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        response = await self.node.handle_request_vote(request)
        if response.vote_granted:
            # Granting a vote means a candidate is about to lead: wait for it.
            self.reset_election_timer()
        return response

    # --- elections --------------------------------------------------------

    def reset_election_timer(self) -> None:
        self._deadline = self.clock() + self.rng.uniform(*self.election_timeout)

    async def start_election_now(self) -> NodeStatus:
        """Development/demo trigger: run an election immediately. The replica
        can lose it, exactly as in a timeout-driven election."""
        if self.node.role != "leader":
            await self.run_election()
        return self.node.status()

    async def run_election(self) -> bool:
        """Run one election round. Returns True if this replica became leader."""
        async with self._election_lock:
            node = self.node
            if self._closed or node.role == "leader":
                return node.role == "leader"
            term = await node.begin_candidacy()
            self.elections_started += 1
            self.reset_election_timer()
            last_index, last_term = node.last_log_index_term()
            request = RequestVoteRequest(
                term=term, candidate_id=node.node_id, last_log_index=last_index, last_log_term=last_term
            )
            votes = 1
            pending = [
                asyncio.create_task(self._ask(peer, request)) for peer in node.peer_ids
            ]
            try:
                # Count votes as they arrive, so a dead peer's RPC timeout
                # does not delay a win that a majority has already decided.
                for next_answer in asyncio.as_completed(pending):
                    response = await next_answer
                    if response is None:
                        continue
                    if response.term > node.current_term:
                        await node.become_follower(response.term, None)
                        self.on_higher_term(response.term)
                        return False
                    if response.vote_granted and response.term == term:
                        votes += 1
                    if votes >= node.majority:
                        break
            finally:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

            if votes < node.majority:
                logger.info("%s lost or could not finish the election for term %d (%d votes)",
                            node.node_id, term, votes)
                return False
            if node.role != "candidate" or node.current_term != term:
                return False  # Stepped down while counting (another leader's heartbeat).
            try:
                await node.become_leader(term)
            except (ValueError, RuntimeError) as error:
                logger.warning("%s won term %d but could not lead: %s", node.node_id, term, error)
                return False
            self._leader_since = self.clock()
            logger.info("%s won the election for term %d with %d votes", node.node_id, term, votes)
            return True

    async def _ask(self, peer: str, request: RequestVoteRequest) -> RequestVoteResponse | None:
        try:
            return await self.transport.request_vote(peer, request)
        except PeerUnavailable as error:
            logger.debug("RequestVote to %s failed: %s", peer, error)
            return None

    # --- main loop ----------------------------------------------------------

    async def _run(self) -> None:
        tick = self.node.heartbeat_interval
        while not self._closed:
            try:
                if self.node.role == "leader":
                    await self._check_quorum()
                    await asyncio.sleep(tick)
                    continue
                remaining = self._deadline - self.clock()
                if remaining <= 0:
                    await self.run_election()
                    continue
                await asyncio.sleep(min(remaining, tick))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Election loop error on %s", self.node.node_id)
                await asyncio.sleep(tick)

    async def _check_quorum(self) -> None:
        """Step down if a majority has not acknowledged this leader recently."""
        node = self.node
        if not self.check_quorum or self._leader_since is None:
            return
        window, _ = self.election_timeout
        if self.clock() - self._leader_since < window:
            return  # Grace period: a new leader has not heard from anyone yet.
        if 1 + node.recent_contacts(window) >= node.majority:
            return
        term = node.current_term
        if await node.step_down(term):
            logger.warning("%s lost contact with a majority; stepping down in term %d", node.node_id, term)
            self._leader_since = None
            self.reset_election_timer()
