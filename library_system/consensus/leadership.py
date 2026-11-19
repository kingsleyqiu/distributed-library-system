"""Temporary leader selection until the election module (Aryan) replaces it.

This is NOT leader election: there are no election timeouts and no
RequestVote RPC. A configured replica claims leadership of a fresh cluster,
and an operator can ``promote`` a replica after the leader fails. Both paths
check, like a Raft vote would, that a majority of replicas is reachable and
holds a log no more up to date than the candidate's, so a promoted leader
already has every committed entry.

Votes are not recorded on the peers, so two *simultaneous* promotions could
pick the same term. Promote one replica at a time; real elections fix this.
"""

import asyncio
import logging

from library_system.consensus.replication import RaftNode
from library_system.consensus.transport import PeerTransport
from library_system.models import NodeStatus


logger = logging.getLogger(__name__)


class PromotionRefused(Exception):
    pass


class StaticLeadership:
    def __init__(
        self,
        node: RaftNode,
        transport: PeerTransport,
        initial_leader: str | None,
        retry_interval: float = 0.25,
    ):
        self.node = node
        self.transport = transport
        self.initial_leader = initial_leader
        self.retry_interval = retry_interval
        self._task: asyncio.Task | None = None
        node.hooks = self

    # ElectionHooks: nothing to do without election timers.
    def on_valid_append_entries(self, term: int, leader_id: str) -> None:
        pass

    def on_higher_term(self, term: int) -> None:
        pass

    async def start(self) -> None:
        if self.node.node_id == self.initial_leader:
            self._task = asyncio.create_task(self._claim_initial_leadership())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _claim_initial_leadership(self) -> None:
        """Lead once a majority is reachable, unless a leader already exists
        (for example, this replica is restarting after a failover)."""
        while self.node.role != "leader" and self.node.leader_id is None:
            try:
                await self.promote(defer_to_existing_leader=True)
            except PromotionRefused as reason:
                logger.debug("Initial leadership deferred: %s", reason)
                await asyncio.sleep(self.retry_interval)

    async def promote(self, defer_to_existing_leader: bool = False) -> NodeStatus:
        node = self.node
        if node.role == "leader":
            return node.status()
        answers = await asyncio.gather(
            *(self.transport.status(peer) for peer in node.peer_ids), return_exceptions=True
        )
        reachable = [answer for answer in answers if isinstance(answer, NodeStatus)]
        if 1 + len(reachable) < node.majority:
            raise PromotionRefused(
                f"Only {1 + len(reachable)} of {node.cluster_size} replicas reachable; need {node.majority}"
            )
        if defer_to_existing_leader and any(
            peer.role == "leader" or peer.leader_id is not None for peer in reachable
        ):
            raise PromotionRefused("Another replica already knows a leader")
        last_index, last_term = node.last_log_index_term()
        not_ahead = 1 + sum(
            1 for peer in reachable if (peer.last_log_term, peer.last_log_index) <= (last_term, last_index)
        )
        if not_ahead < node.majority:
            raise PromotionRefused("A majority of replicas has a more up-to-date log")
        term = max([node.current_term, *(peer.term for peer in reachable)]) + 1
        await node.become_leader(term)
        return node.status()
