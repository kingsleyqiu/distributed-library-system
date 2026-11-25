"""Raft log replication, majority commit, and catch-up for one storage replica.

Leader election is deliberately *not* here. Leadership is decided outside this
class (``library_system.consensus.election.RaftElection``) and handed in through
``begin_candidacy()``, ``become_leader(term)``, ``become_follower(term, leader_id)``
and ``step_down(term)``. The node answers votes atomically in
``handle_request_vote`` and reports the events the election module needs
through ``ElectionHooks``.

Replication outline (Raft section 5.3):

* The leader runs one loop per follower. Each loop sends AppendEntries with
  the entries from ``next_index`` on (or none, as a heartbeat), every
  ``heartbeat_interval`` or as soon as new entries arrive.
* A follower accepts entries only if its log contains ``prev_log_index`` with
  ``prev_log_term``. Otherwise it replies with a conflict hint and the leader
  moves ``next_index`` back. This is also how a restarted or partitioned
  replica catches up: its log is reloaded from disk, the leader walks back to
  the point of divergence, the follower drops its uncommitted conflicting
  suffix, and the missing entries are streamed in batches.
* An entry is committed once it is stored on a majority *and* it belongs to
  the leader's current term (section 5.4.2). Earlier entries commit with it.
* Committed entries are applied in order to the catalog state machine, and
  each applied entry is persisted together with the resulting catalog.
"""

import asyncio
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
import logging
from time import monotonic
from typing import Protocol

from library_system.consensus.persistence import RaftStore
from library_system.consensus.state_machine import CatalogStateMachine
from library_system.consensus.transport import PeerTransport, PeerUnavailable
from library_system.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Book,
    CatalogSnapshot,
    CommandEnvelope,
    CommandResult,
    EntryKind,
    LogEntry,
    NodeStatus,
    PeerProgress,
    RaftRole,
    RequestVoteRequest,
    RequestVoteResponse,
)


logger = logging.getLogger(__name__)


class NotLeaderError(Exception):
    def __init__(self, leader_id: str | None):
        super().__init__(f"Not the leader (leader is {leader_id or 'unknown'})")
        self.leader_id = leader_id


class NotReadyError(Exception):
    """The leader cannot yet guarantee that its applied state is up to date."""


class CommitUnknownError(Exception):
    """The entry was appended but not confirmed committed. It may still commit
    later, so a client must retry with the same request ID, not assume failure."""


class ElectionHooks(Protocol):
    def on_valid_append_entries(self, term: int, leader_id: str) -> None:
        """A current leader contacted this node; reset the election timer."""

    def on_higher_term(self, term: int) -> None:
        """This node learned of a newer term and has become a follower."""


class _NoHooks:
    def on_valid_append_entries(self, term: int, leader_id: str) -> None:
        pass

    def on_higher_term(self, term: int) -> None:
        pass


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: Iterable[str],
        store: RaftStore,
        transport: PeerTransport,
        state_machine: CatalogStateMachine | None = None,
        *,
        heartbeat_interval: float = 0.1,
        commit_timeout: float = 5.0,
        batch_size: int = 100,
        read_lease: float | None = None,
        clock: Callable[[], float] = monotonic,
    ):
        self.node_id = node_id
        self.peer_ids = tuple(peer for peer in peer_ids if peer != node_id)
        self.store = store
        self.transport = transport
        self.state_machine = state_machine or CatalogStateMachine()
        self.hooks: ElectionHooks = _NoHooks()
        self.heartbeat_interval = heartbeat_interval
        self.commit_timeout = commit_timeout
        self.batch_size = batch_size
        # How recently a majority must have answered before the leader serves
        # reads; a partitioned ex-leader then stops serving instead of
        # serving data that may be stale.
        self.read_lease = read_lease if read_lease is not None else max(0.5, 5 * heartbeat_interval)
        self.clock = clock

        self.role: RaftRole = "follower"
        self.current_term = 0
        self.voted_for: str | None = None
        self.leader_id: str | None = None
        self.log: list[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.snapshot: CatalogSnapshot | None = None

        self._lock = asyncio.Lock()
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._last_ack: dict[str, float] = {}
        self._wake = {peer: asyncio.Event() for peer in self.peer_ids}
        self._tasks: set[asyncio.Task] = set()
        self._pending: dict[int, tuple[int, asyncio.Future]] = {}
        self._barrier_index = 0
        self._closed = False

    # --- lifecycle ---

    @property
    def cluster_size(self) -> int:
        return len(self.peer_ids) + 1

    @property
    def majority(self) -> int:
        return self.cluster_size // 2 + 1

    async def start(self) -> None:
        """Load durable state. The node always restarts as a follower."""
        await asyncio.to_thread(self.store.initialize)
        state = await asyncio.to_thread(self.store.load)
        self.current_term = state.current_term
        self.voted_for = state.voted_for
        self.log = list(state.log)
        self.last_applied = state.last_applied
        self.commit_index = state.last_applied
        self.snapshot = state.snapshot

    async def stop(self) -> None:
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._fail_pending("Replica is shutting down")

    # --- leadership hooks (driven by the election module) ------------------

    async def become_leader(self, term: int) -> None:
        """Take leadership for ``term``. The caller (the election module) must
        have won it with votes from a majority."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("Replica is stopped")
            if term < self.current_term or (term == self.current_term and self.role == "leader"):
                raise ValueError(f"Cannot lead term {term} from term {self.current_term} ({self.role})")
            if term == self.current_term and self.voted_for not in (None, self.node_id):
                raise ValueError(f"Already voted for {self.voted_for} in term {term}")
            await self._persist_term_vote(term, self.node_id)
            # Raise the read barrier before taking the role: the append below
            # awaits, and a read served in that gap must not see the old
            # barrier and answer from a possibly stale applied state.
            self._barrier_index = len(self.log) + 1
            self.role = "leader"
            self.leader_id = self.node_id
            last_index = len(self.log)
            self._next_index = {peer: last_index + 1 for peer in self.peer_ids}
            self._match_index = {peer: 0 for peer in self.peer_ids}
            self._last_ack = {}
            # A no-op from the new term lets earlier entries commit and acts
            # as the read barrier: once it is applied, so is everything committed.
            noop = self._new_entry("noop")
            await self._append_locked([noop])
            self._barrier_index = noop.index
            logger.info("%s became leader for term %d", self.node_id, term)
            for peer in self.peer_ids:
                task = asyncio.create_task(self._replicate_loop(peer, term))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
            await self._advance_commit_locked()

    async def become_follower(self, term: int, leader_id: str | None = None) -> None:
        async with self._lock:
            await self._step_down_locked(term, leader_id)

    async def set_term_vote(self, term: int, voted_for: str | None) -> None:
        """Durably record a term and vote (for the election module). Moving to
        a newer term also makes this node a follower."""
        async with self._lock:
            if term > self.current_term:
                await self._step_down_locked(term, None)
            await self._persist_term_vote(term, voted_for)

    def last_log_index_term(self) -> tuple[int, int]:
        return (len(self.log), self.log[-1].term) if self.log else (0, 0)

    # --- election support (driven by RaftElection) -------------------------

    async def begin_candidacy(self) -> int:
        """Start an election: move to the next term and durably vote for
        ourselves before any vote request is sent. Returns the new term."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("Replica is stopped")
            if self.role == "leader":
                raise ValueError(f"Already leader for term {self.current_term}")
            term = self.current_term + 1
            await self._persist_term_vote(term, self.node_id)
            self.role = "candidate"
            self.leader_id = None
            logger.info("%s is a candidate for term %d", self.node_id, term)
            return term

    async def step_down(self, term: int) -> bool:
        """Give up leadership of ``term`` (check-quorum). Does nothing if this
        node has meanwhile left that term or stopped leading. Returns True if
        it stepped down."""
        async with self._lock:
            if self.role != "leader" or self.current_term != term:
                return False
            await self._step_down_locked(term, None)
            return True

    async def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """Raft RequestVote (section 5.2 and 5.4.1), atomic with term changes
        from AppendEntries. The vote is on disk before the reply is sent."""
        async with self._lock:
            if request.term < self.current_term:
                return RequestVoteResponse(term=self.current_term, vote_granted=False)
            higher = request.term > self.current_term
            if higher:
                await self._step_down_locked(request.term, None)
            last_index, last_term = self.last_log_index_term()
            up_to_date = (request.last_log_term, request.last_log_index) >= (last_term, last_index)
            grant = up_to_date and self.voted_for in (None, request.candidate_id)
            if grant:
                await self._persist_term_vote(self.current_term, request.candidate_id)
            response = RequestVoteResponse(term=self.current_term, vote_granted=grant)
        if higher:
            self.hooks.on_higher_term(request.term)
        return response

    def recent_contacts(self, within_seconds: float) -> int:
        """How many peers acknowledged this leader within the last
        ``within_seconds`` (0 when not leader). Used for check-quorum."""
        if self.role != "leader":
            return 0
        now = self.clock()
        return sum(1 for at in self._last_ack.values() if now - at <= within_seconds)

    # --- client operations (leader only) ---

    async def submit(
        self,
        kind: EntryKind,
        *,
        command: CommandEnvelope | None = None,
        books: tuple[Book, ...] | None = None,
    ) -> CommandResult:
        """Append an entry and wait until it is committed and applied."""
        async with self._lock:
            if self.role != "leader":
                raise NotLeaderError(self.leader_id)
            entry = self._new_entry(kind, command=command, books=books)
            await self._append_locked([entry])
            future = asyncio.get_running_loop().create_future()
            self._pending[entry.index] = (entry.term, future)
            self._wake_all()
            await self._advance_commit_locked()
        try:
            return await asyncio.wait_for(asyncio.shield(future), self.commit_timeout)
        except TimeoutError:
            if self._pending.get(entry.index, (None, None))[1] is future:
                del self._pending[entry.index]
            raise CommitUnknownError(
                f"Entry {entry.index} was not committed within {self.commit_timeout}s; "
                "a majority of replicas may be unavailable"
            ) from None

    def read_snapshot(self) -> CatalogSnapshot:
        """The committed catalog, as served by the leader."""
        if self.role != "leader":
            raise NotLeaderError(self.leader_id)
        if not self.ready_for_reads:
            raise NotReadyError("Leader has not confirmed its commit point with a majority")
        assert self.snapshot is not None
        return self.snapshot

    @property
    def ready_for_reads(self) -> bool:
        if self.role != "leader" or self.last_applied < self._barrier_index:
            return False
        now = self.clock()
        recent = sum(1 for at in self._last_ack.values() if now - at <= self.read_lease)
        return 1 + recent >= self.majority

    def status(self) -> NodeStatus:
        last_index, last_term = self.last_log_index_term()
        now = self.clock()
        peers = {}
        if self.role == "leader":
            peers = {
                peer: PeerProgress(
                    next_index=self._next_index[peer],
                    match_index=self._match_index[peer],
                    lag=last_index - self._match_index[peer],
                    last_contact_seconds=(
                        round(now - self._last_ack[peer], 3) if peer in self._last_ack else None
                    ),
                )
                for peer in self.peer_ids
            }
        return NodeStatus(
            node_id=self.node_id,
            role=self.role,
            term=self.current_term,
            voted_for=self.voted_for,
            leader_id=self.leader_id,
            commit_index=self.commit_index,
            last_applied=self.last_applied,
            last_log_index=last_index,
            last_log_term=last_term,
            revision=self.snapshot.revision if self.snapshot else 0,
            ready_for_reads=self.ready_for_reads,
            peers=peers,
        )

    # --- follower RPC handler ---

    async def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        async with self._lock:
            if request.term < self.current_term:
                return AppendEntriesResponse(term=self.current_term, success=False)
            if request.term > self.current_term or self.role != "follower":
                if self.role == "leader" and request.term == self.current_term:
                    logger.error("Two leaders in term %d (%s and %s)", request.term, self.node_id, request.leader_id)
                higher = request.term > self.current_term
                await self._step_down_locked(request.term, request.leader_id)
                if higher:
                    self.hooks.on_higher_term(request.term)
            self.leader_id = request.leader_id
            self.hooks.on_valid_append_entries(request.term, request.leader_id)

            # Consistency check: our log must contain prev_log_index/prev_log_term.
            last_index = len(self.log)
            if request.prev_log_index > last_index:
                return AppendEntriesResponse(
                    term=self.current_term, success=False, conflict_index=last_index + 1
                )
            if request.prev_log_index > 0:
                local_term = self.log[request.prev_log_index - 1].term
                if local_term != request.prev_log_term:
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        conflict_term=local_term,
                        conflict_index=self._first_index_of_term(local_term, request.prev_log_index),
                    )

            # Skip entries we already have; truncate only at a real conflict, so
            # a duplicated or delayed request never erases newer entries.
            new_entries: list[LogEntry] = []
            truncate_from: int | None = None
            for position, entry in enumerate(request.entries):
                if entry.index <= len(self.log):
                    if self.log[entry.index - 1].term == entry.term:
                        continue
                    truncate_from = entry.index
                new_entries = list(request.entries[position:])
                break
            if truncate_from is not None:
                if truncate_from <= self.commit_index:
                    raise RuntimeError(f"Leader {request.leader_id} tried to overwrite committed entry {truncate_from}")
                await asyncio.to_thread(self.store.replace_suffix, truncate_from, new_entries)
                self.log[truncate_from - 1 :] = new_entries
            elif new_entries:
                await self._append_locked(new_entries)

            last_new_index = request.prev_log_index + len(request.entries)
            if request.leader_commit > self.commit_index:
                self.commit_index = max(self.commit_index, min(request.leader_commit, last_new_index))
                await self._apply_locked()
            return AppendEntriesResponse(term=self.current_term, success=True, match_index=last_new_index)

    # --- leader replication ------------------------------------------------

    async def _replicate_loop(self, peer: str, term: int) -> None:
        wake = self._wake[peer]
        while not self._closed and self.role == "leader" and self.current_term == term:
            wake.clear()
            try:
                more = await self._replicate_once(peer, term)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Replication to %s failed unexpectedly", peer)
                more = False
            if more:
                continue
            try:
                await asyncio.wait_for(wake.wait(), self.heartbeat_interval)
            except TimeoutError:
                pass

    async def _replicate_once(self, peer: str, term: int) -> bool:
        """Send one AppendEntries; return True if the peer needs more right away."""
        async with self._lock:
            if self.role != "leader" or self.current_term != term:
                return False
            next_index = self._next_index[peer]
            prev_index = next_index - 1
            request = AppendEntriesRequest(
                term=term,
                leader_id=self.node_id,
                prev_log_index=prev_index,
                prev_log_term=self.log[prev_index - 1].term if prev_index else 0,
                entries=tuple(self.log[prev_index : prev_index + self.batch_size]),
                leader_commit=self.commit_index,
            )
        try:
            response = await self.transport.append_entries(peer, request)
        except PeerUnavailable as error:
            logger.debug("AppendEntries to %s failed: %s", peer, error)
            return False

        async with self._lock:
            if response.term > self.current_term:
                await self._step_down_locked(response.term, None)
                self.hooks.on_higher_term(response.term)
                return False
            if self.role != "leader" or self.current_term != term:
                return False
            self._last_ack[peer] = self.clock()
            if response.success:
                self._match_index[peer] = max(self._match_index[peer], response.match_index)
                self._next_index[peer] = max(self._next_index[peer], self._match_index[peer] + 1)
                await self._advance_commit_locked()
                return self._next_index[peer] <= len(self.log)
            # Consistency check failed: back up using the follower's hint.
            if response.conflict_term is not None:
                last_of_term = self._last_index_of_term(response.conflict_term, prev_index)
                target = last_of_term + 1 if last_of_term else response.conflict_index or next_index - 1
            elif response.conflict_index is not None:
                target = response.conflict_index
            else:
                target = next_index - 1
            self._next_index[peer] = max(1, self._match_index[peer] + 1, min(target, next_index - 1))
            return True

    async def _advance_commit_locked(self) -> None:
        """Commit the highest current-term index stored on a majority."""
        for index in range(len(self.log), self.commit_index, -1):
            if self.log[index - 1].term != self.current_term:
                break  # Terms only decrease going back; older entries commit indirectly.
            replicas = 1 + sum(1 for peer in self.peer_ids if self._match_index.get(peer, 0) >= index)
            if replicas >= self.majority:
                self.commit_index = index
                for peer in self.peer_ids:  # Tell followers promptly.
                    self._wake[peer].set()
                break
        await self._apply_locked()

    async def _apply_locked(self) -> None:
        while self.last_applied < self.commit_index:
            entry = self.log[self.last_applied]
            assert self.snapshot is not None
            snapshot, result = await asyncio.to_thread(
                self.store.apply_committed, entry, self.snapshot, self.state_machine,
            )
            self.snapshot = snapshot
            self.last_applied = entry.index
            pending = self._pending.pop(entry.index, None)
            if pending is not None and not pending[1].done():
                if pending[0] == entry.term:
                    pending[1].set_result(result)
                else:  # A different leader's entry replaced ours at this index.
                    pending[1].set_exception(CommitUnknownError("Entry was superseded by a new leader"))

    # --- helpers ---

    async def _step_down_locked(self, term: int, leader_id: str | None) -> None:
        was_leader = self.role == "leader"
        if term > self.current_term:
            await self._persist_term_vote(term, None)
        self.role = "follower"
        self.leader_id = leader_id
        if was_leader:
            logger.info("%s stepped down in term %d", self.node_id, self.current_term)
            self._wake_all()
            self._fail_pending("Leadership was lost before the entry was confirmed committed")

    async def _persist_term_vote(self, term: int, voted_for: str | None) -> None:
        if (term, voted_for) != (self.current_term, self.voted_for):
            await asyncio.to_thread(self.store.save_term_vote, term, voted_for)
            self.current_term, self.voted_for = term, voted_for

    async def _append_locked(self, entries: list[LogEntry]) -> None:
        await asyncio.to_thread(self.store.append, entries)
        self.log.extend(entries)

    def _new_entry(
        self,
        kind: EntryKind,
        *,
        command: CommandEnvelope | None = None,
        books: tuple[Book, ...] | None = None,
    ) -> LogEntry:
        return LogEntry(
            index=len(self.log) + 1,
            term=self.current_term,
            kind=kind,
            timestamp=datetime.now(timezone.utc),
            command=command,
            books=books,
        )

    def _wake_all(self) -> None:
        for event in self._wake.values():
            event.set()

    def _fail_pending(self, reason: str) -> None:
        pending, self._pending = self._pending, {}
        for _, future in pending.values():
            if not future.done():
                future.set_exception(CommitUnknownError(reason))

    def _first_index_of_term(self, term: int, upto: int) -> int:
        index = upto
        while index > 1 and self.log[index - 2].term == term:
            index -= 1
        return index

    def _last_index_of_term(self, term: int, upto: int) -> int:
        for index in range(min(upto, len(self.log)), 0, -1):
            entry_term = self.log[index - 1].term
            if entry_term == term:
                return index
            if entry_term < term:
                break
        return 0
