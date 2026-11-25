"""How replicas reach each other: HTTP in production, in-memory for tests."""

import asyncio

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Protocol

import httpx

from library_system.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    NodeStatus,
    RequestVoteRequest,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    from library_system.consensus.replication import RaftNode


class PeerUnavailable(Exception):
    """The peer could not be reached or did not answer in time."""


class PeerTransport(Protocol):
    async def append_entries(self, peer_id: str, request: AppendEntriesRequest) -> AppendEntriesResponse: ...

    async def request_vote(self, peer_id: str, request: RequestVoteRequest) -> RequestVoteResponse: ...

    async def status(self, peer_id: str) -> NodeStatus: ...


class HttpTransport:
    def __init__(self, peer_urls: Mapping[str, str], timeout: float):
        self.peer_urls = dict(peer_urls)
        self._client = httpx.AsyncClient(timeout=timeout, trust_env=False)

    async def _request(self, method: str, peer_id: str, path: str, **kwargs) -> dict:
        try:
            response = await self._client.request(method, self.peer_urls[peer_id] + path, **kwargs)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise PeerUnavailable(f"{peer_id}: {error}") from error

    async def append_entries(self, peer_id: str, request: AppendEntriesRequest) -> AppendEntriesResponse:
        data = await self._request(
            "POST", peer_id, "/internal/raft/append_entries", json=request.model_dump(mode="json")
        )
        return AppendEntriesResponse.model_validate(data)

    async def request_vote(self, peer_id: str, request: RequestVoteRequest) -> RequestVoteResponse:
        data = await self._request(
            "POST", peer_id, "/internal/raft/request_vote", json=request.model_dump(mode="json")
        )
        return RequestVoteResponse.model_validate(data)

    async def status(self, peer_id: str) -> NodeStatus:
        return NodeStatus.model_validate(await self._request("GET", peer_id, "/internal/raft/status"))

    async def aclose(self) -> None:
        await self._client.aclose()


DropRule = Callable[[str, str, AppendEntriesRequest], bool]
VoteDropRule = Callable[[str, str, RequestVoteRequest], bool]


class InMemoryTransport:
    """Delivers RPCs by calling other in-process nodes directly.

    ``isolate(node_id)`` cuts a node off in both directions (a partition or a
    crash), and ``drop_rule(source, target, request)`` can discard selected
    AppendEntries messages to build precise test scenarios.
    ``vote_drop_rule`` does the same for RequestVote, and ``voters`` maps a
    node ID to the object that answers its votes (the election module).
    """

    def __init__(self):
        self.nodes: dict[str, RaftNode] = {}
        self.isolated: set[str] = set()
        self.drop_rule: DropRule | None = None
        self.vote_drop_rule: VoteDropRule | None = None
        self.voters: dict[str, object] = {}

    def register(self, node: "RaftNode") -> None:
        self.nodes[node.node_id] = node

    def unregister(self, node_id: str) -> None:
        self.nodes.pop(node_id, None)
        self.voters.pop(node_id, None)

    def register_voter(self, node_id: str, voter) -> None:
        """Route RequestVote for ``node_id`` to ``voter.handle_request_vote``
        (the election module); without one, the RaftNode answers directly."""
        self.voters[node_id] = voter

    def isolate(self, *node_ids: str) -> None:
        self.isolated.update(node_ids)

    def heal(self, *node_ids: str) -> None:
        self.isolated.difference_update(node_ids or set(self.isolated))

    def _target(self, source: str | None, peer_id: str) -> "RaftNode":
        node = self.nodes.get(peer_id)
        if node is None or peer_id in self.isolated or source in self.isolated:
            raise PeerUnavailable(f"{peer_id} is unreachable")
        return node

    async def append_entries(self, peer_id: str, request: AppendEntriesRequest) -> AppendEntriesResponse:
        node = self._target(request.leader_id, peer_id)
        if self.drop_rule is not None and self.drop_rule(request.leader_id, peer_id, request):
            raise PeerUnavailable(f"message to {peer_id} dropped")
        # Round-trip through JSON so tests exercise the same validation as HTTP.
        wire = AppendEntriesRequest.model_validate_json(request.model_dump_json())
        # Shielded: over HTTP the receiver finishes handling a request even if
        # the sender gives up (crashes or is cancelled). Without the shield, a
        # cancelled sender would cancel the receiver mid-write in this process.
        return await asyncio.shield(node.handle_append_entries(wire))

    async def request_vote(self, peer_id: str, request: RequestVoteRequest) -> RequestVoteResponse:
        node = self._target(request.candidate_id, peer_id)
        if self.vote_drop_rule is not None and self.vote_drop_rule(request.candidate_id, peer_id, request):
            raise PeerUnavailable(f"vote request to {peer_id} dropped")
        wire = RequestVoteRequest.model_validate_json(request.model_dump_json())
        voter = self.voters.get(peer_id, node)
        return await asyncio.shield(voter.handle_request_vote(wire))

    async def status(self, peer_id: str) -> NodeStatus:
        return self._target(None, peer_id).status()
