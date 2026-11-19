"""One replica of the replicated catalog storage cluster.

Serves the same read contract as the single-node stand-in
(``/internal/catalog`` and ``/internal/catalog/version``), so branches need no
changes. Any replica can be a branch's ``STORAGE_URL``: followers forward
reads and writes to the current leader, which answers from committed state.
"""

from collections.abc import Mapping
from contextlib import asynccontextmanager
import logging
import math
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
import httpx

from library_system.consensus import (
    CatalogStateMachine,
    CommitUnknownError,
    HttpTransport,
    NotLeaderError,
    NotReadyError,
    PromotionRefused,
    RaftNode,
    RaftStore,
    StaticLeadership,
)
from library_system.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    CatalogSnapshot,
    CatalogVersion,
    CommandEnvelope,
    CommandResult,
    NodeStatus,
    ReplaceCatalogRequest,
)
from library_system.storage import DEFAULT_BOOKS_DIR


DEFAULT_CLUSTER = (
    "storage-1=http://127.0.0.1:8101,"
    "storage-2=http://127.0.0.1:8102,"
    "storage-3=http://127.0.0.1:8103"
)
FORWARDED_HEADER = "x-raft-forwarded-by"


class _HideHeartbeats(logging.Filter):
    """Heartbeats arrive many times a second; keep them out of access logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "/internal/raft/append_entries" not in record.getMessage()


logging.getLogger("uvicorn.access").addFilter(_HideHeartbeats())


def parse_cluster(spec: str) -> dict[str, str]:
    """Parse ``id=url,id=url`` (every replica, including this one)."""
    cluster = {}
    for item in filter(None, (part.strip() for part in spec.split(","))):
        node_id, separator, url = item.partition("=")
        if not separator or not node_id.strip() or not url.strip():
            raise ValueError(f"Invalid RAFT_PEERS item {item!r}; expected id=url")
        cluster[node_id.strip()] = url.strip().rstrip("/")
    return cluster


def create_app(
    node_id: str = "storage-1",
    cluster: Mapping[str, str] | None = None,
    db_path: str | Path | None = None,
    initial_leader: str | None = "storage-1",
    heartbeat_interval: float = 0.1,
    rpc_timeout: float = 0.5,
    commit_timeout: float = 5.0,
    enable_test_writes: bool = False,
    state_machine: CatalogStateMachine | None = None,
    books_dir: str | Path = DEFAULT_BOOKS_DIR,
) -> FastAPI:
    cluster = dict(cluster if cluster is not None else parse_cluster(DEFAULT_CLUSTER))
    if node_id not in cluster:
        raise ValueError(f"RAFT_NODE_ID {node_id!r} is not listed in RAFT_PEERS")
    if initial_leader is not None and initial_leader not in cluster:
        raise ValueError(f"RAFT_INITIAL_LEADER {initial_leader!r} is not listed in RAFT_PEERS")
    for name, value in (
        ("heartbeat_interval", heartbeat_interval),
        ("rpc_timeout", rpc_timeout),
        ("commit_timeout", commit_timeout),
    ):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be a positive finite number")
    peer_urls = {peer: url for peer, url in cluster.items() if peer != node_id}
    machine = state_machine or CatalogStateMachine()
    db_path = db_path or f"var/replica-{node_id}.sqlite3"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        transport = HttpTransport(peer_urls, rpc_timeout)
        node = RaftNode(
            node_id,
            peer_urls,
            RaftStore(db_path, books_dir),
            transport,
            machine,
            heartbeat_interval=heartbeat_interval,
            commit_timeout=commit_timeout,
        )
        leadership = StaticLeadership(node, transport, initial_leader)
        await node.start()
        await leadership.start()
        app.state.node = node
        app.state.leadership = leadership
        async with httpx.AsyncClient(trust_env=False) as proxy:
            app.state.proxy = proxy
            try:
                yield
            finally:
                await leadership.stop()
                await node.stop()
                await transport.aclose()

    app = FastAPI(title=f"Library catalog replica {node_id}", lifespan=lifespan)

    async def forward(request: Request, timeout: float, body: dict | None = None) -> Response:
        """Relay a request to the leader (one hop only)."""
        node: RaftNode = app.state.node
        leader = node.leader_id
        if request.headers.get(FORWARDED_HEADER) or leader is None or leader not in peer_urls:
            raise HTTPException(503, f"No leader available (last known: {leader or 'none'})")
        try:
            relayed = await app.state.proxy.request(
                request.method,
                peer_urls[leader] + request.url.path,
                json=body,
                headers={FORWARDED_HEADER: node_id},
                timeout=timeout,
            )
        except httpx.HTTPError as error:
            raise HTTPException(503, f"Leader {leader} is unreachable: {error.__class__.__name__}")
        return Response(
            relayed.content,
            status_code=relayed.status_code,
            media_type=relayed.headers.get("content-type"),
        )

    async def read_or_forward(request: Request) -> CatalogSnapshot | Response:
        node: RaftNode = app.state.node
        if node.role == "leader":
            try:
                return node.read_snapshot()
            except NotReadyError as error:
                raise HTTPException(503, str(error))
            except NotLeaderError:
                pass
        return await forward(request, rpc_timeout)

    async def submit_or_forward(request: Request, body: dict, submit) -> CommandResult | Response:
        node: RaftNode = app.state.node
        if node.role == "leader":
            try:
                return await submit()
            except CommitUnknownError as error:
                raise HTTPException(503, f"Outcome unknown: {error}")
            except NotLeaderError:
                pass
        return await forward(request, commit_timeout + rpc_timeout, body)

    @app.get("/health")
    def health():
        node: RaftNode = app.state.node
        return {
            "status": "ok",
            "mode": "raft-replica",
            "node_id": node_id,
            "role": node.role,
            "term": node.current_term,
            "leader_id": node.leader_id,
            "commit_index": node.commit_index,
            "last_applied": node.last_applied,
        }

    @app.get("/internal/catalog/version", response_model=CatalogVersion)
    async def catalog_version(request: Request):
        result = await read_or_forward(request)
        return CatalogVersion(revision=result.revision) if isinstance(result, CatalogSnapshot) else result

    @app.get("/internal/catalog", response_model=CatalogSnapshot)
    async def catalog_snapshot(request: Request):
        return await read_or_forward(request)

    @app.post("/internal/commands", response_model=CommandResult)
    async def commands(command: CommandEnvelope, request: Request):
        if not machine.supports(command.operation):
            raise HTTPException(501, f"Operation {command.operation!r} is not implemented yet")
        node: RaftNode = app.state.node
        return await submit_or_forward(
            request,
            command.model_dump(mode="json"),
            lambda: node.submit("command", command=command),
        )

    @app.post("/internal/raft/append_entries", response_model=AppendEntriesResponse)
    async def append_entries(append: AppendEntriesRequest):
        try:
            return await app.state.node.handle_append_entries(append)
        except RuntimeError as error:
            raise HTTPException(409, str(error))

    @app.get("/internal/raft/status", response_model=NodeStatus)
    def raft_status():
        return app.state.node.status()

    @app.post("/internal/raft/promote", response_model=NodeStatus)
    async def promote():
        """Development failover until automatic leader election exists."""
        try:
            return await app.state.leadership.promote()
        except PromotionRefused as reason:
            raise HTTPException(409, str(reason))

    if enable_test_writes:

        @app.post("/internal/raft/test/replace_catalog", response_model=CommandResult)
        async def test_replace_catalog(replacement: ReplaceCatalogRequest, request: Request):
            """DEVELOPMENT ONLY: replicate a full catalog replacement."""
            node: RaftNode = app.state.node
            return await submit_or_forward(
                request,
                replacement.model_dump(mode="json"),
                lambda: node.submit("replace_catalog", books=replacement.books),
            )

    return app


def create_app_from_env() -> FastAPI:
    node_id = os.getenv("RAFT_NODE_ID", "storage-1")
    return create_app(
        node_id=node_id,
        cluster=parse_cluster(os.getenv("RAFT_PEERS", DEFAULT_CLUSTER)),
        db_path=os.getenv("RAFT_DB") or f"var/replica-{node_id}.sqlite3",
        initial_leader=os.getenv("RAFT_INITIAL_LEADER", "storage-1") or None,
        heartbeat_interval=float(os.getenv("RAFT_HEARTBEAT_SECONDS", "0.1")),
        rpc_timeout=float(os.getenv("RAFT_RPC_TIMEOUT_SECONDS", "0.5")),
        commit_timeout=float(os.getenv("RAFT_COMMIT_TIMEOUT_SECONDS", "5")),
        enable_test_writes=os.getenv("RAFT_ENABLE_TEST_WRITES") == "1",
    )


app = create_app_from_env()
