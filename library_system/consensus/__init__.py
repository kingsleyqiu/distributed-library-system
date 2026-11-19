"""Replicated catalog storage: durable log, replication, and majority commit."""

from library_system.consensus.leadership import PromotionRefused, StaticLeadership
from library_system.consensus.persistence import PersistentState, RaftStore
from library_system.consensus.replication import (
    CommitUnknownError,
    ElectionHooks,
    NotLeaderError,
    NotReadyError,
    RaftNode,
)
from library_system.consensus.state_machine import (
    CatalogStateMachine,
    CommandHandler,
    CommandOutcome,
    CommandRejected,
    EntryContext,
)
from library_system.consensus.transport import (
    HttpTransport,
    InMemoryTransport,
    PeerTransport,
    PeerUnavailable,
)

__all__ = [
    "CatalogStateMachine",
    "CommandHandler",
    "CommandOutcome",
    "CommandRejected",
    "CommitUnknownError",
    "ElectionHooks",
    "EntryContext",
    "HttpTransport",
    "InMemoryTransport",
    "NotLeaderError",
    "NotReadyError",
    "PeerTransport",
    "PeerUnavailable",
    "PersistentState",
    "PromotionRefused",
    "RaftNode",
    "RaftStore",
    "StaticLeadership",
]
