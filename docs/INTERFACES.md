# Shared interfaces

This phase contains three branch APIs and two interchangeable storage backends:

- the **single-node storage stand-in** (`library_system/storage.py`), with no
  replication, quorum, or failover; and
- the **replicated storage cluster** (`library_system/replica.py` and
  `library_system/consensus/`): three replicas with a durable replicated log,
  majority commit, catch-up for rejoining replicas, and automatic leader
  election; see [Replicated storage](#replicated-storage).

The runnable contract is defined in `library_system/models.py` and in each
service's `/docs` and `/openapi.json`. All example data is synthetic. Services
have no authentication yet and are intended for local development only.

## Process layout

| Service | Local URL | Role |
| --- | --- | --- |
| branch-1 | `http://127.0.0.1:8001` | Branch API and private memory cache |
| branch-2 | `http://127.0.0.1:8002` | Branch API and private memory cache |
| branch-3 | `http://127.0.0.1:8003` | Branch API and private memory cache |
| storage | `http://127.0.0.1:8100` | Durable single-node catalog stand-in |

Compose uses the same host URLs; inside its network the storage URL is
`http://storage:8100`. Branch containers listen on port 8000. Published ports
bind to `127.0.0.1`, and the native runner also binds only to loopback.

## Catalog data

| Field | Meaning |
| --- | --- |
| `book_id` | Stable identifier for a title, distinct from its physical copies |
| `title`, `author` | Display and search text |
| `copies` | The physical copies belonging to this title |
| `copy_id` | Globally unique identifier for one physical copy |
| `branch_id` | Current copy location: `branch-1`, `branch-2`, or `branch-3` |
| `available` | Whether the copy is currently available to borrow |
| `due_date` | ISO date or `null`; an available copy has no due date |
| `version` | Nonnegative copy version, incremented on borrow, return, or renew |
| `borrower_id` | Current patron ID or `null`; legacy snapshots default to `null` |
| `renewals` | Renewals on the current loan; defaults to zero and resets on return |
| `holds` | Title-level FIFO array of `{hold_id, actor_id, created_at}`; defaults to empty |
| `revision` | Nonnegative, durable, monotonically increasing catalog revision |

Snapshot example:

```json
{
  "revision": 1,
  "books": [
    {
      "book_id": "book-001",
      "title": "Example Book",
      "author": "Example Author",
      "copies": [
        {
          "copy_id": "copy-001",
          "branch_id": "branch-1",
          "available": true,
          "due_date": null,
          "version": 0
        }
      ]
    }
  ]
}
```

Book IDs must be unique in a snapshot, and copy IDs must be unique across the
whole snapshot. Branches validate snapshots before installing them atomically.
The catalog revision and each copy's version serve different purposes: the
revision detects catalog changes; a copy version checks concurrent writes on
that specific copy. Hold changes advance the catalog revision without changing
copy versions. Older catalogs without the new circulation fields still load.

## Storage read endpoints

| Method and path | Response |
| --- | --- |
| `GET /health` | Identifies the service as `single-node-stand-in` |
| `GET /internal/catalog/version` | `{"revision": 1}` |
| `GET /internal/catalog` | One atomic `CatalogSnapshot` |

Every catalog change must advance the revision in the same transaction as the
data change. Restarting storage preserves the revision and data. The version
endpoint is a cheap change check; a subsequent snapshot may already have a
newer revision and is valid. It must not be older than the version just read.

The initial catalog is imported from 20 files in `data/books/` only when a new
SQLite database is created. Each file has a JSON metadata object on its first
line, then a blank line and original sample text. Metadata contains `book_id`,
`title`, `author`, `branch_id`, and a positive integer `copies` count. Editing
seed files does not overwrite an existing database. This phase has no staff
catalog editing endpoint; future staff operations must go through the agreed
write path, not direct database edits.

## Branch endpoints

| Method and path | Behavior |
| --- | --- |
| `GET /health` | Reports `ready`, `degraded`, or `unavailable` cache/synchronization state |
| `GET /books` | Searches the branch's complete cached catalog |
| `GET /books/{book_id}` | Returns one title and all its copies; 404 if absent |

Search parameters are optional and combined:

- `q`: trimmed, case-insensitive substring of title **or** author.
- `branch_id`: include copies at that branch only; it does not mean the API
  server's own identity.
- `available`: `true` for available copies, `false` for unavailable copies.

Copy filters apply to the **same copy**. When a copy filter is provided, a title
is included only if it has a matching copy, and the search response contains
only matching copies. With no copy filters, titles with no remaining copies
can also be returned. The detail endpoint returns all copies. `total` counts
titles, not copies.

Search returns `{"books": [...], "total": 20, "cache": {...}}`; detail returns
`{"book": {...}, "cache": {...}}`. Every successful response includes:

```json
{
  "branch_id": "branch-1",
  "revision": 1,
  "last_synced_at": "2026-11-18T15:00:00Z",
  "stale": false
}
```

`last_synced_at` is the last successful check against storage, including a
version check that found no changes. `stale` becomes true after a failed check
or when the last successful check is older than twice the polling interval
plus the storage timeout (three seconds with default settings). False does
not mean that the data is strongly consistent with storage at the instant
of the search.

Likewise, the health response's `storage_available` field reports the result of
the last synchronization check, not a guarantee of current connectivity.

## Cache lifecycle and failures

1. Fetch and validate the full catalog at startup.
2. Every second, check the storage revision; fetch and atomically install a
   snapshot when it advances.
3. On storage failure, keep the last valid snapshot and serve it with
   `stale: true`. A failed check must not erase the cache or return an empty
   catalog as a substitute for an unavailable catalog.
4. On recovery, synchronize again. Reject invalid snapshots or revisions that
   move backwards, keeping the last valid cache and indicating a failed check.

Each branch keeps only an in-memory cache. After a branch restart it must fetch
the catalog again. With no successful initial fetch, searches and detail
requests return HTTP 503 until storage is available. A warm, disconnected
branch can serve old searches; this does not authorize offline writes.

This first phase uses **version polling**, explicitly accepted for development.
It does not implement the proposal's active numbered update notifications.
Normal cached searches can lag behind storage, and an outage can keep them old
for longer. Availability shown in search is advisory; lending checks and commits against
authoritative storage. `available` describes whether a copy is on loan; a hold
queue may still restrict who can borrow an available copy.

## Circulation commands

`POST /commands` on a branch forwards to `POST /internal/commands` on storage.
Both the stand-in and the replica service register the same deterministic
handlers from `library_system/circulation.py`.

```json
{
  "request_id": "client-generated-unique-request-id",
  "operation": "borrow",
  "actor_id": "patron-demo-id",
  "book_id": "book-001",
  "copy_id": "book-001-copy-1",
  "expected_version": 0
}
```

`request_id`, `actor_id`, and `book_id` are required, nonblank strings of at
most 128 characters. Borrow, return, and renew also require a `copy_id` belonging
to the title and an integer `expected_version >= 0` (booleans and strings are
rejected). Hold commands apply to a title and must omit copy and version fields.
`add_book` and `remove_book` remain reserved and return HTTP 501.

### Business rules

- **Borrow:** the requested copy must be available, its version must match, and
  the patron must not already have a copy of the title on loan. The patron at
  the front of the hold queue has exclusive priority while the queue is
  nonempty. Borrowing consumes that patron's hold, records their ID, sets the
  due date to the storage-assigned UTC date plus **14 days**, and increments the
  copy version once.
- **Return:** the version and current borrower must match. Clears the borrower,
  due date, and renewal count; marks the copy available and increments its
  version. Holds remain queued. A return does not automatically create a loan.
- **Renew:** the version and borrower must match, no holds may be pending, and
  the loan must not be overdue (a renewal on its due date is allowed). At most
  **two renewals** per loan, each adding **14 days** to its existing due date
  and incrementing its version.
- **Hold:** appends one hold per patron per title in committed command order.
  The title must have at least one copy, and a current borrower cannot also
  hold that title. Holds may be placed even if a copy is available. `hold_id`
  equals the request ID; `created_at` uses the storage-assigned timestamp. The
  result includes the one-based queue position at placement time. Holds do not
  currently expire or support cancellation.

Copy versions are checked atomically with mutation, so two different requests
using the same version cannot both change the copy. Every successful circulation
command advances the catalog revision once; rejected commands and exact retries
leave it unchanged. Queue order is global across all three branches.

### Results and errors

HTTP 200 means storage has a committed result, which can be a success or a
business refusal. Always inspect `ok`:

```json
{
  "index": 2,
  "term": 1,
  "revision": 2,
  "ok": true,
  "request_id": "client-generated-unique-request-id",
  "result": {
    "operation": "borrow",
    "book_id": "book-001",
    "copy": {
      "copy_id": "book-001-copy-1",
      "branch_id": "branch-1",
      "available": false,
      "due_date": "2026-12-08",
      "version": 1,
      "borrower_id": "patron-demo-id",
      "renewals": 0
    }
  },
  "error": null
}
```

The single-node backend uses a local request sequence for `index` and `term: 1`;
replicated mode uses the original committed log index and term. A hold result
contains `operation`, `book_id`, `hold`, and `position` instead of `copy`.

Business refusals return `ok: false`, `result: {}`, and a stable error string:

| Error | Meaning |
| --- | --- |
| `book_not_found`, `copy_not_found` | The requested title or copy in that title does not exist |
| `version_conflict` | Read the latest copy version and submit a new command with a new request ID |
| `copy_unavailable`, `copy_not_borrowed` | Copy state does not permit the requested operation |
| `not_borrower` | Only the recorded borrower may return or renew |
| `already_borrowed` | Patron already has a copy of this title on loan |
| `already_on_hold` | Patron already has a pending hold for this title |
| `hold_queue_priority` | Another patron is first in the queue |
| `holds_pending` | A waiting patron prevents renewal |
| `renewal_limit`, `loan_overdue` | The loan cannot be renewed |
| `no_copies` | The title has no copies to hold |
| `request_id_conflict` | This ID was already used with a different payload |

Malformed input returns HTTP 422 before mutation. Unsupported staff operations
return 501. A stale/disconnected branch returns 503 without making an offline
change. After forwarding, a timeout, malformed response, or 503 can mean the
outcome is unknown; retry with the same request ID and payload.

### Request IDs and durability

Clients generate a unique ID for each logical command (a UUID is suitable).
The first committed outcome, including a business refusal, and its complete
payload are saved in SQLite in the **same transaction** as the catalog and,
on replicas, `last_applied`. Exact retries return the original `CommandResult`,
including its original revision/index/term, even if the catalog has since
changed. Reusing an ID with a changed operation, actor, title, copy, or version
returns `request_id_conflict` without replacing the original outcome.

Every replica deduplicates while applying the committed log. An unconfirmed
entry and its retry may both enter the log, but change circulation state only
once. Request results survive restarts, catch-up, and leader changes. Request
history is retained without pruning at this demo's scale. HTTP validation,
unsupported-operation, and pre-forwarding failures do not reserve an ID.

Branches validate storage responses and refresh from storage after receiving a
result. A failed cache refresh marks the cache stale but preserves a confirmed
command result. Branches never update a cached copy optimistically.

Authentication is still pending: `actor_id` is a claimed identity supplied by
the client. Borrower checks enforce loan ownership against that value; they do
not authenticate it. The future login layer must bind it to a verified patron.
Catalog responses currently include borrower IDs and hold queues for this local
demo; access control belongs to that integration.

## Replicated storage

Three replicas (`storage-1` to `storage-3`) each keep a durable copy of the
log and the catalog. They implement Raft log replication and majority commit,
and elect their leader automatically (see
[Leader election](#leader-election)).

| Service | Local URL (`run_local.py --replicated`) |
| --- | --- |
| storage-1 | `http://127.0.0.1:8101` (favoured in the first election) |
| storage-2 | `http://127.0.0.1:8102` |
| storage-3 | `http://127.0.0.1:8103` |

### Read contract (unchanged for branches)

Every replica serves `GET /internal/catalog` and `GET /internal/catalog/version`
with the same schemas as the stand-in. **Branch code is unchanged.** Each branch
uses one replica as its `STORAGE_URL` (branch-N uses storage-N):

- The **leader** answers from its committed, applied catalog. It answers only
  after the no-op entry of its own term is applied (so it has applied
  everything committed before it took over), and only while a majority has
  answered it within the read lease (default 0.5 s). Otherwise it returns 503.
- A **follower** forwards the request one hop to the leader it knows. If it
  knows no leader, or that leader is unreachable, it returns 503. The branch
  keeps its cache and marks it stale, as for any storage outage.

`revision` advances by exactly one for each committed entry that changes the
catalog. Every replica applies the same log, so the revision is identical on
all replicas and never moves backwards across leader changes.

### Write path

`POST /internal/commands` (any replica; followers forward to the leader):

1. If no handler is registered for the operation: **501**, and nothing is appended.
2. The leader appends the entry with its term and a leader-assigned timestamp,
   and replicates it.
3. Once the entry is stored on a majority (2 of 3) and committed, it is applied
   on every replica. The response is a `CommandResult` `{index, term, revision,
   ok, result, error, request_id}`. `ok: false` means the handler deterministically
   rejected the command; the catalog is unchanged.
4. If commit is not confirmed within `RAFT_COMMIT_TIMEOUT_SECONDS` (default 5),
   or the leader loses leadership first: **503, outcome unknown**. The entry
   may still commit later, so clients must retry with the **same**
   `request_id`. The transactional apply layer deduplicates commands before
   invoking their handlers.

`POST /internal/raft/test/replace_catalog` `{"books": [...]}` replicates a
whole-catalog replacement. It exists only when `RAFT_ENABLE_TEST_WRITES=1`
and is for tests and demos only, not a staff editing API.

### Durability and catch-up

Each replica stores its term and vote, its log, and its applied catalog in one
SQLite file (`RAFT_DB`, WAL mode, `synchronous=FULL`). Each change is on disk
before the replica acknowledges it. A replica restarts as a follower from
that file. The leader then finds where the two logs diverge, the replica
discards any uncommitted conflicting entries, and the leader sends the missing
entries in batches. An entry that has been applied is never truncated.

### Internal Raft endpoints

| Method and path | Purpose |
| --- | --- |
| `POST /internal/raft/append_entries` | Replication and heartbeat RPC (`AppendEntriesRequest` → `AppendEntriesResponse`) |
| `POST /internal/raft/request_vote` | Election RPC (`RequestVoteRequest` → `RequestVoteResponse`) |
| `GET /internal/raft/status` | `NodeStatus`: role, term, leader, commit/applied index, revision, and, on the leader, per-follower `match_index`, `lag`, and last contact. Intended for the dashboard. |
| `POST /internal/raft/promote` | Demo: start an election on this replica now (a real vote); `NodeStatus` if it wins, 409 if not |
| `GET /health` | `{"status": "ok", "mode": "raft-replica", node_id, role, term, leader_id, ...}` |

### Leader election

`RaftElection` (`library_system/consensus/election.py`) elects leaders
(Raft section 5.2); `RaftNode` handles replication and does not decide who
leads.

- **Failure detection.** The leader's AppendEntries every
  `RAFT_HEARTBEAT_SECONDS` are the heartbeats. Each follower keeps a random
  election timeout between `RAFT_ELECTION_TIMEOUT_MIN_SECONDS` and `_MAX_`
  (default 1.5-3.0 s), reset by every valid heartbeat and every vote it grants.
- **Election.** When the timeout passes, the follower becomes a candidate:
  it moves to the next term, durably votes for itself, and asks both peers for
  a vote in parallel. A replica grants at most one vote per term (saved to disk
  before it answers), and only to a candidate whose log is at least as up to
  date as its own. With 2 of 3 votes the candidate becomes leader, appends the
  term's no-op entry, and starts heartbeats. A split or lost election is retried
  in the next term after a new random timeout.
- **Terms.** Any message with a higher term makes a replica a follower in that
  term, so an old leader that returns steps down and cannot commit.
- **Check-quorum.** A leader that has not heard from a majority (counting
  itself) within one minimum election timeout steps down, so a cut-off leader
  does not keep claiming leadership.
- **Timing rule.** The minimum election timeout must exceed the read lease
  plus `RAFT_RPC_TIMEOUT_SECONDS`, so an old leader stops serving reads before
  a new one can be elected. `create_app` refuses configurations that break it.
- `RAFT_INITIAL_LEADER` only shortens that replica's *first* timeout, so a
  fresh cluster usually elects it; it is still a normal vote.

Interfaces between the two modules: `RaftElection` calls
`node.begin_candidacy()`, `node.become_leader(term)`,
`node.become_follower(term, leader_id)`, `node.step_down(term)`,
`node.handle_request_vote(request)` (atomic under the node's lock), and
`node.recent_contacts(seconds)`. `RaftNode` calls back through
`ElectionHooks`: `on_valid_append_entries(term, leader_id)` and
`on_higher_term(term)`. `scripts/measure_failover.py` times failover on real
processes (about 1.9 s on average with the defaults).

### Command handlers (circulation team)

The default replica service installs borrow, return, renew, and hold handlers.
To customize operations, register handlers on the `CatalogStateMachine` passed
to `create_app(state_machine=...)` (this replaces the default machine):

```python
def borrow(snapshot, command, context) -> CommandOutcome:
    ...  # raise CommandRejected("reason") to refuse
    return CommandOutcome(books=new_books, result={"due_date": ...})
```

Handlers run on **every replica**, in log order, and must be deterministic:

- Use `context.timestamp` (fixed by the leader), never the wall clock.
- No randomness and no I/O.
- Return `books=None` to leave the catalog unchanged.

All replicas must run the same handler code and business constants. Restart
all replicas with the updated code before submitting circulation commands.
Request deduplication is performed by persistence, outside the handlers, so
handlers remain pure functions of their snapshot, command, and context.

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `RAFT_NODE_ID` | `storage-1` | This replica's ID |
| `RAFT_PEERS` | three loopback replicas on 8101-8103 | `id=url,...` for **every** replica, including this one |
| `RAFT_DB` | `var/replica-<id>.sqlite3` | Durable state file |
| `RAFT_INITIAL_LEADER` | `storage-1` | Replica with a short first election timeout, so a fresh cluster usually elects it; empty to disable |
| `RAFT_HEARTBEAT_SECONDS` | `0.1` | Heartbeat / replication interval |
| `RAFT_RPC_TIMEOUT_SECONDS` | `0.5` | Timeout for peer RPCs and forwarded reads |
| `RAFT_COMMIT_TIMEOUT_SECONDS` | `5` | How long a write waits for a majority |
| `RAFT_ELECTION_TIMEOUT_MIN_SECONDS` | `1.5` | Shortest random election timeout; must exceed read lease + RPC timeout |
| `RAFT_ELECTION_TIMEOUT_MAX_SECONDS` | `3.0` | Longest random election timeout |
| `RAFT_ENABLE_TEST_WRITES` | off | `1` enables the development replace endpoint |

### Not yet provided

- Pre-vote: a replica that was cut off can force one extra election when it
  rejoins, because it comes back with a higher term.
- Log compaction or snapshots: the log grows without bound, which is
  acceptable for the demo's scale.
- Numbered change notifications to branches: branches still poll the version.
- Linearizable reads under clock drift: the read lease assumes clocks run at
  roughly the same rate on every replica.
