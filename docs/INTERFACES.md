# Shared interfaces: first implementation

This phase contains three branch APIs and one **single-node storage stand-in**.
The stand-in is an application-team development dependency. It has no Raft,
leader election, replication, quorum acknowledgement, or failover.

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
| `version` | Nonnegative version of a copy, reserved for checked writes |
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
revision detects catalog changes; a copy version is for future concurrency
checks on that specific copy.

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
for longer. Availability shown in search is advisory; future lending must
check and commit against authoritative storage.

## Reserved writes: no implementation in this phase

`POST /commands` on a branch and `POST /internal/commands` on storage reserve
the future write path. The shared `CommandEnvelope` names the common fields:

```json
{
  "request_id": "client-generated-unique-request-id",
  "operation": "borrow",
  "actor_id": "patron-demo-id",
  "book_id": "book-001",
  "copy_id": "copy-001",
  "expected_version": 0
}
```

Operations are `borrow`, `return`, `renew`, `hold`, `add_book`, and
`remove_book`. The envelope is scaffolding, not a complete business schema:
for example, staff book metadata and an authenticated actor must still be
specified by the responsible teammates. Neither service executes these
operations. Valid requests return HTTP 501; a branch without usable storage
synchronization returns HTTP 503. Validation errors return HTTP 422. No loan,
hold, login, or staff operation should be represented as successfully completed.

Before implementing writes, agree on authentication, operation-specific input
and output schemas, loan durations, renew limits, hold ordering, and removal
rules. The eventual leader must atomically check copy versions, deduplicate
`request_id`, and return success only after the replicated commit succeeds.
Branches must never treat an optimistic cache update as an authoritative loan.

## Replacing the stand-in

The consensus implementation must preserve the read contract while advancing
the catalog revision consistently across leader changes. The branch storage
client is the integration point for leader discovery, redirects/retries,
committed reads, and numbered notifications. A single configured storage URL
today does not provide leader discovery or prove failover behavior.

Agree on these responsibilities with the consensus team before replacement;
do not simulate a quorum or present the stand-in as replicated storage.
