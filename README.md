# Distributed Multi-Branch Library System

SOFE4790U Distributed Systems, Fall 2026 - Group 9.

## Project

The proposal describes three library branch nodes backed by a three-replica,
Raft-style storage cluster. Planned features include catalog search, borrowing,
returns, renewals, holds, and a dashboard for observing concurrency and failures.
The proposed stack is Python, FastAPI, asyncio, SQLite, and Docker Compose.

The first implementation contains the shared HTTP/data contracts, three branch
APIs, catalog search and details, version-polled in-memory caches, and a persistent
single-node storage stand-in with 20 original sample books.

A **replicated storage mode** runs three storage replicas with a durable
replicated log, majority commit, and catch-up for rejoining replicas. Leaders
are currently chosen by configuration or manual promotion; automatic leader
election is still to come. Login, circulation, holds, the dashboard, and active
update notifications remain with their assigned teammates. Command routes return
`501 Not Implemented` until circulation handlers are registered, or `503` at a
branch without a usable storage synchronization. They never report a
successful loan.

## Run locally

Use Python 3.14. From this repository in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe scripts/run_local.py
```

On macOS/Linux, use `python3 -m venv .venv` and `.venv/bin/python` in place of
the Windows Python path. Keep this terminal open; Ctrl+C stops all four services.

| Service | Interactive API documentation |
| --- | --- |
| Branch 1 | <http://127.0.0.1:8001/docs> |
| Branch 2 | <http://127.0.0.1:8002/docs> |
| Branch 3 | <http://127.0.0.1:8003/docs> |
| Storage stand-in | <http://127.0.0.1:8100/docs> |

Try <http://127.0.0.1:8001/books> to search all branches. Query parameters:
`q` matches title or author, `branch_id` filters location (`branch-1` through
`branch-3`), and `available=true` or `false` filters copies. Combined filters
must match the same copy. `/books/book-001` returns details and all copies.

Each result includes the catalog revision, branch ID, last successful sync
check time, and a `stale` flag. Branches check the catalog version every second.
After a failed synchronization they retain the last snapshot and mark it stale.
A disconnected branch that has no cached snapshot returns `503`; it recovers
automatically when storage becomes reachable. A recently successful check does
not guarantee that cached availability is current; future loans must be checked
and committed by storage.

The stand-in persists its catalog and revision in `var/catalog.sqlite3`.
Sample `.txt` books are imported only when creating a new database. Restarting
does not erase catalog changes or reset its revision. Branch caches are memory
only and are rebuilt from storage after restart.

### Replicated storage mode

```powershell
.\.venv\Scripts\python.exe scripts/run_local.py --replicated --test-writes
```

This starts replicas on ports 8101-8103, with `storage-1` as the initial leader.
Branch-N reads through replica storage-N, and followers forward to the
leader. To watch replication, open <http://127.0.0.1:8101/internal/raft/status>,
which shows the role, term, commit index, and each follower's lag. Some things
to try:

- Send a write to a follower with `POST /internal/raft/test/replace_catalog`
  (development only; requires `--test-writes`).
- Stop one replica: writes still commit with 2 of 3.
- Restart it: it catches up from its log.
- Stop the leader, then `POST /internal/raft/promote` on a follower to fail over.

Replica state lives in `var/replica-storage-N.sqlite3`. See
[Replicated storage](docs/INTERFACES.md#replicated-storage) for the full
contract and configuration.

## Verify

```powershell
.\.venv\Scripts\python.exe -m pytest -q --basetemp=.pytest_cache/tmp
```

Tests cover search/filter contracts, malformed snapshots, revision rollback,
cache failures/recovery, and durable storage. The acceptance test launches four
real Python processes on temporary loopback ports, changes the storage catalog,
stops storage, restarts a disconnected branch, and then checks reconnection.

Replication tests cover:

- majority commit, and refusal without a majority;
- the Raft rule that an earlier-term entry commits only alongside a
  current-term entry;
- truncation of divergent logs, idempotent and reordered RPCs, and step-down
  on a higher term;
- the leader read barrier and read lease;
- catch-up from disk.

A second acceptance test kills and restarts three real replica processes to
check majority commit, catch-up, and manual failover. Automatic leader election
is not yet tested.

## Docker Compose

When Docker is available:

```bash
docker compose up --build
docker compose down
```

The same service ports are published on localhost. Stop the native runner before
starting Compose. Catalog data is retained in the `catalog-data` volume.
For replicated storage use `docker compose -f compose.replicated.yaml up --build`.
Each replica keeps its state in its own volume.
Docker execution has not been verified in the current development environment.
These services are a local development prototype without authentication; do not
expose them to an external network.

## Integration and team ownership

[Interface contracts](docs/INTERFACES.md) describe IDs, schemas, endpoints, error
codes, and replacing the stand-in with Raft. [Team tasks](docs/TEAM_TASKS.md)
separate the completed branch work from each teammate's remaining implementation.

## Team workflow

**Current protection status:** a `main` protection rule is saved, but GitHub
reports that it is **not enforced** on this private repository under the current
plan. Follow the PR workflow below; direct pushes are not currently blocked.

- `main`: shared integration branch. Submit changes through a Pull Request (PR).
- Teammates: create a branch using your own name after accepting a collaborator
  invitation. All branches share the same project files; separate code copies
  or folders per person are unnecessary.

Push commits to your own branch, open a PR into `main`, ask another teammate to 
test before merging. Synchronize your branch with `origin/main`
before starting more work. Use **Create a merge commit** for these ongoing
personal branches, and keep the branches after merging.

See [CONTRIBUTING.md](CONTRIBUTING.md) for exact commands and repository settings.
