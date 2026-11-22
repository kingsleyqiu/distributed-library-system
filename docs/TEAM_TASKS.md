# Integration ownership

This implementation establishes the shared development skeleton, then focuses
on Qian/Kingsley's branch API, cross-branch search, and cache. The proposal's
other responsibilities remain with the following owners.

| Owner | Responsibility | Integration boundary |
| --- | --- | --- |
| Qian Qiu / Kingsley | Branch REST API, search, caches | Shared catalog models, replaceable storage client, cache metadata |
| Aryan Kumar | Leader election, terms, failure detection, failover testing | Storage service and leader-discovery contract |
| Logan Yee | Replication, majority commit, catch-up, persistence | Committed catalog snapshots, durable revision, command results |
| Titobi Odufuye | Borrow, return, renew, holds, version checks, request IDs | Implemented shared circulation handlers, checked writes, and durable request results in both storage modes |
| Mohit Gajula | Login, sample books, dashboard, Compose, demo setup | Authentication boundary, node status, runtime configuration |

The initial sample books and Compose configuration are shared scaffolding for
development. They do not mark another member's full responsibilities complete.
All members still contribute to the final report and participate in the demo.

Before implementing a teammate's module, agree on its remaining contract in
`INTERFACES.md`. In particular, business rules and authentication are not
silently fixed by a reserved command envelope. Keep work on personal branches
and integrate through reviewed pull requests.

## Verification boundary

This phase can demonstrate:

- cross-branch search, version polling, warm-cache queries during an outage,
  unavailable cold starts, and resynchronization;
- three-replica storage with log replication, majority commit (writes succeed
  with 2 of 3 replicas and are refused with 1), catch-up of a restarted or
  rejoining replica from its durable log, and **automatic** leader election
  with heartbeat failure detection, terms, and check-quorum (failover in about
  2 s; see `scripts/measure_failover.py`).

Circulation tests also demonstrate concurrent lending safety, FIFO hold
priority, renewal restrictions, durable request deduplication, and retry safety
through replica failover. The agreed API and business defaults are documented
in [Circulation commands](INTERFACES.md#circulation-commands).

It cannot yet demonstrate authenticated account permissions. Actor IDs remain
client supplied until the login module is integrated.

Docker Compose is supplied as the intended container configuration. The initial
accepted verification uses native Python processes because Docker is not
available in the development environment; container execution must be checked
once Docker is available.

## Branch component verification (2026-11-21)

The API, cross-branch search, and in-memory cache have been checked against these
acceptance criteria:

| Area | Verified behavior |
| --- | --- |
| Search | All three branches, title/author keywords, location and availability filters applied to the same copy |
| Details | All copies and due dates; URL-encoded IDs including slashes and Unicode; missing titles return 404 |
| Cache | Version polling, atomic replacement, malformed-data and revision-rollback rejection |
| Outage/recovery | Warm cache serves marked stale data; cold cache returns 503; reconnect and restart recover |
| API boundary | Validation errors return 422; reserved mutations return 501 or 503 without fake successful writes |

The complete native suite passed **54 tests**. An isolated checkout of Logan's
`85600b2ca60ac0b0b81f335d7c8523c66fcc1581`, using this branch API, passed
**46 focused API/model/replica tests**. That includes real-process reads through
a follower, updates from replicated revisions, quorum loss and recovery,
manual leader promotion, and replica catch-up. No consensus implementation
was copied into this branch.

The replicated-backend check uses the development catalog-replacement endpoint,
not real lending. It demonstrates recovery through a surviving configured
replica; a branch pointing at a stopped replica keeps its cache stale until
that endpoint returns. Automatic election, multi-endpoint discovery, active
update notifications, authentication, and circulation remain separate integration
work. The currently verified cache transport is version polling.
