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
