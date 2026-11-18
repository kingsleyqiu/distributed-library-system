# Integration ownership

This implementation establishes the shared development skeleton, then focuses
on Qian/Kingsley's branch API, cross-branch search, and cache. The proposal's
other responsibilities remain with the following owners.

| Owner | Responsibility | Integration boundary |
| --- | --- | --- |
| Qian Qiu / Kingsley | Branch REST API, search, caches | Shared catalog models, replaceable storage client, cache metadata |
| Aryan Kumar | Leader election, terms, failure detection, failover testing | Storage service and leader-discovery contract |
| Logan Yee | Replication, majority commit, catch-up, persistence | Committed catalog snapshots, durable revision, command results |
| Titobi Odufuye | Borrow, return, renew, holds, version checks, request IDs | Reserved command envelope, authoritative atomic write path |
| Mohit Gajula | Login, sample books, dashboard, Compose, demo setup | Authentication boundary, node status, runtime configuration |

The initial sample books and Compose configuration are shared scaffolding for
development. They do not mark another member's full responsibilities complete.
All members still contribute to the final report and participate in the demo.

Before implementing a teammate's module, agree on its remaining contract in
`INTERFACES.md`. In particular, business rules and authentication are not
silently fixed by a reserved command envelope. Keep work on personal branches
and integrate through reviewed pull requests.

## Verification boundary

This phase can demonstrate cross-branch search, version polling, warm-cache
queries during an outage, unavailable cold starts, and resynchronization.
It cannot demonstrate concurrent lending safety, majority commits, leader
failover, replica recovery, or account permissions until the corresponding
modules are implemented and tested.

Docker Compose is supplied as the intended container configuration. The initial
accepted verification uses native Python processes because Docker is not
available in the development environment; container execution must be checked
once Docker is available.
