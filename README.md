# Distributed Multi-Branch Library System

SOFE4790U Distributed Systems, Fall 2026 - Group 9.

## Project

The proposal describes three library branch nodes backed by a three-replica,
Raft-style storage cluster. Planned features include catalog search, borrowing,
returns, renewals, holds, and a dashboard for observing concurrency and failures.
The proposed stack is Python, FastAPI, asyncio, SQLite, and Docker Compose.

This repository currently contains collaboration setup only. Application code
and verified installation, run, and test instructions will be added as the team
implements the system.

## Team workflow

- `main`: shared integration branch. Submit changes through a Pull Request (PR).
- `kingsley`: Kingsley's personal working branch.
- Teammates: create a branch using your own name after accepting a collaborator
  invitation. All branches share the same project files; separate code copies
  or folders per person are unnecessary.

Push commits to your own branch, open a PR into `main`, ask another teammate to
review, and test before merging. Synchronize your branch with `origin/main`
before starting more work. Use **Create a merge commit** for these ongoing
personal branches, and keep the branches after merging.

See [CONTRIBUTING.md](CONTRIBUTING.md) for exact commands and repository settings.
