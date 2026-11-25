"""Measure normal-operation write/commit latency on the running replicated cluster.

Usage (while `python scripts/run_local.py --replicated --dashboard --auth` or
`docker compose -f compose.replicated.yaml up` is running):
    python scripts/measure_latency.py --runs 10
"""

from __future__ import annotations

import argparse
import statistics
import time
import uuid

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branch-url", default="http://127.0.0.1:8001")
    parser.add_argument("--runs", type=int, default=10, help="number of borrow+return cycles")
    args = parser.parse_args()

    with httpx.Client(base_url=args.branch_url, timeout=10.0, trust_env=False) as client:
        login = client.post("/auth/login", json={"user_id": "patron-1", "password": "password123"})
        login.raise_for_status()
        headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

        books = client.get("/books?available=true").json()["books"]
        book = books[0]
        copy = book["copies"][0]
        version = copy["version"]

        latencies_ms: list[float] = []
        for i in range(args.runs):
            for op in ("borrow", "return"):
                cmd = {
                    "request_id": f"bench-{op}-{i}-{uuid.uuid4().hex[:8]}",
                    "operation": op,
                    "actor_id": "patron-1",
                    "book_id": book["book_id"],
                    "copy_id": copy["copy_id"],
                    "expected_version": version,
                }
                t0 = time.perf_counter()
                res = client.post("/commands", json=cmd, headers=headers)
                dt_ms = (time.perf_counter() - t0) * 1000
                res.raise_for_status()
                body = res.json()
                assert body["ok"], f"{op} failed: {body}"
                version = body["result"]["copy"]["version"]
                latencies_ms.append(dt_ms)

        latencies_ms.sort()
        p95_idx = min(len(latencies_ms) - 1, int(0.95 * len(latencies_ms)))
        print(f"Committed {len(latencies_ms)} replicated writes (borrow/return) via {args.branch_url}:")
        print(f"  min:    {latencies_ms[0]:.1f} ms")
        print(f"  median: {statistics.median(latencies_ms):.1f} ms")
        print(f"  mean:   {statistics.mean(latencies_ms):.1f} ms")
        print(f"  p95:    {latencies_ms[p95_idx]:.1f} ms")
        print(f"  max:    {latencies_ms[-1]:.1f} ms")


if __name__ == "__main__":
    main()