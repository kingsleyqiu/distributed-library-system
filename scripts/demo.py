"""Interactive 4-scenario evaluation & demo runner (SOFE4790U Proposal Sec. 4).

Run while the replicated cluster is up:
  Option A (Docker):  docker compose -f compose.replicated.yaml up --build
                      python scripts/demo.py --docker
  Option B (Local):   python scripts/run_local.py --replicated --dashboard --auth
                      python scripts/demo.py
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import time
import uuid

import httpx

BRANCHES = {
    "branch-1": "http://127.0.0.1:8001",
    "branch-2": "http://127.0.0.1:8002",
    "branch-3": "http://127.0.0.1:8003",
}
REPLICAS = {
    "storage-1": "http://127.0.0.1:8101",
    "storage-2": "http://127.0.0.1:8102",
    "storage-3": "http://127.0.0.1:8103",
}
DASHBOARD_URL = "http://127.0.0.1:8080"


def banner(title: str) -> None:
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


async def login(client: httpx.AsyncClient, branch_url: str, user_id: str, password: str) -> str:
    res = await client.post(f"{branch_url}/auth/login", json={"user_id": user_id, "password": password})
    res.raise_for_status()
    return res.json()["access_token"]


async def find_leader(client: httpx.AsyncClient) -> tuple[str | None, dict[str, dict]]:
    statuses: dict[str, dict] = {}
    leader = None
    for nid, url in REPLICAS.items():
        try:
            r = await client.get(f"{url}/internal/raft/status", timeout=1.0)
            if r.status_code == 200:
                data = r.json()
                statuses[nid] = data
                if data["role"] == "leader":
                    leader = nid
        except httpx.HTTPError:
            pass
    return leader, statuses


async def run_demo(use_docker: bool) -> None:
    async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
        banner("0. Cluster Check & Stateless Login")
        print(f"Open live dashboard in your browser: {DASHBOARD_URL}")
        leader, statuses = await find_leader(client)
        print(f"Online replicas: {list(statuses.keys())} | Current leader: {leader}")

        tok1 = await login(client, BRANCHES["branch-1"], "patron-1", "password123")
        tok2 = await login(client, BRANCHES["branch-2"], "patron-2", "password123")
        print("Logged in patron-1 (branch-1) and patron-2 (branch-2).")

        # Pick an available copy to race on
        catalog = (await client.get(f"{BRANCHES['branch-1']}/books?available=true")).json()
        book = catalog["books"][0]
        copy = book["copies"][0]
        print(f"Target copy: {book['book_id']} / {copy['copy_id']} (version={copy['version']})")

        # ------------------------------------------------------------------
        banner("1. Competing Borrow Requests (Optimistic Concurrency Control)")
        cmd1 = {
            "request_id": f"demo-race-1-{uuid.uuid4().hex[:8]}",
            "operation": "borrow",
            "actor_id": "patron-1",
            "book_id": book["book_id"],
            "copy_id": copy["copy_id"],
            "expected_version": copy["version"],
        }
        cmd2 = {
            "request_id": f"demo-race-2-{uuid.uuid4().hex[:8]}",
            "operation": "borrow",
            "actor_id": "patron-2",
            "book_id": book["book_id"],
            "copy_id": copy["copy_id"],
            "expected_version": copy["version"],
        }
        t0 = time.perf_counter()
        r1, r2 = await asyncio.gather(
            client.post(f"{BRANCHES['branch-1']}/commands", json=cmd1, headers={"Authorization": f"Bearer {tok1}"}),
            client.post(f"{BRANCHES['branch-2']}/commands", json=cmd2, headers={"Authorization": f"Bearer {tok2}"}),
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        j1, j2 = r1.json(), r2.json()
        print(f"branch-1 (patron-1): ok={j1['ok']} error={j1.get('error')} rev={j1.get('revision')}")
        print(f"branch-2 (patron-2): ok={j2['ok']} error={j2.get('error')} rev={j2.get('revision')}")
        print(f"Concurrent commit latency: {elapsed_ms:.1f} ms")
        assert (j1["ok"] != j2["ok"]), "Expected exactly one borrow to commit!"
        winner = "patron-1" if j1["ok"] else "patron-2"
        print(f"PASS: Exactly one borrow committed (winner={winner}); double booking prevented.")

        # ------------------------------------------------------------------
        banner("2. Leader Failover & Confirmed Loan Durability")
        old_leader, _ = await find_leader(client)
        assert old_leader is not None, "No leader found"
        print(f"Current leader is {old_leader}.")
        if use_docker:
            print(f"Stopping Docker container {old_leader}...")
            t_kill = time.perf_counter()
            subprocess.run(["docker", "compose", "-f", "compose.replicated.yaml", "stop", old_leader], check=True)
        else:
            input(f"--> Stop {old_leader} now (or press Enter after stopping it)...")
            t_kill = time.perf_counter()

        new_leader = None
        for _ in range(40):
            cand, _ = await find_leader(client)
            if cand and cand != old_leader:
                new_leader = cand
                break
            await asyncio.sleep(0.25)
        failover_s = time.perf_counter() - t_kill
        print(f"New leader elected: {new_leader} in {failover_s:.2f}s")

        # Verify confirmed loan survived on a surviving branch
        surviving_branch = BRANCHES["branch-2"] if old_leader == "storage-1" else BRANCHES["branch-1"]
        await asyncio.sleep(1.5)
        detail = (await client.get(f"{surviving_branch}/books/{book['book_id']}")).json()
        updated_copy = next(c for c in detail["book"]["copies"] if c["copy_id"] == copy["copy_id"])
        print(f"Confirmed loan persisted: available={updated_copy['available']} borrower={updated_copy['borrower_id']}")
        assert updated_copy["available"] is False and updated_copy["borrower_id"] == winner

        # ------------------------------------------------------------------
        banner("3. Disconnected Branch: Cached Searches Work, Writes Refused")
        # The branch paired with old_leader cannot reach its configured storage replica
        paired_branch = f"branch-{old_leader.split('-')[-1]}"
        paired_url = BRANCHES[paired_branch]
        await asyncio.sleep(2.5)  # allow cache staleness window to trigger
        search_res = await client.get(f"{paired_url}/books")
        print(f"{paired_branch} GET /books -> HTTP {search_res.status_code}, cache.stale={search_res.json()['cache']['stale']}")
        write_res = await client.post(
            f"{paired_url}/commands",
            json={
                "request_id": f"demo-offline-{uuid.uuid4().hex[:8]}",
                "operation": "hold",
                "actor_id": "patron-1",
                "book_id": book["book_id"],
            },
            headers={"Authorization": f"Bearer {tok1}"},
        )
        print(f"{paired_branch} POST /commands while disconnected -> HTTP {write_res.status_code} ({write_res.json().get('detail')})")
        assert search_res.status_code == 200 and write_res.status_code == 503
        print("PASS: Disconnected branch serves stale cached searches and refuses new loans.")

        # ------------------------------------------------------------------
        banner("4. Restart Failed Replica & Verify Log Catch-Up")
        if use_docker:
            print(f"Restarting Docker container {old_leader}...")
            subprocess.run(["docker", "compose", "-f", "compose.replicated.yaml", "start", old_leader], check=True)
        else:
            input(f"--> Restart {old_leader} now and press Enter...")

        for _ in range(40):
            _, st = await find_leader(client)
            if old_leader in st and new_leader in st:
                if st[old_leader]["commit_index"] >= st[new_leader]["commit_index"]:
                    print(
                        f"PASS: {old_leader} rejoined as {st[old_leader]['role']} "
                        f"(term={st[old_leader]['term']}, commit_index={st[old_leader]['commit_index']}, "
                        f"revision={st[old_leader]['revision']})."
                    )
                    break
            await asyncio.sleep(0.25)

        banner("Demo Complete — Check Live Dashboard Events")
        dash = (await client.get(f"{DASHBOARD_URL}/api/cluster")).json()
        print(f"Dashboard shows leader={dash['leader_id']}, term={dash['term']}, events={len(dash['events'])}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", action="store_true", help="automatically stop/start containers via docker compose")
    args = parser.parse_args()
    asyncio.run(run_demo(args.docker))