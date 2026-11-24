"""Measure leader failover time on three real replica processes.

Each run kills the current leader and times how long the other two take to
elect a new leader that is ready to serve reads (the moment branches can read
and write again). The killed replica is then restarted and rejoins as a
follower before the next run. Uses the production timing defaults unless
overridden: heartbeat 0.1 s, election timeout 1.5-3.0 s.

    python scripts/measure_failover.py --runs 10

Ports 8201-8203 are used so this can run next to ``run_local.py``.
"""

import argparse
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPLICAS = ("storage-1", "storage-2", "storage-3")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--base-port", type=int, default=8201)
    parser.add_argument("--election-min", default=os.getenv("RAFT_ELECTION_TIMEOUT_MIN_SECONDS", "1.5"))
    parser.add_argument("--election-max", default=os.getenv("RAFT_ELECTION_TIMEOUT_MAX_SECONDS", "3.0"))
    args = parser.parse_args()

    ports = [args.base_port + i for i in range(3)]
    urls = [f"http://127.0.0.1:{port}" for port in ports]
    cluster = ",".join(f"{node}={url}" for node, url in zip(REPLICAS, urls))
    processes: dict[int, subprocess.Popen] = {}

    with tempfile.TemporaryDirectory() as data_dir, httpx.Client(timeout=0.5, trust_env=False) as client:

        def launch(index: int) -> None:
            env = os.environ.copy()
            env.update(
                RAFT_NODE_ID=REPLICAS[index],
                RAFT_PEERS=cluster,
                RAFT_DB=str(Path(data_dir) / f"{REPLICAS[index]}.sqlite3"),
                RAFT_INITIAL_LEADER="storage-1",
                RAFT_ELECTION_TIMEOUT_MIN_SECONDS=args.election_min,
                RAFT_ELECTION_TIMEOUT_MAX_SECONDS=args.election_max,
            )
            processes[index] = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "library_system.replica:app",
                 "--host", "127.0.0.1", "--port", str(ports[index]), "--log-level", "warning"],
                cwd=PROJECT_ROOT,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )

        def status(index: int) -> dict | None:
            try:
                return client.get(urls[index] + "/internal/raft/status").json()
            except httpx.HTTPError:
                return None

        def ready_leader(indexes) -> int | None:
            for index in indexes:
                current = status(index)
                if current and current["role"] == "leader" and current["ready_for_reads"]:
                    return index
            return None

        def wait_for(check, timeout=30.0, poll=0.01):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                result = check()
                if result is not None:
                    return result
                time.sleep(poll)
            raise TimeoutError("Cluster did not settle in time")

        try:
            for index in range(3):
                launch(index)
            times = []
            for run in range(1, args.runs + 1):
                leader = wait_for(lambda: ready_leader(range(3)))
                # Let the restarted replica rejoin and the cluster settle.
                wait_for(lambda: True if all(
                    (s := status(i)) and s["leader_id"] == REPLICAS[leader] for i in range(3) if i != leader
                ) else None)
                time.sleep(0.5)
                term = status(leader)["term"]
                survivors = [i for i in range(3) if i != leader]
                killed_at = time.monotonic()
                processes.pop(leader).kill()
                new_leader = wait_for(lambda: ready_leader(survivors))
                elapsed = time.monotonic() - killed_at
                times.append(elapsed)
                print(f"run {run:2d}: killed {REPLICAS[leader]} (term {term}) -> "
                      f"{REPLICAS[new_leader]} ready in {elapsed:.2f} s (term {status(new_leader)['term']})",
                      flush=True)
                launch(leader)
            print(f"\nfailover over {len(times)} runs: mean {statistics.mean(times):.2f} s, "
                  f"median {statistics.median(times):.2f} s, min {min(times):.2f} s, max {max(times):.2f} s")
            return 0
        finally:
            for process in processes.values():
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
