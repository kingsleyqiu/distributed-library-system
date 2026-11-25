"""Run storage and three branch nodes; Ctrl+C stops them all.

Default: the single-node storage stand-in on port 8100.
``--replicated``: three storage replicas on ports 8101-8103 that elect their
own leader (storage-1 is favoured in the first election), with branch-N
reading through replica storage-N.
``--dashboard``: also start the live web dashboard on port 8080.
``--auth``: require bearer authentication on branch write commands.
"""

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPLICA_PORTS = {f"storage-{number}": 8100 + number for number in range(1, 4)}


def storage_services(replicated: bool, test_writes: bool) -> list[tuple[str, int, dict[str, str]]]:
    if not replicated:
        return [("storage", 8100, {"CATALOG_DB": os.getenv("CATALOG_DB", "var/catalog.sqlite3")})]
    cluster = ",".join(f"{node}=http://127.0.0.1:{port}" for node, port in REPLICA_PORTS.items())
    return [
        (
            "replica",
            port,
            {
                "RAFT_NODE_ID": node,
                "RAFT_PEERS": cluster,
                "RAFT_DB": f"var/replica-{node}.sqlite3",
                "RAFT_INITIAL_LEADER": "storage-1",
                "RAFT_ELECTION_TIMEOUT_MIN_SECONDS": os.getenv("RAFT_ELECTION_TIMEOUT_MIN_SECONDS", "1.5"),
                "RAFT_ELECTION_TIMEOUT_MAX_SECONDS": os.getenv("RAFT_ELECTION_TIMEOUT_MAX_SECONDS", "3.0"),
                "RAFT_ENABLE_TEST_WRITES": "1" if test_writes else "0",
            },
        )
        for node, port in REPLICA_PORTS.items()
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replicated", action="store_true", help="run three storage replicas")
    parser.add_argument(
        "--test-writes",
        action="store_true",
        help="with --replicated, enable the development-only catalog replace endpoint",
    )
    parser.add_argument("--dashboard", action="store_true", help="also start the live dashboard on port 8080")
    parser.add_argument("--auth", action="store_true", help="require bearer login on branch write commands")
    args = parser.parse_args()

    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGBREAK if os.name == "nt" else signal.SIGTERM, stop)
    services = storage_services(args.replicated, args.test_writes)
    services.extend(
        (
            "branch",
            8000 + number,
            {
                "BRANCH_ID": f"branch-{number}",
                "STORAGE_URL": (
                    f"http://127.0.0.1:{8100 + number}" if args.replicated else "http://127.0.0.1:8100"
                ),
                "SYNC_INTERVAL_SECONDS": os.getenv("SYNC_INTERVAL_SECONDS", "1"),
                "STORAGE_TIMEOUT_SECONDS": os.getenv("STORAGE_TIMEOUT_SECONDS", "1"),
                "REQUIRE_AUTH": "1" if args.auth else os.getenv("REQUIRE_AUTH", "0"),
            },
        )
        for number in range(1, 4)
    )
    if args.dashboard:
        services.append(("dashboard", 8080, {}))

    processes: list[tuple[str, subprocess.Popen[bytes]]] = []
    try:
        for module, port, extra_env in services:
            env = {**os.environ, **extra_env}
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    f"library_system.{module}:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
                cwd=PROJECT_ROOT,
                env=env,
            )
            processes.append((f"{module}:{port}", process))
        while True:
            for name, process in processes:
                code = process.poll()
                if code is not None:
                    print(f"{name} exited with code {code}", file=sys.stderr)
                    return code or 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        return 0
    finally:
        for _, process in processes:
            if process.poll() is None:
                process.terminate()
        for _, process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    raise SystemExit(main())