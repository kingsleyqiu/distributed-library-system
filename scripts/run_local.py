"""Run storage and three branch nodes; Ctrl+C stops them all.

Default: the single-node storage stand-in on port 8100.
``--replicated``: three storage replicas on ports 8101-8103 (storage-1 starts
as leader), with branch-N reading through replica storage-N.
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
                    f"http://127.0.0.1:{REPLICA_PORTS[f'storage-{number}']}"
                    if args.replicated
                    else "http://127.0.0.1:8100"
                ),
                "SYNC_INTERVAL_SECONDS": "1",
                "STORAGE_TIMEOUT_SECONDS": "1",
            },
        )
        for number in range(1, 4)
    )
    children: list[subprocess.Popen] = []
    try:
        for module, port, settings in services:
            env = os.environ.copy()
            env.update(settings)
            children.append(
                subprocess.Popen(
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
                    start_new_session=os.name != "nt",
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                    ),
                )
            )
        if args.replicated:
            print("Replicas: http://127.0.0.1:8101/docs (also 8102 and 8103)", flush=True)
        else:
            print("Storage: http://127.0.0.1:8100/docs", flush=True)
        print("Branches: http://127.0.0.1:8001/docs (also 8002 and 8003)", flush=True)
        print("Press Ctrl+C to stop all services.", flush=True)
        while True:
            for child in children:
                if child.poll() is not None:
                    print("A service exited; stopping the remaining services.", file=sys.stderr)
                    return child.returncode or 1
            time.sleep(0.2)
    except KeyboardInterrupt:
        return 0
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    raise SystemExit(main())
