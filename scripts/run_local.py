"""Run the storage stand-in and three branch nodes; Ctrl+C stops all four."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    def stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGBREAK if os.name == "nt" else signal.SIGTERM, stop)
    services = [("storage", 8100, {"CATALOG_DB": os.getenv("CATALOG_DB", "var/catalog.sqlite3")})]
    services.extend(
        (
            "branch",
            8000 + number,
            {
                "BRANCH_ID": f"branch-{number}",
                "STORAGE_URL": "http://127.0.0.1:8100",
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
