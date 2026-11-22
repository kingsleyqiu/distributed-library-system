"""Acceptance test with real HTTP and four independently running services."""

from contextlib import ExitStack
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import httpx

from library_system.storage import CatalogRepository


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def wait_until(check, description, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (httpx.HTTPError, KeyError):
            pass
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


def test_three_branches_refresh_disconnect_cold_restart_and_recover(tmp_path):
    # Reserve distinct loopback ports, then release each immediately before its service starts.
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(4)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in sockets]
        database = tmp_path / "catalog.sqlite3"
        running = []

        def launch(module, index):
            sockets[index].close()
            env = os.environ.copy()
            env.update(
                CATALOG_DB=str(database),
                BRANCH_ID=f"branch-{index}" if index else "branch-1",
                STORAGE_URL=f"http://127.0.0.1:{ports[0]}",
                SYNC_INTERVAL_SECONDS="0.1",
                STORAGE_TIMEOUT_SECONDS="0.2",
            )
            log = stack.enter_context((tmp_path / f"service-{index}-{len(running)}.log").open("w"))
            process = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", f"library_system.{module}:app",
                 "--host", "127.0.0.1", "--port", str(ports[index]), "--log-level", "warning"],
                cwd=PROJECT_ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            running.append(process)
            return process

        def stop(process):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

        with httpx.Client(timeout=0.5, trust_env=False) as client:
            urls = [f"http://127.0.0.1:{port}" for port in ports]

            def healthy(index, status):
                return client.get(urls[index] + "/health").json()["status"] == status

            try:
                storage = launch("storage", 0)
                wait_until(lambda: healthy(0, "ok"), "storage startup")
                branches = [launch("branch", index) for index in range(1, 4)]
                for index in range(1, 4):
                    wait_until(lambda: healthy(index, "ready"), f"branch-{index} startup")
                    result = client.get(urls[index] + "/books").json()
                    assert result["total"] == 20
                    assert result["cache"]["branch_id"] == f"branch-{index}"
                    assert not result["cache"]["stale"]

                repository = CatalogRepository(database)
                initial = repository.snapshot()
                books = list(initial.books)
                books[0] = books[0].model_copy(update={"title": "Updated Sample Title"})
                updated = repository.replace_books(tuple(books))
                for index in range(1, 4):
                    wait_until(
                        lambda: client.get(urls[index] + "/books").json()["cache"]["revision"]
                        == updated.revision,
                        f"branch-{index} catalog refresh",
                    )
                    assert client.get(urls[index] + "/books", params={"q": "updated sample"}).json()["total"] == 1

                stop(storage)
                for index in range(1, 4):
                    wait_until(lambda: healthy(index, "degraded"), f"branch-{index} detects disconnection")
                    cached = client.get(urls[index] + "/books").json()
                    assert cached["total"] == 20
                    assert cached["cache"]["stale"]
                    assert cached["cache"]["revision"] == updated.revision
                    command = {"request_id": "offline-1", "operation": "borrow", "actor_id": "demo", "book_id": "book-001"}
                    assert client.post(urls[index] + "/commands", json=command).status_code == 503

                stop(branches[2])
                branches[2] = launch("branch", 3)
                wait_until(lambda: healthy(3, "unavailable"), "cold branch without storage")
                assert client.get(urls[3] + "/books").status_code == 503

                storage = launch("storage", 0)
                wait_until(lambda: healthy(0, "ok"), "storage restart")
                assert client.get(urls[0] + "/internal/catalog").json()["revision"] == updated.revision
                for index in range(1, 4):
                    wait_until(lambda: healthy(index, "ready"), f"branch-{index} reconnects")
                    result = client.get(urls[index] + "/books", params={"q": "updated sample"}).json()
                    assert result["total"] == 1
                    assert not result["cache"]["stale"]
            finally:
                for process in reversed(running):
                    stop(process)
