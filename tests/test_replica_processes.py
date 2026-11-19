"""Acceptance test: three replica processes and one branch over real HTTP.

Covers replicated writes through a follower, majority commit with one replica
down, catch-up of a restarted replica, refusal without a majority, and manual
failover to a promoted follower. Processes are killed, not shut down cleanly.
"""

from contextlib import ExitStack
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPLICAS = ("storage-1", "storage-2", "storage-3")


def wait_until(check, description, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


def test_replicated_cluster_commit_catch_up_and_failover(tmp_path):
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(4)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in sockets]
        urls = [f"http://127.0.0.1:{port}" for port in ports]
        cluster = ",".join(f"{node}={url}" for node, url in zip(REPLICAS, urls))
        processes: dict[int, subprocess.Popen] = {}
        launches = 0

        def launch(index):
            nonlocal launches
            if index < len(sockets) and sockets[index].fileno() != -1:
                sockets[index].close()
            env = os.environ.copy()
            if index < 3:
                module = "replica"
                env.update(
                    RAFT_NODE_ID=REPLICAS[index],
                    RAFT_PEERS=cluster,
                    RAFT_DB=str(tmp_path / f"{REPLICAS[index]}.sqlite3"),
                    RAFT_INITIAL_LEADER="storage-1",
                    RAFT_HEARTBEAT_SECONDS="0.05",
                    RAFT_RPC_TIMEOUT_SECONDS="0.3",
                    RAFT_COMMIT_TIMEOUT_SECONDS="2",
                    RAFT_ENABLE_TEST_WRITES="1",
                )
            else:
                module = "branch"
                env.update(
                    BRANCH_ID="branch-2",
                    STORAGE_URL=urls[1],  # A follower at first: reads are forwarded.
                    SYNC_INTERVAL_SECONDS="0.1",
                    STORAGE_TIMEOUT_SECONDS="1",
                )
            launches += 1
            log = stack.enter_context((tmp_path / f"service-{index}-{launches}.log").open("w"))
            processes[index] = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", f"library_system.{module}:app",
                 "--host", "127.0.0.1", "--port", str(ports[index]), "--log-level", "warning"],
                cwd=PROJECT_ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )

        def kill(index):
            process = processes.pop(index)
            process.kill()
            process.wait(timeout=5)

        with httpx.Client(timeout=0.5, trust_env=False) as client:

            def status(index):
                return client.get(urls[index] + "/internal/raft/status").json()

            def revisions(*indexes):
                return [status(index)["revision"] for index in indexes]

            def replace_title(index, title):
                books = client.get(urls[index] + "/internal/catalog").json()["books"]
                books[0]["title"] = title
                return client.post(
                    urls[index] + "/internal/raft/test/replace_catalog", json={"books": books}, timeout=5
                )

            def branch_sees(revision, query):
                result = client.get(urls[3] + "/books", params={"q": query}).json()
                return result["cache"]["revision"] == revision and result["total"] == 1

            try:
                for index in range(3):
                    launch(index)
                wait_until(lambda: status(0)["role"] == "leader" and status(0)["ready_for_reads"], "initial leader")
                launch(3)
                wait_until(lambda: client.get(urls[3] + "/health").json()["status"] == "ready", "branch startup")
                assert client.get(urls[3] + "/books").json()["total"] == 20

                # 1. A write sent to a follower is forwarded, committed, and replicated.
                response = replace_title(1, "Written Through Follower")
                assert response.status_code == 200, response.text
                assert response.json()["revision"] == 2
                wait_until(lambda: revisions(0, 1, 2) == [2, 2, 2], "replication to all replicas")
                wait_until(lambda: branch_sees(2, "written through follower"), "branch refresh")

                # 2. With one replica down, two of three still form a majority.
                kill(2)
                response = replace_title(0, "Committed Without Storage Three")
                assert response.status_code == 200, response.text
                assert response.json()["revision"] == 3
                wait_until(lambda: revisions(0, 1) == [3, 3], "commit with two replicas")

                # 3. The restarted replica reloads its log from disk and catches up.
                launch(2)
                wait_until(
                    lambda: status(2)["revision"] == 3
                    and status(2)["last_applied"] == status(0)["last_applied"]
                    and status(2)["leader_id"] == "storage-1",
                    "restarted replica catch-up",
                )
                assert client.get(urls[0] + "/internal/raft/status").json()["peers"]["storage-3"]["lag"] == 0

                # 4. Without a majority nothing is committed; the outcome is reported unknown.
                kill(1)
                kill(2)
                response = replace_title(0, "Needs A Majority")
                assert response.status_code == 503
                assert status(0)["revision"] == 3
                launch(1)
                launch(2)
                wait_until(lambda: revisions(0, 1, 2) == [4, 4, 4], "pending entry commits once a majority returns")

                # 5. Fail over: kill the leader, promote an up-to-date follower.
                kill(0)
                promoted = client.post(urls[1] + "/internal/raft/promote", timeout=5)
                assert promoted.status_code == 200, promoted.text
                assert promoted.json()["role"] == "leader" and promoted.json()["term"] == 2
                wait_until(lambda: status(1)["ready_for_reads"], "promoted leader ready")
                response = replace_title(1, "After Failover")
                assert response.status_code == 200, response.text
                assert response.json()["revision"] == 5
                wait_until(lambda: status(2)["revision"] == 5, "replication after failover")
                wait_until(lambda: branch_sees(5, "after failover"), "branch sees post-failover catalog")

                # 6. The old leader returns as a follower and catches up instead of taking over.
                launch(0)
                wait_until(
                    lambda: status(0)["role"] == "follower"
                    and status(0)["leader_id"] == "storage-2"
                    and status(0)["revision"] == 5,
                    "old leader rejoins as follower",
                )
                assert status(1)["role"] == "leader" and status(1)["term"] == 2
            finally:
                for index in list(processes):
                    kill(index)
