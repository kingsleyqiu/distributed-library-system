"""Acceptance test: three replica processes and one branch over real HTTP.

Covers replicated writes through a follower, majority commit with one replica
down, catch-up of a restarted replica, refusal without a majority, automatic
leader election after the leader is killed or paused, and the "start election
now" demo trigger. Processes are killed, not shut down cleanly.
"""

from contextlib import ExitStack
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest


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
                    RAFT_ELECTION_TIMEOUT_MIN_SECONDS="1.0",
                    RAFT_ELECTION_TIMEOUT_MAX_SECONDS="2.0",
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

            def leader_among(*indexes):
                """The single ready leader that every other listed replica follows."""
                statuses = {index: status(index) for index in indexes}
                leaders = [i for i, s in statuses.items() if s["role"] == "leader" and s["ready_for_reads"]]
                if len(leaders) != 1:
                    return None
                leader_id = statuses[leaders[0]]["node_id"]
                if all(s["leader_id"] == leader_id for i, s in statuses.items() if i != leaders[0]):
                    return leaders[0]
                return None

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

                # 4. Without a majority nothing is committed; the outcome is reported unknown,
                #    and the cut-off leader steps down (check-quorum).
                kill(1)
                kill(2)
                response = replace_title(0, "Needs A Majority")
                assert response.status_code == 503
                assert status(0)["revision"] == 3
                wait_until(lambda: status(0)["role"] != "leader", "lone leader steps down")
                launch(1)
                launch(2)
                # A leader is elected again. The unconfirmed entry may or may not have
                # survived (it was on one replica only), but every replica agrees.
                wait_until(lambda: leader_among(0, 1, 2) is not None, "leader elected after majority returns")
                wait_until(lambda: len(set(revisions(0, 1, 2))) == 1, "replicas agree")
                agreed = revisions(0)[0]
                assert agreed in (3, 4)

                # 5. Demo trigger: start an election on an up-to-date follower now; it wins.
                #    (Not storage-2: the branch reads through it, and the winner is killed next.)
                wait_until(
                    lambda: len({status(i)["last_log_index"] for i in range(3)}) == 1, "logs caught up"
                )
                current = leader_among(0, 1, 2)
                target = 2 if current == 0 else 0
                old_term = status(current)["term"]
                elected = client.post(urls[target] + "/internal/raft/promote", timeout=5)
                assert elected.status_code == 200, elected.text
                assert elected.json()["role"] == "leader" and elected.json()["term"] > old_term
                wait_until(lambda: leader_among(0, 1, 2) == target, "promoted replica leads")

                # 6. Automatic failover: kill the leader; the others elect a new one by themselves.
                survivors = tuple(i for i in range(3) if i != target)
                killed_at = time.monotonic()
                kill(target)
                wait_until(lambda: leader_among(*survivors) is not None, "automatic failover")
                failover_seconds = time.monotonic() - killed_at
                assert failover_seconds < 5, failover_seconds
                new_leader = leader_among(*survivors)
                response = replace_title(1, "After Failover")
                assert response.status_code == 200, response.text
                assert response.json()["revision"] == agreed + 1
                wait_until(lambda: revisions(*survivors) == [agreed + 1] * 2, "replication after failover")
                wait_until(lambda: branch_sees(agreed + 1, "after failover"), "branch sees post-failover catalog")

                # 7. The old leader returns as a follower and catches up instead of taking over.
                launch(target)
                wait_until(
                    lambda: status(target)["role"] == "follower"
                    and status(target)["leader_id"] == REPLICAS[new_leader]
                    and status(target)["revision"] == agreed + 1,
                    "old leader rejoins as follower",
                )
                assert leader_among(0, 1, 2) == new_leader
            finally:
                for index in list(processes):
                    kill(index)


@pytest.mark.skipif(not hasattr(signal, "SIGSTOP"), reason="pausing a process needs SIGSTOP (POSIX only)")
def test_paused_leader_is_replaced_and_steps_down_when_resumed(tmp_path):
    """A frozen leader (like `docker compose pause`) looks dead to the others.
    They elect a new leader; when the old one resumes, the higher term makes
    it step down."""
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(3)]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        ports = [listener.getsockname()[1] for listener in sockets]
        urls = [f"http://127.0.0.1:{port}" for port in ports]
        cluster = ",".join(f"{node}={url}" for node, url in zip(REPLICAS, urls))
        processes = []
        for index, listener in enumerate(sockets):
            listener.close()
            env = os.environ.copy()
            env.update(
                RAFT_NODE_ID=REPLICAS[index],
                RAFT_PEERS=cluster,
                RAFT_DB=str(tmp_path / f"{REPLICAS[index]}.sqlite3"),
                RAFT_INITIAL_LEADER="storage-1",
                RAFT_HEARTBEAT_SECONDS="0.05",
                RAFT_RPC_TIMEOUT_SECONDS="0.3",
                RAFT_ELECTION_TIMEOUT_MIN_SECONDS="1.0",
                RAFT_ELECTION_TIMEOUT_MAX_SECONDS="2.0",
            )
            log = stack.enter_context((tmp_path / f"replica-{index}.log").open("w"))
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "library_system.replica:app",
                 "--host", "127.0.0.1", "--port", str(ports[index]), "--log-level", "warning"],
                cwd=PROJECT_ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
            ))
        paused = None
        with httpx.Client(timeout=0.5, trust_env=False) as client:

            def status(index):
                return client.get(urls[index] + "/internal/raft/status").json()

            try:
                wait_until(lambda: status(0)["role"] == "leader" and status(0)["ready_for_reads"], "initial leader")
                old_term = status(0)["term"]
                processes[0].send_signal(signal.SIGSTOP)
                paused = processes[0]
                wait_until(
                    lambda: any(status(i)["role"] == "leader" and status(i)["ready_for_reads"] for i in (1, 2)),
                    "new leader while the old one is paused",
                )
                new_leader = next(i for i in (1, 2) if status(i)["role"] == "leader")
                new_term = status(new_leader)["term"]
                assert new_term > old_term

                processes[0].send_signal(signal.SIGCONT)
                paused = None
                wait_until(
                    lambda: status(0)["role"] == "follower"
                    and status(0)["leader_id"] == REPLICAS[new_leader]
                    and status(0)["term"] >= new_term,
                    "resumed old leader steps down",
                )
            finally:
                if paused is not None:
                    paused.send_signal(signal.SIGCONT)
                for process in processes:
                    process.kill()
                    process.wait(timeout=5)
