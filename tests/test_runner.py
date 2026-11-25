"""Regression check for the real four-process runner and owned-child cleanup."""

import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PORTS = (8100, 8001, 8002, 8003)
STOP_SIGNAL = signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM


def listening(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.1)
        return probe.connect_ex(("127.0.0.1", port)) == 0


def test_runner_stops_all_services_on_interrupt(tmp_path):
    if any(listening(port) for port in PORTS):
        pytest.skip("The runner's fixed development ports are already in use")

    env = os.environ.copy()
    env["CATALOG_DB"] = str(tmp_path / "catalog.sqlite3")
    log_path = tmp_path / "runner.log"
    stopped = False
    with log_path.open("w", encoding="utf-8") as output:
        runner = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "scripts" / "run_local.py")],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=os.name != "nt",
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
        try:
            deadline = time.monotonic() + 20
            with httpx.Client(timeout=0.3, trust_env=False) as client:
                while time.monotonic() < deadline:
                    ready = []
                    for port in PORTS:
                        try:
                            response = client.get(f"http://127.0.0.1:{port}/health")
                            response.raise_for_status()
                            health = response.json()
                            ready.append(
                                health.get("mode") == "single-node-stand-in"
                                if port == 8100 else health.get("status") == "ready"
                            )
                        except (httpx.HTTPError, ValueError):
                            ready.append(False)
                    if all(ready):
                        break
                    assert runner.poll() is None, log_path.read_text(encoding="utf-8")
                    time.sleep(0.1)
                else:
                    pytest.fail("Runner services did not become ready:\n" + log_path.read_text(encoding="utf-8"))

            runner.send_signal(STOP_SIGNAL)
            assert runner.wait(timeout=10) == 0
            deadline = time.monotonic() + 3
            while any(listening(port) for port in PORTS) and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not any(listening(port) for port in PORTS), "Runner left a child service listening"
            stopped = True
        finally:
            if runner.poll() is None:
                runner.send_signal(STOP_SIGNAL)
                try:
                    runner.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    runner.kill()
                    runner.wait()
            if not stopped:
                # Only children identified in this runner's own server logs are ours.
                for pid in re.findall(r"Started server process \[(\d+)\]", log_path.read_text(encoding="utf-8")):
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                    except OSError:
                        pass
