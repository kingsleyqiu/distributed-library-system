"""Live web dashboard showing Raft node roles, terms, commit progress,
branch cache health, and a Lamport-clock-ordered event feed."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import httpx


DEFAULT_REPLICAS = (
    "storage-1=http://127.0.0.1:8101,"
    "storage-2=http://127.0.0.1:8102,"
    "storage-3=http://127.0.0.1:8103"
)
DEFAULT_BRANCHES = (
    "branch-1=http://127.0.0.1:8001,"
    "branch-2=http://127.0.0.1:8002,"
    "branch-3=http://127.0.0.1:8003"
)


def parse_targets(spec: str) -> dict[str, str]:
    targets: dict[str, str] = {}
    for item in filter(None, (part.strip() for part in spec.split(","))):
        name, sep, url = item.partition("=")
        if not sep or not name.strip() or not url.strip():
            raise ValueError(f"Invalid target {item!r}; expected name=url")
        targets[name.strip()] = url.strip().rstrip("/")
    return targets


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any] | None:
    try:
        res = await client.get(url)
        res.raise_for_status()
        return res.json()
    except (httpx.HTTPError, ValueError):
        return None


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Distributed Library Cluster Dashboard</title>
<style>
  :root {
    --bg: #0f172a; --card: #1e293b; --border: #334155; --text: #f1f5f9;
    --muted: #94a3b8; --green: #22c55e; --amber: #f59e0b; --red: #ef4444; --blue: #38bdf8;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); padding: 20px; }
  h1 { margin: 0 0 4px; font-size: 22px; }
  .sub { color: var(--muted); font-size: 13px; margin-bottom: 18px; }
  .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .stat { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; }
  .stat .label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
  .stat .val { font-size: 22px; font-weight: 700; margin-top: 4px; }
  h2 { font-size: 15px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin: 20px 0 10px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(270px, 1fr)); gap: 12px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px; }
  .card-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
  .node-name { font-weight: 700; font-size: 16px; }
  .badge { padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 700; text-transform: uppercase; }
  .leader, .ready { background: rgba(34,197,94,.18); color: var(--green); }
  .follower { background: rgba(56,189,248,.18); color: var(--blue); }
  .candidate, .degraded { background: rgba(245,158,11,.18); color: var(--amber); }
  .down, .unavailable { background: rgba(239,68,68,.18); color: var(--red); }
  .kv { display: grid; grid-template-columns: 1fr auto; gap: 5px 12px; font-size: 13px; }
  .kv span:nth-child(odd) { color: var(--muted); }
  table { width: 100%; border-collapse: collapse; background: var(--card); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; font-size: 13px; }
  th, td { padding: 9px 12px; text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .05em; background: #172033; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
</style>
</head>
<body>
  <h1>Distributed Multi-Branch Library Dashboard</h1>
  <div class="sub">SOFE4790U Group 9 &middot; 3-Replica Raft Storage + 3 Stateless Branches &middot; Auto-refreshes every 1s</div>

  <div class="summary" id="summary"></div>
  <h2>Storage Cluster (Raft Replicas)</h2>
  <div class="grid" id="replicas"></div>
  <h2>Branch Nodes (Stateless Compute + Cache)</h2>
  <div class="grid" id="branches"></div>
  <h2>Recent Events (Ordered by Lamport Clock)</h2>
  <table>
    <thead><tr><th>L-Clock</th><th>Node</th><th>Event</th><th>Summary</th><th>UTC Time</th></tr></thead>
    <tbody id="events"></tbody>
  </table>

<script>
async function refresh() {
  try {
    const res = await fetch('/api/cluster');
    const data = await res.json();

    const upReplicas = data.replicas.filter(r => r.online).length;
    const upBranches = data.branches.filter(b => b.online).length;
    document.getElementById('summary').innerHTML = `
      <div class="stat"><div class="label">Current Leader</div><div class="val">${data.leader_id || 'None'}</div></div>
      <div class="stat"><div class="label">Highest Term</div><div class="val">${data.term}</div></div>
      <div class="stat"><div class="label">Commit Index</div><div class="val">${data.commit_index}</div></div>
      <div class="stat"><div class="label">Replicas Online</div><div class="val">${upReplicas} / ${data.replicas.length}</div></div>
      <div class="stat"><div class="label">Branches Online</div><div class="val">${upBranches} / ${data.branches.length}</div></div>
    `;

    document.getElementById('replicas').innerHTML = data.replicas.map(r => {
      const badge = r.online ? r.role : 'down';
      return `<div class="card">
        <div class="card-head">
          <span class="node-name">${r.node_id}</span>
          <span class="badge ${badge}">${badge}</span>
        </div>
        <div class="kv">
          <span>Term</span><span class="mono">${r.online ? r.term : '-'}</span>
          <span>Leader</span><span class="mono">${r.online ? (r.leader_id || '-') : '-'}</span>
          <span>Commit / Applied</span><span class="mono">${r.online ? `${r.commit_index} / ${r.last_applied}` : '-'}</span>
          <span>Log Length</span><span class="mono">${r.online ? r.last_log_index : '-'}</span>
          <span>Catalog Revision</span><span class="mono">${r.online ? r.revision : '-'}</span>
          <span>Lamport Clock</span><span class="mono">${r.online ? r.lamport_clock : '-'}</span>
        </div>
      </div>`;
    }).join('');

    document.getElementById('branches').innerHTML = data.branches.map(b => {
      const badge = b.online ? b.status : 'down';
      return `<div class="card">
        <div class="card-head">
          <span class="node-name">${b.branch_id}</span>
          <span class="badge ${badge}">${badge}</span>
        </div>
        <div class="kv">
          <span>Storage Reachable</span><span class="mono">${b.online ? String(b.storage_available) : 'false'}</span>
          <span>Cached Revision</span><span class="mono">${b.online ? (b.revision ?? '-') : '-'}</span>
          <span>Lamport Clock</span><span class="mono">${b.online ? b.lamport_clock : '-'}</span>
        </div>
      </div>`;
    }).join('');

    const rows = data.events.slice(-40).reverse().map(e => `
      <tr>
        <td class="mono">L=${e.lamport_time}</td>
        <td class="mono">${e.node_id}</td>
        <td class="mono">${e.event_type}</td>
        <td>${e.summary}</td>
        <td class="mono">${e.wall_time.slice(11, 19)}Z</td>
      </tr>
    `).join('');
    document.getElementById('events').innerHTML = rows || '<tr><td colspan="5" style="color:var(--muted)">No events recorded yet.</td></tr>';
  } catch (err) {
    console.error(err);
  }
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""


def create_app(
    replicas: Mapping[str, str] | None = None,
    branches: Mapping[str, str] | None = None,
    poll_timeout: float = 0.8,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    replica_map = dict(replicas if replicas is not None else parse_targets(DEFAULT_REPLICAS))
    branch_map = dict(branches if branches is not None else parse_targets(DEFAULT_BRANCHES))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with httpx.AsyncClient(timeout=poll_timeout, transport=transport, trust_env=False) as client:
            app.state.client = client
            yield

    app = FastAPI(title="Distributed Library Dashboard", lifespan=lifespan)

    @app.get("/health")
    def health():
        return {"status": "ok", "mode": "dashboard"}

    @app.get("/", response_class=HTMLResponse)
    def index():
        return DASHBOARD_HTML

    @app.get("/api/cluster")
    async def cluster_status():
        client: httpx.AsyncClient = app.state.client

        async def poll_replica(node_id: str, url: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            status, ev = await asyncio.gather(
                _fetch_json(client, f"{url}/internal/raft/status"),
                _fetch_json(client, f"{url}/internal/events"),
            )
            if status is None:
                return {"node_id": node_id, "online": False}, []
            return (
                {
                    **status,
                    "online": True,
                    "lamport_clock": ev["lamport_clock"] if ev else 0,
                },
                ev["events"] if ev else [],
            )

        async def poll_branch(branch_id: str, url: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            health_data, ev = await asyncio.gather(
                _fetch_json(client, f"{url}/health"),
                _fetch_json(client, f"{url}/internal/events"),
            )
            if health_data is None:
                return {"branch_id": branch_id, "online": False}, []
            return (
                {
                    **health_data,
                    "online": True,
                    "lamport_clock": ev["lamport_clock"] if ev else 0,
                },
                ev["events"] if ev else [],
            )

        replica_results = await asyncio.gather(
            *(poll_replica(nid, url) for nid, url in replica_map.items())
        )
        branch_results = await asyncio.gather(
            *(poll_branch(bid, url) for bid, url in branch_map.items())
        )

        replica_cards = [r for r, _ in replica_results]
        branch_cards = [b for b, _ in branch_results]
        all_events: list[dict[str, Any]] = []
        for _, evs in (*replica_results, *branch_results):
            all_events.extend(evs)

        # Total ordering via Lamport clock with node_id tie-breaker (Lamport 1978).
        all_events.sort(key=lambda e: (e["lamport_time"], e["node_id"], e["wall_time"]))

        online_replicas = [r for r in replica_cards if r["online"]]
        leader = next((r["node_id"] for r in online_replicas if r.get("role") == "leader"), None)
        highest_term = max((r.get("term", 0) for r in online_replicas), default=0)
        commit_index = max((r.get("commit_index", 0) for r in online_replicas), default=0)

        return {
            "leader_id": leader,
            "term": highest_term,
            "commit_index": commit_index,
            "replicas": replica_cards,
            "branches": branch_cards,
            "events": all_events[-100:],
        }

    return app


app = create_app(
    replicas=parse_targets(os.getenv("DASHBOARD_REPLICAS", DEFAULT_REPLICAS)),
    branches=parse_targets(os.getenv("DASHBOARD_BRANCHES", DEFAULT_BRANCHES)),
)