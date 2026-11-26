"""Live web dashboard + interactive Patron & Staff console.

Shows Raft node roles, terms, commit progress, branch cache health, and a
Lamport-clock-ordered event feed, plus an interactive UI to log in, search,
borrow, return, renew, place holds, and add/remove books through any branch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
import os
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, Response
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
  .topbar { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; margin-bottom: 14px; }
  h1 { margin: 0 0 4px; font-size: 22px; }
  .sub { color: var(--muted); font-size: 13px; }
  .tabs { display: flex; gap: 8px; }
  .tab { background: var(--card); color: var(--muted); border: 1px solid var(--border); border-radius: 8px; padding: 8px 14px; cursor: pointer; font-weight: 600; font-size: 13px; }
  .tab.active { background: var(--blue); color: #0f172a; border-color: var(--blue); }
  .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .stat { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; }
  .stat .label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
  .stat .val { font-size: 22px; font-weight: 700; margin-top: 4px; }
  h2 { font-size: 14px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin: 18px 0 10px; }
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
  th, td { padding: 9px 12px; text-align: left; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .05em; background: #172033; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .hidden { display: none; }
  .row { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }
  input, select { background: #0f172a; color: var(--text); border: 1px solid var(--border); border-radius: 6px; padding: 7px 10px; font-size: 13px; }
  button { background: var(--blue); color: #0f172a; border: none; border-radius: 6px; padding: 7px 12px; font-size: 12px; font-weight: 700; cursor: pointer; }
  button.secondary { background: #334155; color: var(--text); }
  button.danger { background: rgba(239,68,68,.22); color: #fca5a5; border: 1px solid rgba(239,68,68,.4); }
  button.ok { background: rgba(34,197,94,.22); color: #86efac; border: 1px solid rgba(34,197,94,.4); }
  button.warn { background: rgba(245,158,11,.22); color: #fcd34d; border: 1px solid rgba(245,158,11,.4); }
  .toast { margin: 12px 0; padding: 10px 14px; border-radius: 8px; font-size: 13px; border: 1px solid var(--border); background: #172033; }
  .toast.good { border-color: var(--green); color: #86efac; }
  .toast.bad { border-color: var(--red); color: #fca5a5; }
  .copy-pill { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; padding: 5px 0; border-bottom: 1px dashed #293548; }
  .copy-pill:last-child { border-bottom: none; }
</style>
</head>
<body>
  <div class="topbar">
    <div>
      <h1>Distributed Multi-Branch Library Dashboard</h1>
      <div class="sub">SOFE4790U Group 9 &middot; 3-Replica Raft Storage + 3 Stateless Branches &middot; Auto-refreshes every 1s</div>
    </div>
    <div class="tabs">
      <button class="tab active" id="tab-cluster" onclick="switchTab('cluster')">Cluster Dashboard</button>
      <button class="tab" id="tab-console" onclick="switchTab('console')">Patron &amp; Staff Console</button>
    </div>
  </div>

  <div class="summary" id="summary"></div>

  <div id="view-cluster">
    <h2>Storage Cluster (Raft Replicas)</h2>
    <div class="grid" id="replicas"></div>
    <h2>Branch Nodes (Stateless Compute + Cache)</h2>
    <div class="grid" id="branches"></div>
    <h2>Recent Events (Ordered by Lamport Clock)</h2>
    <table>
      <thead><tr><th>L-Clock</th><th>Node</th><th>Event</th><th>Summary</th><th>UTC Time</th></tr></thead>
      <tbody id="events"></tbody>
    </table>
  </div>

  <div id="view-console" class="hidden">
    <div class="card" style="margin-bottom:14px;">
      <div class="row" style="justify-content:space-between;">
        <div class="row">
          <label class="sub">Active Branch:</label>
          <select id="branchSelect" onchange="loadBooks()">
            <option value="branch-1">branch-1 (port 8001)</option>
            <option value="branch-2">branch-2 (port 8002)</option>
            <option value="branch-3">branch-3 (port 8003)</option>
          </select>
          <label class="sub">Account:</label>
          <select id="userSelect" onchange="syncDefaultPassword()">
            <option value="patron-1">patron-1 (Patron)</option>
            <option value="patron-2">patron-2 (Patron)</option>
            <option value="patron-3">patron-3 (Patron)</option>
            <option value="staff-1">staff-1 (Staff)</option>
            <option value="staff-2">staff-2 (Staff)</option>
          </select>
          <input id="passwordInput" type="password" value="password123" placeholder="Password" style="width:130px;" />
          <button onclick="doLogin()">Login</button>
          <button class="secondary" onclick="doLogout()">Logout</button>
        </div>
        <div id="authBadge" class="badge unavailable">Not Logged In</div>
      </div>
    </div>

    <div id="toast" class="toast hidden"></div>

    <div class="card" style="margin-bottom:14px;">
      <div class="row" style="justify-content:space-between; margin-bottom:10px;">
        <div class="row">
          <input id="searchInput" type="text" placeholder="Search title or author..." style="width:240px;" oninput="loadBooks()" />
          <label class="sub"><input type="checkbox" id="availOnly" onchange="loadBooks()" /> Available only</label>
          <button class="secondary" onclick="loadBooks()">Refresh Catalog</button>
        </div>
        <span id="cacheInfo" class="sub mono"></span>
      </div>
      <table>
        <thead><tr><th>Book</th><th>Copies &amp; Actions</th><th>Title Actions</th></tr></thead>
        <tbody id="catalogBody"></tbody>
      </table>
    </div>

    <div class="card">
      <h2 style="margin-top:0;">Staff: Add Book to Catalog</h2>
      <div class="row">
        <input id="addBookId" placeholder="Book ID (e.g. book-021)" style="width:165px;" />
        <input id="addTitle" placeholder="Title" style="width:220px;" />
        <input id="addAuthor" placeholder="Author" style="width:180px;" />
        <select id="addHomeBranch">
          <option value="branch-1">branch-1</option>
          <option value="branch-2">branch-2</option>
          <option value="branch-3">branch-3</option>
        </select>
        <input id="addCopies" type="number" min="1" max="10" value="2" style="width:70px;" />
        <button class="ok" onclick="doAddBook()">Add Book</button>
      </div>
    </div>
  </div>

<script>
let authToken = null;
let authUser = null;

function reqId(prefix) {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
}

function switchTab(tab) {
  document.getElementById('view-cluster').classList.toggle('hidden', tab !== 'cluster');
  document.getElementById('view-console').classList.toggle('hidden', tab !== 'console');
  document.getElementById('tab-cluster').classList.toggle('active', tab === 'cluster');
  document.getElementById('tab-console').classList.toggle('active', tab === 'console');
  if (tab === 'console') loadBooks();
}

function syncDefaultPassword() {
  const uid = document.getElementById('userSelect').value;
  document.getElementById('passwordInput').value = uid.startsWith('staff') ? 'staff123' : 'password123';
}

function showToast(msg, ok = true) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `toast ${ok ? 'good' : 'bad'}`;
}

function branch() {
  return document.getElementById('branchSelect').value;
}

async function doLogin() {
  const user_id = document.getElementById('userSelect').value;
  const password = document.getElementById('passwordInput').value;
  const res = await fetch(`/api/proxy/${branch()}/auth/login`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user_id, password}),
  });
  const data = await res.json();
  if (!res.ok) {
    showToast(`Login failed (${res.status}): ${data.detail || JSON.stringify(data)}`, false);
    return;
  }
  authToken = data.access_token;
  authUser = data.user;
  const badge = document.getElementById('authBadge');
  badge.textContent = `${authUser.user_id} (${authUser.role})`;
  badge.className = `badge ${authUser.role === 'staff' ? 'leader' : 'follower'}`;
  showToast(`Logged in as ${authUser.name} (${authUser.role}) via ${branch()} — token works across all branches!`, true);
  loadBooks();
}

function doLogout() {
  authToken = null;
  authUser = null;
  const badge = document.getElementById('authBadge');
  badge.textContent = 'Not Logged In';
  badge.className = 'badge unavailable';
  showToast('Logged out.', true);
}

async function sendCommand(payload) {
  const headers = {'Content-Type': 'application/json'};
  if (authToken) headers['Authorization'] = `Bearer ${authToken}`;
  const res = await fetch(`/api/proxy/${branch()}/commands`, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch { data = {detail: text}; }
  if (!res.ok) {
    showToast(`HTTP ${res.status} on ${branch()}: ${data.detail || text}`, false);
  } else if (!data.ok) {
    showToast(`Committed rejection on ${branch()} (rev ${data.revision}): ${data.error}`, false);
  } else {
    showToast(`Committed ${payload.operation} on ${payload.book_id} via ${branch()} (term ${data.term}, idx ${data.index}, rev ${data.revision})`, true);
  }
  await loadBooks();
  await refresh();
}

function actorId() {
  return authUser ? authUser.user_id : document.getElementById('userSelect').value;
}

function doCopyOp(op, bookId, copyId, version) {
  sendCommand({
    request_id: reqId(op),
    operation: op,
    actor_id: actorId(),
    book_id: bookId,
    copy_id: copyId,
    expected_version: version,
  });
}

function doHold(bookId) {
  sendCommand({
    request_id: reqId('hold'),
    operation: 'hold',
    actor_id: actorId(),
    book_id: bookId,
  });
}

function doRemoveBook(bookId) {
  sendCommand({
    request_id: reqId('remove'),
    operation: 'remove_book',
    actor_id: actorId(),
    book_id: bookId,
  });
}

function doAddBook() {
  const book_id = document.getElementById('addBookId').value.trim();
  const title = document.getElementById('addTitle').value.trim();
  const author = document.getElementById('addAuthor').value.trim();
  const home_branch = document.getElementById('addHomeBranch').value;
  const num_copies = parseInt(document.getElementById('addCopies').value, 10) || 1;
  if (!book_id || !title || !author) {
    showToast('Fill in Book ID, Title, and Author first.', false);
    return;
  }
  sendCommand({
    request_id: reqId('add'),
    operation: 'add_book',
    actor_id: actorId(),
    book_id,
    title,
    author,
    home_branch,
    num_copies,
  });
}

async function loadBooks() {
  const q = document.getElementById('searchInput').value;
  const avail = document.getElementById('availOnly').checked;
  const params = new URLSearchParams();
  if (q) params.set('q', q);
  if (avail) params.set('available', 'true');
  const res = await fetch(`/api/proxy/${branch()}/books?${params.toString()}`);
  if (!res.ok) {
    document.getElementById('cacheInfo').textContent = `${branch()} unavailable (HTTP ${res.status})`;
    document.getElementById('catalogBody').innerHTML = '';
    return;
  }
  const data = await res.json();
  document.getElementById('cacheInfo').textContent =
    `${data.cache.branch_id} | rev=${data.cache.revision} | stale=${data.cache.stale} | titles=${data.total}`;

  document.getElementById('catalogBody').innerHTML = data.books.map(b => {
    const copiesHtml = b.copies.map(c => {
      const status = c.available
        ? `<span class="badge ready">Available</span>`
        : `<span class="badge degraded">Borrowed by ${c.borrower_id} (due ${c.due_date}, ren=${c.renewals})</span>`;
      const btns = c.available
        ? `<button class="ok" onclick="doCopyOp('borrow','${b.book_id}','${c.copy_id}',${c.version})">Borrow</button>`
        : `<button class="secondary" onclick="doCopyOp('return','${b.book_id}','${c.copy_id}',${c.version})">Return</button>
           <button class="warn" onclick="doCopyOp('renew','${b.book_id}','${c.copy_id}',${c.version})">Renew</button>`;
      return `<div class="copy-pill"><span class="mono">${c.copy_id}</span> <span class="sub">(${c.branch_id}, v${c.version})</span> ${status} ${btns}</div>`;
    }).join('');
    const holds = b.holds.length ? `<div class="sub" style="margin-top:4px;">Holds (${b.holds.length}): ${b.holds.map(h => h.actor_id).join(', ')}</div>` : '';
    return `<tr>
      <td><strong>${b.title}</strong><div class="sub">${b.author} &middot; <span class="mono">${b.book_id}</span></div>${holds}</td>
      <td>${copiesHtml}</td>
      <td><div class="row">
        <button class="secondary" onclick="doHold('${b.book_id}')">Place Hold</button>
        <button class="danger" onclick="doRemoveBook('${b.book_id}')">Remove</button>
      </div></td>
    </tr>`;
  }).join('') || '<tr><td colspan="3" class="sub">No matching books.</td></tr>';
}

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

    def _branch_url(branch_id: str) -> str:
        url = branch_map.get(branch_id)
        if url is None:
            raise HTTPException(404, f"Unknown branch {branch_id!r}")
        return url

    @app.get("/health")
    def health():
        return {"status": "ok", "mode": "dashboard"}

    @app.get("/", response_class=HTMLResponse)
    def index():
        return DASHBOARD_HTML

    @app.post("/api/proxy/{branch_id}/auth/login")
    async def proxy_login(branch_id: str, request: Request):
        body = await request.json()
        try:
            res = await app.state.client.post(f"{_branch_url(branch_id)}/auth/login", json=body, timeout=5.0)
        except httpx.HTTPError as err:
            raise HTTPException(503, f"{branch_id} unreachable: {err}") from err
        return Response(res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))

    @app.get("/api/proxy/{branch_id}/books")
    async def proxy_books(branch_id: str, request: Request):
        try:
            res = await app.state.client.get(
                f"{_branch_url(branch_id)}/books", params=dict(request.query_params), timeout=5.0
            )
        except httpx.HTTPError as err:
            raise HTTPException(503, f"{branch_id} unreachable: {err}") from err
        return Response(res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))

    @app.post("/api/proxy/{branch_id}/commands")
    async def proxy_commands(
        branch_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        body = await request.json()
        headers = {"Authorization": authorization} if authorization else {}
        try:
            res = await app.state.client.post(
                f"{_branch_url(branch_id)}/commands", json=body, headers=headers, timeout=10.0
            )
        except httpx.HTTPError as err:
            raise HTTPException(503, f"{branch_id} unreachable: {err}") from err
        return Response(res.content, status_code=res.status_code, media_type=res.headers.get("content-type"))

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