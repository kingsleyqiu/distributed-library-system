"""Branches forward immutable commands and never invent loan outcomes."""

import json

from fastapi.testclient import TestClient
import httpx
import pytest

from library_system.branch import create_app
from library_system.models import CommandEnvelope
from library_system.storage import CatalogRepository
from test_circulation import command


class StorageTransport:
    def __init__(self, repository):
        self.repository = repository
        self.commands = []
        self.fail_after_commit = False
        self.fail_reads = False
        self.bad_result = False

    def __call__(self, request):
        if request.method == "POST":
            payload = json.loads(request.content)
            self.commands.append(payload)
            result = self.repository.execute(CommandEnvelope(**payload))
            if self.fail_after_commit:
                raise httpx.ReadTimeout("Lost response", request=request)
            if self.bad_result:
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(200, json=result.model_dump(mode="json"))
        if self.fail_reads:
            raise httpx.ConnectError("Read unavailable", request=request)
        if request.url.path == "/internal/catalog/version":
            return httpx.Response(200, json=self.repository.version().model_dump(mode="json"))
        return httpx.Response(200, json=self.repository.snapshot().model_dump(mode="json"))


@pytest.fixture
def upstream(tmp_path):
    repository = CatalogRepository(tmp_path / "catalog.sqlite3")
    repository.initialize()
    return StorageTransport(repository)


def branch(upstream, branch_id="branch-1"):
    return TestClient(create_app(
        branch_id=branch_id, storage_url="http://storage", sync_interval=3600,
        transport=httpx.MockTransport(upstream),
    ))


def test_branch_refreshes_catalog_after_commit_and_uses_storage_for_versions(upstream):
    with branch(upstream) as first, branch(upstream, "branch-2") as second:
        borrowed = first.post("/commands", json=command())
        assert borrowed.status_code == 200 and borrowed.json()["ok"]
        detail = first.get("/books/book-001").json()
        assert detail["cache"]["revision"] == 2 and not detail["book"]["copies"][0]["available"]
        assert second.get("/books/book-001").json()["book"]["copies"][0]["available"]
        refused = second.post("/commands", json=command("bob", actor_id="bob"))
        assert refused.status_code == 200 and refused.json()["error"] == "version_conflict"
        assert second.get("/books/book-001").json()["cache"]["revision"] == 2
        assert upstream.commands[0] == command()
        assert second.post("/commands", json=command()).json() == borrowed.json()


def test_commit_response_lost_then_retried_with_same_id(upstream):
    with branch(upstream) as client:
        upstream.fail_after_commit = True
        response = client.post("/commands", json=command())
        assert response.status_code == 503 and "same request_id" in response.json()["detail"]
        assert client.get("/books/book-001").json()["book"]["copies"][0]["available"]
        assert client.get("/health").json()["status"] == "degraded"
        upstream.fail_after_commit = False
        client.portal.call(client.app.state.catalog_cache.refresh)
        retried = client.post("/commands", json=command())
        assert retried.status_code == 200 and retried.json()["ok"]
        assert upstream.repository.snapshot().revision == 2
        assert retried.json()["result"]["copy"]["version"] == 1


def test_success_is_preserved_when_cache_refresh_fails(upstream):
    with branch(upstream) as client:
        upstream.fail_reads = True
        result = client.post("/commands", json=command())
        assert result.status_code == 200 and result.json()["ok"]
        assert client.get("/health").json()["status"] == "degraded"
        assert upstream.repository.snapshot().revision == 2


def test_malformed_command_result_is_unknown_not_success(upstream):
    with branch(upstream) as client:
        upstream.bad_result = True
        assert client.post("/commands", json=command()).status_code == 503
        assert client.get("/health").json()["status"] == "degraded"
