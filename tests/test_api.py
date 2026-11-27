"""Unit tests for ScholarAssist API dependencies, middleware, and query validation."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.dependencies import get_opensearch_client, parse_opensearch_host
from src.api.middleware import RateLimitMiddleware


def _ensure_opensearch_stub(monkeypatch) -> None:
    """Allow API router tests to run cleanly when opensearchpy is not installed."""
    try:
        import opensearchpy  # noqa: F401
        return
    except ImportError:
        pass

    os_mod = ModuleType("opensearchpy")
    os_mod.OpenSearch = object  # type: ignore[attr-defined]
    exc_mod = ModuleType("opensearchpy.exceptions")
    exc_mod.NotFoundError = type("NotFoundError", (Exception,), {})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "opensearchpy", os_mod)
    monkeypatch.setitem(sys.modules, "opensearchpy.exceptions", exc_mod)


def test_parse_opensearch_host_handles_urls_without_explicit_port():
    https_cfg = parse_opensearch_host("https://search.scholarassist.org")
    assert https_cfg == {"host": "search.scholarassist.org", "port": 443, "use_ssl": True}

    http_cfg = parse_opensearch_host("http://opensearch:9205")
    assert http_cfg == {"host": "opensearch", "port": 9205, "use_ssl": False}

    bare_cfg = parse_opensearch_host("localhost")
    assert bare_cfg == {"host": "localhost", "port": 9200, "use_ssl": False}


def test_rate_limit_middleware_enforces_quota_and_exempts_health():
    test_app = FastAPI()
    test_app.add_middleware(RateLimitMiddleware, max_requests=2, window_seconds=60)

    @test_app.get("/v1/ping")
    def ping():
        return {"ok": True}

    @test_app.get("/v1/health")
    def health():
        return {"status": "ok"}

    client = TestClient(test_app)
    r1 = client.get("/v1/ping", headers={"X-API-Key": "test-client"})
    assert r1.status_code == 200
    assert r1.headers["X-RateLimit-Limit"] == "2"
    assert r1.headers["X-RateLimit-Remaining"] == "1"

    r2 = client.get("/v1/ping", headers={"X-API-Key": "test-client"})
    assert r2.status_code == 200
    assert r2.headers["X-RateLimit-Remaining"] == "0"

    r3 = client.get("/v1/ping", headers={"X-API-Key": "test-client"})
    assert r3.status_code == 429
    assert int(r3.headers["Retry-After"]) >= 1

    # Health check bypasses rate limit
    rh = client.get("/v1/health", headers={"X-API-Key": "test-client"})
    assert rh.status_code == 200


def test_records_search_rejects_inverted_year_range(monkeypatch):
    _ensure_opensearch_stub(monkeypatch)
    records_mod = importlib.import_module("src.api.routers.records")
    test_app = FastAPI()
    test_app.include_router(records_mod.router, prefix="/v1/records")

    class DummyOS:
        def search(self, **kwargs):
            return {"hits": {"hits": [], "total": {"value": 0}}}

        def close(self):
            pass

    test_app.dependency_overrides[get_opensearch_client] = lambda: DummyOS()
    try:
        client = TestClient(test_app)
        resp = client.get("/v1/records/search?q=transformers&year_start=2025&year_end=2020")
        assert resp.status_code == 422
        assert "year_start" in resp.json()["detail"]
    finally:
        test_app.dependency_overrides.clear()
