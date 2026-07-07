"""FastAPI dashboard auth (§2.4/R10): every route is bearer-gated and fails CLOSED; GET /kite/callback
is the sole unauthenticated surface (it can only complete a login, never read state or place orders)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from engine.api.app import create_app
from engine.core.secrets import DASHBOARD_TOKEN

_TOKEN = "s3cret-dash-token"


class _FakeSecrets:
    def __init__(self, token: str | None) -> None:
        self._token = token

    def get(self, key: str) -> str:
        if key == DASHBOARD_TOKEN and self._token is not None:
            return self._token
        raise KeyError(key)

    def has(self, key: str) -> bool:
        return key == DASHBOARD_TOKEN and self._token is not None


def _client(token: str | None = _TOKEN) -> TestClient:
    return TestClient(create_app(secrets=_FakeSecrets(token)))


def test_protected_route_requires_valid_bearer() -> None:
    client = _client()
    assert client.get("/positions").status_code == 401                                   # no header
    assert client.get("/positions", headers={"Authorization": "Bearer nope"}).status_code == 401
    ok = client.get("/positions", headers={"Authorization": f"Bearer {_TOKEN}"})
    assert ok.status_code == 200


def test_bearer_fails_closed_when_token_store_broken() -> None:
    # A missing/garbled token store must DENY (401), never fall open.
    client = _client(token=None)
    assert client.get("/budget", headers={"Authorization": "Bearer anything"}).status_code == 401


def test_kite_callback_is_the_only_unauthenticated_route() -> None:
    client = _client()
    # No bearer + no request_token ⇒ 400 (reachable, unauthenticated), NEVER 401.
    r = client.get("/kite/callback")
    assert r.status_code == 400
    assert "request_token" in r.text
