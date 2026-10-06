"""Route-enumeration auth test: every route of ``create_app()`` is bearer-gated unless allowlisted."""

from __future__ import annotations

import pytest
from fastapi import WebSocketDisconnect
from fastapi.routing import APIRoute, APIWebSocketRoute
from fastapi.testclient import TestClient
from starlette.routing import Mount

from engine.api.app import _require_owner, create_app
from engine.core.secrets import DASHBOARD_TOKEN

_UNAUTHENTICATED = {"/kite/callback", "/notifications-ui", "/notifications-ui/"}


class _Secrets:
    def get(self, key: str) -> str:
        if key == DASHBOARD_TOKEN:
            return "tok"
        raise KeyError(key)

    def has(self, key: str) -> bool:
        return key == DASHBOARD_TOKEN


@pytest.fixture(scope="module")
def app():
    return create_app(secrets=_Secrets())


def _walk(routes):
    for route in routes:
        inner = getattr(route, "original_router", None)
        yield from _walk(inner.routes) if inner is not None else [route]


def _requires_owner(route: APIRoute) -> bool:
    return any(d.call is _require_owner for d in route.dependant.dependencies)


def test_every_http_route_is_gated_or_allowlisted(app):
    routes = list(_walk(app.routes))
    assert any(isinstance(r, APIRoute) for r in routes)
    for route in routes:
        if isinstance(route, (APIWebSocketRoute, Mount)):
            continue
        assert isinstance(route, APIRoute), f"non-APIRoute HTTP route {route.path}"
        assert route.path in _UNAUTHENTICATED or _requires_owner(route), f"unauthenticated {route.path}"


def test_openapi_not_served(app):
    assert TestClient(app).get("/openapi.json").status_code == 404


def test_ws_live_rejects_tokenless_socket(app):
    with pytest.raises(WebSocketDisconnect) as exc, TestClient(app).websocket_connect("/ws/live"):
        pass
    assert exc.value.code == 1008


def test_static_mount_is_last(app):
    mounts = [i for i, r in enumerate(app.routes) if isinstance(r, Mount)]
    assert mounts in ([], [len(app.routes) - 1])
