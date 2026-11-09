"""Kite daily-login callback route.

The ONE unauthenticated HTTP surface: ``GET /kite/callback?request_token=...`` completes the daily Kite
login and does nothing else. Kite redirects the owner's phone browser (same LAN) to the registered URL
``http://<pc-lan-ip>:8400/kite/callback``; the route hands ``request_token`` to
:meth:`SessionManager.complete_login` (checksum exchange, token stored via ``Secrets``/DPAPI).

**Why unauthenticated is safe:**

* It can only complete a login: no code path from here to the OMS, the risk gate, or any read view.
* The ``request_token`` is single-use and seconds-lived (invalidated on the checksum exchange), so a replay
  buys nothing.
* The dashboard binds LAN-only (``:8400``); every other route requires the bearer token.
* A bad/foreign token is rejected by the checksum exchange and surfaced as HTTP 400.

**Fallback:** if the phone is off-LAN, the owner copies the ``request_token`` from the address bar and sends
``/token <value>`` on Telegram; the 400 page repeats that hint.

This is deliberately the *only* ``engine.api`` route exempt from bearer auth.
"""

from __future__ import annotations

import html

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

from engine.core.log import get_logger

_log = get_logger("engine.api.kite_callback")


# Minimal, self-contained HTML — the owner sees this in a phone browser, not a rendered dashboard.
_SUCCESS_HTML = (
    "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
    "<meta name='viewport' content='width=device-width, initial-scale=1'>"
    "<title>Login complete</title></head>"
    "<body style='font-family:system-ui,sans-serif;text-align:center;padding:2.5rem'>"
    "<h2>Session live</h2>"
    "<p>Login complete &mdash; you can close this tab.</p>"
    "</body></html>"
)


def _failure_html(detail: str) -> str:
    """Render the 400 body with a manual-paste fallback hint.

    ``detail`` is escaped before interpolation; it may carry a provider/exception message.
    """
    safe = html.escape(detail)
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>Login failed</title></head>"
        "<body style='font-family:system-ui,sans-serif;text-align:center;padding:2.5rem'>"
        "<h2>Login failed</h2>"
        f"<p>{safe}</p>"
        "<p>Fallback: copy the <code>request_token</code> from this page's URL and send "
        "<code>/token &lt;value&gt;</code> on Telegram.</p>"
        "</body></html>"
    )


def build_kite_callback_router(session_manager, clock) -> APIRouter:
    """Build the unauthenticated ``GET /kite/callback`` router (mounted without the bearer dependency).

    ``session_manager.complete_login(request_token)`` is awaited and raises on failure rather than
    returning a flag. ``clock`` only stamps the completed-login log line.
    """
    router = APIRouter(tags=["kite-login"])

    @router.get("/kite/callback", response_class=HTMLResponse)
    async def kite_callback(
        request_token: str | None = Query(
            default=None,
            description="Single-use Kite login token appended by the provider redirect.",
        ),
        status: str | None = Query(
            default=None,
            description="Provider-supplied login status (informational; e.g. 'success').",
        ),
        action: str | None = Query(
            default=None,
            description="Provider-supplied action hint (informational).",
        ),
    ) -> HTMLResponse:
        """Complete the daily Kite login: 200 on success, 400 with the `/token` fallback hint on any failure."""
        # A missing/blank request_token (URL opened directly, or Kite returned an error) is a 400, not a 500.
        if not request_token or not request_token.strip():
            _log.warning(
                "kite_callback.missing_token",
                status=status,
                action=action,
                has_token=bool(request_token),
            )
            return HTMLResponse(
                content=_failure_html("No request_token in the callback URL."),
                status_code=400,
            )

        token = request_token.strip()
        # Never log the token value itself (single-use credential); a length is enough to diagnose.
        _log.info(
            "kite_callback.received",
            token_len=len(token),
            status=status,
            action=action,
        )

        try:
            await session_manager.complete_login(token)
        except Exception as exc:  # noqa: BLE001 — any failure becomes the owner-visible 400 fallback.
            _log.error(
                "kite_callback.failed",
                error=type(exc).__name__,
                detail=str(exc),
                status=status,
            )
            return HTMLResponse(
                content=_failure_html(f"Could not complete login ({type(exc).__name__})."),
                status_code=400,
            )

        _log.info("kite_callback.completed", at=clock.now().isoformat())
        return HTMLResponse(content=_SUCCESS_HTML, status_code=200)

    return router
