"""Kite daily-login callback route (§3.2.11; R6, R10).

The ONE unauthenticated HTTP surface in the platform. It exposes a single route::

    GET /kite/callback?request_token=...

which completes the daily Kite Connect login (R6) and does NOTHING else. After the owner taps the
login link on their phone and clears Kite's TOTP screen, Kite redirects the phone's browser to the
registered redirect URL ``http://<pc-lan-ip>:8400/kite/callback`` (the phone is on the same LAN, §10.1),
appending ``request_token`` (and informational ``status``/``action``) as query parameters. This route
hands that ``request_token`` to :meth:`SessionManager.complete_login`, which performs the checksum
exchange and stores the resulting access token via ``Secrets`` (DPAPI).

**Why it is safe to leave unauthenticated (§2.4, trust-boundary R10):**

* It can *only complete a login*. It never reads platform/book state and never originates an order —
  there is no code path from here to the OMS, the risk gate, or any read view.
* The ``request_token`` is single-use and seconds-lived: Kite invalidates it on the checksum exchange,
  so a replay (or a stale value lingering in a browser history / proxy log) buys an attacker nothing.
* The dashboard binds LAN-only (``:8400``); this token transits the owner's own Wi-Fi (TLS/Tailscale is
  the documented upgrade path, §14 Q11). Every other route requires the bearer token.
* The worst an unauthenticated caller can do is submit a bad/foreign ``request_token``, which the
  checksum exchange rejects — surfaced here as an HTTP 400 with a manual-paste fallback hint.

**Fallback (§10.1 step 4):** if the phone is off-LAN (so the redirect can't reach the PC), the owner
copies the ``request_token`` out of the browser address bar and sends ``/token <value>`` on Telegram,
or pastes it into the dashboard. The 400 page returned on failure repeats that hint.

This route is deliberately the *only* member of ``engine.api`` exempt from bearer auth.
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
    """Render the 400 body with a manual-paste fallback hint (§10.1 step 4).

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
        "<code>/token &lt;value&gt;</code> on Telegram (or paste it into the dashboard).</p>"
        "</body></html>"
    )


def build_kite_callback_router(session_manager, clock) -> APIRouter:
    """Build the unauthenticated ``GET /kite/callback`` router (R6).

    Args:
        session_manager: a ``SessionManager`` (§3.2.4); only ``complete_login`` is used here. Its
            ``complete_login(request_token)`` is awaited and raises on failure (checksum mismatch,
            expired/foreign token, provider error) rather than returning a flag.
        clock: the platform ``Clock`` (the single sanctioned "now"). Held for symmetry with the rest of
            ``engine.api`` and to stamp the IST instant of a completed login in the success log; this
            route makes no trading-relevant time decision of its own.

    Returns:
        An ``APIRouter`` with exactly one route, mounted by the app *without* the bearer-auth
        dependency that guards every other route (§2.4).
    """
    router = APIRouter(tags=["kite-login"])

    @router.get("/kite/callback", response_class=HTMLResponse)
    async def kite_callback(
        request_token: str | None = Query(
            default=None,
            description="Single-use Kite login token appended by the provider redirect (R6).",
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
        """Complete the daily Kite login and return a tiny phone-friendly page.

        On success: HTTP 200 with "Login complete — you can close this tab".
        On any failure: HTTP 400 with the manual-paste (`/token`) fallback hint.
        """
        # Guard the provider redirect shape: a missing/blank request_token is the common
        # "owner opened the URL directly / Kite returned an error" case — treat as a 400, not a 500.
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
