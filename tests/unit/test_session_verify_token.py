"""SessionManager.verify_token — the startup live liveness probe (fourth cold-start-family defect,
2026-07-21 lockout). Behavioural ``token_valid()`` cannot tell a still-honoured token from an expired
one no live call has yet hit, so a boot on yesterday's token passed the self-test and ground the
warm-up backfill on a dead token. The probe makes one authenticated ``kc.profile()`` call and pins
``token_valid()`` to the broker's live verdict BEFORE hydrate/warm-up run — see R6/A5.

``_connect`` is monkeypatched to a fake pykiteconnect (mirrors test_post_login_recovery) so no network
checksum exchange or real credential store is touched; the fake Secrets follows the seams used by the
real :class:`Secrets` (has/get_optional/get/set)."""

from __future__ import annotations

import pytest
from kiteconnect.exceptions import TokenException

from engine.broker.session import SessionManager
from engine.core.secrets import KITE_ACCESS_TOKEN, KITE_API_KEY, KITE_API_SECRET


class _FakeSecrets:
    """The Secrets surface SessionManager consumes (has/get_optional/get/set)."""

    def __init__(self, values: dict[str, str] | None = None) -> None:
        self._d = dict(values or {})

    def has(self, key: str) -> bool:
        return key in self._d

    def get_optional(self, key: str) -> str | None:
        return self._d.get(key)

    def get(self, key: str) -> str:
        if key not in self._d:
            raise KeyError(key)
        return self._d[key]

    def set(self, key: str, value: str) -> None:
        self._d[key] = value


class _FakeKC:
    """pykiteconnect stand-in: ``profile()`` returns/raises as configured; ``generate_session`` +
    ``set_access_token`` back a subsequent complete_login (the recovery path)."""

    def __init__(self, *, profile_result=None, profile_raises: Exception | None = None) -> None:
        self._profile_result = profile_result
        self._profile_raises = profile_raises
        self.access_token: str | None = None

    def profile(self):
        if self._profile_raises is not None:
            raise self._profile_raises
        return self._profile_result

    def generate_session(self, request_token, api_secret):
        return {"access_token": "fresh-tok"}

    def set_access_token(self, token):
        self.access_token = token


def _session(secrets: _FakeSecrets, kc: _FakeKC | None = None) -> SessionManager:
    session = SessionManager(secrets, None)  # clock unused on the branches under test
    if kc is not None:
        session._connect = lambda: kc  # avoid the real network / KiteConnect construction
    return session


@pytest.mark.asyncio
async def test_verify_token_valid_keeps_token_valid(clock) -> None:
    secrets = _FakeSecrets({KITE_API_KEY: "ak", KITE_ACCESS_TOKEN: "tok"})
    session = SessionManager(secrets, clock)
    session._connect = lambda: _FakeKC(profile_result={"user_id": "AB1234"})

    assert await session.verify_token() == "valid"
    assert session.token_valid() is True


@pytest.mark.asyncio
async def test_verify_token_rejected_then_login_recovers(clock) -> None:
    """A live TokenException marks the token dead; a subsequent complete_login flips it back valid."""
    secrets = _FakeSecrets({KITE_API_KEY: "ak", KITE_API_SECRET: "as", KITE_ACCESS_TOKEN: "tok"})
    session = SessionManager(secrets, clock)
    kc = _FakeKC(profile_raises=TokenException("Incorrect api_key or access_token"))
    session._connect = lambda: kc

    assert await session.verify_token() == "rejected"
    assert session.token_valid() is False

    await session.complete_login("req-token")   # both login paths land here
    assert session.token_valid() is True


@pytest.mark.asyncio
async def test_verify_token_inconclusive_leaves_state_untouched(clock) -> None:
    """A network error is NOT invalidity: state is untouched and token_valid() stays True."""
    secrets = _FakeSecrets({KITE_API_KEY: "ak", KITE_ACCESS_TOKEN: "tok"})
    session = SessionManager(secrets, clock)
    session._connect = lambda: _FakeKC(profile_raises=ConnectionError("dns down"))

    assert await session.verify_token() == "inconclusive"
    assert session.token_valid() is True


@pytest.mark.asyncio
async def test_verify_token_absent_when_api_key_but_no_token(clock) -> None:
    session = _session(_FakeSecrets({KITE_API_KEY: "ak"}))   # api_key, no access token
    assert await session.verify_token() == "absent"
    assert session.token_valid() is False


@pytest.mark.asyncio
async def test_verify_token_no_api_key_on_fresh_install(clock) -> None:
    session = _session(_FakeSecrets({}))   # nothing seeded
    assert await session.verify_token() == "no_api_key"


# ===================================================================== WO-23: breaker-vs-probe split
# 2026-08-20 live incident: the 11:26:40 LIVE TokenException produced ZERO owner notifications. The
# mid-day circuit breaker (``on_token_rejected`` → freeze entries + critical alert + login prompt)
# deduped against ``_rejected``, which the boot probe ALSO sets (deliberately silently) — so the
# day's first genuine rejection looked like a burst duplicate and the alert was swallowed. The
# breaker now dedups on its own ``_breaker_fired`` flag, which the probe never touches.


def _breaker_session(clock, kc: _FakeKC, fired: list[int]) -> SessionManager:
    """A session wired the way ``engine.ops.main`` wires it: the invalidation hook is the freeze +
    critical-alert + login-prompt path, recorded here as one entry per firing."""
    secrets = _FakeSecrets({KITE_API_KEY: "ak", KITE_API_SECRET: "as", KITE_ACCESS_TOKEN: "tok"})
    session = SessionManager(secrets, clock)
    session._connect = lambda: kc

    async def hook() -> None:
        fired.append(1)

    session.set_invalidation_hook(hook)
    return session


@pytest.mark.asyncio
async def test_boot_probe_rejection_no_longer_suppresses_the_live_breaker(clock) -> None:
    """THE incident: boot probe rejected, then a live call rejects ⇒ the owner IS alerted (once)."""
    fired: list[int] = []
    session = _breaker_session(clock, _FakeKC(profile_raises=TokenException("bad token")), fired)

    assert await session.verify_token() == "rejected"
    assert fired == []                       # boot comms stay with the boot path (login_prompt)

    await session.on_token_rejected()        # 11:26:40 — the real live rejection
    assert fired == [1]                      # pre-fix: [] (silently swallowed)
    assert session.token_valid() is False


@pytest.mark.asyncio
async def test_live_rejection_burst_still_fires_once(clock) -> None:
    """The dedup that MATTERS is preserved: a burst of 403s is one alert, not one per call."""
    fired: list[int] = []
    session = _breaker_session(clock, _FakeKC(), fired)

    await session.on_token_rejected()
    await session.on_token_rejected()
    await session.on_token_rejected()
    assert fired == [1]


@pytest.mark.asyncio
async def test_relogin_rearms_the_breaker(clock) -> None:
    """``complete_login`` clears ``_breaker_fired`` alongside ``_rejected`` — a token that dies
    AGAIN after a re-login must alert again (twice in one day is a real shape)."""
    fired: list[int] = []
    session = _breaker_session(clock, _FakeKC(), fired)

    await session.on_token_rejected()
    assert fired == [1]

    await session.complete_login("req-token")
    assert session.token_valid() is True

    await session.on_token_rejected()
    assert fired == [1, 1]


@pytest.mark.asyncio
async def test_boot_probe_rejection_alone_never_fires_the_breaker(clock) -> None:
    """The probe's deliberate silence is intact: no freeze/critical-alert from the boot path itself
    (main.py's needs_login self-test owns boot comms), and it does not pre-arm the breaker either."""
    fired: list[int] = []
    session = _breaker_session(clock, _FakeKC(profile_raises=TokenException("bad token")), fired)

    assert await session.verify_token() == "rejected"
    assert fired == []
    assert session._breaker_fired is False   # nothing to dedup against later
