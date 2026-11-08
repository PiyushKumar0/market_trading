"""Secrets via Windows Credential Manager / DPAPI.

Backed by ``keyring`` (DPAPI on Windows). Secrets NEVER live in the repo, YAML, or plaintext on disk. Seed them once with ``scripts/dpapi_set.py``. The Claude OAuth token is stored here but no
engine code reads it or puts it in any environment today: the harness passes the pinned CLI only
``env={"DISABLE_AUTOUPDATER": "1"}``, and that CLI authenticates with its own stored login.
"""

from __future__ import annotations

import keyring

SERVICE = "market_trading"

# Canonical secret keys. Documented (names only) in .env.example.
KITE_API_KEY = "kite_api_key"
KITE_API_SECRET = "kite_api_secret"
KITE_ACCESS_TOKEN = "kite_access_token"          # rotates daily (~06:00 IST)
TELEGRAM_BOT_TOKEN = "telegram_bot_token"
DASHBOARD_TOKEN = "dashboard_token"
CLAUDE_CODE_OAUTH_TOKEN = "claude_code_oauth_token"  # stored only, read by no engine code

# Secrets that must be present for the engine to operate (checked by the startup self-test).
REQUIRED_AT_STARTUP = (
    KITE_API_KEY,
    KITE_API_SECRET,
    TELEGRAM_BOT_TOKEN,
    DASHBOARD_TOKEN,
)


class MissingSecretError(KeyError):
    """Raised when a required secret is absent from the credential store."""


class Secrets:
    """Read/write secrets in the Windows Credential Manager."""

    def __init__(self, service: str = SERVICE) -> None:
        self._service = service

    def get(self, key: str) -> str:
        value = keyring.get_password(self._service, key)
        if value is None:
            raise MissingSecretError(f"secret '{key}' not found in credential store '{self._service}'")
        return value

    def get_optional(self, key: str) -> str | None:
        return keyring.get_password(self._service, key)

    def has(self, key: str) -> bool:
        return keyring.get_password(self._service, key) is not None

    def set(self, key: str, value: str) -> None:
        keyring.set_password(self._service, key, value)

    def delete(self, key: str) -> None:
        try:
            keyring.delete_password(self._service, key)
        except keyring.errors.PasswordDeleteError:
            pass

    def missing_required(self) -> list[str]:
        """Return the required-at-startup secrets that are absent (for the self-test)."""
        return [k for k in REQUIRED_AT_STARTUP if not self.has(k)]
