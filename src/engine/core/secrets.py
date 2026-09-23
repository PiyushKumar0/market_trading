"""Secrets via Windows Credential Manager / DPAPI (R10).

Backed by ``keyring`` (DPAPI on Windows). Secrets NEVER live in the repo, YAML, or plaintext on disk
(§2.4). Seed them once with ``scripts/dpapi_set.py``. The engine reads the Claude OAuth token from here
at startup and injects it into the SDK CLI child's environment (§2.2) rather than persisting it in a
service/user env var.
"""

from __future__ import annotations

import keyring

SERVICE = "market_trading"

# Canonical secret keys (§3.2.1). Documented (names only) in .env.example.
KITE_API_KEY = "kite_api_key"
KITE_API_SECRET = "kite_api_secret"
KITE_ACCESS_TOKEN = "kite_access_token"          # rotates daily (~06:00 IST, A5)
TELEGRAM_BOT_TOKEN = "telegram_bot_token"
DASHBOARD_TOKEN = "dashboard_token"
CLAUDE_CODE_OAUTH_TOKEN = "claude_code_oauth_token"  # D2; injected into the SDK CLI env at startup

# Secrets that must be present for the engine to operate (checked by the startup self-test, D11).
REQUIRED_AT_STARTUP = (
    KITE_API_KEY,
    KITE_API_SECRET,
    TELEGRAM_BOT_TOKEN,
    DASHBOARD_TOKEN,
)


class MissingSecretError(KeyError):
    """Raised when a required secret is absent from the credential store."""


class Secrets:
    """Read/write secrets in the Windows Credential Manager (R10)."""

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
        """Return the required-at-startup secrets that are absent (for the self-test, D11)."""
        return [k for k in REQUIRED_AT_STARTUP if not self.has(k)]
