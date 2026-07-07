"""Interactive secret-seeding CLI into the Windows Credential Manager (R10, §2.4).

Seeds secrets ONCE into the DPAPI-backed keyring via ``engine.core.secrets.Secrets``. Nothing secret
ever lives in the repo, in YAML, or in plaintext on disk (§2.4); this script is the sanctioned way to
populate the credential store. Secret VALUES are read with ``getpass`` (never echoed) and are NEVER
logged or printed back -- the sole exception is ``--generate-dashboard-token``, which prints a freshly
minted token exactly once so the owner can copy it.

The daily ``kite_access_token`` is intentionally NOT seedable here: it is minted by the Kite login flow
(~06:00 IST, A5) and rotated automatically, so offering it interactively would be misleading.

Usage::

    python scripts/dpapi_set.py                       # interactive menu
    python scripts/dpapi_set.py kite_api_key          # set one key (prompts for value)
    python scripts/dpapi_set.py --list                # show which keys are present (no values)
    python scripts/dpapi_set.py --generate-dashboard-token   # mint + store dashboard_token, print once
"""

from __future__ import annotations

import argparse
import os
import secrets as _secrets
import sys
from getpass import getpass

# Make the repo's ``src`` tree importable when run as a loose script (engine.* lives under src/).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from engine.core.secrets import (  # noqa: E402  (path insert must precede engine import)
    CLAUDE_CODE_OAUTH_TOKEN,
    DASHBOARD_TOKEN,
    KITE_ACCESS_TOKEN,
    KITE_API_KEY,
    KITE_API_SECRET,
    TELEGRAM_BOT_TOKEN,
    Secrets,
)

# Keys this CLI is allowed to seed, with one-line human descriptions (names only -- never values).
SEEDABLE: dict[str, str] = {
    KITE_API_KEY: "Zerodha Kite Connect API key",
    KITE_API_SECRET: "Zerodha Kite Connect API secret",
    TELEGRAM_BOT_TOKEN: "Telegram bot token (BotFather)",
    DASHBOARD_TOKEN: "Local dashboard bearer token (or use --generate-dashboard-token)",
    CLAUDE_CODE_OAUTH_TOKEN: "Claude Code SDK OAuth token (D2; injected into the SDK CLI env)",
}

# Length, in URL-safe bytes, of an auto-generated dashboard token (~43 chars of entropy).
_DASHBOARD_TOKEN_NBYTES = 32


def _validate_key(key: str) -> str:
    """Return ``key`` if it is a known seedable secret, else exit with a helpful message.

    Refuses unknown keys outright (R10: never invent credential-store entries). Gives a targeted
    hint for ``kite_access_token`` since it is a real secret but is minted daily, not seeded here.
    """
    if key in SEEDABLE:
        return key
    if key == KITE_ACCESS_TOKEN:
        _die(
            f"'{KITE_ACCESS_TOKEN}' is minted daily by the Kite login flow (A5) and is not seeded here."
        )
    known = ", ".join(sorted(SEEDABLE))
    _die(f"unknown secret key '{key}'. Seedable keys: {known}")


def _set_key(store: Secrets, key: str) -> None:
    """Prompt (no echo) for a value and store it under ``key``. Never prints the value."""
    desc = SEEDABLE[key]
    print(f"\nSetting '{key}' -- {desc}")
    if store.has(key):
        print(f"  (a value for '{key}' already exists; entering a new one overwrites it)")
    value = getpass("  value (hidden, blank to cancel): ")
    if not value:
        print("  cancelled; nothing changed.")
        return
    confirm = getpass("  re-enter to confirm: ")
    if value != confirm:
        _die("values did not match; nothing changed.")
    store.set(key, value)
    print(f"  stored '{key}' in the Windows Credential Manager.")


def _list_present(store: Secrets) -> None:
    """Print presence/absence of every seedable key (plus the daily token) -- WITHOUT any value."""
    print("Secret presence in the credential store (values never shown):")
    for key in sorted(SEEDABLE):
        mark = "present" if store.has(key) else "MISSING"
        print(f"  [{mark:>7}] {key}")
    # The daily Kite access token is informational: shown but flagged as login-flow managed.
    mark = "present" if store.has(KITE_ACCESS_TOKEN) else "absent"
    print(f"  [{mark:>7}] {KITE_ACCESS_TOKEN}  (minted daily by login flow, A5; not seeded here)")


def _generate_dashboard_token(store: Secrets) -> None:
    """Mint a strong random dashboard token, store it, and print it EXACTLY once."""
    if store.has(DASHBOARD_TOKEN):
        ans = input(f"'{DASHBOARD_TOKEN}' already exists; overwrite it? [y/N]: ").strip().lower()
        if ans not in ("y", "yes"):
            print("cancelled; existing dashboard token unchanged.")
            return
    token = _secrets.token_urlsafe(_DASHBOARD_TOKEN_NBYTES)
    store.set(DASHBOARD_TOKEN, token)
    print(f"stored '{DASHBOARD_TOKEN}' in the Windows Credential Manager.")
    print("Copy this token now -- it will NOT be shown again:")
    print(f"\n    {token}\n")


def _interactive_menu(store: Secrets) -> None:
    """Drive a simple numbered menu to set any seedable key or list presence."""
    keys = sorted(SEEDABLE)
    while True:
        print("\nSeed a secret into the Windows Credential Manager (R10, §2.4):")
        for i, key in enumerate(keys, start=1):
            mark = "set" if store.has(key) else "   "
            print(f"  {i}. [{mark}] {key} -- {SEEDABLE[key]}")
        print("  g. generate + store a strong dashboard_token")
        print("  l. list which secrets are present")
        print("  q. quit")
        choice = input("choose: ").strip().lower()
        if choice in ("q", "quit", ""):
            return
        if choice in ("l", "list"):
            _list_present(store)
            continue
        if choice in ("g", "gen", "generate"):
            _generate_dashboard_token(store)
            continue
        if choice.isdigit() and 1 <= int(choice) <= len(keys):
            _set_key(store, keys[int(choice) - 1])
            continue
        print(f"  unrecognised choice: {choice!r}")


def _die(message: str) -> None:
    """Print an error to stderr and exit non-zero. Used for refusals/validation only."""
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dpapi_set.py",
        description="Seed secrets into the Windows Credential Manager (R10, §2.4). "
        "Secret values are read without echo and are never logged or printed.",
    )
    parser.add_argument(
        "key",
        nargs="?",
        help="optional single key to set non-interactively; one of: " + ", ".join(sorted(SEEDABLE)),
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--list",
        action="store_true",
        help="print which secrets are present (never prints values) and exit",
    )
    group.add_argument(
        "--generate-dashboard-token",
        action="store_true",
        help="mint a strong random dashboard_token, store it, and print it once",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    store = Secrets()

    if args.list:
        if args.key:
            _die("--list does not take a key argument.")
        _list_present(store)
        return 0

    if args.generate_dashboard_token:
        if args.key:
            _die("--generate-dashboard-token does not take a key argument.")
        _generate_dashboard_token(store)
        return 0

    if args.key:
        _set_key(store, _validate_key(args.key))
        return 0

    _interactive_menu(store)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\naborted; nothing changed.", file=sys.stderr)
        raise SystemExit(130)
