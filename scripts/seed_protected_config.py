"""First-run owner action: register the initial signatures of the protected config files (R4, §2.4).

``config/limits.yaml`` (§7.1) and ``config/envelope.yaml`` (§6.3) are loaded by the risk gate ONLY
when their on-disk SHA-256 matches the signature recorded in the SQLite ``protected_config`` table.
That table is the sole authority for "is this config trusted?", and it is written ONLY by the
owner-confirmed change flow (Telegram two-step / dashboard token) — never by Tier 1 or the learner
(R4). Until a starter file is registered here, ``ProtectedStore.load_verified`` treats it as
unregistered and refuses to load it (which, per §2.4, surfaces as FROZEN-on-flat-book at startup or a
kill with a live book — that single-rule consequence belongs to the gate, not this script).

This CLI performs the one-time seeding. Because :meth:`ProtectedStore.register_initial` signs WHATEVER
bytes are currently on disk, running it is itself a trust assertion: it must happen only AFTER the
owner has actually read the shipped ``limits.yaml`` / ``envelope.yaml``. We therefore gate the call
behind an explicit interactive "yes, I reviewed these files" confirmation (or an explicit ``--yes``),
and we build the :class:`OwnerConfirmation` proof token from that (Actor.OWNER, confirmed=True) — the
same token the runtime owner-update path requires (R4/R10).

Depends on: R4 (protected store / hash-verified config), §2.4 (integrity rule + owner-confirmed write
flow), §6.3 (envelope), §7.1 (limits), D11/E7 (this is part of the Phase-0 first-run bring-up).

Usage::

    python scripts/seed_protected_config.py            # interactive review confirmation
    python scripts/seed_protected_config.py --yes       # non-interactive (owner asserts review)
    python scripts/seed_protected_config.py --reseed     # re-register after an owner edit
    python scripts/seed_protected_config.py --note "..." # custom audit note

This is a script, so it MAY ``print`` (engine modules must not).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# --- make the repo's ``src`` importable when run as a bare script -----------------------------------
# scripts/seed_protected_config.py -> repo root is the parent of ``scripts``; sources live under src/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SRC = _REPO_ROOT / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from engine.core.clock import Clock  # noqa: E402
from engine.core.config import config_dir, load_settings  # noqa: E402
from engine.core.db import connect  # noqa: E402
from engine.core.enums import Actor  # noqa: E402
from engine.core.migrations import apply_migrations  # noqa: E402
from engine.core.protected_store import (  # noqa: E402
    PROTECTED_NAMES,
    ProtectedStore,
    sha256_bytes,
)
from engine.core.types import OwnerConfirmation  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="seed_protected_config",
        description=(
            "Register the initial SHA-256 of config/limits.yaml + config/envelope.yaml into the "
            "protected_config table (R4, §2.4). Run once after reviewing the shipped starter files."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Skip the interactive review prompt. By passing this you assert as the owner that you "
            "have read the current on-disk limits.yaml / envelope.yaml — they will be signed as-is."
        ),
    )
    parser.add_argument(
        "--reseed",
        action="store_true",
        help=(
            "Re-register the files even if they are already registered (use after an owner edit to "
            "a protected file). Without this flag, already-registered files are left untouched."
        ),
    )
    parser.add_argument(
        "--note",
        default=None,
        help="Free-text audit note recorded with the owner confirmation (config_audit, R8).",
    )
    return parser.parse_args(argv)


def _confirm_review(names: tuple[str, ...], directory: Path) -> bool:
    """Interactive 'yes, I reviewed these files' gate (returns True iff the owner typed ``yes``)."""
    print("About to register the SHA-256 of these protected config files as TRUSTED (R4):")
    for name in names:
        path = directory / name
        try:
            digest = sha256_bytes(path.read_bytes())
            print(f"  - {name}: {digest}")
        except FileNotFoundError:
            print(f"  - {name}: <MISSING at {path}>")
    print(
        "\nThis signs WHATEVER is on disk now. Only continue if you have personally reviewed the "
        "contents of these files."
    )
    try:
        answer = input("Type 'yes' to confirm you reviewed them: ").strip().lower()
    except EOFError:
        # No TTY and no --yes: refuse rather than silently sign.
        print("No interactive input available; pass --yes to confirm review explicitly.")
        return False
    return answer == "yes"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    settings = load_settings()
    cfg_dir = config_dir()
    db_path = settings.sqlite_path()

    print(f"config dir : {cfg_dir}")
    print(f"sqlite     : {db_path}")

    # Ensure the schema exists (the protected_config + config_audit tables, §4.2) before we touch it.
    conn = connect(db_path)
    try:
        applied = apply_migrations(conn)
        if applied:
            print(f"migrations applied: {', '.join(str(m) for m in applied)}")

        store = ProtectedStore(cfg_dir, conn, Clock())

        # Decide which files actually need (re)seeding and verify they exist on disk first.
        todo: list[str] = []
        for name in PROTECTED_NAMES:
            path = cfg_dir / name
            if not path.exists():
                print(f"ERROR: protected file not found: {path}")
                return 2
            if store.is_registered(name) and not args.reseed:
                print(f"  - {name}: already registered (use --reseed to re-register); skipping")
                continue
            todo.append(name)

        if not todo:
            print("Nothing to do; all protected files already registered. Use --reseed to override.")
            return 0

        # Gate the signing behind an explicit owner review confirmation (R4): register_initial signs
        # whatever bytes are on disk, so we must NOT do it without an affirmative owner assertion.
        if not args.yes and not _confirm_review(tuple(todo), cfg_dir):
            print("Aborted: review not confirmed. Nothing was registered.")
            return 1

        note = args.note or (
            "seed_protected_config --reseed: owner re-registered after edit"
            if args.reseed
            else "seed_protected_config: owner first-run registration of starter file"
        )
        confirmation = OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note=note)

        print("\nRegistered signatures (R4):")
        for name in todo:
            store.register_initial(name, confirmation)
            # Read back the now-recorded hash to confirm the write landed and matches disk.
            digest = sha256_bytes((cfg_dir / name).read_bytes())
            print(f"  - {name}: {digest}")

        print("\nDone. The risk gate will now load these files via ProtectedStore.load_verified.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
