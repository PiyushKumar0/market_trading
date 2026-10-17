"""Protected-config status: the recorded signature, the signed snapshot and the on-disk file, side by side.

``config/limits.yaml`` and ``config/envelope.yaml`` load only when their on-disk SHA-256 equals the
signature in ``state.db`` ``protected_config``. A dashboard capital change (O19) re-signs
``limits.yaml`` and leaves it modified in the git working tree. A ``git stash`` / ``checkout`` /
``reset --hard`` before it is committed puts the OLD bytes back under the NEW signature, and the next
boot FREEZES on integrity. This tool shows that state and repairs it without signing anything.

* Default (read-only, ``state.db`` opened ``mode=ro``): for each file, print the recorded sha256, the
  sha256 of ``content_snapshot`` and of the on-disk bytes. Exit 0 when every on-disk file matches
  its signature, 1 otherwise.
* ``--restore NAME``: write ``content_snapshot`` back to ``config/NAME``, but ONLY when the snapshot
  hashes to the recorded signature. It restores exactly the bytes the signature already covers, so
  it can never sign or launder an unreviewed edit. Owner-run, with the engine stopped.

Usage::

    .venv\\Scripts\\python.exe scripts\\protected_config_status.py
    .venv\\Scripts\\python.exe scripts\\protected_config_status.py --restore limits.yaml

This is a script, so it MAY ``print``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
NAMES = ("limits.yaml", "envelope.yaml")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def status(conn: sqlite3.Connection, config_dir: Path) -> list[dict[str, str | None]]:
    """One row per protected file: recorded / snapshot / on-disk sha256 (None when absent)."""
    rows = []
    for name in NAMES:
        rec = conn.execute(
            "SELECT sha256, content_snapshot FROM protected_config WHERE name = ?", (name,)
        ).fetchone()
        path = config_dir / name
        rows.append({
            "name": name,
            "recorded": rec[0] if rec else None,
            "snapshot": _sha(rec[1].encode("utf-8")) if rec and rec[1] is not None else None,
            "on_disk": _sha(path.read_bytes()) if path.exists() else None,
        })
    return rows


def restore(conn: sqlite3.Connection, config_dir: Path, name: str) -> str:
    """Write the signed snapshot back to ``config_dir/name``. Refuses unless the snapshot hashes to the
    recorded signature; a no-op when the file already matches. Returns what it did."""
    if name not in NAMES:
        raise ValueError(f"{name!r} is not a protected config (expected one of {NAMES})")
    rec = conn.execute(
        "SELECT sha256, content_snapshot FROM protected_config WHERE name = ?", (name,)
    ).fetchone()
    if rec is None or rec[1] is None:
        raise RuntimeError(f"{name}: no signed snapshot recorded; nothing to restore")
    snapshot = rec[1].encode("utf-8")
    if _sha(snapshot) != rec[0]:
        raise RuntimeError(f"{name}: the snapshot does not hash to the recorded signature; refusing")
    path = config_dir / name
    if path.exists() and _sha(path.read_bytes()) == rec[0]:
        return f"{name}: already matches its signature; nothing written"
    tmp = path.with_name(f".{path.name}.{os.getpid()}.restore.tmp")
    tmp.write_bytes(snapshot)
    os.replace(tmp, path)
    return f"{name}: restored the signed snapshot ({rec[0][:12]}…)"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Protected-config signature status / snapshot restore.")
    ap.add_argument("--state-db", type=Path, default=_REPO_ROOT / "data" / "state.db")
    ap.add_argument("--config-dir", type=Path, default=_REPO_ROOT / "config")
    ap.add_argument("--restore", metavar="NAME", default=None,
                    help="write the signed content_snapshot back to config/NAME (engine stopped)")
    args = ap.parse_args(argv)
    if args.restore:
        with sqlite3.connect(str(args.state_db), timeout=5.0) as conn:
            print(restore(conn, args.config_dir, args.restore))
        return 0
    conn = sqlite3.connect(f"file:{args.state_db.resolve().as_posix()}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = status(conn, args.config_dir)
    finally:
        conn.close()
    ok = True
    for r in rows:
        match = r["recorded"] is not None and r["on_disk"] == r["recorded"]
        ok = ok and match
        print(f"{r['name']:<14} {'OK      ' if match else 'MISMATCH'} recorded {r['recorded']} "
              f"snapshot {r['snapshot']} on-disk {r['on_disk']}")
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover - CLI
    sys.exit(main())
