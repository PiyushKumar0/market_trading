"""Entry point for ``python -m engine.core.migrations [db_path]`` (§4.2)."""

from __future__ import annotations

from engine.core.migrations import main

if __name__ == "__main__":
    raise SystemExit(main())
