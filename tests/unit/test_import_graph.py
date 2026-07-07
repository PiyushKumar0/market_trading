"""Import-graph guard (R1, §2.3/§9.1): the deterministic tiers never import the LLM tier.

``engine.risk`` and ``engine.oms`` must contain NO import of ``engine.intelligence`` — the three-tier
separation is structural, not conventional. Proposals reach the gate only as schema-validated data. This
test parses the AST of every module under those packages (no runtime import needed, so it does not pull
the heavy deps) and fails if any ``import engine.intelligence...`` appears.

``engine.ops`` is exempt: it is the composition root, the only module allowed to import everything (§3.2.12).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from engine.core.config import repo_root

GUARDED_PACKAGES = ["risk", "oms"]
FORBIDDEN_PREFIX = "engine.intelligence"


def _py_files(pkg: str) -> list[Path]:
    base = repo_root() / "src" / "engine" / pkg
    return list(base.rglob("*.py"))


def _module_package(path: Path) -> str:
    """Dotted package of the module at ``path`` (e.g. src/engine/risk/kill.py -> 'engine.risk';
    src/engine/risk/__init__.py -> 'engine.risk'). Used to resolve relative imports absolutely."""
    rel = path.relative_to(repo_root() / "src").with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]          # a package's own __init__ resolves to the package itself
    else:
        parts = parts[:-1]          # a module resolves to its containing package
    return ".".join(parts)


def _resolve_relative(pkg: str, level: int, module: str | None) -> str:
    """Resolve a relative ImportFrom (level>0) to its absolute dotted target. level=1 is the current
    package, level=2 the parent, etc.: strip (level-1) trailing components, then append ``module``."""
    base = pkg.split(".") if pkg else []
    if level - 1 > 0:
        base = base[: len(base) - (level - 1)]
    if module:
        base = [*base, *module.split(".")]
    return ".".join(base)


def _imports_in_source(source: str, pkg: str, filename: str = "<test>") -> set[str]:
    """Every imported module name in ``source``, with RELATIVE imports resolved absolutely against
    ``pkg`` — so ``from ..intelligence import x`` in engine.risk is caught as ``engine.intelligence``
    (the pre-fix version silently skipped level>0 imports, a structural-guard blind spot, §2.3)."""
    tree = ast.parse(source, filename=filename)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                if node.module:
                    names.add(node.module)
            else:
                names.add(_resolve_relative(pkg, node.level, node.module))
    return names


def _imports(path: Path) -> set[str]:
    return _imports_in_source(path.read_text(encoding="utf-8"), _module_package(path), str(path))


@pytest.mark.parametrize("pkg", GUARDED_PACKAGES)
def test_tier_does_not_import_intelligence(pkg: str) -> None:
    offenders: list[str] = []
    for path in _py_files(pkg):
        for imported in _imports(path):
            if imported == FORBIDDEN_PREFIX or imported.startswith(FORBIDDEN_PREFIX + "."):
                offenders.append(f"{path.name} imports {imported}")
    assert not offenders, (
        f"R1 violation: engine.{pkg} must not import {FORBIDDEN_PREFIX} (§2.3): " + "; ".join(offenders)
    )


def test_guard_actually_scans_files() -> None:
    # Guard against a vacuous pass: there must BE python files under the guarded packages.
    assert any(_py_files(pkg) for pkg in GUARDED_PACKAGES)


def test_guard_catches_relative_and_absolute_intelligence_imports() -> None:
    # Regression: relative cross-package imports must not slip past the guard (the level>0 blind spot).
    assert "engine.intelligence" in _imports_in_source("from ..intelligence import context\n", "engine.risk")
    # A deeper submodule reaching up three levels to the sibling intelligence package.
    assert "engine.intelligence" in _imports_in_source("from ...intelligence import x\n", "engine.oms.sub")
    assert "engine.intelligence" in _imports_in_source("from engine.intelligence import x\n", "engine.oms")
    # A benign relative import within the tier resolves to itself, not a false positive.
    assert "engine.intelligence" not in _imports_in_source("from .kill import KillSwitch\n", "engine.risk")


def test_module_package_resolution() -> None:
    src = repo_root() / "src"
    assert _module_package(src / "engine" / "risk" / "kill.py") == "engine.risk"
    assert _module_package(src / "engine" / "risk" / "__init__.py") == "engine.risk"
    assert _resolve_relative("engine.risk", 2, "intelligence") == "engine.intelligence"
