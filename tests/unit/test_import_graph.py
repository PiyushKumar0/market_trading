"""Import-graph guard (R1, §2.3/§9.1): the deterministic tiers never import the LLM tier, and the
paper tier never imports the live-broker package.

``engine.risk`` and ``engine.oms`` must contain NO import of ``engine.intelligence`` — the three-tier
separation is structural, not conventional. Proposals reach the gate only as schema-validated data. This
test parses the AST of every module under those packages (no runtime import needed, so it does not pull
the heavy deps) and fails if any ``import engine.intelligence...`` appears.

``engine.paper`` (WO-P3-2, 2026-09-10, §8.4 addendum) carries a SECOND forbidden prefix,
``engine.broker``: §3.2.9 pins the paper tier's dependency to ``core`` alone. That is not cosmetic —
``engine.broker.kite_client`` pulls the pykiteconnect import chain, and a paper simulation that can
reach the live broker package is one refactor away from being able to reach the live broker. The wire
contract the two brokers share (``ORDER_UPDATE_TOPIC`` / ``OrderUpdateFrame``) therefore lives in
``engine.core.contracts``, which is what makes this guard satisfiable.

``engine.paper`` additionally carries a PER-MODULE budget (round 3, 2026-09-10): the tier-wide "depends
on ``core`` alone" claim is not true of every module in it, because §8.4's WO-P3-3 replay harness is
sanctioned to drive the real ``BarBuilder``/``MarketStore``. Relaxing the tier-wide guard to admit that
would have handed ``engine.marketdata`` to ``broker``/``fill_model``/``surface`` as well, so the guard is
split: ``PAPER_MODULE_ALLOWED`` states each module's budget, and the modules' own docstrings state the
same thing in prose. A module added to the package with no entry there fails the coverage test.

``engine.ops`` is exempt: it is the composition root, the only module allowed to import everything (§3.2.12).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from engine.core.config import repo_root

#: package under ``src/engine`` -> the import prefixes it may never contain.
GUARDED_PACKAGES: dict[str, tuple[str, ...]] = {
    "risk": ("engine.intelligence",),
    "oms": ("engine.intelligence",),
    "paper": ("engine.intelligence", "engine.broker"),
}
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


@pytest.mark.parametrize(("pkg", "forbidden"), sorted(GUARDED_PACKAGES.items()))
def test_tier_does_not_import_the_forbidden_packages(pkg: str, forbidden: tuple[str, ...]) -> None:
    offenders: list[str] = []
    for path in _py_files(pkg):
        for imported in _imports(path):
            for prefix in forbidden:
                if imported == prefix or imported.startswith(prefix + "."):
                    offenders.append(f"{path.name} imports {imported}")
    assert not offenders, (
        f"import-graph violation: engine.{pkg} must not import {list(forbidden)} "
        f"(§2.3 / §3.2.9): " + "; ".join(offenders)
    )


# --------------------------------------------------------------------------- per-module (paper)
#: ``engine.paper`` module -> the ``engine.*`` prefixes THAT MODULE may import (§3.2.9 + the §8.4
#: WO-P3-3 harness exception). The package-level guard above only says what the tier may never
#: touch; this says what each module may, which is the claim the module docstrings make:
#:
#: * ``broker`` / ``fill_model`` / ``surface`` / ``__init__`` depend on ``engine.core`` ALONE. That
#:   is what lets a PaperBroker be constructed in any process without dragging in the market-data
#:   or live-broker import chains, and it is why the order-postback wire contract had to move to
#:   ``engine.core.contracts`` (both brokers emit it).
#: * ``replay`` additionally depends on ``engine.marketdata``: the harness drives the REAL
#:   ``BarBuilder`` into a scratch ``MarketStore`` (§8.4 WO-P3-3, "one code path"), which is the
#:   whole point of it and the one plan-sanctioned exception. Nothing else in the tier gets it --
#:   without this per-module split the tier-wide guard would have to be relaxed for everyone.
#:
#: ``engine.paper`` itself is always allowed (intra-package imports); stdlib/third-party is out of
#: scope here — this guard is about the ENGINE graph.
PAPER_MODULE_ALLOWED: dict[str, tuple[str, ...]] = {
    "__init__": ("engine.core", "engine.paper"),
    "broker": ("engine.core", "engine.paper"),
    "fill_model": ("engine.core", "engine.paper"),
    "surface": ("engine.core", "engine.paper"),
    "replay": ("engine.core", "engine.marketdata", "engine.paper"),
}


def _is_under(imported: str, prefix: str) -> bool:
    return imported == prefix or imported.startswith(prefix + ".")


def test_every_paper_module_is_covered_by_the_per_module_allowlist() -> None:
    # A new module in engine.paper must declare its dependency budget here, or the per-module guard
    # silently ignores it (the vacuous-pass failure mode again, one level down).
    on_disk = {p.stem for p in _py_files("paper")}
    assert on_disk == set(PAPER_MODULE_ALLOWED), (
        "engine.paper modules and PAPER_MODULE_ALLOWED disagree: "
        f"undeclared={sorted(on_disk - set(PAPER_MODULE_ALLOWED))}, "
        f"stale={sorted(set(PAPER_MODULE_ALLOWED) - on_disk)}"
    )


@pytest.mark.parametrize(("module", "allowed"), sorted(PAPER_MODULE_ALLOWED.items()))
def test_paper_module_imports_only_its_declared_engine_dependencies(
    module: str, allowed: tuple[str, ...]
) -> None:
    path = repo_root() / "src" / "engine" / "paper" / f"{module}.py"
    offenders = [
        imported
        for imported in _imports(path)
        if _is_under(imported, "engine")
        and not any(_is_under(imported, prefix) for prefix in allowed)
    ]
    assert not offenders, (
        f"engine.paper.{module} may import only {list(allowed)} from the engine (§3.2.9 / §8.4): "
        + "; ".join(sorted(offenders))
    )


@pytest.mark.parametrize("module", sorted(PAPER_MODULE_ALLOWED))
def test_no_paper_module_may_reach_the_llm_tier_or_the_live_broker(module: str) -> None:
    # Restated per module rather than left to the package guard: the allowlist above is a positive
    # list, and a positive list is one careless edit away from admitting the two prefixes that
    # actually matter (R1's LLM ban and §3.2.9's pykiteconnect ban).
    path = repo_root() / "src" / "engine" / "paper" / f"{module}.py"
    banned = [
        imported
        for imported in _imports(path)
        for prefix in ("engine.intelligence", "engine.broker")
        if _is_under(imported, prefix)
    ]
    assert not banned, f"engine.paper.{module} imports {sorted(banned)}"


def test_guard_actually_scans_files() -> None:
    # Guard against a vacuous pass: there must BE python files under EVERY guarded package (a
    # renamed/moved package would otherwise turn its guard into a silent no-op).
    for pkg in GUARDED_PACKAGES:
        assert _py_files(pkg), f"no python files found under engine.{pkg}: the guard is vacuous"


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
