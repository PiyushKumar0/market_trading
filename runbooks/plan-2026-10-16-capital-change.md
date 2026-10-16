# Capital change (O19) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the owner can change the trading capital from the dashboard. Real and paper books, the two rupee caps and every brake follow it safely, and the brakes become base-agnostic.

**Architecture:**
- The base gets one source: `LimitsEngine`, read live by every holder. A change is one hardened, signed write of three `limits.yaml` lines, followed by `LimitsEngine.reload()`.
- Day loss and drawdown compare P&L to P&L.
- A change is refused if it would newly trip a brake, in either scope.
- hi52's registered edge is capped by its live derivation.

**Tech stack:** Python 3.12, FastAPI, SQLite (`state.db`), PyYAML, pydantic, pytest; React and TypeScript (Vite), node test runner.

**Spec:** `runbooks/design-2026-10-16-dashboard-capital-change.md` (v2). Plan of record: `IMPLEMENTATION_PLAN.md` §7.1 **O19**.

## Global Constraints

- **Workspace.** All work happens in the worktree `C:\Users\piyush.kumar_atari\projects\personal\mt_capital`, branch `wip/capital-change`. Never edit `C:\Users\piyush.kumar_atari\projects\personal\market_trading`, which is the live engine's boot tree.
- **Tests and tooling.**
  - Python is `.\.venv\Scripts\python.exe`, run from the worktree; that venv is a junction to the live venv. Never `uv run`, `uv sync` or `uv pip`.
  - Run only the test files named in your task, inline (never in the background), and paste the output. Run no full suite.
  - Lint with `.\.venv\Scripts\python.exe -m ruff check <changed files>`. The 4 pre-existing warnings in untouched lines of `src/engine/api/app.py` are known.
- **Git.** Agents run no state-changing git commands; git stays with the manager.
- **No engine control.** Do not start, stop or restart `mt-engine` or any engine process.
- **No live config edits.** Never edit `config/limits.yaml` or `config/envelope.yaml`. Tests use temporary copies.
- **Money.** Decimal everywhere. Whole rupees for capital. Scaled caps use `floor(cap × new / old)`.
- **Window.** Refused when `calendar.session(today)` is not None and `session.open <= now <= session.close`, and when `calendar.verified_horizon()` is None or earlier than today.
- **Bounds.** `old / 2 <= new <= old * 2`, `new != old`, `new` an int ≥ 1.
- **Owner copy.** The dashboard copy rules in `runbooks/design-2026-10-08-dashboard-redesign.md` §15 apply. No new jargon on the page.

## Review Focus

These five inputs and conditions are the most likely to bite the owner while no task's base tests cover them. Each one's test is added to the owning task.
1. **Lowering the capital while lifetime P&L is negative.** The owner expects a clear refusal naming the rung, never a silent go-flat or kill at the next boot. The test is in Task 4.
2. **Pressing Confirm twice, or from two tabs.** The second POST must answer 409 (stale `expected_current_inr`), not apply twice. The test is in Task 4.
3. **`limits.yaml` was edited and not reseeded before the owner changes capital.** The answer must be 503 with nothing written; never re-sign the edit. The test is in Task 4.
4. **The engine restarts after a change.** The day baseline and the floors must read the same as before the restart. The test is in Task 1.
5. **The dashboard is open across a change.** The paper equity, "Started with" and the chart must show the new base at once, with no cliff. The test is in Task 5.

---

### Task 1: Single-source, base-agnostic ExposureTracker and PaperRisk

**Files:**
- Modify `src/engine/risk/exposure.py`: `ExposureTracker.__init__` (~152-175), `capital_base` (~176), `equity` (~214-218), `set_day_baseline` / `day_baseline` (~226-249), `persist_snapshot` (~355-383), `weekly_drawdown_peak` (~386-414), `evaluate_floors` (~420), `evaluate_day_loss` (~453).
- Modify `src/engine/ops/paper_risk.py`: `PaperRisk.__init__` (~85-97), `_start_epoch_if_flat` (~192-195).
- Tests: `tests/unit/test_exposure_tracker.py`, `tests/unit/test_paper_risk.py`, `tests/unit/test_scope_isolation.py`.

**Interfaces:**
- Produces:
  - `ExposureTracker(conn, clock, capital_base: Decimal | Callable[[], Decimal], mark_price=None, *, scope="real")`.
  - Property `capital_base -> Decimal`, read live on every call.
  - `pnl() -> Decimal` (realized net + open MTM).
  - `with_capital_base(base: Decimal) -> ExposureTracker`: a what-if copy that never persists. Same conn, marks and cached baseline P&L; the original is untouched.
  - `PaperRisk(..., capital_base: Decimal | Callable[[], Decimal], ...)`.
- Semantics (spec §2 and §3.1):
  - `_day_baseline` caches `(d, baseline P&L)`, and `day_baseline(d)` returns `capital_base + baseline P&L`.
  - A snapshot row's P&L is `realized_pnl + open_mtm`. When either column is NULL, it is `equity − capital_base`, which reproduces the old read.
  - No prior snapshot: baseline P&L is `pnl() − realized_net_closed_on(d)`.
  - `set_day_baseline(equity, d)` stores `equity − capital_base`.
  - `weekly_drawdown_peak` returns `capital_base + max(row P&L)` over the same rows as today, or None as today.
  - `persist_snapshot` computes realized and open once and stores `equity = base + realized + open`, so the stored identity holds exactly even if marks move mid-call.
  - `evaluate_day_loss` uses `self.capital_base`. `evaluate_floors` falls back to `self.capital_base` when `limits.capital_base` is None.
  - `PaperRisk` drops `self._capital_base` and uses `self.tracker.capital_base` for `reseed_margin` and the log line.

- [ ] **Step 1: Write the failing tests** (add to `tests/unit/test_exposure_tracker.py`, reusing its fixtures and helpers):

```python
def _snap(conn, at, equity, realized, open_mtm, table="equity_snapshots"):
    conn.execute(f"INSERT INTO {table} (at, equity, realized_pnl, open_mtm, day_mtm, positions_open) "
                 "VALUES (?,?,?,?,?,?)", (at, str(equity), str(realized), str(open_mtm), "0", 0))

def test_a_base_change_is_not_day_pnl(conn, clock):
    """2026-09-07 replay: the base moved 20k -> 40k mid-session. Day MTM is the P&L move only."""
    base = {"v": Decimal("20000")}
    t = ExposureTracker(conn, clock, lambda: base["v"])
    _snap(conn, "2026-06-16T15:29:00+05:30", "19154.40", "0", "-845.60")   # prior session, base 20k
    base["v"] = Decimal("40000")
    # no positions on the fixture book: P&L now is 0, so day MTM is 0 - (-845.60), not +20,845.60
    assert t.day_mtm(date(2026, 6, 17)) == Decimal("845.60")
    assert t.day_baseline(date(2026, 6, 17)) == Decimal("40000") + Decimal("-845.60")

def test_drawdown_peak_is_base_agnostic(conn, clock):
    base = {"v": Decimal("40000")}
    t = ExposureTracker(conn, clock, lambda: base["v"])
    _snap(conn, "2026-06-15T15:29:00+05:30", "40500", "500", "0")
    _snap(conn, "2026-06-16T15:29:00+05:30", "39850", "-150", "0")
    assert t.weekly_drawdown_peak(date(2026, 6, 17)) == Decimal("40500")
    base["v"] = Decimal("20000")
    assert t.weekly_drawdown_peak(date(2026, 6, 17)) == Decimal("20500")

def test_a_null_pnl_row_reads_like_before(conn, clock):
    t = ExposureTracker(conn, clock, Decimal("40000"))
    conn.execute("INSERT INTO equity_snapshots (at, equity) VALUES ('2026-06-16T15:29:00+05:30', '39000')")
    assert t.day_baseline(date(2026, 6, 17)) == Decimal("39000")

def test_with_capital_base_is_a_what_if_copy(conn, clock):
    t = ExposureTracker(conn, clock, Decimal("40000"))
    w = t.with_capital_base(Decimal("20000"))
    assert (w.capital_base, t.capital_base) == (Decimal("20000"), Decimal("40000"))
    assert w.equity() - t.equity() == Decimal("-20000")

def test_baseline_survives_a_restart_and_a_base_change(conn, clock):
    """Review focus 4: a fresh tracker (restart) reads the same baseline from the P&L columns."""
    _snap(conn, "2026-06-16T15:29:00+05:30", "39500", "-500", "0")
    before = ExposureTracker(conn, clock, Decimal("40000")).day_mtm(date(2026, 6, 17))
    after = ExposureTracker(conn, clock, Decimal("60000")).day_mtm(date(2026, 6, 17))
    assert before == after == Decimal("500")

def test_persisted_rows_satisfy_the_identity(conn, clock):
    t = ExposureTracker(conn, clock, Decimal("40000"))
    snap = t.persist_snapshot()
    assert snap.equity == Decimal("40000") + snap.realized_pnl + snap.open_mtm
```

Also add, to `tests/unit/test_paper_risk.py`: a test where `PaperRisk` gets `capital_base=lambda: base["v"]`. After `base["v"]` changes, `risk.tracker.capital_base` follows, and an epoch start calls `reseed_margin` with the new value.

- [ ] **Step 2: Run them; they must fail.** Run `.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider tests/unit/test_exposure_tracker.py tests/unit/test_paper_risk.py`. Expected: the new tests FAIL (TypeError: `'function'`, or wrong values).
- [ ] **Step 3: Implement** the Interfaces semantics in `exposure.py` and `paper_risk.py`. Use `copy.copy` for `with_capital_base`, then set the twin's `_base_fn`.
- [ ] **Step 4: Rewrite the equity-only fixtures** so each stored row satisfies `equity = base + realized_pnl + open_mtm` and each test keeps its meaning:
  - `tests/unit/test_exposure_tracker.py` ~55-60, with the assertions at ~117-118, ~267 and ~284-290;
  - `tests/unit/test_paper_risk.py` ~86-87, ~158, ~244;
  - `tests/unit/test_scope_isolation.py` ~79, ~108-109.

  NULL-P&L fixtures may stay: the NULL fallback keeps their meaning. Do not weaken any assertion. If one cannot be kept, stop and report it.
- [ ] **Step 5: Run** `tests/unit/test_exposure_tracker.py tests/unit/test_paper_risk.py tests/unit/test_scope_isolation.py tests/unit/test_risk_gate.py tests/unit/test_lifecycle_selftest.py`. All must PASS; paste the tail. Then run ruff on the changed files.

### Task 2: Hardened ProtectedStore.owner_update

**Files:**
- Modify `src/engine/core/protected_store.py`: `owner_update` (~88-129), plus a new `read_verified_bytes`.
- Test: `tests/unit/test_protected_store.py`.

**Interfaces:**
- Produces:
  - `ProtectedStore.read_verified_bytes(name) -> tuple[bytes, str]`: the raw bytes and their sha256. Raises `IntegrityError` when unregistered or on a mismatch, with the same messages as `load_verified`.
  - `ProtectedStore.owner_update(name, content, confirmation, *, expected_sha256: str | None = None) -> None`.
- Semantics (spec §4):
  - Auth checked as today, then YAML parse validation as today.
  - When `expected_sha256` is given and the recorded signature differs, raise `IntegrityError("… signature changed since it was read")` and touch nothing.
  - Write the encoded bytes to `path.with_name("." + path.name + ".tmp")`.
  - Inside `with transaction(self._conn):`
    1. Without `expected_sha256`, the upsert as today. With it, `UPDATE protected_config SET sha256=?, updated_by=?, updated_at=?, content_snapshot=? WHERE name=? AND sha256=?`, raising `IntegrityError` unless `rowcount == 1`.
    2. Insert into `config_audit` as today.
    3. `os.replace(tmp, path)` as the last statement.
  - On any `BaseException`: unlink the tmp file (missing_ok). If the file's bytes no longer equal the old bytes, restore the old bytes (write a second tmp, then `os.replace`). If there were no old bytes, unlink the new file. Then re-raise.
  - The seed script path (`register_initial` → `owner_update` without `expected_sha256`) behaves exactly as today.

- [ ] **Step 1: Write the failing tests** (use the file's existing store, conn and `OWNER_OK` fixtures):

```python
def test_read_verified_bytes_matches_the_signature(store): ...   # bytes == path bytes, sha == recorded
def test_read_verified_bytes_refuses_an_unsigned_edit(store, tmp_path): ...   # append a byte -> IntegrityError
def test_cas_refuses_a_changed_signature(store):
    content = "schema_version: 1\n"
    with pytest.raises(IntegrityError):
        store.owner_update("limits.yaml", content, OWNER_OK, expected_sha256="0" * 64)
    # file bytes and signature unchanged
def test_a_failure_inside_the_transaction_restores_the_old_bytes(store, monkeypatch):
    import engine.core.protected_store as ps
    before = (path.read_bytes(), store._signature("limits.yaml"))
    real_replace = ps.os.replace
    def replace_then_fail(src, dst):
        real_replace(src, dst)
        raise OSError("disk full")      # after the swap, before COMMIT
    monkeypatch.setattr(ps.os, "replace", replace_then_fail)
    with pytest.raises(OSError):
        store.owner_update("limits.yaml", "schema_version: 1\n", OWNER_OK)
    assert (path.read_bytes(), store._signature("limits.yaml")) == before
    assert not list(path.parent.glob(".limits.yaml*.tmp"))
def test_cas_success_resigns_and_audits(store): ...   # expected_sha256 = current, then load_verified works and one audit row
```

The restore-path test must restore through `real_replace`: patch only the first call, or capture `real_replace` before patching.

- [ ] **Step 2: Run** `tests/unit/test_protected_store.py`. The new tests must FAIL.
- [ ] **Step 3: Implement** the semantics above.
- [ ] **Step 4: Run** `tests/unit/test_protected_store.py tests/unit/test_seed_protected_config.py` (if present) and `tests/unit/test_limits_engine.py`. All must PASS; paste the tail. Then run ruff.

### Task 3: hi52 edge derivation in the engine, and the live edges map

**Files:**
- Create `src/engine/strategy/hi52_edge.py`.
- Modify `scripts/hi52_forward_verdict.py`: import the arithmetic; keep every public name the tests use.
- Modify `src/engine/risk/gate.py:417` and `src/engine/ops/pipeline.py:995`: keep the mapping by reference, with no `dict(...)` copy.
- Tests: `tests/unit/test_hi52_edge.py` (new) and `tests/unit/test_hi52_forward_verdict.py` (the pin at ~337-354).

**Interfaces:**
- Produces, in `engine.strategy.hi52_edge`:
  - constants `MEASURED_T20_MEDIAN_GROSS_PCT = Decimal("2.0336")`, `PRODUCT = "CNC"`, `STOP_PCT = Decimal(str(hi52.DEFAULT_PARAMS["stop_pct"]))`;
  - `sizing_notional(equity, *, position_pct, gap_mult, stop_pct) -> Decimal`, moved verbatim with its `ValueError`;
  - `cost_floor_pct(notional, cost_model=None) -> Decimal`;
  - `edge_at(base, rung_pct, position_pct, gap_mult, *, stop_pct=STOP_PCT, cost_model=None) -> Decimal`: equity = base × (1 + rung_pct / 100); `ValueError` when base ≤ 0, equity ≤ 0 or equity ≥ base; then `MEASURED − cost_floor_pct(sizing_notional(...))` quantized to 0.01 with ROUND_FLOOR;
  - `derived_edge_pct(table: LimitTable, cost_model=None) -> Decimal`: `edge_at` from `table.capital_base_inr`, `table.limits.equity_floor_rung.equity_pct_of_base`, `table.limits.per_trade_risk.swing_position_pct` and `table.limits.per_trade_risk.overnight_gap_mult`;
  - `class StrategyEdges(Mapping[str, Decimal])`, constructed as `StrategyEdges(registered: Mapping[str, Decimal], *, table_fn: Callable[[], LimitTable] | None = None, cost_model=None)`. `__getitem__` returns `registered[sid]`, except for `hi52.STRATEGY_ID` with a `table_fn`, where it returns `min(registered, derived_edge_pct(table_fn(), cost_model))`. The derived value is cached per table object (`is`). Any exception from the derivation logs `hi52_edge_derivation_failed` once and returns the registered edge. `__iter__` and `__len__` come from `registered`.
- `scripts/hi52_forward_verdict.py` keeps `registered_edge_pct(limits_path=None)`, `cost_floor_pct(notional)`, `sizing_notional` and the constants as names, so its tests still import them. They delegate to the engine module, so there is still one derivation. Its file-reading fallbacks and provenance strings stay in the script.
- The gate and the pipeline store `strategy_expected_edge_pct` / `strategy_edge_pct` as given (`… if … is not None else {}`), typed `Mapping[str, Decimal]`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/test_hi52_edge.py`):

```python
from decimal import Decimal
from engine.risk.limits import LimitTable
from engine.core.config import config_dir, load_yaml
from engine.strategy import hi52_edge as he
from engine.strategy.scanners import hi52

def _table(base):
    raw = load_yaml(config_dir() / "limits.yaml")
    raw["capital_base_inr"] = base
    return LimitTable.model_validate(raw)

def test_the_derivation_reproduces_the_registered_1_53_at_the_registration_base():
    assert he.derived_edge_pct(_table(40000)) == Decimal("1.53")

def test_a_smaller_base_derives_a_smaller_edge():
    assert he.derived_edge_pct(_table(20000)) < Decimal("1.53") < he.derived_edge_pct(_table(80000))

def test_the_live_map_caps_hi52_at_its_derivation_and_never_raises_it():
    reg = {hi52.STRATEGY_ID: Decimal("1.53"), "ins": Decimal("1.58")}
    cut = he.StrategyEdges(reg, table_fn=lambda: _table(20000))
    raise_ = he.StrategyEdges(reg, table_fn=lambda: _table(80000))
    assert cut[hi52.STRATEGY_ID] == he.derived_edge_pct(_table(20000))
    assert raise_[hi52.STRATEGY_ID] == Decimal("1.53")
    assert cut["ins"] == Decimal("1.58") and dict(cut).keys() == reg.keys()

def test_a_failing_derivation_keeps_the_registered_edge():
    def boom(): raise RuntimeError("limits unverified")
    assert he.StrategyEdges({hi52.STRATEGY_ID: Decimal("1.53")}, table_fn=boom)[hi52.STRATEGY_ID] == Decimal("1.53")
```

In `tests/unit/test_hi52_forward_verdict.py::test_the_registered_edge_is_this_script_s_derivation`, pin at the **registration inputs**. Copy the shipped `limits.yaml` text into `tmp_path`, with the `capital_base_inr:` line value replaced by `40000` using a regex anchored at `^capital_base_inr:`. Then call `fv.registered_edge_pct(limits_path=<that copy>)`. Keep every other assertion. Add a short comment explaining that the runtime cap (`StrategyEdges`) now carries the live-limits guard (O19).

Also add, to the gate's tests (`tests/unit/test_risk_gate.py` or the file that builds `RiskGate` with `strategy_expected_edge_pct`): a test showing that a mapping changed after construction is seen by the gate. Pass a `StrategyEdges` whose `table_fn` reads a mutable holder.

- [ ] **Step 2: Run** `tests/unit/test_hi52_edge.py tests/unit/test_hi52_forward_verdict.py` and the gate test file. The new tests must FAIL.
- [ ] **Step 3: Implement** the module, the script delegation and the two no-copy edits.
- [ ] **Step 4: Run** `tests/unit/test_hi52_edge.py tests/unit/test_hi52_forward_verdict.py tests/unit/test_risk_gate.py tests/unit/test_reco_pipeline.py tests/unit/test_import_graph.py` and paste the tail. Also run `.\.venv\Scripts\python.exe scripts\hi52_forward_verdict.py --help` to show the script still imports. Then run ruff.

### Task 4: The capital module (`engine/ops/capital.py`) and the Telegram message

**Files:**
- Create `src/engine/ops/capital.py`.
- Modify `src/engine/notify/catalog.py`: `MessageKind.CAPITAL_CHANGED` and `capital_changed(...)`.
- Tests: `tests/unit/test_capital.py` (new); the kind list in `tests/unit/test_telegram_commands.py` (~1549) if it is exhaustive.

**Interfaces:**
- Consumes:
  - Task 1: `ExposureTracker.with_capital_base`, `.capital_base`, `.evaluate_floors`, `.evaluate_day_loss`.
  - Task 2: `read_verified_bytes` and `owner_update(..., expected_sha256=)`.
  - Task 3: `derived_edge_pct` and `StrategyEdges`.
- Produces:
  - `class CapitalRefused(Exception)` with `status: int` (409, 422 or 503) and `note: str`.
  - Constants `BASE_PATH = ("capital_base_inr",)` and `SCALED_PATHS = (("limits", "capital_cap", "max_deployed_capital_inr"), ("limits", "per_stock_exposure", "cnc_notional_inr"))`.
  - `scaled_values(table: LimitTable, new_base: int) -> dict[tuple[str, ...], int]`: all three paths, with caps floored.
  - `rewrite_limits_text(text: str, old: dict, new: dict, stamp: str) -> str`.
    - For each path, the leaf key must match exactly one line `^(\s*)<leaf>:\s*(\d+)(\s*)(#[^\r\n]*)?$` (MULTILINE); otherwise `CapitalRefused(422, "limits.yaml layout: <leaf> found N times")`.
    - The value is replaced. The comment is kept, and `f" · owner via dashboard {stamp}: {old} → {new}"` is appended to it (a `# ` comment is created if there was none).
    - The text keeps LF. Every other byte is unchanged.
  - `check_structure(old_text, new_text, new: dict) -> LimitTable`.
    - Both texts are parsed with a duplicate-key-rejecting SafeLoader subclass.
    - The parsed new text must equal the parsed old one with exactly `new`'s paths changed, each to an `int` equal to the intended value.
    - Returns `LimitTable.model_validate(parsed_new)`. Any violation raises `CapitalRefused(422, …)`.
  - `class CapitalChanger`:
    - **Constructor:** `CapitalChanger(*, conn, clock, calendar, store: ProtectedStore, limits: LimitsEngine, mode: ModeManager, real: ExposureTracker, paper_mark: Callable[[str], Decimal | None] | None, edges: StrategyEdges | None, notify: Callable[[CatalogMessage], Awaitable[None]] | None)`.
    - **`preview(proposed: int | None) -> dict`:** returns `{"current": figures, "proposed": figures | None, "allowed": bool, "refusal": str | None, "would_trip": [str]}`. `figures` is a dict of strings: `capital_inr`, `max_deployed_capital_inr`, `cnc_notional_inr`, `equity_inr`, `paper_equity_inr` (or None), `risk_intraday_inr` / `risk_swing_inr` (pct × equity), `day_loss_soft_inr` / `day_loss_hard_inr` (pct × base, negative), `equity_floor_inr` / `cumulative_floor_inr` (base × (1 + pct/100)), `hi52_edge_pct` (or None).
    - **`async apply(capital_inr: int, expected_current_inr: int) -> dict`:** returns `{"ok": True, "old": figures, "new": figures, "commit_hint": "commit config/limits.yaml"}`.
- `apply` order, all synchronous until step 6:
  1. Refusals, in this order:
     - `expected_current_inr != current base` → 409;
     - mode AUTO → 409;
     - the window or calendar coverage → 409;
     - bounds → 422;
     - `read_verified_bytes` raising `IntegrityError` → 503 "protected config unverified: …".
  2. Build the new text, then `check_structure`.
  3. Brakes: for the real book (`real` vs `real.with_capital_base(new)`), and for paper when `paper_mark` is set (`ExposureTracker(conn, clock, base, mark_price=paper_mark, scope="paper")` at the old and new base), collect `evaluate_floors(floor_limits_from(table))` rung names plus `evaluate_day_loss(soft, hard)` names. Any name present at the new base and absent at the current one gives 422 "would trip: <scope>:<rung> (<₹ line>)".
  4. `owner_update(LIMITS_FILE, new_text, OwnerConfirmation(actor=Actor.OWNER, confirmed=True, note=<json of old/new figures>), expected_sha256=sha)`.
  5. `limits.reload()`.
  6. `notify(catalog.capital_changed(...))`. A failure is logged and never raised.

- [ ] **Step 1: Write the failing tests** (`tests/unit/test_capital.py`). Use a temp config dir holding a copy of the shipped `limits.yaml` and `envelope.yaml`, a `ProtectedStore` on the test `conn` registered via `register_initial`, a `LimitsEngine` on it, the repo `NSECalendar`, a `ModeManager`, a frozen `Clock`, and `ExposureTracker(conn, clock, lambda: limits.load().capital_base_inr)`. Cases:
  - **Rewrite:** only three lines change and every other line is byte-identical (compare `splitlines()`). Comments are kept and appended. A duplicate `capital_base_inr:` line gives 422. The value written is an int (`yaml.safe_load` gives `int`). Caps are floored: `12000 × 50001/40000` gives `15000`.
  - **Bounds:** ×2 and ÷2 are accepted; ×2 + 1 and ÷2 − 1 give 422; equal gives 422; 0 gives 422.
  - **Window:** with the clock at a trading day's `session.open` and `session.close`, 409; one second before open or after close, OK; a holiday, OK; a date past `verified_horizon()`, 409.
  - **Mode AUTO** → 409.
  - **Stale `expected_current_inr`** → 409 (review focus 2): apply once OK, then repeat the identical POST, which gives 409 and no second change.
  - **Unverified file** → 503 and nothing written (review focus 3): append a byte to the temp `limits.yaml`, apply, then the bytes are still the tampered ones, the signature is unchanged and there is no audit row.
  - **Brake refusal** (review focus 1): insert a closed real position with `realized_pnl = -3000` and costs 0. A cut 40000 → 20000 gives 422 naming `real:equity_floor_rung`. A raise to 60000 is OK.
  - **Paper brake refusal:** the same, with a paper position and `paper_mark` set.
  - **An already-tripped rung does not block a raise:** with lifetime P&L −4,500 at 40k (the equity floor is already tripped), raising to 60000 is OK.
  - **Real path:** apply 40000 → 60000. Then `store.load_verified("limits.yaml")` passes, `limits.load().capital_base_inr == 60000`, `real.capital_base == 60000` with no setter, the caps are 60000 and 18000, `config_audit` has one new row whose note contains "40000" and "60000", and `notify` got exactly one `CAPITAL_CHANGED`.
  - **The hi52 figure** in the preview equals `min(registered, derived)`.
- [ ] **Step 2: Run** `tests/unit/test_capital.py`; it must FAIL.
- [ ] **Step 3: Implement** `capital.py` and the catalog kind and builder. Mirror `trade_window_changed`'s style: severity "warning", title `Capital ₹old → ₹new`, the body listing every figure plus "commit config/limits.yaml".
- [ ] **Step 4: Run** `tests/unit/test_capital.py tests/unit/test_telegram_commands.py tests/unit/test_notify*.py` and paste the tail. Then run ruff.

### Task 5: Wiring (composition, API routes, live-base paper summary)

**Files:**
- Modify `src/engine/ops/main.py`:
  - the real tracker (~638-646): `_capital_base_fn` with the IntegrityError fallback, logged once, plus the `_FALLBACK_CAPITAL_BASE_INR` comment (~237-239);
  - the edges (~898-905, ~1014): a `StrategyEdges(_strategy_expected_edge_pct(settings), table_fn=limits_engine.load, cost_model=cost_model)` built once and passed at both sites;
  - paper (`PaperRisk(capital_base=lambda: limits.load().capital_base_inr, …)` ~3778);
  - a `CapitalChanger` built before `_create_app` (~1780) and passed as `capital=`.
- Modify `src/engine/api/app.py`: `create_app(..., capital=None)`, `GET /config/capital`, `POST /config/capital`, and `/paper` passing the live base.
- Modify `src/engine/ops/paper_summary.py`: `paper_summary(..., capital_base: Decimal | None = None)`. When given:
  - `equity` = base + row P&L, and `capital_base` = base;
  - each curve point's `equity` = base + point P&L (the stored equity is the fallback for a NULL row);
  - the percentages use the live base.
- Tests: `tests/unit/test_api_routes.py`, `tests/unit/test_paper_summary.py`, `tests/unit/test_ops_main_wiring.py`.

**Interfaces:**
- API contract (Task 6 consumes it):
  - `GET /config/capital?proposed=<int>` → 200 with `CapitalChanger.preview(...)`, or 501 `{ok:false, note}` when `capital` is not wired.
  - `POST /config/capital {capital_inr: int, expected_current_inr: int}` → 200 with the `apply` result; on `CapitalRefused`, `JSONResponse(status_code=e.status, content={"ok": False, "note": e.note})`.
  - `GET /paper`: as today, with the live-base semantics above.

- [ ] **Step 1: Write the failing tests:**
  - **API:** a wired `CapitalChanger` (Task 4's fixtures) gives preview 200, apply 200, a 409 shape, a 422 shape, and 501 when unwired.
  - **`paper_summary` with a live base** (review focus 5): two snapshots at base 40000 (P&L +100 and +200), called with `capital_base=Decimal("60000")`. Then `equity.capital_base == "60000.00"`, `equity.equity == "60200.00"`, and the curve is `["60100.00", "60200.00"]`, with no cliff.
  - **Wiring:** `_strategy_expected_edge_pct` results reach the gate as a `StrategyEdges` (`test_ops_main_wiring` pattern), and the real tracker's base reads through `limits_engine`.
- [ ] **Step 2: Run them; they must FAIL.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `tests/unit/test_api_routes.py tests/unit/test_paper_summary.py tests/unit/test_ops_main_wiring.py tests/unit/test_paper_composition.py tests/replay/test_paper_e2e.py tests/chaos` and paste the tail. Then run ruff.

### Task 6: Dashboard (dialog, live-base paper copy)

**Files:**
- Create `dashboard/src/views/parts/CapitalDialog.tsx`.
- Modify `dashboard/src/views/parts/RiskLimits.tsx` (a "Change capital" button), `dashboard/src/views/System.tsx` (pass the handler), `dashboard/src/App.tsx` (the `changeCapital` handler, following `setHours` / `rearm`: `apiPost`, toasts, `poll.refresh()`), `dashboard/src/types.ts` (`CapitalFigures`, `CapitalPreview`), `dashboard/src/copy/glossary.ts` (dialog copy in `COPY`), and the chart and table labels in `EquityChart.tsx`.
- Modify `dashboard/fixture_server.mjs`: `GET` and `POST /config/capital` with a scenario where `allowed` is false.
- Tests: `dashboard/tests/*.test.ts` for the pure helpers.

**Interfaces:**
- Consumes Task 5's API contract.
- `CapitalDialog` props: `{ current: string; onClose(); onConfirm(capital: number, expected: number): Promise<void> }`.
- Behaviour:
  - It fetches `apiGet<CapitalPreview>('/config/capital?proposed=' + n)` with a 300 ms debounce.
  - It shows a two-column old → new table of the engine's strings, the `refusal` text, and `would_trip`.
  - Confirm is enabled only when `allowed`.
  - The input label is "Starting capital (the base every limit is a percent of)". A line shows "Equity at this capital: ₹…" from `proposed.equity_inr`.
  - After success the toast reads: "Capital set to ₹X. Commit config/limits.yaml to keep it."

- [ ] **Step 1:** Add pure helpers with failing tests. `capitalRows(preview)` turns a preview into rows `{label, old, new}`, in a fixed order with no missing labels. `parseCapitalInput(text)` parses `"60,000"` to `60000`; `"abc"`, `"0"` and `"-5"` give null.
- [ ] **Step 2: Implement** the component, the wiring and the fixture endpoints. Follow §15 of `runbooks/design-2026-10-08-dashboard-redesign.md`: plain words, and the existing Dialog and Toast components.
- [ ] **Step 3: Run** `npm test` and `npx tsc -b` in `dashboard/` (inline) and paste the output. Do not run `npm run build`.

### Task 7: Docs

**Files:** `runbooks/RUNBOOK.md` (a capital-change section), `runbooks/COMMANDS.md` (restore `limits.yaml` from `protected_config.content_snapshot`; a note on the reseed flow after a dashboard change), the design doc, and a WORKLOG entry.

- [ ] **Step 1: Restore recipe.** Write the COMMANDS recipe and test it read-only: a Python one-liner prints the `content_snapshot` sha256 next to the recorded `sha256` and the on-disk sha256.
- [ ] **Step 2: RUNBOOK section.** What the button does; the refusals; commit `config/limits.yaml`; never stash or checkout it; the hi52 edge rule.
- [ ] **Step 3: Owner-applied comments-only patch** `runbooks/briefs/limits_comments_base_relative_2026-10-16.patch`. Restate the ₹ figures in the `limits.yaml` comments (lines 13-15, 24-25, 30-31, 36, 40, 49, 53, 75) as percentages of the base. Values do not change. Check it with `git apply --check` against a scratch copy. Add COMMANDS steps (engine stopped, apply, `seed_protected_config.py --reseed`). The manager never applies it.

### Task 8: Verification on live data, and the whole-branch review

- [ ] **Step 1: Replay** (read-only, a scratch script with `PYTHONPATH=<worktree>\src`). Over the live `state.db`, compute for every session day the old and new day baseline P&L (old: `equity − base_then`; new: the P&L columns), and the 5-session peaks. They must be identical except the windows spanning 2026-09-07, which are listed. Paste the summary.
- [ ] **Step 2: Full suite, once and alone,** from the worktree: `.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider`. Paste the tail.
- [ ] **Step 3: Multi-angle review** of `git diff 179cd45...HEAD` with line-by-line, removed-behaviour and cross-file angles. Verify each finding, fix confirmed ones, and re-run the affected tests.

### Task 9: Merge and deploy (manager; after hours)

- [ ] **Step 1: Commit** each audited task on `wip/capital-change`, with `git commit -F <file>` and no AI co-author.
- [ ] **Step 2: Fast-forward** `phase3` to the branch, in `market_trading`.
- [ ] **Step 3: Pre-flight.**
  - Check that `Get-Date` is outside the session.
  - Read the engine log tail for in-flight jobs.
  - Check that the `.pth` points at `market_trading\src`.
  - Back up `state.db`.
- [ ] **Step 4: Restart and rebuild.** Stop and start mt-engine in one command; `npm run build` in `dashboard/`.
- [ ] **Step 5: Verify the boot.** Selftest all PASS; `startup_complete`; `GET /config/capital` 200 with the token and 401 without; `GET /paper` shows the live base; no ERROR lines. Then write the WORKLOG entry.
