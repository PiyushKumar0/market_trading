"""Mechanical per-test inventory (CSV + shape groups). Stdlib only."""
import argparse
import ast
import copy
import csv
import hashlib
import re
import statistics
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

COLUMNS = [
    "nodeid", "file", "line", "class", "function", "cases", "tier_guess", "tier_reason",
    "dur_total_s", "dur_setup_s", "dur_call_s", "dur_teardown_s", "outcome", "body_lines",
    "asserts", "raises", "fixtures", "monkeypatch", "n_monkeypatch", "caplog", "getsource",
    "markers", "parametrized", "cite_wo", "cite_section", "cite_date", "cite_memory",
    "cite_plan_id", "shape_hash", "shape_hash_loose", "docstring",
]

A_DIRS = ("tests/chaos/", "tests/replay/", "tests/property/")
A_GUARDS = {
    "tests/unit/test_live_impossibility.py", "tests/unit/test_import_graph.py",
    "tests/unit/test_scope_isolation.py", "tests/unit/test_api_route_auth.py",
    "tests/unit/test_single_instance.py",
}
KW = ["pre-registered", "preregistered", "pre_registered", "registered baseline",
      "registered_baseline", "registered construct", "kill rule", "kill_rule",
      "shadow threshold", "frozen threshold", "thresholds frozen", "thresholds are frozen",
      "verdict threshold"]
B_STEMS = ["kill", "watchdog", "heartbeat", "limits", "protected", "envelope", "risk", "warmup",
           "ticker_supervisor", "token", "session_verify", "post_login", "catchup", "reconcil",
           "oms", "order_", "paper_broker", "fill_model", "protection", "exit_manager",
           "contracts_placement", "broker_surface", "cost_model", "money", "capital",
           "position_book", "exposure", "calendar", "clock", "migration", "mode_manager",
           "holds", "stop_path", "db_transaction", "_wo"]
MP_METHODS = {"setattr", "setenv", "delattr", "delenv", "setitem", "delitem", "chdir",
              "syspath_prepend"}
RE_WO = re.compile(r"\bWO-?\d+[a-z]?(?:-prime)?\b")
RE_SEC = re.compile(r"§\s?\d+(?:\.\d+)*")
RE_DATE = re.compile(r"\b20\d\d-\d\d-\d\d\b")
RE_PLAN = re.compile(r"\b(?:O\d{1,2}|D\d{1,2}|Q\d\.\d+|P3-\d+|G[12]|C[1-5])\b")
RE_DUR = re.compile(r"^\s*(\d+(?:\.\d+)?)s\s+(setup|call|teardown)\s+(\S.*?)\s*$")


def distinct(rx, text):
    seen = []
    for m in rx.findall(text):
        if m not in seen:
            seen.append(m)
    return ";".join(seen)


def junit_key(nodeid):
    i = nodeid.find("[")
    head, tail = (nodeid, "") if i < 0 else (nodeid[:i], nodeid[i:])
    return head.replace(".py::", ".").replace("::", ".").replace("/", ".") + tail


def load_junit(path, keymap):
    """Return {nodeid: (time, outcome)} for one junit file."""
    out = {}
    for tc in ET.parse(path).getroot().iter("testcase"):
        key = tc.get("classname", "") + "." + tc.get("name", "")
        nodeids = keymap.get(key)
        if not nodeids:
            continue
        if tc.find("error") is not None:
            oc = "error"
        elif tc.find("failure") is not None:
            oc = "failed"
        elif tc.find("skipped") is not None:
            oc = "skipped"
        else:
            oc = "passed"
        for n in nodeids:
            out[n] = (float(tc.get("time", "0") or 0), oc)
    return out


def load_durations(path, known):
    out = {}
    on = False
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if "slowest durations" in line:
                on = True
                continue
            if not on:
                continue
            m = RE_DUR.match(line)
            if m and m.group(3) in known:
                out[(m.group(3), m.group(2))] = float(m.group(1))
    return out


# ---------------------------------------------------------------- AST helpers
def is_pytest_mark(node):
    """Return the mark name if node is pytest.mark.<x> or a call of it."""
    if isinstance(node, ast.Call):
        node = node.func
    if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Attribute)
            and node.value.attr == "mark" and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "pytest"):
        return node.attr
    return None


def marks_of(decorators):
    return [m for m in (is_pytest_mark(d) for d in decorators) if m]


def module_pytestmark(tree):
    names = []
    for st in tree.body:
        if isinstance(st, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "pytestmark" for t in st.targets):
            v = st.value
            elts = v.elts if isinstance(v, (ast.List, ast.Tuple)) else [v]
            names += [m for m in (is_pytest_mark(e) for e in elts) if m]
    return names


def find_def(tree, classes, name):
    """Last definition of `name` inside the class chain; returns (node, class_nodes)."""
    body, chain = tree.body, []
    for c in classes:
        cand = [s for s in body if isinstance(s, ast.ClassDef) and s.name == c]
        if not cand:
            return None, chain
        chain.append(cand[-1])
        body = cand[-1].body
    cand = [s for s in body if isinstance(s, (ast.FunctionDef, ast.AsyncFunctionDef))
            and s.name == name]
    return (cand[-1] if cand else None), chain


def preorder(node):
    yield node
    for ch in ast.iter_child_nodes(node):
        yield from preorder(ch)


class Normalizer(ast.NodeTransformer):
    def __init__(self, rename):
        self.rename = rename  # None or dict id -> new name (filled lazily)
        self.bound = set()
        self.counter = 0

    def _ren(self, ident):
        if self.rename is None or ident not in self.bound:
            return ident
        if ident not in self.rename:
            self.rename[ident] = "v%d" % self.counter
            self.counter += 1
        return self.rename[ident]

    def visit_Constant(self, node):
        return ast.Constant(value=type(node.value).__name__)

    def visit_Name(self, node):
        node.id = self._ren(node.id)
        return node

    def visit_ExceptHandler(self, node):
        if node.name:
            node.name = self._ren(node.name)
        self.generic_visit(node)
        return node

    def _def(self, node):
        node.name = self._ren(node.name)
        self.generic_visit(node)
        return node

    visit_FunctionDef = visit_AsyncFunctionDef = visit_ClassDef = _def


def bound_names(stmts):
    b = set()
    for st in stmts:
        for n in preorder(st):
            if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
                b.add(n.id)
            elif isinstance(n, ast.ExceptHandler) and n.name:
                b.add(n.name)
            elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                b.add(n.name)
    return b


def shape_hashes(fn):
    body = list(fn.body)
    if (body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)):
        body = body[1:]
    res = []
    for loose in (False, True):
        stmts = copy.deepcopy(body)
        nz = Normalizer({} if loose else None)
        if loose:
            nz.bound = bound_names(stmts)
        # pre-order renaming: NodeTransformer visits fields in order, depth-first.
        mod = ast.Module(body=[nz.visit(s) for s in stmts], type_ignores=[])
        d = ast.dump(mod, annotate_fields=False, include_attributes=False)
        res.append(hashlib.sha1(d.encode("utf-8")).hexdigest()[:12])
    return res


def analyse(fn, classes, tree, lines, slugs, pm_module):
    start = min([fn.lineno] + [d.lineno for d in fn.decorator_list])
    text = "\n".join(lines[start - 1:fn.end_lineno])
    nodes = list(ast.walk(fn))
    args = fn.args
    params = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs]
    if args.vararg:
        params.append(args.vararg.arg)
    if args.kwarg:
        params.append(args.kwarg.arg)
    fixtures = [p for p in params if p != "self"]
    raises = mp = 0
    mps = []
    getsrc = 0
    caplog = 1 if "caplog" in params else 0
    for n in nodes:
        if isinstance(n, ast.Call):
            f = n.func
            if (isinstance(f, ast.Attribute) and f.attr == "raises"
                    and isinstance(f.value, ast.Name) and f.value.id == "pytest"):
                raises += 1
            if (isinstance(f, ast.Attribute) and f.attr in MP_METHODS
                    and isinstance(f.value, ast.Name) and f.value.id == "monkeypatch"):
                first = ast.unparse(n.args[0]) if n.args else ""
                mps.append("%s:%s" % (f.attr, first))
        if isinstance(n, ast.Attribute) and n.attr == "getsource":
            getsrc = 1
        if isinstance(n, ast.Name):
            if n.id == "getsource":
                getsrc = 1
            if n.id == "caplog":
                caplog = 1
    decs = list(fn.decorator_list)
    for c in classes:
        decs += c.decorator_list
    markers = []
    for m in marks_of(fn.decorator_list) + sum((marks_of(c.decorator_list) for c in classes), []) \
            + pm_module:
        if m != "parametrize" and m not in markers:
            markers.append(m)
    parametrized = 1 if "parametrize" in marks_of(decs) else 0
    doc = ast.get_docstring(fn, clean=True) or ""
    doc = doc.strip().split("\n")[0][:160].replace("\r", "") if doc.strip() else ""
    sh, shl = shape_hashes(fn)
    return {
        "line": fn.lineno, "body_lines": fn.end_lineno - fn.lineno + 1,
        "asserts": sum(isinstance(n, ast.Assert) for n in nodes), "raises": raises,
        "fixtures": ";".join(fixtures), "monkeypatch": ";".join(mps), "n_monkeypatch": len(mps),
        "caplog": caplog, "getsource": getsrc, "markers": ";".join(markers),
        "parametrized": parametrized, "cite_wo": distinct(RE_WO, text),
        "cite_section": distinct(RE_SEC, text), "cite_date": distinct(RE_DATE, text),
        "cite_memory": ";".join(s for s in slugs if s in text),
        "cite_plan_id": distinct(RE_PLAN, text), "shape_hash": sh, "shape_hash_loose": shl,
        "docstring": doc, "_text": text,
    }


def tier(file, stem, text):
    if file.startswith(A_DIRS):
        return "A", "path:a-dir"
    if file in A_GUARDS:
        return "A", "path:a-guard"
    low = text.lower()
    for k in KW:
        if k in low:
            return "A?", "kw:" + k
    s = stem[5:] if stem.startswith("test_") else stem
    for k in B_STEMS:
        if k in s:
            return "B", "path:b-module"
    return None


def fmt(v):
    return "" if v is None else ("%.4f" % v).rstrip("0").rstrip(".") if isinstance(v, float) else v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodeids", required=True)
    ap.add_argument("--junit", nargs="+", required=True)
    ap.add_argument("--durations-text", nargs="+", required=True)
    ap.add_argument("--memory-index", required=True)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    t0 = time.time()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    nodeids = [l.strip() for l in Path(a.nodeids).read_text(encoding="utf-8").splitlines()
               if l.strip()]
    known = set(nodeids)
    keymap = defaultdict(list)
    for n in nodeids:
        keymap[junit_key(n)].append(n)
    junits = [load_junit(p, keymap) for p in a.junit]
    durs = [load_durations(p, known) for p in a.durations_text]
    mem = Path(a.memory_index).read_text(encoding="utf-8")
    slugs = list(dict.fromkeys(re.findall(r"\(([^()\s]+)\.md\)", mem)))

    def func_of(n):
        i = n.find("[")
        return n if i < 0 else n[:i]

    cases = defaultdict(int)
    for n in nodeids:
        cases[func_of(n)] += 1

    root = Path.cwd()
    parsed = {}
    parse_errors = {}

    def get_file(f):
        if f not in parsed:
            p = root / f
            try:
                src = p.read_text(encoding="utf-8-sig")
                tree = ast.parse(src)
                lines = re.split(r"\r\n|\r|\n", src)
                parsed[f] = (tree, lines, module_pytestmark(tree))
            except (OSError, SyntaxError, UnicodeDecodeError) as e:
                parse_errors[f] = "%s: %s" % (type(e).__name__, e)
                parsed[f] = None
        return parsed[f]

    cache = {}
    rows, unmapped = [], []
    for n in nodeids:
        func = func_of(n)
        parts = func.split("::")
        file, name, classes = parts[0], parts[-1], parts[1:-1]
        row = {c: "" for c in COLUMNS}
        row.update(nodeid=n, file=file, function=func, cases=cases[func],
                   **{"class": ".".join(classes)})
        info = None
        pf = get_file(file)
        if pf:
            if func not in cache:
                tree, lines, pm = pf
                fn, chain = find_def(tree, classes, name)
                cache[func] = analyse(fn, chain, tree, lines, slugs, pm) if fn else None
            info = cache[func]
        if info is None:
            unmapped.append(n)
            text = ""
        else:
            text = info["_text"]
            row.update({k: v for k, v in info.items() if k != "_text"})
        if "[" in n:
            row["parametrized"] = 1
        elif info is None:
            row["parametrized"] = ""
        t = tier(file, Path(file).stem, text)
        if t is None:
            cited = any(row[c] != "" for c in ("cite_wo", "cite_section", "cite_date",
                                               "cite_memory"))
            t = ("B", "cite") if cited else ("C", "default")
        row["tier_guess"], row["tier_reason"] = t
        times = [j[n][0] for j in junits if n in j]
        row["dur_total_s"] = fmt(statistics.median(times)) if times else ""
        for ph in ("setup", "call", "teardown"):
            row["dur_%s_s" % ph] = fmt(statistics.median([d.get((n, ph), 0.0) for d in durs]))
        row["outcome"] = junits[0][n][1] if n in junits[0] else ""
        rows.append(row)

    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "inventory.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    # ---- shape groups
    funcs = {}
    for r in rows:
        if r["shape_hash"] and r["function"] not in funcs:
            funcs[r["function"]] = r
    strict, loose = defaultdict(list), defaultdict(list)
    for r in funcs.values():
        strict[r["shape_hash"]].append(r)
        loose[r["shape_hash_loose"]].append(r)
    s_groups = [(h, m) for h, m in strict.items() if len(m) >= 2]
    l_groups = [(h, m) for h, m in loose.items()
                if len(m) >= 2 and len({x["shape_hash"] for x in m}) > 1]

    def key(g):
        ms = sorted(g[1], key=lambda r: (r["file"], r["line"]))
        return (-len(ms), ms[0]["file"], ms[0]["line"])

    def render(title, groups):
        o = ["== %s: %d groups ==" % (title, len(groups)), ""]
        for h, m in sorted(groups, key=key):
            o.append("%s  members=%d" % (h, len(m)))
            for r in sorted(m, key=lambda r: (r["file"], r["line"])):
                o.append("  %s:%s %s" % (r["file"], r["line"], r["function"].split("::", 1)[1]))
            o.append("")
        return o

    lines_out = render("STRICT shape_hash", s_groups) + render(
        "LOOSE-ONLY shape_hash_loose (members differ in strict hash)", l_groups)
    (out / "shape_groups.txt").write_text("\n".join(lines_out), encoding="utf-8")

    # ---- summary
    print("rows written: %d (nodeids in input: %d)" % (len(rows), len(nodeids)))
    print("unmapped to a function definition: %d" % len(unmapped))
    for n in unmapped:
        print("  " + n)
    print("files that failed to read/parse: %d" % len(parse_errors))
    for f, e in parse_errors.items():
        print("  %s -> %s" % (f, e))
    for p, j in zip(a.junit, junits):
        miss = [n for n in nodeids if n not in j]
        print("missing from %s: %d" % (p, len(miss)))
        for n in miss:
            print("  " + n)
    cnt = defaultdict(int)
    for r in rows:
        cnt[r["tier_guess"]] += 1
    print("tier_guess counts: " + ", ".join("%s=%d" % kv for kv in sorted(cnt.items())))
    print("shape groups: strict=%d loose-only=%d" % (len(s_groups), len(l_groups)))
    print("30 slowest by dur_total_s:")
    timed = [r for r in rows if r["dur_total_s"] != ""]
    for r in sorted(timed, key=lambda r: -float(r["dur_total_s"]))[:30]:
        print("  %8s  %s" % (r["dur_total_s"], r["nodeid"]))
    print("runtime: %.2fs" % (time.time() - t0))


if __name__ == "__main__":
    main()
