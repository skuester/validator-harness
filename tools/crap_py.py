#!/usr/bin/env python3
"""CRAP (Change Risk Anti-Patterns) for the Python runner, standard library only.

    crap = cc^2 * (1 - coverage)^3 + cc

cc is cyclomatic complexity from the ast: one point per if, elif, for, while, except, if-expression,
comprehension clause, non-wildcard match case, and each `and`/`or` operand beyond the first. Nested
functions and lambdas are scored on their own and do not count toward the function that contains them.

Coverage is the share of a function's own executable lines, taken from its compiled code object so nested
functions' lines are never attributed to the parent, that ran while the unit tests ran under `trace`. A
function the tests never called scores 0% even if its only line is the `def` line that created it.

Forked validator children exit with os._exit, so lines that run only in a child count as uncovered here;
the TypeScript integration tests exercise those for real.

    python3 tools/crap_py.py [--max 12] [--tests runner] [--show 15]
"""
import argparse
import ast
import dis
import os
import sys
import trace
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

DECISION_NODES = (ast.If, ast.For, ast.AsyncFor, ast.While, ast.ExceptHandler, ast.IfExp)
FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
MATCH_CASE = getattr(ast, "match_case", None)
MATCH_AS = getattr(ast, "MatchAs", None)


def is_wildcard_case(case):
    pattern = case.pattern
    return MATCH_AS is not None and isinstance(pattern, MATCH_AS) and pattern.pattern is None and pattern.name is None


def complexity(function):
    """McCabe over one function body; stops at nested functions, which get their own score."""
    count = 1

    def visit(node):
        nonlocal count
        for child in ast.iter_child_nodes(node):
            if isinstance(child, FUNCTION_NODES):
                continue
            if isinstance(child, DECISION_NODES):
                count += 1
            elif isinstance(child, ast.BoolOp):
                count += len(child.values) - 1
            elif isinstance(child, ast.comprehension):
                count += 1 + len(child.ifs)
            elif MATCH_CASE is not None and isinstance(child, MATCH_CASE) and not is_wildcard_case(child):
                count += 1
            visit(child)

    visit(function)
    return count


def functions_in(tree):
    """(qualified name, def line, code-object name, complexity) for every def and lambda, nested ones included."""
    found = []

    def visit(node, prefix):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, prefix + child.name + ".")
            elif isinstance(child, FUNCTION_NODES):
                is_lambda = isinstance(child, ast.Lambda)
                name = prefix + ("<lambda>" if is_lambda else child.name)
                found.append((name, child.lineno, "<lambda>" if is_lambda else child.name, complexity(child)))
                visit(child, name + "/")
            else:
                visit(child, prefix)

    visit(tree, "")
    return found


def code_objects(path, source):
    """Every code object in the module with its own executable lines (nested code objects excluded by construction)."""
    found = []
    pending = [compile(source, path, "exec")]
    while pending:
        code = pending.pop()
        lines = {line for _, line in dis.findlinestarts(code) if line is not None}
        found.append((code.co_name, code.co_firstlineno, lines))
        pending.extend(const for const in code.co_consts if hasattr(const, "co_code"))
    return found


def crap_score(cc, coverage):
    return cc ** 2 * (1 - coverage) ** 3 + cc


def run_tests_traced(test_dir):
    """Two traced runs: one counting lines, one counting function calls (trace cannot do both at once)."""
    # No ignoredirs: trace caches ignore decisions by bare module name, so ignoring the stdlib's
    # unittest/runner.py would silently ignore our runner.py as well.
    line_tracer = trace.Trace(count=True, trace=False)
    result = line_tracer.runfunc(unittest.TextTestRunner(verbosity=1, stream=sys.stderr).run, unittest.defaultTestLoader.discover(test_dir, pattern="test_*.py"))
    hit_lines = {}
    for (filename, line), _count in line_tracer.results().counts.items():
        hit_lines.setdefault(os.path.abspath(filename), set()).add(line)
    call_tracer = trace.Trace(count=False, trace=False, countfuncs=True)
    call_tracer.runfunc(unittest.TextTestRunner(verbosity=0, stream=open(os.devnull, "w")).run, unittest.defaultTestLoader.discover(test_dir, pattern="test_*.py"))
    called = {}
    for filename, _module, funcname in call_tracer.results().calledfuncs:
        called.setdefault(os.path.abspath(filename), set()).add(funcname)
    return result, hit_lines, called


def report(path, hit_lines, called):
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    tree = ast.parse(source, path)
    objects = code_objects(path, source)
    rows = []
    for name, lineno, code_name, cc in functions_in(tree):
        # The code object whose first line is the def (or the decorator above it) and whose name matches.
        matches = [lines for co_name, first, lines in objects if co_name == code_name and first <= lineno and lineno - first <= 8]
        lines = max(matches, key=len) if matches else set()
        # trace reports methods as Class.method and nested functions by bare name; our names are Class.method or outer/inner.
        called_here = called.get(path, set())
        if code_name != "<lambda>" and name not in called_here and code_name not in called_here:
            coverage = 0.0
        elif lines:
            coverage = len([line for line in lines if line in hit_lines]) / len(lines)
        else:
            coverage = 1.0
        rows.append({"file": os.path.relpath(path, ROOT), "line": lineno, "name": name, "cc": cc, "coverage": coverage, "crap": crap_score(cc, coverage)})
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=float, default=12.0)
    parser.add_argument("--tests", default=os.path.join(ROOT, "runner"))
    parser.add_argument("--show", type=int, default=15)
    parser.add_argument("--files", nargs="*", default=[os.path.join(ROOT, "runner", "runner.py"), os.path.join(ROOT, "runner", "adapter_example.py")])
    args = parser.parse_args()

    sys.path.insert(0, args.tests)
    result, hit_lines, called = run_tests_traced(args.tests)
    if not result.wasSuccessful():
        print("tests failed; not scoring", file=sys.stderr)
        return 2

    rows = []
    for path in args.files:
        path = os.path.abspath(path)
        rows.extend(report(path, hit_lines.get(path, set()), called))
    rows.sort(key=lambda row: (-row["crap"], -row["cc"]))
    failing = [row for row in rows if row["crap"] >= args.max]
    width = max([len(row["name"]) for row in rows] + [8])
    print(f"{'function'.ljust(width)}  {'cc':>3}  {'cov':>5}  {'crap':>6}  location")
    for row in rows[: max(args.show, len(failing))]:
        flag = "  <-- over" if row["crap"] >= args.max else ""
        print(f"{row['name'].ljust(width)}  {row['cc']:>3}  {round(row['coverage'] * 100):>4}%  {row['crap']:>6.1f}  {row['file']}:{row['line']}{flag}")
    worst = rows[0] if rows else None
    print(f"\n{len(rows)} functions; worst {worst['crap']:.1f} ({worst['name']}); threshold {args.max:g}; {len(failing)} over" if worst else "no functions")
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
