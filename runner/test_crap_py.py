"""The scorer's own rules, checked against a fixture so the gate is not measuring itself wrong."""
import ast
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import crap_py  # noqa: E402

FIXTURE = '''
def outer(a, b):
    if a and b or not a:
        pass
    def inner(x):
        if x:
            return 1
        return 0
    return [i for i in range(3) if i] + [inner(a)]

one_liner = lambda z: z + 1

class Thing:
    def method(self, items):
        for item in items:
            try:
                item()
            except ValueError:
                pass
        return 2 if items else 3
'''


class ComplexityTest(unittest.TestCase):
    def setUp(self):
        self.tree = ast.parse(FIXTURE)
        self.by_name = {name: cc for name, _line, _code, cc in crap_py.functions_in(self.tree)}

    def test_nested_functions_do_not_count_toward_their_parent(self):
        # outer: if (+1), `a and b` (+1), `... or ...` (+1), comprehension (+1), its `if` (+1) = 6; inner's `if` is inner's
        self.assertEqual(self.by_name["outer"], 6)
        self.assertEqual(self.by_name["outer/inner"], 2)

    def test_loops_excepts_and_ternaries_count(self):
        self.assertEqual(self.by_name["Thing.method"], 4)

    def test_lambdas_are_scored_on_their_own(self):
        self.assertEqual(self.by_name["<lambda>"], 1)


class CoverageAttributionTest(unittest.TestCase):
    def test_a_function_the_tests_never_called_scores_zero_even_when_its_def_line_ran(self):
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as handle:
            handle.write("def never(): return 1\n\ndef called():\n    return 2\n")
            path = handle.name
        try:
            rows = crap_py.report(path, hit_lines={1, 3, 4}, called={path: {"called"}})
            by_name = {row["name"]: row for row in rows}
            self.assertEqual(by_name["never"]["coverage"], 0.0)
            self.assertEqual(by_name["called"]["coverage"], 1.0)
        finally:
            os.unlink(path)

    def test_crap_formula(self):
        self.assertEqual(crap_py.crap_score(1, 1.0), 1)
        self.assertEqual(crap_py.crap_score(3, 0.0), 12)
        self.assertAlmostEqual(crap_py.crap_score(7, 0.6), 49 * 0.064 + 7)


if __name__ == "__main__":
    unittest.main()
