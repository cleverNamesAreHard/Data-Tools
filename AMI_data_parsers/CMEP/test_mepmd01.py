import sys
import unittest
from pathlib import Path

from test.unit_tests import TestParseMEPMD01


def main():
    root_dir = Path(__file__).resolve().parent
    test_dir = root_dir / "test"

    if not test_dir.is_dir():
        print(f"Test directory not found: {test_dir}", file=sys.stderr)
        return 1

    root_str = str(root_dir)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    suite = unittest.TestLoader().loadTestsFromTestCase(TestParseMEPMD01)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    raise SystemExit(main())