#!/usr/bin/env python3
"""Run the platform q test files under pykx and fail if any check fails.

The q tests (schema_test.q, analytics_test.q) maintain a `fails` counter and end
with `exit 0/1`. Under a real q binary the exit code is the verdict, but q's
`exit` keyword raises 'nyi inside pykx's embedded q, so here we strip the exit
lines and read `fails` directly.

Each file runs in its OWN subprocess (a fresh q): q is single-threaded per
process and schema.q/analytics.q define overlapping globals (`fails`, `assert`,
...), so running them in one session would cross-contaminate.

Usage:
    python platform/run_q_tests.py                 # all platform/*_test.q
    python platform/run_q_tests.py path/to/x_test.q ...
Exit code 0 iff every file reports fails=0.
"""
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# a bare `exit` statement: start-of-line or after `;`, then exit + space/[/;/eol
_EXIT = re.compile(r"(^|;)\s*exit(\s|\[|;|$)")


def _fails_for(path: Path) -> int:
    """Load one test file with its exit lines stripped and return `fails`.
    Raises if the q load errors or the file defines no `fails` counter."""
    import pykx as kx  # deferred: only the worker needs a licensed q

    body = "\n".join(ln for ln in path.read_text().splitlines() if not _EXIT.search(ln))
    # Temp file can live anywhere: the test's own `\l platform/*.q` is relative to
    # the process cwd (ROOT), not to this file's location.
    with tempfile.NamedTemporaryFile("w", suffix=".q", delete=False) as fh:
        fh.write(body)
        tmp = fh.name
    try:
        kx.q("system", ("l " + Path(tmp).as_posix()).encode())
        return int(kx.q("fails").py())
    finally:
        os.unlink(tmp)


def _run_worker(path: Path) -> int:
    """Single-file worker (own process): print the verdict, return an exit code
    (0 pass, 1 fail, 2 load/setup error)."""
    try:
        fails = _fails_for(path)
    except Exception as exc:  # q error, missing counter, license failure, ...
        print(f"ERROR {path}: {type(exc).__name__}: {exc}")
        return 2
    verdict = "PASS" if fails == 0 else "FAIL"
    print(f"{verdict} {path}: {fails} failure(s)")
    return 0 if fails == 0 else 1


def main(argv: list[str]) -> int:
    os.chdir(ROOT)  # so each test's relative `\l platform/*.q` resolves
    if argv and argv[0] == "--worker":
        return _run_worker(Path(argv[1]))

    files = [Path(a) for a in argv] or sorted((ROOT / "platform").glob("*_test.q"))
    if not files:
        print("no q test files found")
        return 1
    failures = [f for f in files
                if subprocess.call([sys.executable, __file__, "--worker", str(f)]) != 0]
    ok = not failures
    print(f"\n{'ALL Q TESTS PASSED' if ok else 'Q TESTS FAILED'} ({len(files)} file(s)"
          + ("" if ok else f"; failed: {', '.join(map(str, failures))}") + ")")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
