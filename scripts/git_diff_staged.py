#!/usr/bin/env python3
"""
Generate per-file diffs for currently staged changes and write them under the
project root directory as `git_diff_staged/`.

Usage (run from project root):
  python scripts/git_diff_staged.py

Output:
  - Creates (or overwrites) the `git_diff_staged/` directory in the project root.
  - For each staged file, writes a `<path>.diff` file.
    Example: `src/example.rs` -> `git_diff_staged/src/example.rs.diff`
"""

import shutil
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).absolute().parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_ROOT / "git_diff_staged"


def main() -> None:
    if not (PROJECT_ROOT / ".git").exists():
        print(f"❌ .git directory not found in project root: {PROJECT_ROOT}")
        sys.exit(1)

    _prepare_output_dir()

    changed_files = _git_diff_name_only_staged()
    if not changed_files:
        print("✅ No staged changes found; nothing to generate")
        return

    count = 0
    for rel_path in changed_files:
        diff_text = _git_diff_file_staged(rel_path)
        if not diff_text.strip():
            continue

        out_path = OUTPUT_DIR / (rel_path + ".diff")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(diff_text, encoding="utf-8")
        count += 1

    print(f"✅ Generated {count} file diffs under: {OUTPUT_DIR}")


def _prepare_output_dir() -> None:
    if OUTPUT_DIR.exists():
        if OUTPUT_DIR.is_file():
            print(f"❌ Path exists and is a file, cannot use as output dir: {OUTPUT_DIR}")
            sys.exit(1)
        shutil.rmtree(OUTPUT_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _run_git(args: list[str]) -> str:
    cmd = ["git"] + args
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
    except subprocess.CalledProcessError as err:
        print("❌ git command failed; cannot generate stash diffs")
        print(f"   Command: {' '.join(cmd)}")
        if err.stdout:
            print("   stdout:")
            print(err.stdout)
        if err.stderr:
            print("   stderr:")
            print(err.stderr)
        sys.exit(1)

    return result.stdout


def _git_diff_name_only_staged() -> list[str]:
    output = _run_git(["diff", "--cached", "--name-only"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def _git_diff_file_staged(relpath: str) -> str:
    return _run_git(["diff", "--cached", "--", relpath])


if __name__ == "__main__":
    main()
