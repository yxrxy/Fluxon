#!/usr/bin/env python3
"""
Generate per-file diffs of the current workspace against origin/master and write them
into the project-root git_diff_to_master directory.

Usage:
  Run from the project root:
    python scripts/git_diff_to_master.py

Effect:
  - Creates (or overwrites) the git_diff_to_master directory at project root
  - For each changed file, generates a corresponding .diff file under that directory
    For example, src/example.rs -> git_diff_to_master/src/example.rs.diff
"""

import subprocess
import sys
from pathlib import Path
import shutil


SCRIPT_DIR = Path(__file__).absolute().parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_ROOT / "git_diff_to_master"


def main() -> None:
    if not (PROJECT_ROOT / ".git").exists():
        print(f"❌ .git directory not found in project root: {PROJECT_ROOT}")
        sys.exit(1)

    _prepare_output_dir()

    changed_files = _git_diff_name_only()
    if not changed_files:
        print("✅ No differences from origin/master; no diff files generated")
        return

    count = 0
    for rel_path in changed_files:
        diff_text = _git_diff_file(rel_path)
        if not diff_text.strip():
            # This should not happen; if it does, the file likely matches origin/master again.
            continue

        out_path = OUTPUT_DIR / (rel_path + ".diff")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(diff_text, encoding="utf-8")
        count += 1

    print(f"✅ Generated diffs for {count} files under {OUTPUT_DIR}")


def _prepare_output_dir() -> None:
    if OUTPUT_DIR.exists():
        if OUTPUT_DIR.is_file():
            print(f"❌ Path {OUTPUT_DIR} exists and is a file; cannot use it as an output directory")
            sys.exit(1)

        # To avoid stale historical diff files, delete the whole directory and recreate it.
        # If you want to keep historical diffs, replace this with deleting only .diff files.
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
        print("❌ git command failed; cannot generate diffs")
        print(f"   command: {' '.join(cmd)}")
        if err.stdout:
            print("   stdout:")
            print(err.stdout)
        if err.stderr:
            print("   stderr:")
            print(err.stderr)
        sys.exit(1)

    return result.stdout


def _git_diff_name_only() -> list[str]:
    # List files changed compared to origin/master (paths relative to project root).
    output = _run_git(["diff", "--name-only", "origin/master"])
    return [line.strip() for line in output.splitlines() if line.strip()]


def _git_diff_file(relpath: str) -> str:
    # Get the full diff for a single file relative to origin/master.
    return _run_git(["diff", "origin/master", "--", relpath])


if __name__ == "__main__":
    main()
