from __future__ import annotations

import inspect
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Optional

__all__ = [
    "chdir_to_cur_file",
    "run_cmd",
    "run_cmd_sure",
    "_sudo_prefix",
    "run_root_cmd",
    "run_root_cmd_sure",
    "require_cmd",
    "run_cmd_argv",
]



def chdir_to_cur_file() -> None:
    """chdir to the directory of the immediate caller file."""
    caller = inspect.stack()[1].filename
    os.chdir(os.path.dirname(os.path.abspath(caller)))


def run_cmd(cmd: str, *, cwd: Optional[str] = None) -> int:
    """Run a shell command and return its exit code."""
    print(f"[cmd] {cmd}")
    p = subprocess.run(cmd, shell=True, cwd=cwd)
    return int(p.returncode)


def run_cmd_sure(cmd: str, *, cwd: Optional[str] = None) -> None:
    """Run a shell command; raise on non-zero."""
    print(f"[cmd] {cmd}")
    subprocess.check_call(cmd, shell=True, cwd=cwd)


def _sudo_prefix() -> str:
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        return "sudo -E "
    return ""


def run_root_cmd(cmd: str, *, cwd: Optional[str] = None) -> int:
    """Run a shell command with sudo when needed; return exit code."""
    return run_cmd(_sudo_prefix() + cmd, cwd=cwd)


def run_root_cmd_sure(cmd: str, *, cwd: Optional[str] = None) -> None:
    """Run a shell command with sudo when needed; raise on non-zero."""
    run_cmd_sure(_sudo_prefix() + cmd, cwd=cwd)


def require_cmd(name: str) -> None:
    if shutil.which(name) is None:
        print(f"Missing required command in PATH: {name}")
        raise SystemExit(1)


def run_cmd_argv(argv: list[str], *, cwd: Path | None = None) -> None:
    printable = " ".join(shlex.quote(x) for x in argv)
    print(f"[cmd] {printable}")
    subprocess.check_call(argv, cwd=str(cwd) if cwd is not None else None)
