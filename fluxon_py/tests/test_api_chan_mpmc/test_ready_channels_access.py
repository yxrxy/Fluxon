#!/usr/bin/env python3
"""Static analysis for direct ready_channels access in the MPMC implementation."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List, Optional, Tuple


class ReadyChannelsAccessError(RuntimeError):
    """Raised when direct ready_channels access is detected."""


class ReadyChannelsAccessChecker(ast.NodeVisitor):
    """AST visitor used to detect direct access to ready_channels."""

    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.violations: List[Tuple[int, int, str]] = []
        self.in_getter_setter = False
        self.current_function: Optional[str] = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Track functions that are allowed to touch ready_channels."""
        previous_function = self.current_function
        previous_flag = self.in_getter_setter

        self.current_function = node.name
        self.in_getter_setter = node.name in {
            "get_ready_channels",
            "set_ready_channels",
            "_update_ready_channels",
            "__init__",
        }

        self.generic_visit(node)

        self.current_function = previous_function
        self.in_getter_setter = previous_flag

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: D401 - inherited docstring
        if (
            isinstance(node.value, ast.Name)
            and node.value.id == "self"
            and node.attr == "ready_channels"
            and not self.in_getter_setter
        ):
            self.violations.append(
                (
                    node.lineno,
                    node.col_offset,
                    "Direct access to 'self.ready_channels' detected. "
                    "Use get_ready_channels()/set_ready_channels() instead.",
                )
            )

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: D401 - inherited docstring
        for target in node.targets:
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
                and target.attr == "ready_channels"
                and not self.in_getter_setter
            ):
                self.violations.append(
                    (
                        target.lineno,
                        target.col_offset,
                        "Direct assignment to 'self.ready_channels' detected. "
                        "Use set_ready_channels() instead.",
                    )
                )

        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: D401 - inherited docstring
        target = node.target
        if (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
            and target.attr == "ready_channels"
            and not self.in_getter_setter
        ):
            self.violations.append(
                (
                    target.lineno,
                    target.col_offset,
                    "Direct augmented assignment to 'self.ready_channels' detected. "
                    "Use get_ready_channels()/set_ready_channels() instead.",
                )
            )

        self.generic_visit(node)


def check_file(filepath: Path) -> List[Tuple[int, int, str]]:
    """Check a single Python file for direct ready_channels access."""
    try:
        content = filepath.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReadyChannelsAccessError(f"Error reading {filepath}: {exc}") from exc

    try:
        tree = ast.parse(content, filename=str(filepath))
    except SyntaxError as exc:
        raise ReadyChannelsAccessError(f"Syntax error in {filepath}: {exc}") from exc

    checker = ReadyChannelsAccessChecker(str(filepath))
    checker.visit(tree)
    return checker.violations


def validate_ready_channels() -> None:
    """Run the ready_channels static analysis and raise on violations."""
    script_dir = Path(__file__).parent
    package_root = script_dir.parents[1]
    mpmc_file = package_root / "_api_ext_chan" / "mpmc.py"

    if not mpmc_file.exists():
        raise ReadyChannelsAccessError(f"Error: MPMC file not found at {mpmc_file}")

    print(f"Checking MPMC ready_channels access in: {mpmc_file}")
    violations = check_file(mpmc_file)

    if violations:
        print(f"\n{mpmc_file}:")
        for line, column, message in violations:
            print(f"  Line {line}, Column {column}: {message}")

        raise ReadyChannelsAccessError(
            "Found direct ready_channels access violations in MPMC module."
        )

    print("\nSummary:")
    print("Files checked: 1 (mpmc.py)")
    print("Total violations: 0")
    print("✅ No direct ready_channels access violations found in MPMC module!")


if __name__ == "__main__":
    validate_ready_channels()
