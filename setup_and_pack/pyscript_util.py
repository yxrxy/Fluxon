"""Compatibility wrapper for legacy helper imports.

Closed-side pack scripts still import `pyscript_util` as a flat module. The
implementation now lives under `setup_and_pack.utils`.
"""

from setup_and_pack.utils import *  # noqa: F401,F403
