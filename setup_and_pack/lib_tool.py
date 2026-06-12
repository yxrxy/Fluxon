"""Compatibility wrapper for legacy helper imports.

Legacy pack/build entrypoints import `lib_tool` from the public helper
directory. Re-export the supported helper surface from `setup_and_pack.utils`.
"""

from setup_and_pack.utils import *  # noqa: F401,F403
