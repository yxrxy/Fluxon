from __future__ import annotations

import importlib.util
import os
import site
from pathlib import Path
from typing import Any

_FLUXON_PYO3_MODULE_LAZY: Any = None


def _path_contains_fluxon_pyo3_libs_dir(path: Path) -> bool:
    return "fluxon_pyo3.libs" in path.parts


def _path_is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_fluxon_pyo3_module_origin() -> Path:
    spec = importlib.util.find_spec("fluxon_pyo3")
    if spec is None or spec.origin is None:
        raise RuntimeError("failed to resolve fluxon_pyo3 module spec before import")
    return Path(spec.origin).resolve()


def _resolve_fluxon_pyo3_libs_dir(module_origin: Path) -> Path | None:
    module_parent = module_origin.parent
    if module_parent.name == "fluxon_pyo3":
        libs_dir = module_parent.parent / "fluxon_pyo3.libs"
    else:
        libs_dir = module_parent / "fluxon_pyo3.libs"
    if not libs_dir.is_dir():
        return None
    return libs_dir


def _set_authoritative_bundled_ld_library_path(libs_dir: Path) -> None:
    authoritative_entry = str(libs_dir)
    sanitized_entries = [authoritative_entry]
    seen_entries = {authoritative_entry}
    current_ld_library_path = os.environ.get("LD_LIBRARY_PATH")
    if current_ld_library_path is not None:
        for raw_entry in current_ld_library_path.split(":"):
            entry = raw_entry.strip()
            if not entry:
                continue
            if entry in seen_entries:
                continue
            if _path_contains_fluxon_pyo3_libs_dir(Path(entry)):
                continue
            seen_entries.add(entry)
            sanitized_entries.append(entry)
    os.environ["LD_LIBRARY_PATH"] = ":".join(sanitized_entries)
    os.environ["FLUXON_PYO3_LIBS_DIR"] = authoritative_entry


def _verify_fluxon_pyo3_authority(
    *,
    module_origin: Path,
    sys_prefix: Path,
    sys_base_prefix: Path,
    sys_executable: str,
    user_site: Path | None,
) -> None:
    if sys_prefix == sys_base_prefix:
        return

    if _path_is_within_root(module_origin, sys_prefix):
        return

    origin_kind = "outside active venv"
    if user_site is not None and _path_is_within_root(module_origin, user_site):
        origin_kind = "user site-packages outside active venv"
    raise RuntimeError(
        "fluxon_pyo3 import authority mismatch; "
        f"expected module under active venv root {sys_prefix}, "
        f"but resolved {module_origin} from {origin_kind}. "
        f"sys.executable={sys_executable} sys.prefix={sys_prefix} "
        f"sys.base_prefix={sys_base_prefix}"
    )


def import_fluxon_pyo3_local():
    import sys

    global _FLUXON_PYO3_MODULE_LAZY
    if _FLUXON_PYO3_MODULE_LAZY is not None:
        return _FLUXON_PYO3_MODULE_LAZY

    if not hasattr(sys, "getdlopenflags") or not hasattr(sys, "setdlopenflags"):
        raise RuntimeError("Python extension import isolation requires sys.getdlopenflags/setdlopenflags")

    if not hasattr(os, "RTLD_LOCAL") or not hasattr(os, "RTLD_NOW"):
        raise RuntimeError("Python extension import isolation requires os.RTLD_LOCAL/os.RTLD_NOW")

    module_origin = _resolve_fluxon_pyo3_module_origin()
    user_site_raw = site.getusersitepackages()
    user_site = Path(user_site_raw).resolve() if user_site_raw else None
    _verify_fluxon_pyo3_authority(
        module_origin=module_origin,
        sys_prefix=Path(sys.prefix).resolve(),
        sys_base_prefix=Path(sys.base_prefix).resolve(),
        sys_executable=sys.executable,
        user_site=user_site,
    )

    libs_dir = _resolve_fluxon_pyo3_libs_dir(module_origin)
    # Editable local builds place the extension next to the Python package without a
    # wheel-local fluxon_pyo3.libs directory. Rust-side bootstrap already accepts that
    # layout and falls back to the extension RPATH / system loader search path.
    if libs_dir is not None:
        _set_authoritative_bundled_ld_library_path(libs_dir)

    old_flags = sys.getdlopenflags()
    sys.setdlopenflags(os.RTLD_NOW | os.RTLD_LOCAL)
    try:
        import fluxon_pyo3  # type: ignore

        _FLUXON_PYO3_MODULE_LAZY = fluxon_pyo3
        return fluxon_pyo3
    finally:
        sys.setdlopenflags(old_flags)
