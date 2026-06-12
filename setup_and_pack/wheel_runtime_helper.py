from __future__ import annotations

import base64
import csv
import hashlib
import subprocess
import shutil
import tempfile
import zipfile
from pathlib import Path


def extract_wheel(wheel_path: str) -> str:
    temp_dir = tempfile.mkdtemp(prefix="fluxon_wheel_")
    with zipfile.ZipFile(wheel_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
    return temp_dir


def _wheel_record_path(source_root: Path) -> Path | None:
    dist_info_dirs = sorted(path for path in source_root.glob("*.dist-info") if path.is_dir())
    if not dist_info_dirs:
        return None
    if len(dist_info_dirs) != 1:
        raise RuntimeError(f"expected exactly one dist-info directory, found {len(dist_info_dirs)} in {source_root}")
    return dist_info_dirs[0] / "RECORD"


def _hash_file_for_record(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256(path.read_bytes()).digest()
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"sha256={encoded}", path.stat().st_size


def _refresh_wheel_record(source_root: Path) -> None:
    record_path = _wheel_record_path(source_root)
    if record_path is None:
        return

    record_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[list[str]] = []
    for path in sorted(source_root.rglob("*")):
        if path.is_dir() or path == record_path:
            continue
        relpath = path.relative_to(source_root).as_posix()
        digest, size = _hash_file_for_record(path)
        rows.append([relpath, digest, str(size)])
    rows.append([record_path.relative_to(source_root).as_posix(), "", ""])

    with record_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(rows)


def create_wheel(wheel_path: str, source_dir: str) -> None:
    source_root = Path(source_dir)
    _refresh_wheel_record(source_root)
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
        for path in sorted(source_root.rglob("*")):
            if path.is_dir():
                continue
            zip_out.write(path, path.relative_to(source_root).as_posix())


def _set_rpath(path: Path, rpath: str) -> None:
    subprocess.run(
        ["patchelf", "--set-rpath", rpath, str(path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def normalize_wheel_lib_rpaths(wheel_path: str) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_rpath_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        pkg_dir = temp_dir / pkg_name
        libs_dir = temp_dir / f"{pkg_name}.libs"

        for ext_path in sorted(pkg_dir.glob("*.so")):
            _set_rpath(ext_path, "$ORIGIN:$ORIGIN/../fluxon_pyo3.libs")

        if libs_dir.is_dir():
            for lib_path in sorted(libs_dir.glob("*.so*")):
                if not lib_path.is_file():
                    continue
                _set_rpath(lib_path, "$ORIGIN")

        create_wheel(wheel_path, str(temp_dir))


def get_repaired_lib_name_map(lib_dir: str) -> dict[str, str]:
    root = Path(lib_dir)
    if not root.is_dir():
        return {}
    return {
        path.name: str(path)
        for path in sorted(root.iterdir())
        if path.is_file() and (".so" in path.name or path.suffix in {".dylib", ".dll"})
    }


def add_shared_libraries(
    wheel_path: str,
    library_paths: list[str],
    *,
    packaged_lib_replacements: dict[str, str] | None = None,
) -> None:
    packaged_lib_replacements = packaged_lib_replacements or {}
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_libs_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        libs_dir = temp_dir / f"{pkg_name}.libs"
        libs_dir.mkdir(parents=True, exist_ok=True)

        for raw_path in library_paths:
            source_path = Path(raw_path)
            if not source_path.is_file():
                raise RuntimeError(f"shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / source_path.name)

        for base_name, replacement_path in packaged_lib_replacements.items():
            source_path = Path(replacement_path)
            if not source_path.is_file():
                raise RuntimeError(f"replacement shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / base_name)

        create_wheel(wheel_path, str(temp_dir))


def install_shared_libraries(
    wheel_path: str,
    library_paths_by_name: dict[str, str],
) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_install_libs_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        libs_dir = temp_dir / f"{pkg_name}.libs"
        libs_dir.mkdir(parents=True, exist_ok=True)

        for dest_name, raw_source_path in sorted(library_paths_by_name.items()):
            source_path = Path(raw_source_path)
            if not source_path.is_file():
                raise RuntimeError(f"shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / dest_name)

        create_wheel(wheel_path, str(temp_dir))


def add_plugins(wheel_path: str, plugins_dir: str, bundle_name: str) -> None:
    source_root = Path(plugins_dir)
    if not source_root.is_dir():
        raise RuntimeError(f"plugin directory is missing: {source_root}")
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_plugins_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        dst_root = temp_dir / f"{pkg_name}.libs" / bundle_name
        dst_root.mkdir(parents=True, exist_ok=True)
        for path in sorted(source_root.rglob("*")):
            if path.is_dir():
                continue
            relpath = path.relative_to(source_root)
            dst_path = dst_root / relpath
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dst_path)
        create_wheel(wheel_path, str(temp_dir))


__all__ = [
    "extract_wheel",
    "create_wheel",
    "normalize_wheel_lib_rpaths",
    "get_repaired_lib_name_map",
    "add_shared_libraries",
    "install_shared_libraries",
    "add_plugins",
]
