from __future__ import annotations

import re
from pathlib import Path


CLOSED_SDK_CONSUMER_BOUNDARY_MODE = "closed-sdk-consumer"

_EXPORT_FILE_NAMES = (
    "FluxonNativeConfig.cmake",
    "FluxonNativeTargets.cmake",
    "FluxonNativeLinkArgs.txt",
)


def rewrite_fluxon_native_export_bundle(
    *,
    export_root: Path,
    native_root_rewrites: dict[str, str],
    system_library_rewrites: dict[str, str],
) -> None:
    for file_name in _EXPORT_FILE_NAMES:
        export_path = export_root / file_name
        if not export_path.is_file():
            continue
        original = export_path.read_text(encoding="utf-8")
        rewritten = _rewrite_export_text(
            original,
            native_root_rewrites=native_root_rewrites,
            system_library_rewrites=system_library_rewrites,
        )
        if rewritten != original:
            export_path.write_text(rewritten, encoding="utf-8")


def _rewrite_export_text(
    text: str,
    *,
    native_root_rewrites: dict[str, str],
    system_library_rewrites: dict[str, str],
) -> str:
    return re.sub(
        r'[^;"\'\s()]+',
        lambda match: _rewrite_export_token(
            match.group(0),
            native_root_rewrites=native_root_rewrites,
            system_library_rewrites=system_library_rewrites,
        ),
        text,
    )


def _rewrite_export_token(
    token: str,
    *,
    native_root_rewrites: dict[str, str],
    system_library_rewrites: dict[str, str],
) -> str:
    rewritten = token
    basename = rewritten.rsplit("/", 1)[-1]
    if basename in system_library_rewrites and (rewritten == basename or rewritten.endswith(f"/{basename}")):
        return system_library_rewrites[basename]

    parts = rewritten.split("/")
    for root_name, target_root in native_root_rewrites.items():
        if root_name not in parts:
            continue
        root_index = parts.index(root_name)
        suffix = "/".join(parts[root_index + 1 :])
        rewritten = target_root if not suffix else f"{target_root}/{suffix}"
        break

    basename = rewritten.rsplit("/", 1)[-1]
    if basename in system_library_rewrites and (rewritten == basename or rewritten.endswith(f"/{basename}")):
        return system_library_rewrites[basename]
    return rewritten


__all__ = [
    "CLOSED_SDK_CONSUMER_BOUNDARY_MODE",
    "rewrite_fluxon_native_export_bundle",
]
