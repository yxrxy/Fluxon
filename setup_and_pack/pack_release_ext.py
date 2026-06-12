#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

from utils.docker_build_runtime_utils import docker_check_call, docker_check_output
from utils.sudo_prefix_utils import sudo_prefix


@dataclass(frozen=True)
class RawBinaryExportSpec:
    image_key: str
    bin_name: str
    out_name: str
    absolute_candidates: tuple[str, ...] = ()


RAW_BINARY_IMAGE_KEYS_BASE = ("etcd", "greptime")
RAW_BINARY_IMAGE_KEYS_TIKV = ("tikv_pd", "tikv")
RAW_BINARY_SPECS: dict[str, tuple[RawBinaryExportSpec, ...]] = {
    "etcd": (
        RawBinaryExportSpec(image_key="etcd", bin_name="etcd", out_name="etcd"),
        RawBinaryExportSpec(image_key="etcd", bin_name="etcdctl", out_name="etcdctl"),
    ),
    "greptime": (
        RawBinaryExportSpec(image_key="greptime", bin_name="greptime", out_name="greptime"),
    ),
    "tikv": (
        RawBinaryExportSpec(
            image_key="tikv_pd",
            bin_name="pd-server",
            out_name="pd-server",
            absolute_candidates=("/pd-server",),
        ),
        RawBinaryExportSpec(
            image_key="tikv",
            bin_name="tikv-server",
            out_name="tikv-server",
            absolute_candidates=("/tikv-server",),
        ),
    ),
}
STARTUP_SCRIPT_NAMES: dict[str, tuple[str, ...]] = {
    "etcd": ("start.sh",),
    "greptime": ("start.sh",),
    "tikv": ("start_pd.sh", "start_tikv.sh"),
}
EXT_IMAGES_INPUT_STAMP_FILE_NAME = "ext_images.input.sha256"
ANSI_GREEN = "\033[32m"
ANSI_RESET = "\033[0m"


def main() -> int:
    # Permission contract:
    # - ext_images is a release subtree authority object materialized from an empty-or-absent root on each run.
    # - Host-side files and directories created by this script must converge at creation time instead of relying on
    #   recursive chmod over the subtree after the fact.
    # - Set umask(0) here for newly created host objects; executable outputs still use explicit per-file chmod at
    #   their authority write sites.
    os.umask(0)
    parser = argparse.ArgumentParser(
        description=(
            "Export release runtime binaries into fluxon_release/ext_images.\n\n"
            "Notes:\n"
            "- This script owns the ext_images runtime tree used by pack->dispatch->start_test_bed.\n"
            "- It intentionally exports only the runtime binaries and tiny helper scripts that are "
            "actually consumed by the deploy flow.\n"
        )
    )
    parser.add_argument(
        "--release-dir",
        default=None,
        help="Release directory path. If relative, it is resolved against repo root.",
    )
    parser.add_argument(
        "--with-tikv-runtime",
        choices=("true", "false"),
        required=True,
        help=(
            "Whether this release authority object includes TiKV runtime binaries under ext_images/tikv. "
            "Use false for KV benchmark-only releases that do not exercise transfer/TiKV flows."
        ),
    )
    args = parser.parse_args()
    with_tikv_runtime = args.with_tikv_runtime == "true"

    repo_root = Path(__file__).resolve().parent.parent
    config_path = repo_root / "deployment" / "deployconf.yaml"
    if not config_path.exists():
        print(f"Missing config file: {config_path}")
        return 1

    raw_binary_image_keys = _raw_binary_image_keys(with_tikv_runtime=with_tikv_runtime)
    images = read_images_from_deployconf(config_path, service_keys=raw_binary_image_keys)
    release_dir = _resolve_release_dir(repo_root=repo_root, raw_path=args.release_dir)
    ext_dir = release_dir / "ext_images"
    ext_inputs_digest = _compute_ext_images_inputs_digest(
        script_path=Path(__file__).resolve(),
        images=images,
        with_tikv_runtime=with_tikv_runtime,
    )
    ext_inputs_stamp_path = release_dir / EXT_IMAGES_INPUT_STAMP_FILE_NAME

    print("Runtime images to inspect (from deployment/deployconf.yaml release_ext_images):")
    for key in raw_binary_image_keys:
        print(f"- {key}: {images[key]}")

    if _ext_images_cache_ready(
        ext_dir=ext_dir,
        stamp_path=ext_inputs_stamp_path,
        expected_digest=ext_inputs_digest,
        with_tikv_runtime=with_tikv_runtime,
    ):
        print(f"Reusing cached runtime ext dir without rebuild: {ext_dir}")
        print(f"Using runtime ext manifest: {ext_dir / 'ext_images.sha256'}")
        return 0

    _reset_ext_dir(ext_dir)

    out_files: list[Path] = []
    out_files.extend(
        _export_raw_binaries(
            ext_dir=ext_dir,
            images=images,
            with_tikv_runtime=with_tikv_runtime,
        )
    )
    out_files.extend(_write_startup_scripts(ext_dir=ext_dir, with_tikv_runtime=with_tikv_runtime))

    sha_path = ext_dir / "ext_images.sha256"
    _write_sha256_manifest(out_path=sha_path, base_dir=ext_dir, files=out_files)
    _write_ext_images_input_stamp(stamp_path=ext_inputs_stamp_path, digest=ext_inputs_digest)

    print(f"Exported runtime ext dir: {ext_dir}")
    print(f"Wrote runtime ext manifest: {sha_path}")
    return 0


def _raw_binary_image_keys(*, with_tikv_runtime: bool) -> tuple[str, ...]:
    if with_tikv_runtime:
        return RAW_BINARY_IMAGE_KEYS_BASE + RAW_BINARY_IMAGE_KEYS_TIKV
    return RAW_BINARY_IMAGE_KEYS_BASE


def _bundle_names(*, with_tikv_runtime: bool) -> list[str]:
    bundle_names = ["etcd", "greptime"]
    if with_tikv_runtime:
        bundle_names.append("tikv")
    return bundle_names


def read_images_from_deployconf(config_path: Path, *, service_keys: tuple[str, ...]) -> dict[str, str]:
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(cfg, dict):
        raise SystemExit(f"Invalid deploy config YAML (expected a mapping): {config_path}")

    release_ext_images = cfg.get("release_ext_images")
    if not isinstance(release_ext_images, dict):
        raise SystemExit(
            f"Deploy config missing mapping field: release_ext_images (file: {config_path})"
        )

    images: dict[str, str] = {}
    for key in service_keys:
        svc = release_ext_images.get(key)
        if not isinstance(svc, dict):
            raise SystemExit(
                f"Deploy config missing mapping field: release_ext_images.{key} (file: {config_path})"
            )
        image = str(svc.get("image") or "").strip()
        if not image:
            raise SystemExit(
                f"Deploy config missing required field: release_ext_images.{key}.image (file: {config_path})"
            )
        images[key] = image

    if len(set(images.values())) != len(images):
        raise SystemExit(
            f"Deploy config contains duplicate release_ext_images refs among {service_keys}: {images}"
        )
    return images


def _resolve_release_dir(*, repo_root: Path, raw_path: str | None) -> Path:
    if raw_path is None:
        return repo_root / "fluxon_release"
    path = Path(raw_path)
    if path.is_absolute():
        return path.resolve()
    return (repo_root / path).resolve()


def _reset_ext_dir(ext_dir: Path) -> None:
    # Causal chain:
    # - ext_images is the single authority object for deploy runtime binaries.
    # - The previous mixed script left stale files behind (for example offline image tarballs) when
    #   the desired output set changed, so reruns did not actually shrink the runtime payload.
    # - Reset the directory before writing the new object tree so pack_release always sees one
    #   converged ext_images layout.
    if ext_dir.exists():
        shutil.rmtree(ext_dir)
    ext_dir.mkdir(parents=True, exist_ok=False)


def _compute_ext_images_inputs_digest(
    *,
    script_path: Path,
    images: dict[str, str],
    with_tikv_runtime: bool,
) -> str:
    h = hashlib.sha256()
    h.update(b"fluxon_ext_images_export_v1\n")
    h.update(f"with_tikv_runtime={with_tikv_runtime}\n".encode("utf-8"))
    h.update(f"script_sha256={_sha256_file(script_path)}\n".encode("utf-8"))
    for key in sorted(images):
        h.update(f"{key}={images[key]}\n".encode("utf-8"))
    return h.hexdigest()


def _write_ext_images_input_stamp(*, stamp_path: Path, digest: str) -> None:
    stamp_path.write_text(digest + "\n", encoding="utf-8")


def _read_ext_images_input_stamp_or_none(stamp_path: Path) -> str | None:
    if not stamp_path.exists():
        return None
    text = stamp_path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    return text


def _expected_ext_images_output_relpaths(*, with_tikv_runtime: bool) -> set[str]:
    relpaths: set[str] = {"ext_images.sha256"}
    for bundle_name in _bundle_names(with_tikv_runtime=with_tikv_runtime):
        for spec in RAW_BINARY_SPECS[bundle_name]:
            relpaths.add(f"{bundle_name}/{spec.out_name}")
        for script_name in STARTUP_SCRIPT_NAMES[bundle_name]:
            relpaths.add(f"{bundle_name}/{script_name}")
    return relpaths


def _expected_ext_images_manifest_relpaths(*, with_tikv_runtime: bool) -> set[str]:
    relpaths: set[str] = set()
    for bundle_name in _bundle_names(with_tikv_runtime=with_tikv_runtime):
        for spec in RAW_BINARY_SPECS[bundle_name]:
            relpaths.add(f"{bundle_name}/{spec.out_name}")
        for script_name in STARTUP_SCRIPT_NAMES[bundle_name]:
            relpaths.add(f"{bundle_name}/{script_name}")
    return relpaths


def _collect_ext_images_file_relpaths(ext_dir: Path) -> set[str]:
    relpaths: set[str] = set()
    for path in ext_dir.rglob("*"):
        if not path.is_file():
            continue
        relpaths.add(path.relative_to(ext_dir).as_posix())
    return relpaths


def _load_manifest_relpaths_or_none(manifest_path: Path) -> set[str] | None:
    if not manifest_path.exists():
        return None
    relpaths: set[str] = set()
    for raw in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        checksum, sep, relpath = line.partition("  ")
        if sep != "  ":
            return None
        checksum = checksum.strip().lower()
        relpath = relpath.strip()
        if len(checksum) != 64:
            return None
        if any(ch not in "0123456789abcdef" for ch in checksum):
            return None
        if not relpath:
            return None
        relpaths.add(relpath)
    return relpaths


def _ext_images_cache_ready(
    *,
    ext_dir: Path,
    stamp_path: Path,
    expected_digest: str,
    with_tikv_runtime: bool,
) -> bool:
    cached_digest = _read_ext_images_input_stamp_or_none(stamp_path)
    if cached_digest != expected_digest:
        return False
    if not ext_dir.is_dir():
        return False
    expected_output_relpaths = _expected_ext_images_output_relpaths(
        with_tikv_runtime=with_tikv_runtime
    )
    actual_output_relpaths = _collect_ext_images_file_relpaths(ext_dir)
    if actual_output_relpaths != expected_output_relpaths:
        return False
    manifest_relpaths = _load_manifest_relpaths_or_none(ext_dir / "ext_images.sha256")
    if manifest_relpaths is None:
        return False
    expected_manifest_relpaths = _expected_ext_images_manifest_relpaths(
        with_tikv_runtime=with_tikv_runtime
    )
    return manifest_relpaths == expected_manifest_relpaths


def _export_raw_binaries(
    *,
    ext_dir: Path,
    images: dict[str, str],
    with_tikv_runtime: bool,
) -> list[Path]:
    exported: list[Path] = []
    for bundle_name in _bundle_names(with_tikv_runtime=with_tikv_runtime):
        specs = RAW_BINARY_SPECS[bundle_name]
        dst_dir = ext_dir / bundle_name
        dst_dir.mkdir(parents=True, exist_ok=False)
        for spec in specs:
            image = images.get(spec.image_key)
            if not image:
                raise SystemExit(f"Missing image ref for rawbinary export: {spec.image_key}")

            _ensure_docker_image_available(image)
            cid = docker_check_output(["create", image]).strip()
            if not cid:
                raise SystemExit(
                    f"Failed to create container for rawbinary export: {spec.image_key} ({image})"
                )

            try:
                src_path = _resolve_binary_path_in_image(
                    image=image,
                    bin_name=spec.bin_name,
                    absolute_candidates=spec.absolute_candidates,
                )
                dst_path = dst_dir / spec.out_name
                docker_check_call(["cp", f"{cid}:{src_path}", str(dst_path)])
                if not dst_path.exists():
                    raise SystemExit(
                        f"Rawbinary export failed: docker cp did not produce output file: {dst_path}"
                    )
                exported.append(dst_path)
                subprocess.check_call(["sudo", "chmod", "777", str(dst_path)])
                print(
                    f"{ANSI_GREEN}>>> exported rawbinary {bundle_name}/{spec.out_name}: {dst_path}{ANSI_RESET}"
                )
            finally:
                docker_check_call(["rm", "-f", cid])

    return exported


def _ensure_docker_image_available(image: str) -> None:
    inspect_cmd = sudo_prefix() + ["docker", "image", "inspect", image]
    result = subprocess.run(
        inspect_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode == 0:
        print(f"Reusing local docker image without pull: {image}")
        return
    docker_check_call(["pull", image])


def _resolve_binary_path_in_image(
    *, image: str, bin_name: str, absolute_candidates: tuple[str, ...]
) -> str:
    cmd = _path_resolve_cmd(bin_name, absolute_candidates)
    out = docker_check_output(["run", "--rm", "--entrypoint", "/bin/sh", image, "-lc", cmd])
    path = out.strip()
    if not path:
        raise SystemExit(f"Failed to resolve binary path in image: image={image} bin={bin_name}")
    if "\n" in path:
        raise SystemExit(f"Ambiguous resolved path (multiple lines): image={image} bin={bin_name} out={path!r}")
    if not path.startswith("/"):
        raise SystemExit(
            f"Binary path resolved to non-absolute path: image={image} bin={bin_name} path={path!r}"
        )
    return path


def _path_resolve_cmd(bin_name: str, absolute_candidates: tuple[str, ...]) -> str:
    absolute_candidate_checks = "".join(
        f"if [ -x {path!r} ] && [ ! -d {path!r} ]; then printf %s {path!r}; exit 0; fi; "
        for path in absolute_candidates
    )
    return (
        "set -eu; "
        + absolute_candidate_checks
        + f"bn={bin_name!r}; "
        + "IFS=:; "
        + "for d in $PATH; do "
        + "[ -n \"$d\" ] || continue; "
        + "p=\"$d/$bn\"; "
        + "if [ -x \"$p\" ] && [ ! -d \"$p\" ]; then printf %s \"$p\"; exit 0; fi; "
        + "done; "
        + "echo \"binary not found in PATH: $bn\" 1>&2; exit 1"
    )


def _write_startup_scripts(*, ext_dir: Path, with_tikv_runtime: bool) -> list[Path]:
    etcd_dir = ext_dir / "etcd"
    greptime_dir = ext_dir / "greptime"

    etcd_script = etcd_dir / "start.sh"
    greptime_script = greptime_dir / "start.sh"

    etcd_script.write_text(_start_etcd_raw_sh(), encoding="utf-8")
    greptime_script.write_text(_start_greptime_raw_sh(), encoding="utf-8")
    subprocess.check_call(["sudo", "chmod", "777", str(etcd_script)])
    subprocess.check_call(["sudo", "chmod", "777", str(greptime_script)])
    scripts = [etcd_script, greptime_script]
    if with_tikv_runtime:
        tikv_dir = ext_dir / "tikv"
        pd_script = tikv_dir / "start_pd.sh"
        tikv_script = tikv_dir / "start_tikv.sh"
        pd_script.write_text(_start_pd_raw_sh(), encoding="utf-8")
        tikv_script.write_text(_start_tikv_raw_sh(), encoding="utf-8")
        subprocess.check_call(["sudo", "chmod", "777", str(pd_script)])
        subprocess.check_call(["sudo", "chmod", "777", str(tikv_script)])
        scripts.extend((pd_script, tikv_script))
    print(
        f"{ANSI_GREEN}>>> wrote startup scripts: {' '.join(str(path) for path in scripts)}{ANSI_RESET}"
    )
    return scripts


def _start_etcd_raw_sh() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start.sh --config <config.sh> --workdir <workdir>

Required:
  -c, --config   Shell config file to source (must define ETCD_ARGS as a bash array)
  -w, --workdir  Work directory path; exposed to config as $WORKDIR

Example config.sh:
  ETCD_ARGS=(
    --data-dir "$WORKDIR/etcd-data"
    --name etcd0
    --advertise-client-urls "http://0.0.0.0:2379"
    --listen-client-urls "http://0.0.0.0:2379"
    --listen-peer-urls "http://0.0.0.0:2380"
    --initial-advertise-peer-urls "http://0.0.0.0:2380"
    --initial-cluster "etcd0=http://0.0.0.0:2380"
    --initial-cluster-token "etcd-cluster"
    --initial-cluster-state "new"
    --auto-compaction-retention=1
  )
EOF
}

CONFIG=""
WORKDIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config) CONFIG="${2:-}"; shift 2 ;;
    -w|--workdir) WORKDIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 2 ;;
  esac
done

if [ -z "$CONFIG" ]; then echo "Missing required argument: --config"; usage; exit 2; fi
if [ -z "$WORKDIR" ]; then echo "Missing required argument: --workdir"; usage; exit 2; fi
if [ ! -f "$CONFIG" ]; then echo "Config file not found: $CONFIG"; exit 2; fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ETCD_BIN="$SCRIPT_DIR/etcd"
ETCDCTL_BIN="$SCRIPT_DIR/etcdctl"
if [ ! -x "$ETCD_BIN" ]; then echo "Missing etcd binary (expected executable): $ETCD_BIN"; exit 2; fi
if [ ! -x "$ETCDCTL_BIN" ]; then echo "Missing etcdctl binary (expected executable): $ETCDCTL_BIN"; exit 2; fi

export WORKDIR

# shellcheck source=/dev/null
source "$CONFIG"

if ! declare -p ETCD_ARGS >/dev/null 2>&1; then
  echo "Config must define ETCD_ARGS as a bash array (declare -a)."
  exit 2
fi
if ! declare -p ETCD_ARGS | grep -q 'declare -a'; then
  echo "ETCD_ARGS must be a bash array (declare -a)."
  exit 2
fi
if [ "${#ETCD_ARGS[@]}" -eq 0 ]; then
  echo "ETCD_ARGS is empty; provide at least one argument."
  exit 2
fi

exec "$ETCD_BIN" "${ETCD_ARGS[@]}"
"""


def _start_greptime_raw_sh() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start.sh --config <config.sh> --workdir <workdir>

Required:
  -c, --config   Shell config file to source (must define GREPTIME_ARGS as a bash array)
  -w, --workdir  Work directory path; exposed to config as $WORKDIR

Example config.sh:
  GREPTIME_ARGS=(
    standalone start
    --data-home "$WORKDIR/greptimedb"
    --http-addr 0.0.0.0:34030
  )
EOF
}

CONFIG=""
WORKDIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config) CONFIG="${2:-}"; shift 2 ;;
    -w|--workdir) WORKDIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 2 ;;
  esac
done

if [ -z "$CONFIG" ]; then echo "Missing required argument: --config"; usage; exit 2; fi
if [ -z "$WORKDIR" ]; then echo "Missing required argument: --workdir"; usage; exit 2; fi
if [ ! -f "$CONFIG" ]; then echo "Config file not found: $CONFIG"; exit 2; fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
GREPTIME_BIN="$SCRIPT_DIR/greptime"
if [ ! -x "$GREPTIME_BIN" ]; then echo "Missing greptime binary (expected executable): $GREPTIME_BIN"; exit 2; fi

export WORKDIR

# shellcheck source=/dev/null
source "$CONFIG"

if ! declare -p GREPTIME_ARGS >/dev/null 2>&1; then
  echo "Config must define GREPTIME_ARGS as a bash array (declare -a)."
  exit 2
fi
if ! declare -p GREPTIME_ARGS | grep -q 'declare -a'; then
  echo "GREPTIME_ARGS must be a bash array (declare -a)."
  exit 2
fi
if [ "${#GREPTIME_ARGS[@]}" -eq 0 ]; then
  echo "GREPTIME_ARGS is empty; provide at least one argument."
  exit 2
fi

exec "$GREPTIME_BIN" "${GREPTIME_ARGS[@]}"
"""


def _start_pd_raw_sh() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start_pd.sh --config <config.sh> --workdir <workdir>

Required:
  -c, --config   Shell config file to source (must define PD_ARGS as a bash array)
  -w, --workdir  Work directory path; exposed to config as $WORKDIR

Example config.sh:
  PD_ARGS=(
    --name pd0
    --data-dir "$WORKDIR/pd-data"
    --client-urls "http://127.0.0.1:2379"
    --advertise-client-urls "http://127.0.0.1:2379"
    --peer-urls "http://127.0.0.1:2380"
    --advertise-peer-urls "http://127.0.0.1:2380"
    --initial-cluster "pd0=http://127.0.0.1:2380"
    --log-file "$WORKDIR/pd.log"
  )
EOF
}

CONFIG=""
WORKDIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config) CONFIG="${2:-}"; shift 2 ;;
    -w|--workdir) WORKDIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 2 ;;
  esac
done

if [ -z "$CONFIG" ]; then echo "Missing required argument: --config"; usage; exit 2; fi
if [ -z "$WORKDIR" ]; then echo "Missing required argument: --workdir"; usage; exit 2; fi
if [ ! -f "$CONFIG" ]; then echo "Config file not found: $CONFIG"; exit 2; fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PD_BIN="$SCRIPT_DIR/pd-server"
if [ ! -x "$PD_BIN" ]; then echo "Missing pd-server binary (expected executable): $PD_BIN"; exit 2; fi

export WORKDIR

# shellcheck source=/dev/null
source "$CONFIG"

if ! declare -p PD_ARGS >/dev/null 2>&1; then
  echo "Config must define PD_ARGS as a bash array (declare -a)."
  exit 2
fi
if ! declare -p PD_ARGS | grep -q 'declare -a'; then
  echo "PD_ARGS must be a bash array (declare -a)."
  exit 2
fi
if [ "${#PD_ARGS[@]}" -eq 0 ]; then
  echo "PD_ARGS is empty; provide at least one argument."
  exit 2
fi

exec "$PD_BIN" "${PD_ARGS[@]}"
"""


def _start_tikv_raw_sh() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./start_tikv.sh --config <config.sh> --workdir <workdir>

Required:
  -c, --config   Shell config file to source (must define TIKV_ARGS as a bash array)
  -w, --workdir  Work directory path; exposed to config as $WORKDIR

Example config.sh:
  TIKV_ARGS=(
    --pd-endpoints "127.0.0.1:2379"
    --addr "127.0.0.1:20160"
    --advertise-addr "127.0.0.1:20160"
    --status-addr "127.0.0.1:20180"
    --data-dir "$WORKDIR/tikv-data"
    --log-file "$WORKDIR/tikv.log"
  )
EOF
}

CONFIG=""
WORKDIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config) CONFIG="${2:-}"; shift 2 ;;
    -w|--workdir) WORKDIR="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 2 ;;
  esac
done

if [ -z "$CONFIG" ]; then echo "Missing required argument: --config"; usage; exit 2; fi
if [ -z "$WORKDIR" ]; then echo "Missing required argument: --workdir"; usage; exit 2; fi
if [ ! -f "$CONFIG" ]; then echo "Config file not found: $CONFIG"; exit 2; fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TIKV_BIN="$SCRIPT_DIR/tikv-server"
if [ ! -x "$TIKV_BIN" ]; then echo "Missing tikv-server binary (expected executable): $TIKV_BIN"; exit 2; fi

export WORKDIR

# shellcheck source=/dev/null
source "$CONFIG"

if ! declare -p TIKV_ARGS >/dev/null 2>&1; then
  echo "Config must define TIKV_ARGS as a bash array (declare -a)."
  exit 2
fi
if ! declare -p TIKV_ARGS | grep -q 'declare -a'; then
  echo "TIKV_ARGS must be a bash array (declare -a)."
  exit 2
fi
if [ "${#TIKV_ARGS[@]}" -eq 0 ]; then
  echo "TIKV_ARGS is empty; provide at least one argument."
  exit 2
fi

exec "$TIKV_BIN" "${TIKV_ARGS[@]}"
"""


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _write_sha256_manifest(*, out_path: Path, base_dir: Path, files: list[Path]) -> None:
    lines: list[str] = []
    for p in files:
        if not p.exists():
            raise SystemExit(f"Cannot write sha256 manifest: missing file: {p}")
        rel = p.relative_to(base_dir).as_posix()
        lines.append(f"{_sha256_file(p)}  {rel}\n")
    out_path.write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
