#!/usr/bin/env python3
"""
Generate per-service Docker deploy scripts from deployment/deployconf.yaml.

Outputs to gen_docker_deploy_bash/<service>.sh and stop_and_rm_<service>.sh.

Notes:
- Keep scripts simple: no rollback. If config is incomplete, print hints and exit.
- Avoid project env-vars; only pass container envs as configured.
- Python style per AGENTS.md: def main at top; main() call at bottom.
"""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
from typing import Any, Dict, List, Optional, Tuple

# Placeholder utilities are factored into a dedicated module.
# Support both package and script execution.
_UTILS_DIR = os.path.join(os.path.dirname(__file__), "utils")
sys.path.insert(0, _UTILS_DIR)
from placeholder_utils import (  # type: ignore
    build_mapping_for_cfg as _ph_build_mapping,
    resolve_values_or_raise as _ph_resolve_or_raise,
)

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    print("Missing dependency: PyYAML (pip install pyyaml)")
    raise SystemExit(1)


# Unified bash script template for a service. Placeholders like __FOO__ are
# replaced by Python before writing to disk to keep the script contiguous and readable.
SERVICE_SCRIPT_TEMPLATE = """#!/usr/bin/env bash
set -euo pipefail

# === Basic Config (exports) ===
export SERVICE='__SERVICE__'
export NAME_PREFIX='__NAME_PREFIX__'
export IMAGE='__IMAGE__'
__SHARED_EXPORT__
__TOKEN_EXPORTS_EARLY__
__DOCKER_NETWORK_EXPORTS__

# === Run Mode Flags ===
# Usage:
#   ./${SERVICE}.sh --dry-run | -n
#   ./${SERVICE}.sh --no-follow
DRY_RUN=0
FOLLOW_LOGS=1
while true; do
  case "${1:-}" in
    --dry-run|-n) DRY_RUN=1; shift ;;
    --no-follow) FOLLOW_LOGS=0; shift ;;
    *) break ;;
  esac
done
run_cmd() {
  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] '
    printf '%q ' "$@"
    echo
  else
    "$@"
  fi
}

# === Cluster Topology (IPs, allowed nodes) ===
__ALLOWED_NODES__
__ALL_NODES__

# === Detect Local Node (by hostname or IP) ===
LOCAL_HOSTNAME=$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)
LOCAL_FQDN=$(hostname -f 2>/dev/null || echo "$LOCAL_HOSTNAME")
NODE_ID=""
for n in "${ALL_NODES[@]}"; do if [ "$n" = "$LOCAL_HOSTNAME" ] || [ "$n" = "$LOCAL_FQDN" ]; then NODE_ID="$n"; break; fi; done
if [ -z "$NODE_ID" ] && [ ${#ALL_NODES[@]} -eq 1 ]; then NODE_ID="${ALL_NODES[0]}"; fi
if [ -z "$NODE_ID" ]; then
  for ip in $(hostname -I 2>/dev/null); do
    for n in "${ALL_NODES[@]}"; do
      _ip_n=''
      case "$n" in
__IP_BY_NODE_CASE__
        *) _ip_n='';;
      esac
      if [ "$_ip_n" = "$ip" ]; then NODE_ID="$n"; break; fi
    done
    [ -n "$NODE_ID" ] && break
  done
fi
if [ -z "$NODE_ID" ]; then
  echo "Cannot map host to a configured node. Hostname=$LOCAL_HOSTNAME FQDN=$LOCAL_FQDN IPs=$(hostname -I 2>/dev/null)"
  echo "Known nodes: __KNOWN_NODES__"
  exit 1
fi

# === Scheduling Check (node_bind) ===
if [ ${#ALLOWED_NODES[@]} -gt 0 ]; then
  _ok=false
  for n in "${ALLOWED_NODES[@]}"; do if [ "$n" = "$NODE_ID" ]; then _ok=true; fi; done
  if [ "$_ok" != true ]; then echo "Service '$SERVICE' not scheduled on this node ($NODE_ID). Allowed: ${ALLOWED_NODES[*]}"; exit 0; fi
fi

# Precompute container name and init docker args early (for per-node mounts)
CONTAINER_NAME="${NAME_PREFIX}-${SERVICE}-${NODE_ID}"
DOCKER_RUN=(docker run --privileged --ulimit memlock=-1:-1 -d __NETWORK_OPT__ --restart unless-stopped --name "$CONTAINER_NAME")
if [ -n "$SHARED_HOST" ] && [ -n "$SHARED_MOUNT" ]; then DOCKER_RUN+=( -v "$SHARED_HOST:$SHARED_MOUNT" ); fi

# === Node Mapping (HOST_IP/HOSTWORKDIR) and per-node mounts ===
HOST_IP='' ; HOSTWORKDIR=''
case "$NODE_ID" in
__NODE_CASE__
  *) echo "Unknown NODE_ID: '$NODE_ID'. Known: __KNOWN_NODES__"; exit 1;;
esac

echo "Starting $CONTAINER_NAME on $NODE_ID (IP: $HOST_IP)"

# === Token Exports (depends on NODE_ID) ===
__TOKEN_EXPORTS_LATE__

# === Compose docker run ===
# Inlined envs
DOCKER_RUN+=( -e NODE_ID="$NODE_ID" )
DOCKER_RUN+=( -e SERVICE="$SERVICE" )
DOCKER_RUN+=( -e NAME_PREFIX="$NAME_PREFIX" )
__ENV_BLOCK__

# Inlined ports
__PORT_FLAGS__

# === Entrypoint ===
__ENTRYPOINT_BLOCK__

# === Post-run Docker network attaches (optional) ===
__POST_RUN_BLOCK__

# === Logs Follow ===
if [ "${DRY_RUN}" = "1" ]; then
  echo "[dry-run] docker logs -f \"$CONTAINER_NAME\""
else
  if [ "${FOLLOW_LOGS}" = "1" ]; then
    echo "跟随容器日志: $CONTAINER_NAME (Ctrl+C 退出，仅停止日志跟随)"
    _logs_tail_cleanup() {
      echo
      echo "已停止日志跟随，容器 \"$CONTAINER_NAME\" 继续运行。"
      echo "再次查看日志: docker logs -f \"$CONTAINER_NAME\""
    }
    trap _logs_tail_cleanup INT
    docker logs -f "$CONTAINER_NAME" || true
    trap - INT
  else
    echo "已启动容器: $CONTAINER_NAME"
    echo "查看日志: docker logs -f \"$CONTAINER_NAME\""
  fi
fi
"""


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Generate Docker deploy scripts from deployment/deployconf.yaml"
    )
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    default_cfg = os.path.join(script_dir, "deployconf.yaml")
    parser.add_argument(
        "-c",
        "--config",
        default=default_cfg,
        help="Path to config YAML; if relative, resolve against the repo root inferred from this script path",
    )
    parser.add_argument(
        "-o",
        "--outdir",
        default=os.path.join(script_dir, "gen_docker_deploy_bash"),
        help="Output directory; if relative, resolve against the repo root inferred from this script path",
    )
    args = parser.parse_args(argv)

    config_path = args.config if os.path.isabs(args.config) else os.path.abspath(os.path.join(repo_root, args.config))
    cfg = _load_yaml(config_path)

    outdir = args.outdir if os.path.isabs(args.outdir) else os.path.abspath(os.path.join(repo_root, args.outdir))
    os.makedirs(outdir, exist_ok=True)

    # Context extracted from config
    name_prefix: str = str(cfg.get("name_prefix", "service"))
    cluster_nodes = _ensure_list(cfg.get("cluster_nodes", []))
    if not cluster_nodes:
        print("cluster_nodes is empty in config. Nothing to generate.")
        return 1

    node_ips: Dict[str, str] = {}
    node_hostworkdir: Dict[str, str] = {}
    # per-node additional mounts: hostname -> list of (host, container) pairs
    node_mounts: Dict[str, List[Tuple[str, str]]] = {}
    for node in cluster_nodes:
        nid = str(node.get("hostname", "")).strip()
        ip = str(node.get("ip", "")).strip()
        hostworkdir = str(node.get("hostworkdir", "")).strip()
        if not nid or not ip:
            print(f"Invalid cluster_nodes entry (require hostname, ip): {node}")
            return 1
        node_ips[nid] = ip
        if hostworkdir:
            node_hostworkdir[nid] = hostworkdir
        # Parse per-node mounts if provided
        mounts_val = node.get("mounts", None)
        if mounts_val is not None:
            if not isinstance(mounts_val, list):
                print(
                    f"cluster_nodes[{nid}].mounts must be a list of mappings {{host: container}}; got: {type(mounts_val).__name__}"
                )
                return 1
            parsed: List[Tuple[str, str]] = []
            for item in mounts_val:
                if isinstance(item, dict):
                    for host, cont in item.items():
                        host_s = str(host).strip()
                        cont_s = str(cont).strip()
                        if not host_s or not cont_s:
                            print(
                                f"Invalid mount mapping for node '{nid}': '{item}'. Both host and container paths required"
                            )
                            return 1
                        parsed.append((host_s, cont_s))
                else:
                    print(
                        f"cluster_nodes[{nid}].mounts items must be mappings {{host: container}}; got: {type(item).__name__}"
                    )
                    return 1
            if parsed:
                node_mounts[nid] = parsed

    try:
        top_image_default = _coalesce_image_string(cfg.get("image"))
    except ValueError as e:
        print(f"Invalid image at top-level 'image': {e}")
        return 1
    global_envs: Dict[str, Any] = cfg.get("global_envs", {}) or {}

    services = cfg.get("service", {}) or {}
    if not isinstance(services, dict) or not services:
        print("No services found under 'service:' in config.")
        return 1

    # Build quick lookup for service port and binding nodes
    svc_meta: Dict[str, Dict[str, Any]] = {}
    for sname, scfg in services.items():
        port = _extract_port(scfg)
        in_cport = _extract_in_container_port(scfg, port)
        nodes = _extract_nodes(scfg)
        try:
            svc_image = _coalesce_image_string(scfg.get("image", None)) or top_image_default
        except ValueError as e:
            print(f"Invalid image for service '{sname}': {e}")
            return 1
        hostport = bool(((scfg or {}).get("node_bind") or {}).get("hostport", False))
        hostnetwork = bool(((scfg or {}).get("node_bind") or {}).get("hostnetwork", False))
        docker_networks = ((scfg or {}).get("node_bind") or {}).get("docker_networks", None)
        docker_networks_list: List[str] = []
        if docker_networks is not None:
            if not isinstance(docker_networks, list):
                print(
                    f"Invalid node_bind for service '{sname}': docker_networks must be a list of strings, got: {type(docker_networks).__name__}"
                )
                return 1
            for n in docker_networks:
                if not isinstance(n, str) or not n.strip():
                    print(
                        f"Invalid node_bind for service '{sname}': docker_networks entries must be non-empty strings, got: {n!r}"
                    )
                    return 1
                docker_networks_list.append(n.strip())
            if not docker_networks_list:
                print(
                    f"Invalid node_bind for service '{sname}': docker_networks is present but empty; remove it or provide at least one network name"
                )
                return 1
        if hostnetwork and hostport:
            print(
                f"Invalid node_bind for service '{sname}': hostnetwork=true conflicts with hostport=true. "
                "Pick one: hostnetwork=true (use host network, no -p port mapping) or hostport=true (bridge network with -p mapping)."
            )
            return 1
        if hostnetwork and docker_networks_list:
            print(
                f"Invalid node_bind for service '{sname}': hostnetwork=true conflicts with docker_networks. "
                "Pick one: hostnetwork=true or docker_networks=[...]."
            )
            return 1
        entrypoint = scfg.get("entrypoint", None)
        entrypoint_mode = (scfg.get("entrypoint-mode") or "bash").strip()
        svc_meta[sname] = {
            "port": port,
            "nodes": nodes,
            "image": svc_image,
            "hostport": hostport,
            "hostnetwork": hostnetwork,
            "docker_networks": docker_networks_list,
            "in_container_port": in_cport,
            "entrypoint": entrypoint,
            "entrypoint_mode": entrypoint_mode,
        }

    # Pre-resolve placeholders in global_envs; fail fast if unresolved
    try:
        mapping = _ph_build_mapping(cluster_nodes=cluster_nodes, services=services)
        global_envs = _ph_resolve_or_raise(global_envs, mapping, label="global_envs")
    except Exception as e:
        # Align with project rule: print and exit if config invalid
        print(f"Failed to resolve placeholders in global_envs: {e}")
        return 1

    # Generate scripts per service
    # Optional shared storage mount (dev mode)
    shared = cfg.get("shared_storage") or {}
    shared_host = ""
    shared_mount = ""
    if isinstance(shared, dict):
        shared_host = str(shared.get("host") or "").strip()
        shared_mount = str(shared.get("mount") or "").strip()

    for sname, meta in svc_meta.items():
        script_path = os.path.join(outdir, f"{sname}.sh")
        stop_path = os.path.join(outdir, f"stop_and_rm_{sname}.sh")

        script = _render_service_script(
            service_name=sname,
            name_prefix=name_prefix,
            meta=meta,
            node_ips=node_ips,
            node_hostworkdir=node_hostworkdir,
            node_mounts=node_mounts,
            global_envs=global_envs,
            svc_meta=svc_meta,
            shared_host=shared_host,
            shared_mount=shared_mount,
        )
        stop_script = _render_stop_script(
            service_name=sname,
            name_prefix=name_prefix,
            meta=meta,
        )

        _write_executable(script_path, script)
        _write_executable(stop_path, stop_script)
        print(f"Generated: {os.path.relpath(script_path)} and {os.path.relpath(stop_path)}")

    # Generate a one-shot script to remove all service containers on the local node.
    # It is intentionally container-only (no networks/images/volumes) and idempotent.
    remove_all_path = os.path.join(outdir, "stop_and_rm_all.sh")
    remove_all_script = _render_remove_all_script(
        name_prefix=name_prefix,
        services=list(svc_meta.keys()),
        node_ips=node_ips,
    )
    _write_executable(remove_all_path, remove_all_script)
    print(f"Generated: {os.path.relpath(remove_all_path)}")

    return 0


# -------------- helpers --------------


def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        data = yaml.safe_load(text) or {}
    except FileNotFoundError:
        print(f"Config not found: {path}")
        raise SystemExit(1)
    except Exception as e:
        print(f"Failed to parse YAML {path}: {e}")
        raise SystemExit(1)
    if not isinstance(data, dict):
        print("Top-level YAML must be a mapping")
        raise SystemExit(1)
    return data


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _coalesce_image_string(image_val: Any) -> str:
    """Accept only plain string format 'repository:tag'.
    - None/empty -> return '' (caller may fall back to defaults)
    - str without ':' -> invalid (must include tag)
    - non-str -> invalid
    """
    if image_val is None:
        return ""
    if isinstance(image_val, str):
        s = image_val.strip()
        if not s:
            return ""
        if ":" not in s:
            raise ValueError(f"image must be 'repository:tag' string; got: {s}")
        return s
    raise ValueError(f"image must be 'repository:tag' string; got type {type(image_val).__name__}")


def _extract_port(svc_cfg: Dict[str, Any]) -> Optional[int]:
    if not isinstance(svc_cfg, dict):
        return None
    port = svc_cfg.get("port")
    if isinstance(port, int):
        return port
    nb = svc_cfg.get("node_bind") or {}
    nb_port = nb.get("port")
    if isinstance(nb_port, int):
        return nb_port
    return None


def _extract_in_container_port(svc_cfg: Dict[str, Any], default_port: Optional[int]) -> Optional[int]:
    if not isinstance(svc_cfg, dict):
        return default_port
    v = svc_cfg.get("in_container_port")
    if isinstance(v, int):
        return v
    nb = svc_cfg.get("node_bind") or {}
    nv = nb.get("in_container_port")
    if isinstance(nv, int):
        return nv
    return default_port


def _extract_nodes(svc_cfg: Dict[str, Any]) -> List[str]:
    if not isinstance(svc_cfg, dict):
        return []
    nb = svc_cfg.get("node_bind") or {}
    nodes = nb.get("node") or []
    if isinstance(nodes, list):
        return [str(n) for n in nodes]
    if isinstance(nodes, str):
        return [nodes]
    return []


def _write_executable(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _shell_single_quote(s: str) -> str:
    # Wrap in single quotes, escaping internal single quotes with: '
    # see: https://stackoverflow.com/a/1250279
    return "'" + s.replace("'", "'\\''") + "'"


# (removed) relaxed YAML fixer: require valid YAML to keep generator simple


 


def _render_service_script(
    service_name: str,
    name_prefix: str,
    meta: Dict[str, Any],
    node_ips: Dict[str, str],
    node_hostworkdir: Dict[str, str],
    node_mounts: Dict[str, List[Tuple[str, str]]],
    global_envs: Dict[str, Any],
    svc_meta: Dict[str, Dict[str, Any]],
    shared_host: str = "",
    shared_mount: str = "",
) -> str:
    image = str(meta.get("image", "")).strip()
    hostport = bool(meta.get("hostport", False))
    hostnetwork = bool(meta.get("hostnetwork", False))
    docker_networks_raw = meta.get("docker_networks", []) or []
    docker_networks: List[str] = []
    for n in _ensure_list(docker_networks_raw):
        if isinstance(n, str) and n.strip():
            docker_networks.append(n.strip())
    port: Optional[int] = meta.get("port")
    entrypoint: Optional[str] = meta.get("entrypoint")
    entrypoint_mode: str = str(meta.get("entrypoint_mode", "bash")).strip().lower()
    nodes: List[str] = _ensure_list(meta.get("nodes"))

    # Helper: convert names to ENV keys
    def _to_env_key(x: str) -> str:
        key = re.sub(r"[^A-Za-z0-9_]", "_", x)
        key = re.sub(r"_+", "_", key).strip("_")
        if not key:
            key = "X"
        if key[0].isdigit():
            key = "_" + key
        return key.upper()

    # Precompute dynamic chunks
    # Build case lines for indirect expansion of exported <NODE>__IP
    ip_by_node_case_lines: List[str] = []
    for nid in node_ips.keys():
        env_key = _to_env_key(nid) + "__IP"
        ip_by_node_case_lines.append(f"        {nid}) _ip_n=\"${{{env_key}}}\" ;;")

    allowed_nodes_line = (
        f"ALLOWED_NODES=({' '.join(nodes)})" if nodes else "ALLOWED_NODES=()  # no explicit binding"
    )
    all_nodes_line = f"ALL_NODES=({' '.join(node_ips.keys())})"

    node_case_body = []
    for nid, ip in node_ips.items():
        hw = node_hostworkdir.get(nid, "")
        hw_assign = f"HOSTWORKDIR={_shell_single_quote(hw)}" if hw else "# HOSTWORKDIR not set"
        # Per-node mounts inline into DOCKER_RUN
        mounts = node_mounts.get(nid, [])
        mount_parts: List[str] = []
        if hw:
            mount_parts.append(f"DOCKER_RUN+=( -v {_shell_single_quote(hw + ':/hostworkdir')} )")
        for h, c in mounts:
            mount_parts.append(f"DOCKER_RUN+=( -v {_shell_single_quote(f'{h}:{c}')} )")
        mount_suffix = (" ".join(mount_parts)) if mount_parts else ""
        node_case_body.append(
            f"  {nid}) HOST_IP={_shell_single_quote(ip)}; {hw_assign}; {mount_suffix} ;;"
        )
    known_nodes = " ".join(node_ips.keys())
    node_case_block = "\n".join(node_case_body)

    # No extra mount function needed; mounts are appended inline in the node case

    # Prepare simple token exports (ENV VARs)

    # Early token exports: node IPs, service ports, static service NODE_ID (single-node bind)
    token_lines_early: List[str] = []
    token_env_early: List[str] = []  # DOCKER_RUN -e lines
    # helper to add both export and DOCKER_RUN env line
    def _add_token_early(key: str, val: str) -> None:
        token_lines_early.append(f"export {key}={_shell_single_quote(val)}")
        token_env_early.append(f"DOCKER_RUN+=( -e {_shell_single_quote(f'{key}={val}')} )")

    for nid, ip in node_ips.items():
        _add_token_early(f"{_to_env_key(nid)}__IP", ip)
    for svc, m in svc_meta.items():
        if m.get("port"):
            _add_token_early(f"{_to_env_key(svc)}__PORT", str(m['port']))
    for svc, m in svc_meta.items():
        if svc == service_name:
            continue
        nodes_m = _ensure_list(m.get("nodes"))
        if len(nodes_m) == 1:
            node0 = nodes_m[0]
            _add_token_early(f"{_to_env_key(svc)}__NODE_ID", node0)
            # Also export SERVICE__NODE_ID__IP for unique-bind services so entrypoints can directly use it
            ip0 = node_ips.get(node0, "")
            if ip0:
                _add_token_early(f"{_to_env_key(svc)}__NODE_ID__IP", ip0)
    token_exports_early = "\n".join(token_lines_early)

    # Late token exports: current service's NODE_ID depends on detection
    # Also export SERVICE__NODE_ID__IP using resolved HOST_IP from node case mapping
    token_exports_late = (
        f"export {_to_env_key(service_name)}__NODE_ID=\"$NODE_ID\"\n"
        f"export {_to_env_key(service_name)}__NODE_ID__IP=\"$HOST_IP\"\n"
        f"DOCKER_RUN+=( -e {_shell_single_quote(f'{_to_env_key(service_name)}__NODE_ID=$NODE_ID')} )\n"
        f"DOCKER_RUN+=( -e {_shell_single_quote(f'{_to_env_key(service_name)}__NODE_ID__IP=$HOST_IP')} )"
    )

    # ENV composition (global_envs already pre-resolved in Python)
    env_block: List[str] = []
    for k, v in (global_envs or {}).items():
        key = str(k)
        sval = "" if v is None else str(v)
        env_block.append(f"DOCKER_RUN+=( -e {_shell_single_quote(f'{key}={sval}')} )")
    # Include early token env lines into DOCKER_RUN
    env_block.extend(token_env_early)
    env_block_str = "\n".join(env_block)

    # Port flags (host->container); container port is configurable via in_container_port
    in_cport = meta.get("in_container_port") or port

    # Shared vars line (for template export)
    shared_lines = (
        f"export SHARED_HOST={_shell_single_quote(shared_host)}\nexport SHARED_MOUNT={_shell_single_quote(shared_mount)}\n"
        if shared_host and shared_mount else
        "export SHARED_HOST=''\nexport SHARED_MOUNT='' # no shared storage configured\n"
    )

    # Build unified template rendering (single contiguous block)
    env_block_tail = env_block_str
    shared_export = shared_lines.strip()
    docker_network_exports = ""
    post_run_block = ""
    if docker_networks:
        nets = " ".join(_shell_single_quote(n) for n in docker_networks)
        docker_network_exports = (
            f"DOCKER_NETWORKS=( {nets} )\n"
            f"PRIMARY_DOCKER_NETWORK={_shell_single_quote(docker_networks[0])}"
        )
        # Ensure the networks exist, then attach the extra ones (docker run only supports one primary network)
        extra_networks = docker_networks[1:]
        if extra_networks:
            post_run_block = f"""
# docker_networks configured for this service
DOCKER_NETWORKS=( {nets} )
if [ "${{DRY_RUN:-0}}" != "1" ]; then
  for net in "${{DOCKER_NETWORKS[@]}}"; do
    if ! docker network inspect "$net" >/dev/null 2>&1; then
      echo "Docker network not found: $net"
      echo "Hint: create it first, then rerun this script."
      exit 1
    fi
  done
fi
for net in "${{DOCKER_NETWORKS[@]:1}}"; do
  run_cmd docker network connect "$net" "$CONTAINER_NAME"
done
""".strip()
        else:
            net0 = docker_networks[0]
            post_run_block = f"""
# docker_networks configured for this service
DOCKER_NETWORKS=( {_shell_single_quote(net0)} )
if [ "${{DRY_RUN:-0}}" != "1" ]; then
  if ! docker network inspect "{net0}" >/dev/null 2>&1; then
    echo "Docker network not found: {net0}"
    echo "Hint: create it first, then rerun this script."
    exit 1
  fi
fi
""".strip()
    # Build entrypoint block for template
    if isinstance(entrypoint, str) and entrypoint.strip():
        if entrypoint_mode == "direct":
            import shlex
            text = entrypoint.strip()
            # Join lines with bash-style continuation: remove '\\' + newline, then fold remaining newlines into spaces
            text = re.sub(r"\\\\\s*\n", " ", text)
            text = re.sub(r"\s*\n\s*", " ", text)
            cmd_line = text.strip()
            argv = shlex.split(cmd_line) if cmd_line else []
            if argv:
                ep_prog = argv[0].strip()
                ep_args = [a.strip() for a in argv[1:]]
                # Build array: docker run ... --entrypoint <prog> IMAGE <args...>
                ep_parts = ["DOCKER_RUN_D=( \"${DOCKER_RUN[@]}\" --entrypoint ", _shell_single_quote(ep_prog), " \"$IMAGE\""]
                for a in ep_args:
                    ep_parts.append(" ")
                    ep_parts.append(_shell_single_quote(a))
                ep_parts.append(" )\nrun_cmd \"${DOCKER_RUN_D[@]}\"")
                entrypoint_block = "".join(ep_parts)
            else:
                entrypoint_block = """
DOCKER_RUN_N=( "${DOCKER_RUN[@]}" "$IMAGE" )
run_cmd "${DOCKER_RUN_N[@]}"
"""
        else:
            body = entrypoint.strip()
            entrypoint_block = f"""
DOCKER_RUN_E=( "${{DOCKER_RUN[@]}}" --entrypoint /bin/bash "$IMAGE" )
ENTRYPOINT_SCRIPT=$(cat <<'EOS'
set -e
{body}
EOS
)
run_cmd "${{DOCKER_RUN_E[@]}}" -lc "$ENTRYPOINT_SCRIPT"
"""
    else:
        entrypoint_block = """
DOCKER_RUN_N=( "${DOCKER_RUN[@]}" "$IMAGE" )
run_cmd "${DOCKER_RUN_N[@]}"
"""

    # Port flags tail (no header)
    port_flags_tail = ''
    if (not hostnetwork) and hostport and port:
        port_flags_tail = f"DOCKER_RUN+=( -p {port}:{in_cport} )"

    template = SERVICE_SCRIPT_TEMPLATE
    script = template
    script = script.replace("__SERVICE__", service_name)
    script = script.replace("__NAME_PREFIX__", name_prefix)
    script = script.replace("__IMAGE__", image)
    script = script.replace("__SHARED_EXPORT__", shared_export)
    if hostnetwork:
        network_opt = "--network host"
    elif docker_networks:
        network_opt = f"--network {docker_networks[0]}"
    else:
        network_opt = ""
    script = script.replace("__NETWORK_OPT__", network_opt)
    script = script.replace("__IP_BY_NODE_CASE__", "\n".join(ip_by_node_case_lines))
    script = script.replace("__ALLOWED_NODES__", allowed_nodes_line)
    script = script.replace("__ALL_NODES__", all_nodes_line)
    script = script.replace("__KNOWN_NODES__", known_nodes)
    script = script.replace("__NODE_CASE__", node_case_block if 'node_case_block' in locals() else '')
    script = script.replace("__TOKEN_EXPORTS_EARLY__", token_exports_early)
    script = script.replace("__TOKEN_EXPORTS_LATE__", token_exports_late)
    script = script.replace("__DOCKER_NETWORK_EXPORTS__", docker_network_exports)
    script = script.replace("__ENV_BLOCK__", env_block_tail)
    script = script.replace("__PORT_FLAGS__", port_flags_tail)
    script = script.replace("__ENTRYPOINT_BLOCK__", entrypoint_block)
    script = script.replace("__POST_RUN_BLOCK__", post_run_block)

    return script


def _render_stop_script(service_name: str, name_prefix: str, meta: Dict[str, Any]) -> str:
    nodes: List[str] = _ensure_list(meta.get("nodes"))
    allowed_line = f"ALLOWED_NODES=({' '.join(nodes)})" if nodes else "ALLOWED_NODES=()"
    script = """#!/usr/bin/env bash
set -euo pipefail

# === Dry-Run Mode ===
# Usage: ./stop_and_rm_"$SERVICE".sh --dry-run | -n
DRY_RUN="${DRY_RUN:-0}"
if [ "${1:-}" = "--dry-run" ] || [ "${1:-}" = "-n" ]; then DRY_RUN=1; shift; fi
run_cmd() {
  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] '
    printf '%q ' "$@"
    echo
  else
    "$@"
  fi
}

"""
    script += f"SERVICE={_shell_single_quote(service_name)}\n"
    script += f"NAME_PREFIX={_shell_single_quote(name_prefix)}\n"
    script += allowed_line + "\n"
    script += """
LOCAL_HOSTNAME=$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)
LOCAL_FQDN=$(hostname -f 2>/dev/null || echo "$LOCAL_HOSTNAME")
NODE_ID="$LOCAL_HOSTNAME"
# If service has explicit binding and current node not allowed, skip
if [ ${#ALLOWED_NODES[@]} -gt 0 ]; then
  _ok=false
  for n in "${ALLOWED_NODES[@]}"; do if [ "$n" = "$NODE_ID" ] || [ "$n" = "$LOCAL_FQDN" ]; then _ok=true; fi; done
  if [ "$_ok" != true ]; then echo "Service '$SERVICE' not scheduled on this node ($NODE_ID). Allowed: ${ALLOWED_NODES[*]}"; exit 0; fi
fi

CONTAINER_NAME="${NAME_PREFIX}-${SERVICE}-${NODE_ID}"
echo "Stopping $CONTAINER_NAME"
run_cmd docker stop "$CONTAINER_NAME" || true
run_cmd docker rm "$CONTAINER_NAME" || true
"""
    return script


def _render_remove_all_script(
    *,
    name_prefix: str,
    services: List[str],
    node_ips: Dict[str, str],
) -> str:
    all_nodes_line = f"ALL_NODES=({' '.join(node_ips.keys())})"
    known_nodes = " ".join(node_ips.keys())

    ip_by_node_case_lines: List[str] = []
    for nid, ip in node_ips.items():
        ip_by_node_case_lines.append(f"        {nid}) _ip_n={_shell_single_quote(ip)} ;;")

    svc_items = " ".join(_shell_single_quote(s) for s in services)

    script = """#!/usr/bin/env bash
set -euo pipefail

# === Dry-Run Mode ===
# Usage: ./stop_and_rm_all.sh --dry-run | -n
DRY_RUN="${DRY_RUN:-0}"
if [ "${1:-}" = "--dry-run" ] || [ "${1:-}" = "-n" ]; then DRY_RUN=1; shift; fi
run_cmd() {
  if [ "${DRY_RUN}" = "1" ]; then
    printf '[dry-run] '
    printf '%q ' "$@"
    echo
  else
    "$@"
  fi
}

"""
    script += f"NAME_PREFIX={_shell_single_quote(name_prefix)}\n"
    script += f"SERVICES=({svc_items})\n"
    script += all_nodes_line + "\n"
    script += """

# === Detect Local Node (by hostname or IP) ===
LOCAL_HOSTNAME=$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)
LOCAL_FQDN=$(hostname -f 2>/dev/null || echo "$LOCAL_HOSTNAME")
NODE_ID=""
for n in "${ALL_NODES[@]}"; do if [ "$n" = "$LOCAL_HOSTNAME" ] || [ "$n" = "$LOCAL_FQDN" ]; then NODE_ID="$n"; break; fi; done
if [ -z "$NODE_ID" ] && [ ${#ALL_NODES[@]} -eq 1 ]; then NODE_ID="${ALL_NODES[0]}"; fi
if [ -z "$NODE_ID" ]; then
  for ip in $(hostname -I 2>/dev/null); do
    for n in "${ALL_NODES[@]}"; do
      _ip_n=''
      case "$n" in
__IP_BY_NODE_CASE__
        *) _ip_n='';;
      esac
      if [ "$_ip_n" = "$ip" ]; then NODE_ID="$n"; break; fi
    done
    [ -n "$NODE_ID" ] && break
  done
fi
if [ -z "$NODE_ID" ]; then
  echo "Cannot map host to a configured node. Hostname=$LOCAL_HOSTNAME FQDN=$LOCAL_FQDN IPs=$(hostname -I 2>/dev/null)"
  echo "Known nodes: __KNOWN_NODES__"
  exit 1
fi

echo "Removing containers for name_prefix=$NAME_PREFIX on NODE_ID=$NODE_ID"

if [ "${DRY_RUN}" != "1" ]; then
  # Fail fast if Docker daemon is not reachable; do not treat it as "container not found".
  docker info >/dev/null
fi

for svc in "${SERVICES[@]}"; do
  cname="${NAME_PREFIX}-${svc}-${NODE_ID}"
  if [ "${DRY_RUN}" = "1" ]; then
    run_cmd docker rm -f "$cname"
    continue
  fi
  if docker container inspect "$cname" >/dev/null 2>&1; then
    echo "Removing $cname"
    run_cmd docker rm -f "$cname"
  else
    echo "Skip (container not found): $cname"
  fi
done
"""
    script = script.replace("__IP_BY_NODE_CASE__", "\n".join(ip_by_node_case_lines))
    script = script.replace("__KNOWN_NODES__", known_nodes)
    return script


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
