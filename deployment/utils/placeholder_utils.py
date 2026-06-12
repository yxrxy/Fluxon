#!/usr/bin/env python3
"""
Utilities for resolving ${...} placeholders by generic replacement only.

Principles:
- Only detect ${...} spans and replace via a provided mapping.
- No special-case parsing of what's inside {...}; keys match mapping entries.
- Supports nested placeholders by iterative substitution.

Common keys populated by build_mapping_for_cfg:
- Node IPs: "<node>.IP", "<node>__IP", "<NODE>__IP"
- Service ports: "<svc>.PORT", "<svc>__PORT", "<SVC>__PORT"
- Service node ids (single-node bind): "<svc>.NODE_ID", "<svc>__NODE_ID", "<SVC>__NODE_ID"
- Service node id lists (multi/single-node bind): "<svc>__NODE_IDS", "<SVC>__NODE_IDS"
- Cluster node ids: "CLUSTER_NODE_IDS"
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List


# Only a generic ${...} matcher is needed
_RE_ANY = re.compile(r"\$\{([^{}]+)\}")
_RUNTIME_PLACEHOLDER_KEYS = {"HOSTWORKDIR"}


def has_placeholders(s: str) -> bool:
    return bool(_RE_ANY.search(s))


def extract_placeholder_keys(s: str) -> List[str]:
    """Return raw keys inside ${...} without nested evaluation.
    Example: "${foo} ${bar.BAZ}" -> ["foo", "bar.BAZ"]
    """
    return [m.group(1) for m in _RE_ANY.finditer(s)]


def resolve_placeholders_nested(text: str, mapping: Dict[str, str], max_passes: int = 8) -> str:
    """Resolve ${...} placeholders by repeatedly applying a simple mapping.

    - Nested forms are handled by iteration: inner tokens resolve first.
    - If a key is absent from mapping, it is left as-is.
    - The function stops when no changes occur or after max_passes iterations.
    """
    cur = text
    for _ in range(max_passes):
        changed = False

        def repl(m: re.Match[str]) -> str:
            nonlocal changed
            key = m.group(1).strip()
            if key in mapping:
                changed = True
                return str(mapping[key])
            return m.group(0)

        nxt = _RE_ANY.sub(repl, cur)
        if nxt == cur:
            break
        cur = nxt
    return cur


def build_mapping_for_cfg(
    *,
    cluster_nodes: Iterable[dict],
    services: Dict[str, dict],
) -> Dict[str, str]:
    """Build a best-effort token->value map from YAML sections.

    - Adds "{hostname}__IP" for each cluster node.
    - Adds "{service}__PORT" when integer port available.
    - Adds "{service}__NODE_ID" only when bound to exactly one node.
    """
    m: Dict[str, str] = {}

    def _to_env_key(x: str) -> str:
        key = re.sub(r"[^A-Za-z0-9_]", "_", x)
        key = re.sub(r"_+", "_", key).strip("_")
        if not key:
            key = "X"
        if key[0].isdigit():
            key = "_" + key
        return key.upper()

    host_to_ip: Dict[str, str] = {}
    cluster_node_ids: List[str] = []
    for node in cluster_nodes:
        hn = str(node.get("hostname", "")).strip()
        ip = str(node.get("ip", "")).strip()
        if hn and ip:
            host_to_ip[hn] = ip
            cluster_node_ids.append(hn)
            m[f"{hn}__IP"] = ip
            m[f"{_to_env_key(hn)}__IP"] = ip
    if cluster_node_ids:
        m["CLUSTER_NODE_IDS"] = " ".join(cluster_node_ids)

    for sname, scfg in (services or {}).items():
        port = None
        if isinstance(scfg, dict):
            if isinstance(scfg.get("port"), int):
                port = scfg.get("port")
            else:
                nb = scfg.get("node_bind") or {}
                if isinstance(nb.get("port"), int):
                    port = nb.get("port")
        if port is not None:
            m[f"{sname}__PORT"] = str(port)
            m[f"{_to_env_key(sname)}__PORT"] = str(port)

        nodes: List[str] = []
        if isinstance(scfg, dict):
            nb = scfg.get("node_bind") or {}
            nd = nb.get("node")
            if isinstance(nd, list):
                nodes = [str(x) for x in nd]
            elif isinstance(nd, str):
                nodes = [nd]
        if nodes:
            node_ids_s = " ".join(nodes)
            m[f"{sname}__NODE_IDS"] = node_ids_s
            m[f"{_to_env_key(sname)}__NODE_IDS"] = node_ids_s
        if len(nodes) == 1:
            node0 = nodes[0]
            m[f"{sname}__NODE_ID"] = node0
            m[f"{_to_env_key(sname)}__NODE_ID"] = node0
            ip0 = host_to_ip.get(node0)
            if ip0:
                m[f"{sname}__NODE_ID__IP"] = ip0
                m[f"{_to_env_key(sname)}__NODE_ID__IP"] = ip0

    return m


def resolve_values_or_raise(
    values: Dict[str, object],
    mapping: Dict[str, str],
    *,
    label: str = "values",
) -> Dict[str, object]:
    """Resolve all string values in the given dict using the mapping.

    - For each string, perform nested placeholder resolution.
    - If any placeholders remain unresolved, print a list grouped by key and raise ValueError.
    - Returns a new dict with resolved strings (non-strings are passed through).
    """
    resolved: Dict[str, object] = {}
    unresolved: Dict[str, List[str]] = {}
    for k, v in values.items():
        if isinstance(v, str) and has_placeholders(v):
            r = resolve_placeholders_nested(v, mapping)
            if has_placeholders(r):
                remaining = [tok for tok in extract_placeholder_keys(r) if tok not in _RUNTIME_PLACEHOLDER_KEYS]
                if remaining:
                    unresolved[k] = remaining
            resolved[k] = r
        else:
            resolved[k] = v
    if unresolved:
        print(f"Unresolved placeholders in {label}:")
        for k, toks in unresolved.items():
            uniq = sorted(set(toks))
            print(f"  - {k}: {', '.join(uniq)}")
        raise ValueError(f"Unresolved placeholders detected in {label}")
    return resolved


def env_style_key(name: str) -> str:
    """Convert an arbitrary name to ENV_STYLE key (A-Z0-9 and underscores).

    Example: 'closed-metadata' -> 'CLOSED_METADATA'.
    """
    key = re.sub(r"[^A-Za-z0-9_]", "_", name)
    key = re.sub(r"_+", "_", key).strip("_")
    if not key:
        key = "X"
    if key[0].isdigit():
        key = "_" + key
    return key.upper()


def svc_ip_port_from_mapping(mapping: Dict[str, str], service: str) -> tuple[str, int] | None:
    """Return (ip, port) for a service using a placeholder mapping.

    - Looks up both '<svc>__PORT' and 'ENV_STYLE(<svc>)__PORT'.
    - Uses '<svc>__NODE_ID__IP' or its ENV_STYLE form for IP.
    - Returns None when either value is missing or invalid.
    """
    k_port1 = f"{service}__PORT"
    k_port2 = f"{env_style_key(service)}__PORT"
    k_ip1 = f"{service}__NODE_ID__IP"
    k_ip2 = f"{env_style_key(service)}__NODE_ID__IP"
    port_s = mapping.get(k_port1) or mapping.get(k_port2)
    ip = mapping.get(k_ip1) or mapping.get(k_ip2)
    if not ip or not port_s:
        return None
    try:
        port = int(str(port_s))
    except Exception:
        return None
    return ip, port


def any_node_ip_from_mapping(mapping: Dict[str, str]) -> str | None:
    """Pick any node IP from mapping as a fallback.

    Prefers keys that end with '__IP' and are not service-derived forms containing
    '__NODE_ID__'. Returns the first match if available.
    """
    for k, v in mapping.items():
        if k.endswith("__IP") and "__NODE_ID__" not in k and isinstance(v, str) and v:
            return v
    return None
