#!/usr/bin/env python3

import argparse
import html
import json
import os
import re
import shlex
import shutil
import socketserver
import subprocess
import sys
import threading
import time
import urllib.parse
from http.server import SimpleHTTPRequestHandler
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_ROOT = REPO_ROOT / "fluxon_doc_linked" / "fluxon_doc"
DOC_SITE_CONFIG_PATH = REPO_ROOT / "scripts" / "build_doc_site_config.yaml"
OUTPUT_ROOT = REPO_ROOT / "fluxon_release" / "doc_site"
CACHE_ROOT = REPO_ROOT / ".cached" / "fluxon_doc_site"
PROJECT_ROOT = CACHE_ROOT / "project"
STAGE_DOCS_ROOT = PROJECT_ROOT / "docs"
TOOLCHAIN_ROOT = CACHE_ROOT / "toolchain"
NPM_CACHE_ROOT = CACHE_ROOT / "npm-cache"
TOOLCHAIN_PACKAGE_JSON_PATH = TOOLCHAIN_ROOT / "package.json"
VITEPRESS_ROOT = STAGE_DOCS_ROOT
VITEPRESS_CONFIG_DIR = VITEPRESS_ROOT / ".vitepress"
VITEPRESS_THEME_DIR = VITEPRESS_CONFIG_DIR / "theme"
VITEPRESS_CONFIG_PATH = VITEPRESS_CONFIG_DIR / "config.mts"
THEME_ENTRY_PATH = VITEPRESS_THEME_DIR / "index.ts"
CUSTOM_CSS_PATH = VITEPRESS_THEME_DIR / "custom.css"
VITEPRESS_ENTRY_PATH = TOOLCHAIN_ROOT / "node_modules" / "vitepress" / "bin" / "vitepress.js"
DEFAULT_SERVE_ADDR = "127.0.0.1:18081"
DEFAULT_TRACK_POLL_SECONDS = 1.0
VITEPRESS_VERSION = "1.6.4"
LOCAL_SEARCH_TRANSLATIONS = {
    "button": {
        "buttonText": "搜索",
        "buttonAriaLabel": "搜索文档",
    },
    "modal": {
        "displayDetails": "显示详情",
        "resetButtonTitle": "清空搜索",
        "backButtonTitle": "返回",
        "noResultsText": "未找到结果",
        "footer": {
            "selectText": "选择",
            "selectKeyAriaLabel": "Enter 键",
            "navigateText": "切换",
            "navigateUpKeyAriaLabel": "上方向键",
            "navigateDownKeyAriaLabel": "下方向键",
            "closeText": "关闭",
            "closeKeyAriaLabel": "Esc 键",
        },
    },
}

MARKDOWN_LINK_RE = re.compile(r"(!?\[[^\]]*\]\()([^)]+)(\))")


def load_site_config() -> dict:
    if not DOC_SITE_CONFIG_PATH.is_file():
        raise SystemExit(f"ERROR: doc site config not found: {DOC_SITE_CONFIG_PATH}")

    raw = yaml.safe_load(DOC_SITE_CONFIG_PATH.read_text(encoding="utf-8"))
    config = require_dict(raw, "build_doc_site_config")
    require_allowed_keys(
        config,
        "build_doc_site_config",
        {"site", "root_nav", "page_overrides"},
    )

    site = require_dict(config.get("site"), "build_doc_site_config.site")
    require_allowed_keys(site, "build_doc_site_config.site", {"title", "description"})

    root_nav = parse_root_nav_entries(
        require_list(config.get("root_nav"), "build_doc_site_config.root_nav"),
    )
    page_overrides = parse_page_overrides(
        require_dict(config.get("page_overrides"), "build_doc_site_config.page_overrides"),
    )

    root_nav_dirs: dict[str, dict] = {}
    root_nav_pages: dict[str, dict] = {}
    for entry in root_nav:
        if entry["kind"] == "dir":
            if entry["path"] in root_nav_dirs:
                raise SystemExit(
                    "ERROR: duplicate root_nav dir path in "
                    f"{DOC_SITE_CONFIG_PATH}: {entry['path']}"
                )
            root_nav_dirs[entry["path"]] = entry
            continue
        if entry["path"] in root_nav_pages:
            raise SystemExit(
                "ERROR: duplicate root_nav page path in "
                f"{DOC_SITE_CONFIG_PATH}: {entry['path']}"
            )
        root_nav_pages[entry["path"]] = entry

    return {
        "site": {
            "title": require_str(site.get("title"), "build_doc_site_config.site.title"),
            "description": require_str(
                site.get("description"),
                "build_doc_site_config.site.description",
            ),
        },
        "root_nav": root_nav,
        "root_nav_dirs": root_nav_dirs,
        "root_nav_pages": root_nav_pages,
        "page_overrides": page_overrides,
    }


def parse_root_nav_entries(raw_entries: list) -> list[dict]:
    entries: list[dict] = []
    for idx, raw_entry in enumerate(raw_entries):
        field_name = f"build_doc_site_config.root_nav[{idx}]"
        entry = require_dict(raw_entry, field_name)
        kind = require_str(entry.get("kind"), f"{field_name}.kind")
        if kind == "dir":
            require_allowed_keys(
                entry,
                field_name,
                {"kind", "path", "title", "nav_link_mode"},
            )
            entries.append(
                {
                    "kind": "dir",
                    "path": require_root_dir_path(entry.get("path"), f"{field_name}.path"),
                    "title": require_str(entry.get("title"), f"{field_name}.title"),
                    "nav_link_mode": require_enum(
                        entry.get("nav_link_mode"),
                        f"{field_name}.nav_link_mode",
                        {"index", "first_page", "first_sidebar_link"},
                    ),
                }
            )
            continue
        if kind == "page":
            require_allowed_keys(entry, field_name, {"kind", "path", "title"})
            entries.append(
                {
                    "kind": "page",
                    "path": require_root_page_path(entry.get("path"), f"{field_name}.path"),
                    "title": require_str(entry.get("title"), f"{field_name}.title"),
                }
            )
            continue
        raise SystemExit(
            f"ERROR: unsupported {field_name}.kind in {DOC_SITE_CONFIG_PATH}: {kind}"
        )
    return entries


def parse_page_overrides(raw_overrides: dict) -> dict[str, dict]:
    overrides: dict[str, dict] = {}
    for raw_source_rel, raw_override in raw_overrides.items():
        if not isinstance(raw_source_rel, str):
            raise SystemExit(
                "ERROR: build_doc_site_config.page_overrides keys must be strings in "
                f"{DOC_SITE_CONFIG_PATH}"
            )
        field_name = f"build_doc_site_config.page_overrides[{raw_source_rel!r}]"
        source_rel = require_markdown_rel_path(raw_source_rel, field_name)
        override = require_dict(raw_override, field_name)
        require_allowed_keys(override, field_name, {"publish", "title", "staged_rel"})
        staged_rel = override.get("staged_rel")
        normalized_staged_rel: str | None = None
        if staged_rel is not None:
            normalized_staged_rel = require_markdown_rel_path(
                staged_rel,
                f"{field_name}.staged_rel",
            ).as_posix()
        overrides[source_rel.as_posix()] = {
            "publish": require_bool(override.get("publish"), f"{field_name}.publish"),
            "title": require_optional_str(override.get("title"), f"{field_name}.title"),
            "staged_rel": normalized_staged_rel,
        }
    return overrides


def require_allowed_keys(raw_dict: dict, field_name: str, allowed_keys: set[str]) -> None:
    unexpected = sorted(key for key in raw_dict if key not in allowed_keys)
    if unexpected:
        raise SystemExit(
            f"ERROR: unsupported keys in {field_name} from {DOC_SITE_CONFIG_PATH}: {unexpected}"
        )


def require_dict(value: object, field_name: str) -> dict:
    if not isinstance(value, dict):
        raise SystemExit(f"ERROR: {field_name} must be a mapping in {DOC_SITE_CONFIG_PATH}")
    return value


def require_list(value: object, field_name: str) -> list:
    if not isinstance(value, list):
        raise SystemExit(f"ERROR: {field_name} must be a list in {DOC_SITE_CONFIG_PATH}")
    return value


def require_str(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(
            f"ERROR: {field_name} must be a non-empty string in {DOC_SITE_CONFIG_PATH}"
        )
    return value.strip()


def require_optional_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return require_str(value, field_name)


def require_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise SystemExit(f"ERROR: {field_name} must be a bool in {DOC_SITE_CONFIG_PATH}")
    return value


def require_enum(value: object, field_name: str, allowed_values: set[str]) -> str:
    text = require_str(value, field_name)
    if text not in allowed_values:
        raise SystemExit(
            f"ERROR: {field_name} must be one of {sorted(allowed_values)} in {DOC_SITE_CONFIG_PATH}"
        )
    return text


def require_root_dir_path(value: object, field_name: str) -> str:
    rel = require_rel_path(value, field_name)
    if len(rel.parts) != 1 or rel.suffix:
        raise SystemExit(
            f"ERROR: {field_name} must point to a root directory in {DOC_SITE_CONFIG_PATH}"
        )
    return rel.as_posix()


def require_root_page_path(value: object, field_name: str) -> str:
    rel = require_markdown_rel_path(value, field_name)
    if len(rel.parts) != 1:
        raise SystemExit(
            f"ERROR: {field_name} must point to a root markdown page in {DOC_SITE_CONFIG_PATH}"
        )
    return rel.as_posix()


def require_markdown_rel_path(value: object, field_name: str) -> Path:
    rel = require_rel_path(value, field_name)
    if rel.suffix != ".md":
        raise SystemExit(
            f"ERROR: {field_name} must point to a markdown path in {DOC_SITE_CONFIG_PATH}"
        )
    return rel


def require_rel_path(value: object, field_name: str) -> Path:
    path_text = require_str(value, field_name)
    path = Path(path_text)
    if path.is_absolute() or path_text.startswith("/"):
        raise SystemExit(
            f"ERROR: {field_name} must be a relative path in {DOC_SITE_CONFIG_PATH}"
        )
    rel = normalize_rel_path(path)
    if rel == Path("."):
        raise SystemExit(f"ERROR: {field_name} must not be '.' in {DOC_SITE_CONFIG_PATH}")
    return rel


class OutputHTTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("bootstrap")
    subparsers.add_parser("build")
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--addr", default=DEFAULT_SERVE_ADDR)
    track_parser = subparsers.add_parser("track")
    track_parser.add_argument("--addr", default=DEFAULT_SERVE_ADDR)
    track_parser.add_argument("--poll-seconds", type=float, default=DEFAULT_TRACK_POLL_SECONDS)

    args = parser.parse_args()
    command = args.command or "build"

    if command == "bootstrap":
        return bootstrap_toolchain()
    if command == "build":
        return build_site()
    if command == "serve":
        return serve_site(args.addr)
    if command == "track":
        return track_site(args.addr, args.poll_seconds)

    print(f"ERROR: unsupported command: {command}", file=sys.stderr)
    return 2


def bootstrap_toolchain() -> int:
    npm_path = require_binary("npm")
    require_binary("node")

    ensure_dir_777(CACHE_ROOT)
    ensure_dir_777(TOOLCHAIN_ROOT)
    ensure_dir_777(NPM_CACHE_ROOT)
    write_toolchain_package_json()

    cmd = [
        npm_path,
        "--cache",
        str(NPM_CACHE_ROOT),
        "install",
        "--prefix",
        str(TOOLCHAIN_ROOT),
        "--no-fund",
        "--no-audit",
    ]
    run_cmd(cmd, cwd=REPO_ROOT)
    return 0


def build_site() -> int:
    vitepress_cmd = require_vitepress_toolchain()
    reset_site_project()
    sync_stage_site_project()
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    ensure_dir_777(OUTPUT_ROOT)
    run_vitepress_build(vitepress_cmd)
    chmod_tree_777(OUTPUT_ROOT)
    return 0


def serve_site(addr: str) -> int:
    build_site()
    serve_output_root(addr)
    return 0


def track_site(addr: str, poll_seconds: float) -> int:
    if poll_seconds <= 0:
        print("ERROR: --poll-seconds must be > 0.", file=sys.stderr)
        return 2

    vitepress_cmd = require_vitepress_toolchain()
    reset_site_project()
    sync_stage_site_project()
    run_vitepress_build(vitepress_cmd)
    chmod_tree_777(OUTPUT_ROOT)
    source_state = compute_source_state()
    httpd, server_thread = start_output_http_server(addr)

    try:
        while True:
            time.sleep(poll_seconds)
            next_state = compute_source_state()
            if next_state == source_state:
                continue

            print("doc_site track: source change detected, rebuilding output site...", flush=True)
            sync_stage_site_project()
            run_vitepress_build(vitepress_cmd)
            chmod_tree_777(OUTPUT_ROOT)
            source_state = next_state
    except KeyboardInterrupt:
        print("doc_site track: stopping HTTP server.", flush=True)
    finally:
        stop_output_http_server(httpd, server_thread)
    return 0


def require_vitepress_toolchain() -> list[str]:
    if not VITEPRESS_ENTRY_PATH.is_file():
        print(
            "ERROR: VitePress toolchain not bootstrapped. Run `python3 scripts/build_doc_site.py bootstrap` first.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return [require_binary("node"), str(VITEPRESS_ENTRY_PATH)]


def serve_output_root(addr: str) -> None:
    httpd, server_thread = start_output_http_server(addr)
    try:
        server_thread.join()
    except KeyboardInterrupt:
        print("doc_site serve: stopping HTTP server.", flush=True)
    finally:
        stop_output_http_server(httpd, server_thread)


def start_output_http_server(addr: str) -> tuple[OutputHTTPServer, threading.Thread]:
    host, port = parse_serve_addr(addr)
    handler = lambda *args, **kwargs: SimpleHTTPRequestHandler(
        *args,
        directory=str(OUTPUT_ROOT),
        **kwargs,
    )
    httpd = OutputHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=False)
    server_thread.start()
    print(f"doc_site serve: serving {OUTPUT_ROOT} on http://{addr}/", flush=True)
    return httpd, server_thread


def stop_output_http_server(
    httpd: OutputHTTPServer,
    server_thread: threading.Thread,
) -> None:
    httpd.shutdown()
    httpd.server_close()
    server_thread.join()


def parse_serve_addr(addr: str) -> tuple[str, int]:
    host, sep, port_text = addr.rpartition(":")
    if not sep or not host or not port_text:
        raise SystemExit(f"ERROR: invalid --addr: {addr}. Expected <host>:<port>.")
    if not port_text.isdigit():
        raise SystemExit(f"ERROR: invalid --addr port: {addr}")
    port = int(port_text)
    if port <= 0 or port > 65535:
        raise SystemExit(f"ERROR: invalid --addr port: {addr}")
    return host, port


def reset_site_project() -> None:
    if not DOC_ROOT.is_dir():
        raise SystemExit(f"ERROR: doc root not found: {DOC_ROOT}")

    if PROJECT_ROOT.exists():
        shutil.rmtree(PROJECT_ROOT)
    ensure_dir_777(STAGE_DOCS_ROOT)


def sync_stage_site_project() -> None:
    if not DOC_ROOT.is_dir():
        raise SystemExit(f"ERROR: doc root not found: {DOC_ROOT}")

    site_config = load_site_config()
    ensure_dir_777(STAGE_DOCS_ROOT)
    ensure_project_toolchain_link()

    docs_map, asset_paths = collect_source_files(site_config)
    validate_docs_map(docs_map)
    prune_staged_files(docs_map, asset_paths)
    write_staged_docs(docs_map)
    copy_assets(asset_paths)
    doc_meta = collect_doc_meta(docs_map, site_config)
    write_theme_entry()
    write_custom_css()
    write_vitepress_config(doc_meta, site_config)


def collect_source_files(site_config: dict) -> tuple[dict[Path, Path], list[Path]]:
    docs_map: dict[Path, Path] = {}
    asset_paths: list[Path] = []
    seen_markdown_paths: set[str] = set()
    staged_rel_owners: dict[Path, Path] = {}
    page_overrides = site_config["page_overrides"]

    for src_path in sorted(DOC_ROOT.rglob("*")):
        rel = src_path.relative_to(DOC_ROOT)
        if should_skip_rel_path(rel):
            continue
        if src_path.is_dir():
            continue
        if src_path.suffix == ".md":
            seen_markdown_paths.add(rel.as_posix())
            page_override = page_overrides.get(rel.as_posix())
            if page_override is not None and not page_override["publish"]:
                continue
            staged_rel = doc_stage_rel_path(rel, page_overrides)
            existing_source_rel = staged_rel_owners.get(staged_rel)
            if existing_source_rel is not None:
                raise SystemExit(
                    "ERROR: duplicate staged markdown path after page_overrides: "
                    f"{existing_source_rel} and {rel} -> {staged_rel}"
                )
            staged_rel_owners[staged_rel] = rel
            docs_map[rel] = staged_rel
        else:
            asset_paths.append(rel)

    unused_page_overrides = sorted(set(page_overrides) - seen_markdown_paths)
    if unused_page_overrides:
        raise SystemExit(
            "ERROR: page_overrides point to markdown files that do not exist under "
            f"{DOC_ROOT}: {unused_page_overrides}"
        )
    if not docs_map:
        raise SystemExit(f"ERROR: no markdown documents found under {DOC_ROOT}")
    return docs_map, asset_paths


def should_skip_rel_path(rel: Path) -> bool:
    for part in rel.parts:
        if part.startswith("."):
            return True
        if part == "states":
            return True
    rel_str = rel.as_posix()
    if rel_str.endswith(".canvas") or rel_str.endswith(".canvas.ext"):
        return True
    return False


def doc_stage_rel_path(source_rel: Path, page_overrides: dict[str, dict]) -> Path:
    page_override = page_overrides.get(source_rel.as_posix())
    if page_override is not None and page_override["staged_rel"] is not None:
        return Path(page_override["staged_rel"])
    if source_rel.name == "README.md":
        parent = source_rel.parent
        if str(parent) == ".":
            return Path("index.md")
        return parent / "index.md"
    return source_rel


def write_staged_docs(docs_map: dict[Path, Path]) -> None:
    for source_rel, staged_rel in docs_map.items():
        src_path = DOC_ROOT / source_rel
        dst_path = STAGE_DOCS_ROOT / staged_rel
        ensure_dir_777(dst_path.parent)
        raw_md = src_path.read_text(encoding="utf-8")
        staged_md = rewrite_markdown_links(raw_md, source_rel, staged_rel, docs_map)
        if not markdown_has_h1(raw_md):
            staged_md = ensure_markdown_title_frontmatter(
                staged_md,
                derive_title_from_path(staged_rel),
            )
        write_text_777(dst_path, staged_md)


def copy_assets(asset_paths: list[Path]) -> None:
    for rel in asset_paths:
        src_path = DOC_ROOT / rel
        dst_path = STAGE_DOCS_ROOT / rel
        ensure_dir_777(dst_path.parent)
        shutil.copy2(src_path, dst_path)
        os.chmod(dst_path, 0o777)


def prune_staged_files(docs_map: dict[Path, Path], asset_paths: list[Path]) -> None:
    wanted = {STAGE_DOCS_ROOT / rel for rel in docs_map.values()}
    wanted.update(STAGE_DOCS_ROOT / rel for rel in asset_paths)
    wanted.add(VITEPRESS_CONFIG_PATH)
    wanted.add(THEME_ENTRY_PATH)
    wanted.add(CUSTOM_CSS_PATH)

    if not STAGE_DOCS_ROOT.exists():
        return

    existing_files = sorted(
        path for path in STAGE_DOCS_ROOT.rglob("*") if path.is_file()
    )
    for path in existing_files:
        if path in wanted:
            continue
        path.unlink()

    existing_dirs = sorted(
        (path for path in STAGE_DOCS_ROOT.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for path in existing_dirs:
        if path == STAGE_DOCS_ROOT:
            continue
        if any(path.iterdir()):
            continue
        path.rmdir()


def ensure_project_toolchain_link() -> None:
    ensure_dir_777(PROJECT_ROOT)
    node_modules_link = PROJECT_ROOT / "node_modules"
    target = TOOLCHAIN_ROOT / "node_modules"
    if node_modules_link.is_symlink() or node_modules_link.exists():
        if node_modules_link.resolve() == target.resolve():
            return
        if node_modules_link.is_dir() and not node_modules_link.is_symlink():
            shutil.rmtree(node_modules_link)
        else:
            node_modules_link.unlink()
    os.symlink(target, node_modules_link)


def write_theme_entry() -> None:
    ensure_dir_777(THEME_ENTRY_PATH.parent)
    write_text_777(
        THEME_ENTRY_PATH,
        """\
import DefaultTheme from 'vitepress/theme'
import mediumZoom from 'medium-zoom'
import { nextTick } from 'vue'
import './custom.css'

let zoom = null

function applyImageZoom() {
  if (typeof document === 'undefined') {
    return
  }
  if (zoom !== null) {
    zoom.detach()
  }
  zoom = mediumZoom('.vp-doc img', {
    background: 'rgba(24, 30, 24, 0.82)',
    margin: 24,
  })
}

export default {
  ...DefaultTheme,
  enhanceApp(ctx) {
    if (typeof DefaultTheme.enhanceApp === 'function') {
      DefaultTheme.enhanceApp(ctx)
    }
    if (typeof window === 'undefined') {
      return
    }
    ctx.router.onAfterRouteChanged = async () => {
      await nextTick()
      applyImageZoom()
    }
    window.requestAnimationFrame(() => {
      applyImageZoom()
    })
  },
}
""",
    )


def write_custom_css() -> None:
    ensure_dir_777(CUSTOM_CSS_PATH.parent)
    write_text_777(
        CUSTOM_CSS_PATH,
        """\
:root {
  --vp-c-brand-1: #3f5f3f;
  --vp-c-brand-2: #507450;
  --vp-c-brand-3: #2f4b2f;
  --vp-c-brand-soft: rgba(63, 95, 63, 0.14);
  --vp-c-bg: #fbf7ef;
  --vp-c-bg-soft: #f5f0e7;
  --vp-c-bg-alt: #efe7da;
  --vp-c-divider: rgba(63, 95, 63, 0.14);
  --vp-c-text-1: #283324;
}

body {
  background:
    radial-gradient(circle at top left, rgba(181, 106, 47, 0.08), transparent 26%),
    linear-gradient(180deg, #fbf7ef 0%, #f5f0e7 100%);
}

.VPNavBar {
  backdrop-filter: blur(10px);
  background: rgba(63, 95, 63, 0.92) !important;
  border-bottom: 1px solid rgba(63, 95, 63, 0.18);
}

.VPNavBar .wrapper,
.VPNavBar .container,
.VPNavBar .content,
.VPNavBar .content-body {
  background: rgba(63, 95, 63, 0.92) !important;
}

.VPNavBar .VPNavBarSearch .DocSearch-Button {
  background: rgba(255, 255, 255, 0.1) !important;
  border: 1px solid rgba(255, 255, 255, 0.16) !important;
}

.VPNavBar .VPNavBarSearch .DocSearch-Button:hover,
.VPNavBar .VPNavBarSearch .DocSearch-Button:focus,
.VPNavBar .VPNavBarSearch .DocSearch-Button:active {
  background: rgba(255, 255, 255, 0.16) !important;
}

.VPNavBar .VPNavBarMenu,
.VPNavBar .VPNavBarHamburger {
  display: none !important;
}

.VPNavBar .VPNavBarSearch {
  flex-grow: 0 !important;
  margin-left: auto !important;
  padding-left: 0 !important;
  justify-content: flex-end;
}

.VPNavBar .title,
.VPNavBar .VPNavBarMenuLink,
.VPNavBar .VPNavBarSearch .DocSearch-Button {
  color: rgba(255, 255, 255, 0.92);
}

.VPNavBar .VPNavBarSearch .DocSearch-Button-Placeholder,
.VPNavBar .VPNavBarSearch .DocSearch-Search-Icon,
.VPNavBar .VPNavBarSearch .DocSearch-Button-Key {
  color: rgba(255, 255, 255, 0.92) !important;
}

.VPSidebar {
  background:
    radial-gradient(circle at top left, rgba(181, 106, 47, 0.08), transparent 28%),
    #fbf7ef !important;
}

.vp-doc h1,
.vp-doc h2,
.vp-doc h3 {
  letter-spacing: -0.02em;
}

.vp-doc img {
  border-radius: 14px;
  border: 1px solid rgba(63, 95, 63, 0.12);
  background: white;
  box-shadow: 0 12px 30px rgba(41, 53, 34, 0.08);
  cursor: zoom-in;
}

.medium-zoom-overlay {
  z-index: 200;
}

.medium-zoom-image--opened {
  z-index: 201;
}

.vp-doc :not(pre) > code {
  border-radius: 0.35rem;
}
""",
    )


def write_vitepress_config(doc_meta: dict[Path, dict[str, str]], site_config: dict) -> None:
    validate_root_nav_contract(doc_meta, site_config)
    tree = build_doc_tree(doc_meta)
    sidebar_items = build_root_sidebar_items(tree, site_config)
    out_dir = os.path.relpath(OUTPUT_ROOT, VITEPRESS_ROOT).replace(os.sep, "/")
    config_payload = {
        "title": site_config["site"]["title"],
        "description": site_config["site"]["description"],
        "lang": "zh-CN",
        "cleanUrls": False,
        "ignoreDeadLinks": True,
        "outDir": out_dir,
        "appearance": False,
        "themeConfig": {
            "search": {
                "provider": "local",
                "options": {
                    "translations": LOCAL_SEARCH_TRANSLATIONS,
                },
            },
            "nav": [],
            "sidebar": sidebar_items,
            "outline": {
                "label": "本页目录",
                "level": "deep",
            },
            "docFooter": {
                "prev": "上一页",
                "next": "下一页",
            },
            "sidebarMenuLabel": "导航菜单",
            "returnToTopLabel": "返回顶部",
            "skipToContentLabel": "跳到正文",
        },
    }
    config_text = (
        "import { defineConfig } from 'vitepress'\n\n"
        + "export default defineConfig("
        + json.dumps(config_payload, ensure_ascii=False, indent=2)
        + ")\n"
    )
    ensure_dir_777(VITEPRESS_CONFIG_PATH.parent)
    write_text_777(VITEPRESS_CONFIG_PATH, config_text)


def collect_doc_meta(docs_map: dict[Path, Path], site_config: dict) -> dict[Path, dict[str, str]]:
    meta: dict[Path, dict[str, str]] = {}
    for source_rel, staged_rel in docs_map.items():
        page_override = site_config["page_overrides"].get(source_rel.as_posix())
        title_override = None if page_override is None else page_override["title"]
        meta[staged_rel] = {
            "source_rel": source_rel.as_posix(),
            "staged_rel": staged_rel.as_posix(),
            "title": title_override if title_override is not None else derive_title_from_path(staged_rel),
            "route": route_for_staged_rel(staged_rel),
        }
    return meta


def derive_title_from_path(staged_rel: Path) -> str:
    if staged_rel == Path("index.md"):
        return "首页"
    if staged_rel.name == "index.md":
        return staged_rel.parent.name
    return staged_rel.stem


def route_for_staged_rel(staged_rel: Path) -> str:
    if staged_rel == Path("index.md"):
        return "/"
    if staged_rel.name == "index.md":
        return f"/{staged_rel.parent.as_posix()}/"
    return f"/{staged_rel.with_suffix('').as_posix()}"


def build_doc_tree(doc_meta: dict[Path, dict[str, str]]) -> dict:
    root = new_tree_node("", "根")
    for staged_rel in sorted(doc_meta):
        info = doc_meta[staged_rel]
        parent_node = ensure_tree_node(root, staged_rel.parent)
        if staged_rel.name == "index.md":
            parent_node["index"] = info
        else:
            parent_node["pages"].append(info)
    return root


def new_tree_node(name: str, title: str) -> dict:
    return {
        "name": name,
        "title": title,
        "index": None,
        "pages": [],
        "dirs": {},
    }


def ensure_tree_node(root: dict, rel_dir: Path) -> dict:
    node = root
    if str(rel_dir) == ".":
        return node
    for part in rel_dir.parts:
        dirs = node["dirs"]
        if part not in dirs:
            dirs[part] = new_tree_node(part, part)
        node = dirs[part]
    return node


def build_root_sidebar_items(root: dict, site_config: dict) -> list[dict]:
    items: list[dict] = []
    for entry in site_config["root_nav"]:
        if entry["kind"] == "dir":
            child_group = build_root_dir_sidebar_group(root["dirs"][entry["path"]], entry)
            if child_group is not None:
                items.append(child_group)
            continue
        page = find_root_nav_page(root, entry["path"])
        if page is None:
            raise SystemExit(
                "ERROR: configured root_nav page not found after validation: "
                + entry["path"]
            )
        items.append({"text": entry["title"], "link": page["route"]})
    return items


def build_root_dir_sidebar_group(node: dict, nav_entry: dict) -> dict | None:
    route = resolve_dir_link(node, nav_entry)
    child_items = build_tree_sidebar_items(node, include_index=False)
    if not child_items:
        return {"text": nav_entry["title"], "link": route}
    return {
        "text": nav_entry["title"],
        "collapsed": False,
        "items": child_items,
    }


def build_tree_sidebar_items(node: dict, include_index: bool) -> list[dict]:
    items: list[dict] = []
    if include_index and node["index"] is not None:
        items.append({"text": node["index"]["title"], "link": node["index"]["route"]})
    for page in node["pages"]:
        items.append({"text": page["title"], "link": page["route"]})
    for name in sorted(node["dirs"]):
        child_group = build_tree_sidebar_group(node["dirs"][name])
        if child_group is not None:
            items.append(child_group)
    return items


def build_tree_sidebar_group(node: dict) -> dict | None:
    route = first_route_in_node(node)
    if route is None:
        return None
    child_items = build_tree_sidebar_items(node, include_index=True)
    if not child_items:
        return {"text": node_display_title(node), "link": route}
    return {
        "text": node_display_title(node),
        "collapsed": False,
        "items": child_items,
    }


def node_display_title(node: dict) -> str:
    if node["index"] is not None:
        return node["index"]["title"]
    return node["title"]


def find_root_page(pages: list[dict[str, str]], staged_rel: str) -> dict[str, str] | None:
    for page in pages:
        if page["staged_rel"] == staged_rel:
            return page
    return None


def find_root_nav_page(root: dict, staged_rel: str) -> dict[str, str] | None:
    if staged_rel == "index.md":
        return root["index"]
    return find_root_page(root["pages"], staged_rel)


def iter_markdown_content_lines(raw_md: str) -> list[str]:
    lines: list[str] = []
    in_fence = False
    for line in raw_md.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or not stripped:
            continue
        lines.append(stripped)
    return lines


def markdown_has_h1(raw_md: str) -> bool:
    for stripped in iter_markdown_content_lines(raw_md):
        if stripped.startswith("# "):
            return True
    return False


def ensure_markdown_title_frontmatter(raw_md: str, title: str) -> str:
    if raw_md.startswith("---\n") or raw_md.startswith("---\r\n"):
        return raw_md
    return f"---\ntitle: {json.dumps(title, ensure_ascii=False)}\n---\n\n{raw_md}"


def first_route_in_node(node: dict) -> str | None:
    if node["index"] is not None:
        return node["index"]["route"]
    if node["pages"]:
        return node["pages"][0]["route"]
    for name in sorted(node["dirs"]):
        route = first_route_in_node(node["dirs"][name])
        if route is not None:
            return route
    return None


def resolve_dir_link(node: dict, nav_entry: dict) -> str:
    nav_link_mode = nav_entry["nav_link_mode"]
    section_name = nav_entry["path"]
    if nav_link_mode == "index":
        if node["index"] is None:
            raise SystemExit(
                "ERROR: root_nav dir with nav_link_mode=index is missing README.md: "
                + section_name
            )
        return node["index"]["route"]
    if nav_link_mode == "first_sidebar_link":
        route = first_sidebar_link_in_root_dir(node)
        if route is None:
            raise SystemExit(
                "ERROR: root_nav dir with nav_link_mode=first_sidebar_link has no visible sidebar links: "
                + section_name
            )
        return route
    route = first_route_in_node(node)
    if route is None:
        raise SystemExit(
            "ERROR: root_nav dir with nav_link_mode=first_page has no published pages: "
            + section_name
        )
    return route


def first_sidebar_link_in_root_dir(node: dict) -> str | None:
    return first_tree_sidebar_link(node, include_index=False)


def first_tree_sidebar_link(node: dict, include_index: bool) -> str | None:
    if include_index and node["index"] is not None:
        return node["index"]["route"]
    if node["pages"]:
        return node["pages"][0]["route"]
    for name in sorted(node["dirs"]):
        route = first_tree_sidebar_link(node["dirs"][name], include_index=True)
        if route is not None:
            return route
    return None


def validate_root_nav_contract(doc_meta: dict[Path, dict[str, str]], site_config: dict) -> None:
    root_pages: set[str] = set()
    root_dirs: set[str] = set()

    for staged_rel in doc_meta:
        if len(staged_rel.parts) == 1:
            root_pages.add(staged_rel.as_posix())
            continue
        root_dirs.add(staged_rel.parts[0])

    configured_root_pages = set(site_config["root_nav_pages"])
    configured_root_dirs = set(site_config["root_nav_dirs"])
    missing_root_pages = sorted(page for page in root_pages if page not in configured_root_pages)
    missing_root_dirs = sorted(name for name in root_dirs if name not in configured_root_dirs)
    missing_config_pages = sorted(page for page in configured_root_pages if page not in root_pages)
    missing_config_dirs = sorted(name for name in configured_root_dirs if name not in root_dirs)
    missing_required_indexes = sorted(
        entry["path"]
        for entry in site_config["root_nav"]
        if entry["kind"] == "dir"
        and entry["nav_link_mode"] == "index"
        and Path(entry["path"]) / "index.md" not in doc_meta
    )
    if (
        not missing_root_pages
        and not missing_root_dirs
        and not missing_config_pages
        and not missing_config_dirs
        and not missing_required_indexes
    ):
        return

    errors: list[str] = []
    for page in missing_root_pages:
        errors.append(
            "ERROR: published root markdown page missing from build_doc_site_config.root_nav: "
            f"{page}"
        )
    for name in missing_root_dirs:
        errors.append(
            "ERROR: published root markdown directory missing from build_doc_site_config.root_nav: "
            f"{name}"
        )
    for page in missing_config_pages:
        errors.append(
            "ERROR: build_doc_site_config.root_nav page does not exist in published docs: "
            f"{page}"
        )
    for name in missing_config_dirs:
        errors.append(
            "ERROR: build_doc_site_config.root_nav dir does not exist in published docs: "
            f"{name}"
        )
    for name in missing_required_indexes:
        errors.append(
            "ERROR: build_doc_site_config root dir requires README.md for nav/sidebar but none was published: "
            f"{name}"
        )
    raise SystemExit("\n".join(errors))


def write_toolchain_package_json() -> None:
    ensure_dir_777(TOOLCHAIN_PACKAGE_JSON_PATH.parent)
    payload = {
        "private": True,
        "devDependencies": {
            "medium-zoom": "1.1.0",
            "vitepress": VITEPRESS_VERSION,
        },
    }
    write_text_777(
        TOOLCHAIN_PACKAGE_JSON_PATH,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


def rewrite_markdown_links(
    raw_md: str,
    source_rel: Path,
    staged_rel: Path,
    docs_map: dict[Path, Path],
) -> str:
    lines = raw_md.splitlines(keepends=True)
    out_lines: list[str] = []
    in_fence = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            out_lines.append(line)
            continue
        if in_fence:
            out_lines.append(line)
            continue
        out_lines.append(rewrite_markdown_links_in_line(line, source_rel, staged_rel, docs_map))
    return "".join(out_lines)


def rewrite_markdown_links_in_line(
    line: str,
    source_rel: Path,
    staged_rel: Path,
    docs_map: dict[Path, Path],
) -> str:
    def replace(match: re.Match[str]) -> str:
        prefix = match.group(1)
        raw_target = match.group(2).strip()
        suffix = match.group(3)
        new_target = rewrite_target_path(raw_target, source_rel, staged_rel, docs_map)
        return f"{prefix}{new_target}{suffix}"

    return MARKDOWN_LINK_RE.sub(replace, line)


def rewrite_target_path(
    raw_target: str,
    source_rel: Path,
    staged_rel: Path,
    docs_map: dict[Path, Path],
) -> str:
    unescaped_target = decode_markdown_target(raw_target)
    if (
        not unescaped_target
        or unescaped_target.startswith("#")
        or unescaped_target.startswith("http://")
        or unescaped_target.startswith("https://")
        or unescaped_target.startswith("mailto:")
        or unescaped_target.startswith("tel:")
        or unescaped_target.startswith("data:")
    ):
        return raw_target

    split = urllib.parse.urlsplit(unescaped_target)
    path_part = urllib.parse.unquote(split.path)
    fragment = f"#{split.fragment}" if split.fragment else ""

    if not path_part.endswith(".md"):
        return raw_target

    source_target_rel = normalize_rel_path(source_rel.parent / path_part)
    if source_target_rel not in docs_map:
        raise SystemExit(
            f"ERROR: markdown link target not found: source={source_rel} target={raw_target}"
        )
    staged_target_rel = docs_map[source_target_rel]
    rel_path = os.path.relpath(staged_target_rel, staged_rel.parent).replace(os.sep, "/")
    return urllib.parse.quote(rel_path, safe="/") + fragment


def decode_markdown_target(raw_target: str) -> str:
    unescaped = html.unescape(raw_target).strip()
    if unescaped.startswith("<") and unescaped.endswith(">"):
        return unescaped[1:-1].strip()
    return unescaped


def normalize_rel_path(path: Path) -> Path:
    parts: list[str] = []
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                raise SystemExit(f"ERROR: path escapes doc root: {path}")
            parts.pop()
            continue
        parts.append(part)
    return Path(*parts) if parts else Path(".")


def validate_source_markdown(raw_md: str, source_rel: Path) -> None:
    in_fence = False
    for line_idx, line in enumerate(raw_md.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        in_inline_code = False
        i = 0
        while i < len(line):
            if line[i] == "`":
                in_inline_code = not in_inline_code
                i += 1
                continue
            if not in_inline_code and line[i : i + 3] == "![[":
                raise SystemExit(
                    f"ERROR: unsupported Obsidian embed syntax remains: source={source_rel} line={line_idx}"
                )
            if not in_inline_code and line[i : i + 2] == "[[":
                raise SystemExit(
                    f"ERROR: unsupported Obsidian link syntax remains: source={source_rel} line={line_idx}"
                )
            i += 1

def validate_docs_map(docs_map: dict[Path, Path]) -> None:
    errors: list[str] = []
    for source_rel in docs_map:
        raw_md = (DOC_ROOT / source_rel).read_text(encoding="utf-8")
        validate_source_markdown(raw_md, source_rel)
        errors.extend(collect_markdown_link_errors(raw_md, source_rel, docs_map))
    if errors:
        joined = "\n".join(errors)
        raise SystemExit(f"ERROR: markdown link validation failed:\n{joined}")


def collect_markdown_link_errors(
    raw_md: str,
    source_rel: Path,
    docs_map: dict[Path, Path],
) -> list[str]:
    errors: list[str] = []
    lines = raw_md.splitlines()
    in_fence = False
    for line_idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for match in MARKDOWN_LINK_RE.finditer(line):
            raw_target = match.group(2).strip()
            error = validate_markdown_target(raw_target, source_rel, docs_map)
            if error is not None:
                errors.append(f"{error} line={line_idx}")
    return errors


def validate_markdown_target(
    raw_target: str,
    source_rel: Path,
    docs_map: dict[Path, Path],
) -> str | None:
    unescaped_target = decode_markdown_target(raw_target)
    if (
        not unescaped_target
        or unescaped_target.startswith("#")
        or unescaped_target.startswith("http://")
        or unescaped_target.startswith("https://")
        or unescaped_target.startswith("mailto:")
        or unescaped_target.startswith("tel:")
        or unescaped_target.startswith("data:")
    ):
        return None

    split = urllib.parse.urlsplit(unescaped_target)
    path_part = urllib.parse.unquote(split.path)
    if not path_part.endswith(".md"):
        return None

    source_target_rel = normalize_rel_path(source_rel.parent / path_part)
    if source_target_rel in docs_map:
        return None
    return f"ERROR: markdown link target not found: source={source_rel} target={raw_target}"


def compute_source_state() -> tuple[tuple[str, int, int], ...]:
    rows: list[tuple[str, int, int]] = []
    for path in sorted(DOC_ROOT.rglob("*")):
        rel = path.relative_to(DOC_ROOT)
        if should_skip_rel_path(rel) or not path.is_file():
            continue
        stat = path.stat()
        rows.append((rel.as_posix(), stat.st_mtime_ns, stat.st_size))
    if not DOC_SITE_CONFIG_PATH.is_file():
        raise SystemExit(f"ERROR: doc site config not found: {DOC_SITE_CONFIG_PATH}")
    config_stat = DOC_SITE_CONFIG_PATH.stat()
    rows.append(
        (
            f"@config:{DOC_SITE_CONFIG_PATH.relative_to(REPO_ROOT).as_posix()}",
            config_stat.st_mtime_ns,
            config_stat.st_size,
        )
    )
    return tuple(rows)


def require_binary(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise SystemExit(f"ERROR: `{name}` not found in PATH.")
    return path


def ensure_dir_777(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o777)


def write_text_777(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o777)


def chmod_tree_777(root: Path) -> None:
    if not root.exists():
        return
    for path in sorted(root.rglob("*")):
        os.chmod(path, 0o777)
    os.chmod(root, 0o777)


def run_cmd(cmd: list[str], cwd: Path) -> None:
    print("+ " + " ".join(shlex.quote(v) for v in cmd), flush=True)
    rc = subprocess.run(cmd, cwd=str(cwd)).returncode
    if rc != 0:
        raise SystemExit(rc)


def run_vitepress_build(vitepress_cmd: list[str]) -> None:
    cmd = vitepress_cmd + ["build", str(VITEPRESS_ROOT)]
    run_cmd(cmd, cwd=PROJECT_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
