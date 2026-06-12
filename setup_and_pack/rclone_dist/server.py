from __future__ import annotations

import argparse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any

from setup_and_pack.rclone_dist.common import (
    TaskStore,
    build_rclone_dir_command,
    load_task_manifest,
    shell_join_argv,
    shuffled_tasks,
)


API_PREFIX = "/api/v1"


class RcloneDistHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        task_store: TaskStore,
        *,
        lease_seconds: int,
    ) -> None:
        super().__init__(server_address, RcloneDistHandler)
        self.task_store = task_store
        self.default_lease_seconds = lease_seconds


class RcloneDistHandler(BaseHTTPRequestHandler):
    server: RcloneDistHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[rclone-dist-server] {self.address_string()} - {fmt % args}", flush=True)

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        content_length_raw = self.headers.get("Content-Length", "0")
        try:
            content_length = int(content_length_raw)
        except ValueError as exc:
            raise ValueError(f"invalid Content-Length: {content_length_raw}") from exc
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("request body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _required_str(self, payload: dict[str, Any], key: str) -> str:
        value = str(payload.get(key, "")).strip()
        if not value:
            raise ValueError(f"{key} is required")
        return value

    def _optional_float(self, payload: dict[str, Any], key: str) -> float | None:
        value = payload.get(key)
        if value is None or value == "":
            return None
        return float(value)

    def _optional_int(self, payload: dict[str, Any], key: str, default: int) -> int:
        value = payload.get(key)
        if value is None or value == "":
            return default
        return int(value)

    def do_GET(self) -> None:
        if self.path == f"{API_PREFIX}/healthz":
            self._send_json(HTTPStatus.OK, {"ok": True})
            return
        if self.path == f"{API_PREFIX}/status":
            self._send_json(HTTPStatus.OK, self.server.task_store.status_snapshot())
            return
        if self.path == f"{API_PREFIX}/manifest":
            self._send_json(HTTPStatus.OK, {"tasks": self.server.task_store.manifest()})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"unknown path: {self.path}"})

    def do_POST(self) -> None:
        try:
            payload = self._read_json_body()
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        try:
            if self.path == f"{API_PREFIX}/try_get":
                self._handle_try_get(payload)
                return
            if self.path == f"{API_PREFIX}/ack_launch":
                self._handle_ack_launch(payload)
                return
            if self.path == f"{API_PREFIX}/heartbeat":
                self._handle_heartbeat(payload)
                return
            if self.path == f"{API_PREFIX}/release":
                self._handle_release(payload)
                return
            if self.path == f"{API_PREFIX}/report_finish":
                self._handle_report_finish(payload)
                return
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"unknown path: {self.path}"})

    def _handle_try_get(self, payload: dict[str, Any]) -> None:
        worker_id = self._required_str(payload, "worker_id")
        peer_id = self._required_str(payload, "peer_id")
        src_root = self._required_str(payload, "src_root")
        dst_root = self._required_str(payload, "dst_root")
        lease_seconds = self._optional_int(payload, "lease_seconds", self.server.default_lease_seconds)
        downlink_bps = self._optional_float(payload, "downlink_bps")
        rclone_bin = str(payload.get("rclone_bin", "rclone")).strip() or "rclone"
        rclone_args = [str(item) for item in list(payload.get("rclone_args") or [])]

        task, lease, all_done, reason, gate = self.server.task_store.acquire(
            worker_id=worker_id,
            peer_id=peer_id,
            lease_seconds=lease_seconds,
            downlink_bps=downlink_bps,
        )
        if task is None or lease is None:
            self._send_json(
                HTTPStatus.OK,
                {
                    "granted": False,
                    "all_done": all_done,
                    "reason": reason,
                    "gate": gate,
                    "task": None,
                    "lease": None,
                    "command": None,
                },
            )
            return

        command_argv = build_rclone_dir_command(
            rclone_bin=rclone_bin,
            src_root=src_root,
            dst_root=dst_root,
            relative_path=task.relative_path,
            rclone_args=rclone_args,
        )
        self._send_json(
            HTTPStatus.OK,
            {
                "granted": True,
                "all_done": False,
                "reason": reason,
                "gate": gate,
                "task": task.to_dict(),
                "lease": lease.to_dict(),
                "command": {
                    "argv": command_argv,
                    "shell": shell_join_argv(command_argv),
                },
            },
        )

    def _handle_ack_launch(self, payload: dict[str, Any]) -> None:
        worker_id = self._required_str(payload, "worker_id")
        session_name = self._required_str(payload, "session_name")
        task_id = int(payload.get("task_id"))
        command = [str(item) for item in list(payload.get("command") or [])]
        ok, note = self.server.task_store.ack_launch(
            task_id=task_id,
            worker_id=worker_id,
            session_name=session_name,
            command=command,
            note=str(payload.get("note", "")),
        )
        self._send_json(HTTPStatus.OK, {"ok": ok, "note": note})

    def _handle_heartbeat(self, payload: dict[str, Any]) -> None:
        worker_id = self._required_str(payload, "worker_id")
        peer_id = self._required_str(payload, "peer_id")
        task_id = int(payload.get("task_id"))
        lease_seconds = self._optional_int(payload, "lease_seconds", self.server.default_lease_seconds)
        downlink_bps = self._optional_float(payload, "downlink_bps")
        ok, note = self.server.task_store.heartbeat(
            task_id=task_id,
            worker_id=worker_id,
            peer_id=peer_id,
            lease_seconds=lease_seconds,
            downlink_bps=downlink_bps,
        )
        self._send_json(HTTPStatus.OK, {"ok": ok, "note": note})

    def _handle_release(self, payload: dict[str, Any]) -> None:
        worker_id = self._required_str(payload, "worker_id")
        task_id = int(payload.get("task_id"))
        ok, note = self.server.task_store.release(
            task_id=task_id,
            worker_id=worker_id,
            note=str(payload.get("note", "")),
        )
        self._send_json(HTTPStatus.OK, {"ok": ok, "note": note})

    def _handle_report_finish(self, payload: dict[str, Any]) -> None:
        worker_id = self._required_str(payload, "worker_id")
        peer_id = self._required_str(payload, "peer_id")
        task_id = int(payload.get("task_id"))
        return_code_raw = payload.get("return_code")
        return_code = None if return_code_raw is None else int(return_code_raw)
        elapsed_seconds_raw = payload.get("elapsed_seconds")
        elapsed_seconds = None if elapsed_seconds_raw is None else float(elapsed_seconds_raw)
        finished_at_unix_raw = payload.get("finished_at_unix")
        finished_at_unix = None if finished_at_unix_raw is None else float(finished_at_unix_raw)
        command = [str(item) for item in list(payload.get("command") or [])]
        ok, note = self.server.task_store.report_finish(
            task_id=task_id,
            worker_id=worker_id,
            peer_id=peer_id,
            return_code=return_code,
            finished_at_unix=finished_at_unix,
            elapsed_seconds=elapsed_seconds,
            session_name=str(payload.get("session_name", "")),
            command=command,
            note=str(payload.get("note", "")),
        )
        self._send_json(HTTPStatus.OK, {"ok": ok, "note": note})


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serve a manifest of rclone directory tasks with peer-bandwidth gating.",
    )
    parser.add_argument("--manifest-file", required=True, help="Text file containing one relative directory per line.")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP listen host.")
    parser.add_argument("--port", type=int, default=18080, help="HTTP listen port.")
    parser.add_argument("--lease-seconds", type=int, default=1800, help="Default task lease duration.")
    parser.add_argument("--max-attempts", type=int, default=3, help="Maximum retry attempts per task.")
    parser.add_argument("--shuffle-seed", type=int, default=None, help="Optional task shuffle seed.")
    parser.add_argument(
        "--manifest-json",
        default="",
        help="Optional path to dump the loaded manifest as JSON for inspection.",
    )
    parser.add_argument(
        "--low-bandwidth-threshold-bps",
        type=float,
        required=True,
        help="Only grant new tasks when the peer's recent downlink stays below this threshold.",
    )
    parser.add_argument(
        "--bandwidth-sustain-seconds",
        type=float,
        default=30.0,
        help="Peer downlink must stay below threshold for at least this long.",
    )
    parser.add_argument(
        "--min-bandwidth-samples",
        type=int,
        default=3,
        help="Minimum number of recent bandwidth samples required before the gate can open.",
    )
    return parser


def _write_manifest_json(path: str, payload: dict[str, Any]) -> None:
    if not path:
        return
    manifest_path = Path(path).expanduser()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_server(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest_file).expanduser()
    tasks = shuffled_tasks(load_task_manifest(manifest_path), args.shuffle_seed)
    task_store = TaskStore(
        tasks,
        max_attempts=args.max_attempts,
        low_bandwidth_threshold_bps=args.low_bandwidth_threshold_bps,
        bandwidth_sustain_seconds=args.bandwidth_sustain_seconds,
        min_bandwidth_samples=args.min_bandwidth_samples,
    )

    manifest_payload = {
        "manifest_file": str(manifest_path.resolve()),
        "task_count": len(tasks),
        "shuffle_seed": args.shuffle_seed,
        "tasks": [task.to_dict() for task in tasks],
    }
    _write_manifest_json(args.manifest_json, manifest_payload)

    print(
        "[rclone-dist-server] "
        f"loaded {len(tasks)} manifest tasks from {manifest_path.resolve()} "
        f"(threshold_bps={args.low_bandwidth_threshold_bps}, sustain_s={args.bandwidth_sustain_seconds}, "
        f"min_samples={args.min_bandwidth_samples}, max_attempts={args.max_attempts})",
        flush=True,
    )
    if args.manifest_json:
        print(f"[rclone-dist-server] wrote manifest to {Path(args.manifest_json).expanduser()}", flush=True)

    server = RcloneDistHTTPServer(
        (args.host, args.port),
        task_store,
        lease_seconds=args.lease_seconds,
    )
    print(f"[rclone-dist-server] listening on http://{args.host}:{args.port}{API_PREFIX}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[rclone-dist-server] shutting down", flush=True)
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return run_server(args)


if __name__ == "__main__":
    raise SystemExit(main())
