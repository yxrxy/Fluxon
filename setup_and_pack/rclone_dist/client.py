from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll an rclone-dist server, receive one gated rclone command, and launch it in tmux.",
    )
    parser.add_argument("--server-url", required=True, help="Base server URL, for example http://host:18080")
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}-{os.getpid()}", help="Stable worker identity.")
    parser.add_argument(
        "--peer-id",
        required=True,
        help="Identifier of the remote destination machine whose downlink should be bandwidth-gated.",
    )
    parser.add_argument("--src-root", required=True, help="rclone source root for manifest relative paths.")
    parser.add_argument("--dst-root", required=True, help="rclone destination root for manifest relative paths.")
    parser.add_argument("--rclone-bin", default="rclone", help="rclone binary path.")
    parser.add_argument(
        "--rclone-arg",
        action="append",
        default=[],
        help="Extra argument appended to every rclone command. Repeat this flag for multiple args.",
    )
    parser.add_argument("--lease-seconds", type=int, default=1800, help="Requested lease duration.")
    parser.add_argument("--heartbeat-seconds", type=int, default=15, help="Heartbeat period while tmux task is active.")
    parser.add_argument("--poll-seconds", type=float, default=5.0, help="Sleep between try_get polls.")
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0, help="HTTP request timeout.")
    parser.add_argument(
        "--state-dir",
        required=True,
        help="Local directory used to store tmux logs, finish markers, and client state.",
    )
    parser.add_argument(
        "--tmux-prefix",
        default="rclone-dist",
        help="Prefix used when generating tmux session names.",
    )
    parser.add_argument(
        "--tmux-bin",
        default="tmux",
        help="tmux binary path.",
    )
    parser.add_argument(
        "--downlink-command",
        required=True,
        help="Shell command that prints the peer downlink bytes/sec or bits/sec as a single number.",
    )
    parser.add_argument(
        "--downlink-unit",
        choices=("bps", "Bps"),
        default="bps",
        help="Unit emitted by --downlink-command.",
    )
    parser.add_argument(
        "--single-shot",
        action="store_true",
        help="Exit after one successful launch or one no-work response.",
    )
    return parser


def _normalize_server_url(server_url: str) -> str:
    return server_url.rstrip("/")


def _api_json(
    server_url: str,
    path: str,
    payload: dict[str, Any] | None,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=True).encode("utf-8")
    req = urllib_request.Request(
        f"{_normalize_server_url(server_url)}{path}",
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib_request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read()
    except urllib_error.HTTPError as exc:
        raw = exc.read()
        try:
            payload_obj = json.loads(raw.decode("utf-8"))
        except Exception:
            payload_obj = {"error": raw.decode("utf-8", errors="replace")}
        raise RuntimeError(f"HTTP {exc.code} for {path}: {payload_obj}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"request failed for {path}: {exc}") from exc

    try:
        payload_obj = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"non-JSON response from {path}: {raw[:200]!r}") from exc
    if not isinstance(payload_obj, dict):
        raise RuntimeError(f"unexpected response shape from {path}: {type(payload_obj)!r}")
    return payload_obj


def _ensure_tmux_exists(tmux_bin: str) -> None:
    if shutil.which(tmux_bin) is None:
        raise SystemExit(f"missing tmux binary: {tmux_bin}")


def _run_shell_capture_number(command: str) -> float:
    completed = subprocess.run(
        command,
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    text = completed.stdout.strip().splitlines()
    if not text:
        raise RuntimeError("downlink command produced no stdout")
    raw = text[-1].strip()
    return float(raw)


def _sample_downlink_bps(args: argparse.Namespace) -> float:
    value = _run_shell_capture_number(args.downlink_command)
    if args.downlink_unit == "Bps":
        return value * 8.0
    return value


def _task_dir(state_dir: Path, task_id: int) -> Path:
    return state_dir / f"task_{task_id:08d}"


def _task_meta_path(state_dir: Path, task_id: int) -> Path:
    return _task_dir(state_dir, task_id) / "meta.json"


def _task_finish_path(state_dir: Path, task_id: int) -> Path:
    return _task_dir(state_dir, task_id) / "finish.json"


def _tmux_session_exists(tmux_bin: str, session_name: str) -> bool:
    result = subprocess.run(
        [tmux_bin, "has-session", "-t", session_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _tmux_new_session(
    *,
    tmux_bin: str,
    session_name: str,
    command_script: str,
) -> None:
    subprocess.check_call(
        [tmux_bin, "new-session", "-d", "-s", session_name, command_script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def _write_tmux_wrapper_script(
    *,
    task_dir: Path,
    command_shell: str,
    log_path: Path,
    finish_path: Path,
) -> Path:
    script_path = task_dir / "run.sh"
    script_text = (
        "#!/usr/bin/env bash\n"
        "set +e\n"
        f"rm -f {shlex_quote(finish_path.as_posix())}\n"
        f"{command_shell} >> {shlex_quote(log_path.as_posix())} 2>&1\n"
        "rc=$?\n"
        f"python3 - {shlex_quote(finish_path.as_posix())} \"$rc\" <<'PY'\n"
        "import json\n"
        "import sys\n"
        "import time\n"
        "payload = {'return_code': int(sys.argv[2]), 'finished_at_unix': time.time()}\n"
        "with open(sys.argv[1], 'w', encoding='utf-8') as fh:\n"
        "    json.dump(payload, fh, ensure_ascii=True, sort_keys=True)\n"
        "    fh.write('\\n')\n"
        "PY\n"
        "exit \"$rc\"\n"
    )
    script_path.write_text(script_text, encoding="utf-8")
    script_path.chmod(0o755)
    return script_path


def _launch_tmux_task(
    *,
    args: argparse.Namespace,
    state_dir: Path,
    task: dict[str, Any],
    lease: dict[str, Any],
    command_argv: list[str],
    command_shell: str,
) -> tuple[str, Path]:
    task_id = int(task["task_id"])
    relative_path = str(task["relative_path"])
    task_dir = _task_dir(state_dir, task_id)
    task_dir.mkdir(parents=True, exist_ok=True)

    log_path = task_dir / "rclone.log"
    finish_path = _task_finish_path(state_dir, task_id)
    session_name = f"{args.tmux_prefix}-{task_id:08d}"

    if _tmux_session_exists(args.tmux_bin, session_name):
        raise RuntimeError(f"tmux session already exists: {session_name}")

    script_path = _write_tmux_wrapper_script(
        task_dir=task_dir,
        command_shell=command_shell,
        log_path=log_path,
        finish_path=finish_path,
    )
    command_script = f"bash {shlex_quote(script_path.as_posix())}"

    started_at_unix = time.time()
    _tmux_new_session(
        tmux_bin=args.tmux_bin,
        session_name=session_name,
        command_script=command_script,
    )
    _write_json(
        _task_meta_path(state_dir, task_id),
        {
            "task": task,
            "lease": lease,
            "session_name": session_name,
            "command": command_argv,
            "command_shell": command_shell,
            "relative_path": relative_path,
            "started_at_unix": started_at_unix,
            "log_path": str(log_path),
            "finish_path": str(finish_path),
            "script_path": str(script_path),
        },
    )
    return session_name, finish_path


def _load_active_tasks(state_dir: Path) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    if not state_dir.exists():
        return active
    for meta_path in sorted(state_dir.glob("task_*/meta.json")):
        try:
            meta = _load_json(meta_path)
        except Exception:
            continue
        active.append(meta)
    return active


def _try_report_finished_task(args: argparse.Namespace, meta: dict[str, Any]) -> bool:
    finish_path = Path(str(meta["finish_path"]))
    if not finish_path.exists():
        return False
    finish_payload = _load_json(finish_path)
    finished_at_unix = float(finish_payload["finished_at_unix"])
    return_code = int(finish_payload["return_code"])
    started_at_unix = float(meta["started_at_unix"])
    elapsed_seconds = max(0.0, finished_at_unix - started_at_unix)
    task_id = int(meta["task"]["task_id"])
    response = _api_json(
        args.server_url,
        "/api/v1/report_finish",
        {
            "worker_id": args.worker_id,
            "peer_id": args.peer_id,
            "task_id": task_id,
            "return_code": return_code,
            "finished_at_unix": finished_at_unix,
            "elapsed_seconds": elapsed_seconds,
            "session_name": str(meta["session_name"]),
            "command": list(meta["command"]),
            "note": "tmux task finished",
        },
        timeout_seconds=args.request_timeout_seconds,
    )
    if not response.get("ok", False):
        raise RuntimeError(f"report_finish rejected: {response}")
    meta_path = _task_meta_path(Path(args.state_dir).expanduser(), task_id)
    meta_path.unlink(missing_ok=True)
    finish_path.unlink(missing_ok=True)
    return True


def _heartbeat_active_tasks(args: argparse.Namespace, active_tasks: list[dict[str, Any]], downlink_bps: float) -> None:
    for meta in active_tasks:
        task_id = int(meta["task"]["task_id"])
        session_name = str(meta["session_name"])
        if not _tmux_session_exists(args.tmux_bin, session_name) and not Path(str(meta["finish_path"])).exists():
            _write_json(
                Path(str(meta["finish_path"])),
                {
                    "return_code": 255,
                    "finished_at_unix": time.time(),
                },
            )
            continue
        response = _api_json(
            args.server_url,
            "/api/v1/heartbeat",
            {
                "worker_id": args.worker_id,
                "peer_id": args.peer_id,
                "task_id": task_id,
                "lease_seconds": args.lease_seconds,
                "downlink_bps": downlink_bps,
            },
            timeout_seconds=args.request_timeout_seconds,
        )
        if not response.get("ok", False):
            print(f"[rclone-dist-client] heartbeat rejected task_id={task_id}: {response}", flush=True)


def run_client(args: argparse.Namespace) -> int:
    if args.lease_seconds <= args.heartbeat_seconds:
        raise ValueError("--lease-seconds must be larger than --heartbeat-seconds")
    _ensure_tmux_exists(args.tmux_bin)

    state_dir = Path(args.state_dir).expanduser()
    state_dir.mkdir(parents=True, exist_ok=True)

    print(
        "[rclone-dist-client] "
        f"server={_normalize_server_url(args.server_url)} worker_id={args.worker_id} peer_id={args.peer_id} "
        f"src_root={args.src_root} dst_root={args.dst_root} state_dir={state_dir}",
        flush=True,
    )

    last_try_get_unix = 0.0
    while True:
        try:
            downlink_bps = _sample_downlink_bps(args)
        except Exception as exc:
            print(f"[rclone-dist-client] downlink sample failed: {exc}", flush=True)
            time.sleep(max(0.1, args.poll_seconds))
            continue
        active_tasks = _load_active_tasks(state_dir)

        reported_any = False
        for meta in active_tasks:
            try:
                if _try_report_finished_task(args, meta):
                    reported_any = True
            except Exception as exc:
                print(
                    f"[rclone-dist-client] report_finish retry pending task_id={meta['task']['task_id']}: {exc}",
                    flush=True,
                )
        active_tasks = _load_active_tasks(state_dir)

        if active_tasks:
            try:
                _heartbeat_active_tasks(args, active_tasks, downlink_bps)
            except Exception as exc:
                print(f"[rclone-dist-client] heartbeat loop failed: {exc}", flush=True)
            time.sleep(max(1.0, args.heartbeat_seconds))
            continue

        now_unix = time.time()
        if now_unix - last_try_get_unix < max(0.1, args.poll_seconds):
            time.sleep(max(0.1, args.poll_seconds))
            continue
        last_try_get_unix = now_unix

        try:
            response = _api_json(
                args.server_url,
                "/api/v1/try_get",
                {
                    "worker_id": args.worker_id,
                    "peer_id": args.peer_id,
                    "src_root": args.src_root,
                    "dst_root": args.dst_root,
                    "lease_seconds": args.lease_seconds,
                    "downlink_bps": downlink_bps,
                    "rclone_bin": args.rclone_bin,
                    "rclone_args": list(args.rclone_arg),
                },
                timeout_seconds=args.request_timeout_seconds,
            )
        except Exception as exc:
            print(f"[rclone-dist-client] try_get failed: {exc}", flush=True)
            time.sleep(max(0.1, args.poll_seconds))
            continue

        if not response.get("granted", False):
            reason = str(response.get("reason", "unknown"))
            print(
                f"[rclone-dist-client] no task granted worker_id={args.worker_id} peer_id={args.peer_id} "
                f"reason={reason} downlink_bps={downlink_bps:.3f}",
                flush=True,
            )
            if args.single_shot:
                return 0
            if response.get("all_done", False):
                return 0
            time.sleep(max(0.1, args.poll_seconds))
            continue

        task = dict(response["task"])
        lease = dict(response["lease"])
        command_payload = dict(response["command"])
        command_argv = [str(item) for item in list(command_payload["argv"])]
        command_shell = str(command_payload["shell"])

        try:
            session_name, _finish_path = _launch_tmux_task(
                args=args,
                state_dir=state_dir,
                task=task,
                lease=lease,
                command_argv=command_argv,
                command_shell=command_shell,
            )
        except Exception as exc:
            print(f"[rclone-dist-client] launch failed task_id={task['task_id']}: {exc}", flush=True)
            try:
                _api_json(
                    args.server_url,
                    "/api/v1/release",
                    {
                        "worker_id": args.worker_id,
                        "task_id": int(task["task_id"]),
                        "note": f"tmux launch failed: {exc}",
                    },
                    timeout_seconds=args.request_timeout_seconds,
                )
            except Exception as release_exc:
                print(f"[rclone-dist-client] release after launch failure also failed: {release_exc}", flush=True)
            time.sleep(max(0.1, args.poll_seconds))
            continue
        try:
            ack_response = _api_json(
                args.server_url,
                "/api/v1/ack_launch",
                {
                    "worker_id": args.worker_id,
                    "task_id": int(task["task_id"]),
                    "session_name": session_name,
                    "command": command_argv,
                    "note": "tmux session launched",
                },
                timeout_seconds=args.request_timeout_seconds,
            )
            if not ack_response.get("ok", False):
                print(f"[rclone-dist-client] ack_launch rejected: {ack_response}", flush=True)
        except Exception as exc:
            print(f"[rclone-dist-client] ack_launch failed: {exc}", flush=True)
        print(
            f"[rclone-dist-client] launched task_id={task['task_id']} session={session_name} "
            f"peer_id={args.peer_id} path={task['relative_path']}",
            flush=True,
        )
        if args.single_shot:
            return 0
        if reported_any:
            time.sleep(0.1)


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return run_client(args)


if __name__ == "__main__":
    raise SystemExit(main())
