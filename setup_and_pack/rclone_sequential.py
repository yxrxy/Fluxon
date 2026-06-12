from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
import posixpath
import random
import re
import shlex
import subprocess
import threading
import time


DEFAULT_RCLONE_ARGS = [
    "-P",
    "--transfers",
    "40",
    "--checkers",
    "16",
    "--multi-thread-streams",
    "8",
    "--multi-thread-cutoff",
    "64M",
    "--buffer-size",
    "32M",
    "--size-only",
    "--max-backlog",
    "10000000",
]


@dataclass
class RunStats:
    total_tasks: int = 0
    succeeded_tasks: int = 0
    failed_tasks: int = 0


@dataclass(frozen=True)
class SequentialTask:
    task_id: int
    src: str
    dst: str
    label: str


@dataclass(frozen=True)
class RelativeTask:
    task_id: int
    relative_path: str


@dataclass(frozen=True)
class TaskExecutionResult:
    task: SequentialTask
    command_argv: list[str]
    return_code: int
    elapsed_seconds: float
    error_message: str = ""


class RollingWindowLog:
    def __init__(self, path: Path, *, max_lines: int) -> None:
        self._path = path
        self._max_lines = max(1, max_lines)
        self._lines: deque[str] = deque(maxlen=self._max_lines)
        self._lock = threading.Lock()
        if self._path.exists():
            existing = self._path.read_text(encoding="utf-8", errors="replace").splitlines()
            self._lines.extend(existing[-self._max_lines :])
        self._rewrite()

    def _rewrite(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_name(self._path.name + ".tmp")
        text = ""
        if self._lines:
            text = "\n".join(self._lines) + "\n"
        tmp_path.write_text(text, encoding="utf-8")
        tmp_path.replace(self._path)

    def write_line(self, line: str) -> None:
        sanitized = line.rstrip("\n")
        with self._lock:
            self._lines.append(sanitized)
            self._rewrite()


def _looks_like_rclone_remote(root: str) -> bool:
    if not root:
        return False
    if root.startswith(("/", "./", "../")):
        return False
    if len(root) >= 3 and root[1] == ":" and root[2] in ("/", "\\"):
        return False
    head = root.split("/", 1)[0]
    return ":" in head


def _looks_like_path_value(text: str) -> bool:
    if not text:
        return False
    if _looks_like_rclone_remote(text):
        return True
    if text.startswith(("~", "/", "./", "../")):
        return True
    if len(text) >= 3 and text[1] == ":" and text[2] in ("/", "\\"):
        return True
    return "/" in text or "\\" in text


def join_rclone_root(root: str, relative_path: str) -> str:
    rel = relative_path.strip("/")
    if not rel:
        return root
    if _looks_like_rclone_remote(root):
        remote_name, remote_path = root.split(":", 1)
        remote_path = remote_path.strip("/")
        joined = rel if not remote_path else posixpath.join(remote_path, rel)
        return f"{remote_name}:{joined}"
    return str(Path(root) / Path(*rel.split("/")))


def build_rclone_dir_command(
    *,
    rclone_bin: str,
    src_root: str,
    dst_root: str,
    relative_path: str,
    rclone_args: list[str],
) -> list[str]:
    return [
        rclone_bin,
        "copy",
        join_rclone_root(src_root, relative_path),
        join_rclone_root(dst_root, relative_path),
        *rclone_args,
    ]


def shell_join_argv(argv: list[str]) -> str:
    return shlex.join(argv)


def shuffled_tasks(tasks: list[object], seed: int | None) -> list[object]:
    out = list(tasks)
    random.Random(seed).shuffle(out)
    return out


def load_task_manifest(manifest_path: Path) -> list[RelativeTask]:
    path = manifest_path.expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"manifest_path is not a file: {path}")

    tasks: list[RelativeTask] = []
    seen_paths: set[str] = set()
    next_task_id = 1
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        normalized = line.replace("\\", "/").strip("/")
        if not normalized:
            continue
        parts = [part for part in normalized.split("/") if part and part != "."]
        if not parts:
            continue
        if any(part == ".." for part in parts):
            raise ValueError(f"manifest path must stay relative and must not contain '..': {line}")
        relative_path = "/".join(parts)
        if relative_path in seen_paths:
            continue
        seen_paths.add(relative_path)
        tasks.append(RelativeTask(task_id=next_task_id, relative_path=relative_path))
        next_task_id += 1
    return tasks


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run rclone copy tasks with configurable inflight concurrency and keep only the latest log window.",
    )
    parser.add_argument(
        "--manifest-file",
        default="",
        help="Text file containing one relative directory per line. Use together with --src-root and --dst-root.",
    )
    parser.add_argument(
        "--pair-file",
        default="",
        help=(
            "Text file containing one src/dst pair per line. Prefer TAB as delimiter: "
            "'src<TAB>dst'. Whitespace split is also accepted when the paths themselves do not contain spaces."
        ),
    )
    parser.add_argument("--src-root", default="", help="rclone source root for manifest relative paths.")
    parser.add_argument("--dst-root", default="", help="rclone destination root for manifest relative paths.")
    parser.add_argument("--rclone-bin", default="rclone", help="rclone binary path.")
    parser.add_argument(
        "--rclone-arg",
        action="append",
        default=list(DEFAULT_RCLONE_ARGS),
        help=(
            "Extra argument appended to every rclone command. Repeat this flag for multiple args. "
            "Default args already include: "
            "-P --transfers 40 --checkers 16 --multi-thread-streams 8 "
            "--multi-thread-cutoff 64M --buffer-size 32M --size-only --max-backlog 10000000"
        ),
    )
    parser.add_argument("--log-dir", required=True, help="Directory that stores runner.log and per-task log files.")
    parser.add_argument(
        "--log-window-lines",
        type=int,
        default=1000,
        help="Keep only the most recent N lines in the log file.",
    )
    parser.add_argument(
        "--shuffle-seed",
        type=int,
        default=None,
        help="Optional shuffle seed. Omit this to use manifest order.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with the next task after a failed rclone command.",
    )
    parser.add_argument(
        "--max-inflight",
        type=int,
        default=1,
        help="Maximum number of concurrent inflight rclone processes.",
    )
    return parser


def _timestamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _log_event(log: RollingWindowLog, message: str) -> None:
    line = f"{_timestamp()} {message}"
    print(line, flush=True)
    log.write_line(line)


def _sanitize_label_for_filename(label: str) -> str:
    allowed = []
    for ch in label:
        if ch.isalnum() or ch in ("-", "_", "."):
            allowed.append(ch)
        else:
            allowed.append("_")
    text = "".join(allowed).strip("_")
    if not text:
        return "task"
    return text[:80]


def _task_log_path(log_dir: Path, task: SequentialTask) -> Path:
    suffix = _sanitize_label_for_filename(task.label)
    return log_dir / f"task_{task.task_id:08d}_{suffix}.log"


def _run_logged_command(command_argv: list[str], log: RollingWindowLog) -> int:
    proc = subprocess.Popen(
        command_argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for raw_line in proc.stdout:
        log.write_line(f"{_timestamp()} [rclone] {raw_line.rstrip()}")
    proc.stdout.close()
    return int(proc.wait())


def _execute_task(task: SequentialTask, command_argv: list[str], task_log: RollingWindowLog) -> TaskExecutionResult:
    started_at = time.time()
    try:
        return_code = _run_logged_command(command_argv, task_log)
        elapsed_seconds = time.time() - started_at
        return TaskExecutionResult(
            task=task,
            command_argv=list(command_argv),
            return_code=return_code,
            elapsed_seconds=elapsed_seconds,
        )
    except Exception as exc:
        elapsed_seconds = time.time() - started_at
        task_log.write_line(f"{_timestamp()} [runner-error] {exc}")
        return TaskExecutionResult(
            task=task,
            command_argv=list(command_argv),
            return_code=255,
            elapsed_seconds=elapsed_seconds,
            error_message=str(exc),
        )


def _parse_pair_line(raw_line: str, *, line_number: int) -> tuple[str, str]:
    if "\t" in raw_line:
        parts = [part.strip() for part in raw_line.split("\t")]
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
    else:
        explicit_space_delimiters = list(re.finditer(r" {2,}", raw_line))
        if len(explicit_space_delimiters) == 1:
            match = explicit_space_delimiters[0]
            src = raw_line[: match.start()].strip()
            dst = raw_line[match.end() :].strip()
            if src and dst:
                return src, dst

        parts = raw_line.split()
        if len(parts) == 2:
            return parts[0], parts[1]

        candidates: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for match in re.finditer(r"\s+((?:[^\s/:][^\s/]*:|/|~\/|\.\/|\.\.\/|[A-Za-z]:[\\/]))", raw_line):
            dst_start = match.start(1)
            src = raw_line[:dst_start].rstrip()
            dst = raw_line[dst_start:].strip()
            candidate = (src, dst)
            if (
                src
                and dst
                and candidate not in seen
                and _looks_like_path_value(src)
                and _looks_like_path_value(dst)
            ):
                seen.add(candidate)
                candidates.append(candidate)
        if len(candidates) == 1:
            return candidates[0]

    raise ValueError(
        f"pair file line {line_number} must contain exactly one src and one dst; "
        f"prefer TAB delimiter or a single 2+ space separator when paths are complex: {raw_line!r}"
    )


def load_pair_file(pair_file_path: Path) -> list[SequentialTask]:
    path = pair_file_path.expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"pair_file is not a file: {path}")

    tasks: list[SequentialTask] = []
    next_task_id = 1
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        src, dst = _parse_pair_line(raw_line, line_number=line_number)
        tasks.append(
            SequentialTask(
                task_id=next_task_id,
                src=src,
                dst=dst,
                label=f"{src} -> {dst}",
            )
        )
        next_task_id += 1
    return tasks


def _load_sequential_tasks(args: argparse.Namespace) -> tuple[str, list[SequentialTask]]:
    has_manifest_mode = bool(args.manifest_file)
    has_pair_mode = bool(args.pair_file)
    if has_manifest_mode == has_pair_mode:
        raise ValueError("choose exactly one input mode: either --pair-file or --manifest-file")

    if has_pair_mode:
        if args.src_root or args.dst_root:
            raise ValueError("--src-root/--dst-root must not be set when using --pair-file")
        pair_file_path = Path(args.pair_file).expanduser()
        tasks = load_pair_file(pair_file_path)
        if args.shuffle_seed is not None:
            import random

            out = list(tasks)
            random.Random(args.shuffle_seed).shuffle(out)
            tasks = out
        return f"pair_file={pair_file_path.resolve()}", tasks

    if not args.src_root or not args.dst_root:
        raise ValueError("--src-root and --dst-root are required when using --manifest-file")
    manifest_path = Path(args.manifest_file).expanduser()
    manifest_tasks = load_task_manifest(manifest_path)
    if args.shuffle_seed is not None:
        manifest_tasks = shuffled_tasks(manifest_tasks, args.shuffle_seed)
    tasks = [
        SequentialTask(
            task_id=task.task_id,
            src="",
            dst="",
            label=task.relative_path,
        )
        for task in manifest_tasks
    ]
    return f"manifest={manifest_path.resolve()} src_root={args.src_root} dst_root={args.dst_root}", tasks


def run_sequence(args: argparse.Namespace) -> int:
    if args.max_inflight <= 0:
        raise ValueError("--max-inflight must be >= 1")

    log_dir = Path(args.log_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    runner_log_path = log_dir / "runner.log"
    input_desc, tasks = _load_sequential_tasks(args)
    log = RollingWindowLog(runner_log_path, max_lines=args.log_window_lines)

    stats = RunStats(total_tasks=len(tasks))
    _log_event(
        log,
        f"[start] {input_desc} total_tasks={stats.total_tasks} max_inflight={args.max_inflight}",
    )
    stop_scheduling = False
    next_index = 0
    active: dict[Future[TaskExecutionResult], tuple[int, SequentialTask, str, RollingWindowLog]] = {}
    with ThreadPoolExecutor(max_workers=args.max_inflight) as executor:
        while next_index < len(tasks) or active:
            while not stop_scheduling and next_index < len(tasks) and len(active) < args.max_inflight:
                task = tasks[next_index]
                task_index = next_index + 1
                next_index += 1
                task_log = RollingWindowLog(_task_log_path(log_dir, task), max_lines=args.log_window_lines)
                if args.pair_file:
                    command_argv = [args.rclone_bin, "copy", task.src, task.dst, *list(args.rclone_arg)]
                    path_label = task.label
                else:
                    command_argv = build_rclone_dir_command(
                        rclone_bin=args.rclone_bin,
                        src_root=args.src_root,
                        dst_root=args.dst_root,
                        relative_path=task.label,
                        rclone_args=list(args.rclone_arg),
                    )
                    path_label = task.label
                command_shell = shell_join_argv(command_argv)
                _log_event(
                    log,
                    f"[task-start] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                    f"path={path_label} cmd={command_shell}",
                )
                task_log.write_line(
                    f"{_timestamp()} [task-start] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                    f"path={path_label} cmd={command_shell}"
                )
                future = executor.submit(_execute_task, task, command_argv, task_log)
                active[future] = (task_index, task, path_label, task_log)

            if not active:
                break

            done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                task_index, task, path_label, task_log = active.pop(future)
                result = future.result()
                if result.return_code == 0:
                    stats.succeeded_tasks += 1
                    _log_event(
                        log,
                        f"[task-done] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                        f"path={path_label} rc=0 elapsed_s={result.elapsed_seconds:.3f}",
                    )
                    task_log.write_line(
                        f"{_timestamp()} [task-done] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                        f"path={path_label} rc=0 elapsed_s={result.elapsed_seconds:.3f}"
                    )
                    continue

                stats.failed_tasks += 1
                note = f" error={result.error_message}" if result.error_message else ""
                _log_event(
                    log,
                    f"[task-failed] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                    f"path={path_label} rc={result.return_code} elapsed_s={result.elapsed_seconds:.3f}{note}",
                )
                task_log.write_line(
                    f"{_timestamp()} [task-failed] index={task_index}/{stats.total_tasks} task_id={task.task_id} "
                    f"path={path_label} rc={result.return_code} elapsed_s={result.elapsed_seconds:.3f}{note}"
                )
                if not args.continue_on_error:
                    stop_scheduling = True

        if stop_scheduling and stats.failed_tasks > 0:
            _log_event(
                log,
                f"[stop] stop_on_error=true succeeded={stats.succeeded_tasks} failed={stats.failed_tasks}",
            )
            return 1

    _log_event(
        log,
        f"[done] total={stats.total_tasks} succeeded={stats.succeeded_tasks} failed={stats.failed_tasks}",
    )
    return 0 if stats.failed_tasks == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return run_sequence(args)


if __name__ == "__main__":
    raise SystemExit(main())
