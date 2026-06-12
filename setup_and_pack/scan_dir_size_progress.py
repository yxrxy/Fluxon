from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import queue
import threading
import time


@dataclass(frozen=True)
class RunningTask:
    path: str
    started_at_unix: float


@dataclass(frozen=True)
class ScanSummary:
    root: str
    total_bytes: int
    elapsed_seconds: float
    files: int
    directories: int
    symlinks: int
    other_nodes: int
    completed_directories: int
    errors: int
    skipped_mounts: int


def _format_bytes(num_bytes: int) -> str:
    negative = num_bytes < 0
    value = float(abs(num_bytes))
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]
    unit = units[0]
    for candidate in units:
        unit = candidate
        if value < 1024.0 or candidate == units[-1]:
            break
        value /= 1024.0
    prefix = "-" if negative else ""
    if unit == "B":
        return f"{prefix}{int(value)} {unit}"
    return f"{prefix}{value:.2f} {unit}"


def _stat_size_bytes(stat_result: os.stat_result, *, apparent_size: bool) -> int:
    if apparent_size:
        return int(stat_result.st_size)
    st_blocks = getattr(stat_result, "st_blocks", None)
    if st_blocks is not None:
        return int(st_blocks) * 512
    return int(stat_result.st_size)


class DirectorySizeScanner:
    def __init__(
        self,
        root: Path,
        *,
        threads: int,
        apparent_size: bool,
        one_file_system: bool,
        progress_interval_seconds: float,
        show_running_limit: int,
    ) -> None:
        self._root = root
        self._threads = max(1, threads)
        self._apparent_size = apparent_size
        self._one_file_system = one_file_system
        self._progress_interval_seconds = max(0.2, progress_interval_seconds)
        self._show_running_limit = max(1, show_running_limit)

        self._lock = threading.Lock()
        self._work_queue: queue.Queue[Path | None] = queue.Queue()
        self._root_dev: int | None = None

        self._seen_directories: set[tuple[int, int]] = set()
        self._seen_hardlinks: set[tuple[int, int]] = set()
        self._running_tasks: dict[str, RunningTask] = {}
        self._error_samples: list[str] = []

        self._total_bytes = 0
        self._files = 0
        self._directories = 0
        self._symlinks = 0
        self._other_nodes = 0
        self._completed_directories = 0
        self._errors = 0
        self._skipped_mounts = 0

    def _record_error(self, message: str) -> None:
        with self._lock:
            self._errors += 1
            if len(self._error_samples) < 16:
                self._error_samples.append(message)

    def _track_non_directory_inode(self, stat_result: os.stat_result) -> bool:
        # Only entries with multiple links can appear more than once in the tree.
        if int(getattr(stat_result, "st_nlink", 1)) <= 1:
            return True
        inode_key = (int(stat_result.st_dev), int(stat_result.st_ino))
        with self._lock:
            if inode_key in self._seen_hardlinks:
                return False
            self._seen_hardlinks.add(inode_key)
            return True

    def _track_directory_inode(self, stat_result: os.stat_result) -> bool:
        inode_key = (int(stat_result.st_dev), int(stat_result.st_ino))
        with self._lock:
            if inode_key in self._seen_directories:
                return False
            self._seen_directories.add(inode_key)
            return True

    def _add_file_like_entry(self, stat_result: os.stat_result, *, symlink: bool, other: bool) -> None:
        if not self._track_non_directory_inode(stat_result):
            return
        size_bytes = _stat_size_bytes(stat_result, apparent_size=self._apparent_size)
        with self._lock:
            self._total_bytes += size_bytes
            if symlink:
                self._symlinks += 1
            elif other:
                self._other_nodes += 1
            else:
                self._files += 1

    def _enqueue_directory(self, directory_path: Path, stat_result: os.stat_result) -> None:
        if not self._track_directory_inode(stat_result):
            return
        size_bytes = _stat_size_bytes(stat_result, apparent_size=self._apparent_size)
        with self._lock:
            self._total_bytes += size_bytes
            self._directories += 1
        self._work_queue.put(directory_path)

    def _entry_within_device(self, stat_result: os.stat_result) -> bool:
        if not self._one_file_system or self._root_dev is None:
            return True
        return int(stat_result.st_dev) == self._root_dev

    def _scan_directory(self, directory_path: Path) -> None:
        worker_name = threading.current_thread().name
        with self._lock:
            self._running_tasks[worker_name] = RunningTask(
                path=str(directory_path),
                started_at_unix=time.time(),
            )

        try:
            with os.scandir(directory_path) as it:
                for entry in it:
                    try:
                        stat_result = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        self._record_error(f"stat failed: path={entry.path} err={exc}")
                        continue

                    if not self._entry_within_device(stat_result):
                        with self._lock:
                            self._skipped_mounts += 1
                        continue

                    if entry.is_dir(follow_symlinks=False):
                        self._enqueue_directory(Path(entry.path), stat_result)
                        continue
                    if entry.is_file(follow_symlinks=False):
                        self._add_file_like_entry(stat_result, symlink=False, other=False)
                        continue
                    if entry.is_symlink():
                        self._add_file_like_entry(stat_result, symlink=True, other=False)
                        continue
                    self._add_file_like_entry(stat_result, symlink=False, other=True)
        except OSError as exc:
            self._record_error(f"scandir failed: path={directory_path} err={exc}")
        finally:
            with self._lock:
                self._completed_directories += 1
                self._running_tasks.pop(worker_name, None)

    def _worker_main(self) -> None:
        while True:
            directory_path = self._work_queue.get()
            try:
                if directory_path is None:
                    return
                self._scan_directory(directory_path)
            finally:
                self._work_queue.task_done()

    def _snapshot(self) -> tuple[int, int, int, int, int, int, int, int, int, list[RunningTask], list[str]]:
        with self._lock:
            return (
                self._total_bytes,
                self._files,
                self._directories,
                self._symlinks,
                self._other_nodes,
                self._completed_directories,
                self._errors,
                self._skipped_mounts,
                self._work_queue.qsize(),
                list(self._running_tasks.values()),
                list(self._error_samples),
            )

    def _print_progress(self, *, start_unix: float, final: bool) -> None:
        (
            total_bytes,
            files,
            directories,
            symlinks,
            other_nodes,
            completed_directories,
            errors,
            skipped_mounts,
            queued_directories,
            running_tasks,
            error_samples,
        ) = self._snapshot()
        elapsed = max(0.001, time.time() - start_unix)
        rate = total_bytes / elapsed
        state = "final" if final else "progress"
        print(
            f"[{state}] elapsed={elapsed:.1f}s total={total_bytes}B ({_format_bytes(total_bytes)}) "
            f"rate={_format_bytes(int(rate))}/s dirs_done={completed_directories} dirs_seen={directories} "
            f"queued={queued_directories} running={len(running_tasks)} files={files} symlinks={symlinks} "
            f"other={other_nodes} errors={errors} skipped_xdev={skipped_mounts}",
            flush=True,
        )
        if running_tasks:
            now_unix = time.time()
            slowest = sorted(
                running_tasks,
                key=lambda item: item.started_at_unix,
            )[: self._show_running_limit]
            detail = ", ".join(
                f"{item.path} ({now_unix - item.started_at_unix:.1f}s)"
                for item in slowest
            )
            print(f"[{state}] running: {detail}", flush=True)
        if final and error_samples:
            for sample in error_samples:
                print(f"[final] error-sample: {sample}", flush=True)

    def scan(self) -> ScanSummary:
        root_stat = self._root.lstat()
        self._root_dev = int(root_stat.st_dev)

        if self._root.is_file() or self._root.is_symlink():
            total_bytes = _stat_size_bytes(root_stat, apparent_size=self._apparent_size)
            files = 0
            symlinks = 0
            other_nodes = 0
            if self._root.is_symlink():
                symlinks = 1
            elif self._root.is_file():
                files = 1
            else:
                other_nodes = 1
            return ScanSummary(
                root=str(self._root),
                total_bytes=total_bytes,
                elapsed_seconds=0.0,
                files=files,
                directories=0,
                symlinks=symlinks,
                other_nodes=other_nodes,
                completed_directories=0,
                errors=0,
                skipped_mounts=0,
            )

        self._enqueue_directory(self._root, root_stat)

        start_unix = time.time()
        workers = [
            threading.Thread(
                target=self._worker_main,
                name=f"scan-worker-{index:02d}",
                daemon=True,
            )
            for index in range(self._threads)
        ]
        for worker in workers:
            worker.start()

        last_progress_unix = 0.0
        while True:
            now_unix = time.time()
            if now_unix - last_progress_unix >= self._progress_interval_seconds:
                self._print_progress(start_unix=start_unix, final=False)
                last_progress_unix = now_unix
            if self._work_queue.unfinished_tasks == 0:
                break
            time.sleep(0.2)

        self._work_queue.join()
        for _ in workers:
            self._work_queue.put(None)
        self._work_queue.join()
        for worker in workers:
            worker.join()

        elapsed = time.time() - start_unix
        self._print_progress(start_unix=start_unix, final=True)
        total_bytes, files, directories, symlinks, other_nodes, completed_directories, errors, skipped_mounts, _, _, _ = (
            self._snapshot()
        )
        return ScanSummary(
            root=str(self._root),
            total_bytes=total_bytes,
            elapsed_seconds=elapsed,
            files=files,
            directories=directories,
            symlinks=symlinks,
            other_nodes=other_nodes,
            completed_directories=completed_directories,
            errors=errors,
            skipped_mounts=skipped_mounts,
        )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan a directory tree and print real-time progress while computing total size.",
    )
    parser.add_argument("path", help="Directory or file to scan.")
    parser.add_argument(
        "--threads",
        type=int,
        default=min(32, max(4, (os.cpu_count() or 8) * 2)),
        help="Number of scanning threads.",
    )
    parser.add_argument(
        "--apparent-size",
        action="store_true",
        help="Use logical size (st_size) instead of allocated blocks.",
    )
    parser.add_argument(
        "--cross-file-system",
        action="store_true",
        help="Cross mount points instead of staying within the root filesystem.",
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=2.0,
        help="Seconds between progress prints.",
    )
    parser.add_argument(
        "--show-running",
        type=int,
        default=8,
        help="How many currently running directories to show in progress output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    root = Path(args.path).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"missing path: {root}")

    scanner = DirectorySizeScanner(
        root,
        threads=args.threads,
        apparent_size=args.apparent_size,
        one_file_system=not args.cross_file_system,
        progress_interval_seconds=args.progress_interval,
        show_running_limit=args.show_running,
    )
    summary = scanner.scan()
    print(
        f"SCAN_DONE path={summary.root} total_bytes={summary.total_bytes} "
        f"total_human={_format_bytes(summary.total_bytes)} elapsed_s={summary.elapsed_seconds:.3f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
