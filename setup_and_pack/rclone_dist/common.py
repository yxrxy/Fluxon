from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
import posixpath
import random
import shlex
import threading
import time
from typing import Any


TASK_KIND_DIR = "dir"


@dataclass(frozen=True)
class TaskSpec:
    task_id: int
    relative_path: str
    kind: str = TASK_KIND_DIR

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LeaseRecord:
    task_id: int
    worker_id: str
    peer_id: str
    attempt: int
    leased_at_unix: float
    lease_deadline_unix: float
    launched_at_unix: float | None = None
    session_name: str = ""
    command: list[str] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TaskResultRecord:
    task_id: int
    worker_id: str
    peer_id: str
    attempt: int
    status: str
    return_code: int | None
    leased_at_unix: float
    launched_at_unix: float | None
    finished_at_unix: float
    elapsed_seconds: float | None
    session_name: str = ""
    command: list[str] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BandwidthSample:
    peer_id: str
    worker_id: str
    observed_at_unix: float
    downlink_bps: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_task_manifest(manifest_path: Path) -> list[TaskSpec]:
    path = manifest_path.expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"manifest_path is not a file: {path}")

    tasks: list[TaskSpec] = []
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
        tasks.append(TaskSpec(task_id=next_task_id, relative_path=relative_path))
        next_task_id += 1
    return tasks


def shuffled_tasks(tasks: list[TaskSpec], seed: int | None) -> list[TaskSpec]:
    out = list(tasks)
    rng = random.Random(seed)
    rng.shuffle(out)
    return out


def _looks_like_rclone_remote(root: str) -> bool:
    if not root:
        return False
    if root.startswith(("/", "./", "../")):
        return False
    if len(root) >= 3 and root[1] == ":" and root[2] in ("/", "\\"):
        return False
    head = root.split("/", 1)[0]
    return ":" in head


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
    src = join_rclone_root(src_root, relative_path)
    dst = join_rclone_root(dst_root, relative_path)
    return [rclone_bin, "copy", src, dst, *rclone_args]


def shell_join_argv(argv: list[str]) -> str:
    return shlex.join(argv)


class TaskStore:
    def __init__(
        self,
        tasks: list[TaskSpec],
        *,
        max_attempts: int,
        low_bandwidth_threshold_bps: float,
        bandwidth_sustain_seconds: float,
        min_bandwidth_samples: int,
    ) -> None:
        self._tasks_by_id: dict[int, TaskSpec] = {task.task_id: task for task in tasks}
        self._pending: deque[int] = deque(task.task_id for task in tasks)
        self._leases: dict[int, LeaseRecord] = {}
        self._results: dict[int, TaskResultRecord] = {}
        self._permanent_failures: dict[int, TaskResultRecord] = {}
        self._attempts: dict[int, int] = {task.task_id: 0 for task in tasks}
        self._bandwidth_history: dict[str, deque[BandwidthSample]] = defaultdict(deque)
        self._max_attempts = max_attempts
        self._low_bandwidth_threshold_bps = float(low_bandwidth_threshold_bps)
        self._bandwidth_sustain_seconds = float(bandwidth_sustain_seconds)
        self._min_bandwidth_samples = int(min_bandwidth_samples)
        self._lock = threading.Lock()

    def _all_done_locked(self) -> bool:
        return not self._pending and not self._leases

    def _trim_bandwidth_locked(self, peer_id: str, now_unix: float) -> None:
        history = self._bandwidth_history[peer_id]
        keep_after = now_unix - max(self._bandwidth_sustain_seconds * 4.0, 120.0)
        while history and history[0].observed_at_unix < keep_after:
            history.popleft()

    def _record_bandwidth_locked(
        self,
        *,
        peer_id: str,
        worker_id: str,
        downlink_bps: float | None,
        observed_at_unix: float,
    ) -> None:
        if downlink_bps is None:
            return
        history = self._bandwidth_history[peer_id]
        history.append(
            BandwidthSample(
                peer_id=peer_id,
                worker_id=worker_id,
                observed_at_unix=observed_at_unix,
                downlink_bps=float(downlink_bps),
            )
        )
        self._trim_bandwidth_locked(peer_id, observed_at_unix)

    def _bandwidth_gate_locked(self, peer_id: str, *, now_unix: float) -> dict[str, Any]:
        self._trim_bandwidth_locked(peer_id, now_unix)
        history = self._bandwidth_history.get(peer_id, deque())
        cutoff = now_unix - self._bandwidth_sustain_seconds
        recent = [sample for sample in history if sample.observed_at_unix >= cutoff]
        if not recent:
            return {
                "allowed": False,
                "reason": "missing_bandwidth_samples",
                "sample_count": 0,
                "window_span_seconds": 0.0,
                "latest_downlink_bps": None,
                "max_downlink_bps": None,
                "threshold_bps": self._low_bandwidth_threshold_bps,
                "sustain_seconds": self._bandwidth_sustain_seconds,
                "min_samples": self._min_bandwidth_samples,
            }
        window_span_seconds = max(0.0, recent[-1].observed_at_unix - recent[0].observed_at_unix)
        latest_downlink_bps = recent[-1].downlink_bps
        max_downlink_bps = max(sample.downlink_bps for sample in recent)
        if len(recent) < self._min_bandwidth_samples:
            reason = "insufficient_bandwidth_samples"
            allowed = False
        elif recent[0].observed_at_unix > cutoff:
            reason = "bandwidth_window_not_ready"
            allowed = False
        elif max_downlink_bps >= self._low_bandwidth_threshold_bps:
            reason = "bandwidth_too_high"
            allowed = False
        else:
            reason = "ok"
            allowed = True
        return {
            "allowed": allowed,
            "reason": reason,
            "sample_count": len(recent),
            "window_span_seconds": window_span_seconds,
            "latest_downlink_bps": latest_downlink_bps,
            "max_downlink_bps": max_downlink_bps,
            "threshold_bps": self._low_bandwidth_threshold_bps,
            "sustain_seconds": self._bandwidth_sustain_seconds,
            "min_samples": self._min_bandwidth_samples,
        }

    def _reap_expired_locked(self, now_unix: float) -> None:
        expired_ids = [
            task_id
            for task_id, lease in self._leases.items()
            if lease.lease_deadline_unix <= now_unix
        ]
        for task_id in expired_ids:
            self._pending.appendleft(task_id)
            del self._leases[task_id]

    def acquire(
        self,
        *,
        worker_id: str,
        peer_id: str,
        lease_seconds: int,
        downlink_bps: float | None,
    ) -> tuple[TaskSpec | None, LeaseRecord | None, bool, str, dict[str, Any]]:
        now_unix = time.time()
        with self._lock:
            self._reap_expired_locked(now_unix)
            self._record_bandwidth_locked(
                peer_id=peer_id,
                worker_id=worker_id,
                downlink_bps=downlink_bps,
                observed_at_unix=now_unix,
            )
            gate = self._bandwidth_gate_locked(peer_id, now_unix=now_unix)
            if not gate["allowed"]:
                return None, None, self._all_done_locked(), str(gate["reason"]), gate
            while self._pending:
                task_id = self._pending.popleft()
                if task_id in self._results or task_id in self._permanent_failures:
                    continue
                attempt = self._attempts[task_id] + 1
                self._attempts[task_id] = attempt
                lease = LeaseRecord(
                    task_id=task_id,
                    worker_id=worker_id,
                    peer_id=peer_id,
                    attempt=attempt,
                    leased_at_unix=now_unix,
                    lease_deadline_unix=now_unix + max(1, lease_seconds),
                )
                self._leases[task_id] = lease
                return self._tasks_by_id[task_id], lease, False, "granted", gate
            return None, None, self._all_done_locked(), "no_pending_tasks", gate

    def ack_launch(
        self,
        *,
        task_id: int,
        worker_id: str,
        session_name: str,
        command: list[str],
        note: str,
    ) -> tuple[bool, str]:
        with self._lock:
            lease = self._leases.get(task_id)
            if lease is None:
                return False, f"task {task_id} is not currently leased"
            if lease.worker_id != worker_id:
                return False, f"task {task_id} is leased by {lease.worker_id}, not {worker_id}"
            lease.launched_at_unix = time.time()
            lease.session_name = session_name
            lease.command = list(command)
            lease.note = note
            return True, "launch recorded"

    def heartbeat(
        self,
        *,
        task_id: int,
        worker_id: str,
        peer_id: str,
        lease_seconds: int,
        downlink_bps: float | None,
    ) -> tuple[bool, str]:
        now_unix = time.time()
        with self._lock:
            self._reap_expired_locked(now_unix)
            self._record_bandwidth_locked(
                peer_id=peer_id,
                worker_id=worker_id,
                downlink_bps=downlink_bps,
                observed_at_unix=now_unix,
            )
            lease = self._leases.get(task_id)
            if lease is None:
                return False, f"task {task_id} is not currently leased"
            if lease.worker_id != worker_id:
                return False, f"task {task_id} is leased by {lease.worker_id}, not {worker_id}"
            lease.lease_deadline_unix = now_unix + max(1, lease_seconds)
            return True, "lease extended"

    def release(
        self,
        *,
        task_id: int,
        worker_id: str,
        note: str,
    ) -> tuple[bool, str]:
        with self._lock:
            lease = self._leases.get(task_id)
            if lease is None:
                return False, f"task {task_id} is not currently leased"
            if lease.worker_id != worker_id:
                return False, f"task {task_id} is leased by {lease.worker_id}, not {worker_id}"
            del self._leases[task_id]
            self._pending.appendleft(task_id)
            return True, note or "task requeued"

    def report_finish(
        self,
        *,
        task_id: int,
        worker_id: str,
        peer_id: str,
        return_code: int | None,
        finished_at_unix: float | None,
        elapsed_seconds: float | None,
        session_name: str,
        command: list[str],
        note: str,
    ) -> tuple[bool, str]:
        finish_unix = finished_at_unix if finished_at_unix is not None else time.time()
        with self._lock:
            lease = self._leases.get(task_id)
            if lease is None:
                return False, f"task {task_id} is not currently leased"
            if lease.worker_id != worker_id:
                return False, f"task {task_id} is leased by {lease.worker_id}, not {worker_id}"
            del self._leases[task_id]

            result = TaskResultRecord(
                task_id=task_id,
                worker_id=worker_id,
                peer_id=peer_id,
                attempt=lease.attempt,
                status="success" if return_code == 0 else "failure",
                return_code=return_code,
                leased_at_unix=lease.leased_at_unix,
                launched_at_unix=lease.launched_at_unix,
                finished_at_unix=finish_unix,
                elapsed_seconds=elapsed_seconds,
                session_name=session_name or lease.session_name,
                command=list(command) if command else list(lease.command),
                note=note,
            )
            if return_code == 0:
                self._results[task_id] = result
                return True, "recorded success"
            if lease.attempt >= self._max_attempts:
                self._permanent_failures[task_id] = result
                return True, "recorded permanent failure"
            self._pending.append(task_id)
            return True, "requeued after failure"

    def status_snapshot(self) -> dict[str, Any]:
        now_unix = time.time()
        with self._lock:
            self._reap_expired_locked(now_unix)
            peers = {}
            for peer_id in sorted(self._bandwidth_history.keys()):
                gate = self._bandwidth_gate_locked(peer_id, now_unix=now_unix)
                recent_samples = list(self._bandwidth_history[peer_id])[-8:]
                peers[peer_id] = {
                    **gate,
                    "recent_samples": [sample.to_dict() for sample in recent_samples],
                }
            return {
                "total_tasks": len(self._tasks_by_id),
                "pending_tasks": len(self._pending),
                "leased_tasks": len(self._leases),
                "succeeded_tasks": len(self._results),
                "failed_tasks": len(self._permanent_failures),
                "all_done": self._all_done_locked(),
                "max_attempts": self._max_attempts,
                "low_bandwidth_threshold_bps": self._low_bandwidth_threshold_bps,
                "bandwidth_sustain_seconds": self._bandwidth_sustain_seconds,
                "min_bandwidth_samples": self._min_bandwidth_samples,
                "leases": [lease.to_dict() for lease in sorted(self._leases.values(), key=lambda item: item.task_id)],
                "permanent_failures": [
                    record.to_dict()
                    for record in sorted(self._permanent_failures.values(), key=lambda item: item.task_id)
                ],
                "peers": peers,
            }

    def manifest(self) -> list[dict[str, Any]]:
        return [task.to_dict() for task in sorted(self._tasks_by_id.values(), key=lambda item: item.task_id)]
