#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from _common import REPO_ROOT, call


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]

BYTE_BENCH_PATH = "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench.py"
VIDEO_BENCH_PATH = "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench2.py"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flat index entry for CI-sized MPMC benchmark-style tests."
    )
    parser.add_argument("--python", default=os.environ.get("PYTHON", sys.executable))
    parser.add_argument(
        "--bench",
        choices=("all", "bytes", "video"),
        default="all",
        help="Select which benchmark script to run.",
    )
    parser.add_argument("--duration-seconds", type=int, default=10)
    parser.add_argument("--sample-start-seconds", type=int, default=2)
    parser.add_argument("--sample-duration-seconds", type=int, default=5)
    parser.add_argument("--producer-count", type=int, default=2)
    parser.add_argument("--consumer-counts", default="1,2")
    parser.add_argument("--payload-bytes", type=int, default=1048576)
    parser.add_argument("--video-messages-per-producer", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--channel-capacity", type=int, default=64)
    return parser


def _run_bytes_bench(args: argparse.Namespace) -> int:
    return call(
        [
            args.python,
            "-u",
            str(REPO_ROOT / BYTE_BENCH_PATH),
            "main",
            "--duration-seconds",
            str(args.duration_seconds),
            "--sample-start-seconds",
            str(args.sample_start_seconds),
            "--sample-duration-seconds",
            str(args.sample_duration_seconds),
            "--producer-count",
            str(args.producer_count),
            "--consumer-counts",
            str(args.consumer_counts),
            "--payload-bytes",
            str(args.payload_bytes),
            "--batch-size",
            str(args.batch_size),
            "--channel-capacity",
            str(args.channel_capacity),
        ]
    )


def _run_video_bench(args: argparse.Namespace) -> int:
    return call(
        [
            args.python,
            "-u",
            str(REPO_ROOT / VIDEO_BENCH_PATH),
            "main",
            "--producer-count",
            str(args.producer_count),
            "--video-messages-per-producer",
            str(args.video_messages_per_producer),
            "--batch-size",
            str(args.batch_size),
            "--channel-capacity",
            str(args.channel_capacity),
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    if args.bench in ("all", "bytes"):
        rc = _run_bytes_bench(args)
        if rc != 0:
            return rc
    if args.bench in ("all", "video"):
        rc = _run_video_bench(args)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
