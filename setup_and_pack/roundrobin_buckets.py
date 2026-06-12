from __future__ import annotations

import argparse
from dataclasses import dataclass
import io
from pathlib import Path
import re
import string
from typing import Any


@dataclass(frozen=True)
class InputLineRecord:
    line: str
    source_index: int
    source_path: str
    source_name: str
    source_stem: str
    source_line_number: int
    global_index: int
    global_item_number: int


@dataclass(frozen=True)
class BucketSpec:
    tag: str
    output_file: str


# Copy this script out and edit this list directly when you want a fixed bucket layout.
# Each entry binds one bucket tag to one output file path.
DEFAULT_BUCKETS: list[BucketSpec] = []
bucketnames=["lingang14","lingang13","lingang8"]
for name in bucketnames:
    DEFAULT_BUCKETS.append(BucketSpec(tag=name, output_file=f"{name}.txt"))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read multiple input text files in round-robin order, then dispatch the extracted lines "
            "to output buckets in round-robin order and render each item through a template."
        ),
    )
    parser.add_argument(
        "--input-file",
        nargs="+",
        required=True,
        help="Input text files. One line is taken from each active file in round-robin order.",
    )
    parser.add_argument(
        "--output-file",
        nargs="+",
        default=[],
        help="Output bucket files. Rendered items are distributed to these files in round-robin order.",
    )
    parser.add_argument(
        "--bucket-tag",
        nargs="+",
        default=[],
        help=(
            "Optional tags bound to output buckets one-by-one. "
            "If omitted, each bucket tag defaults to the output file stem. "
            "Ignored when DEFAULT_BUCKETS is populated and no explicit output-file override is provided."
        ),
    )
    template_group = parser.add_mutually_exclusive_group()
    template_group.add_argument(
        "--template",
        default="{line}",
        help=(
            "Inline template used to render each extracted line. "
            "Available placeholders include {line}, {source_name}, {source_line_number}, "
            "{bucket_index}, {bucket_item_number}, {global_item_number}."
        ),
    )
    template_group.add_argument(
        "--template-file",
        default="",
        help="Path to a template file. Full file content is used as the render template.",
    )
    return parser


def _load_template(args: argparse.Namespace) -> str:
    if args.template_file:
        template = Path(args.template_file).expanduser().read_text(encoding="utf-8")
    else:
        template = str(args.template)
    return re.sub(r"\{\{([A-Z0-9_]+)\}\}", r"{\1}", template)


def _resolve_bucket_specs(args: argparse.Namespace) -> list[BucketSpec]:
    if args.output_file:
        output_paths = [Path(path).expanduser() for path in args.output_file]
        if args.bucket_tag and len(args.bucket_tag) != len(output_paths):
            raise ValueError("--bucket-tag count must match --output-file count")
        bucket_tags = list(args.bucket_tag) if args.bucket_tag else [output_path.stem for output_path in output_paths]
        return [
            BucketSpec(tag=bucket_tags[index], output_file=str(output_paths[index]))
            for index in range(len(output_paths))
        ]

    if not DEFAULT_BUCKETS:
        raise ValueError("no buckets configured: set DEFAULT_BUCKETS in the script or pass --output-file")
    if args.bucket_tag:
        raise ValueError("--bucket-tag requires --output-file override")
    return list(DEFAULT_BUCKETS)


def _iter_round_robin_lines(input_paths: list[Path]) -> list[InputLineRecord]:
    active: list[dict[str, Any]] = []
    for source_index, input_path in enumerate(input_paths):
        handle = input_path.open("r", encoding="utf-8", errors="replace")
        active.append(
            {
                "source_index": source_index,
                "path": input_path,
                "handle": handle,
                "next_line_number": 1,
            }
        )

    out: list[InputLineRecord] = []
    cursor = 0
    global_index = 0
    try:
        while active:
            state = active[cursor]
            handle = state["handle"]
            assert isinstance(handle, io.TextIOBase)
            raw_line = handle.readline()
            if raw_line == "":
                handle.close()
                active.pop(cursor)
                if not active:
                    break
                if cursor >= len(active):
                    cursor = 0
                continue

            source_path = state["path"]
            assert isinstance(source_path, Path)
            source_line_number = int(state["next_line_number"])
            state["next_line_number"] = source_line_number + 1
            out.append(
                InputLineRecord(
                    line=raw_line.rstrip("\r\n"),
                    source_index=int(state["source_index"]),
                    source_path=str(source_path),
                    source_name=source_path.name,
                    source_stem=source_path.stem,
                    source_line_number=source_line_number,
                    global_index=global_index,
                    global_item_number=global_index + 1,
                )
            )
            global_index += 1
            cursor = (cursor + 1) % len(active)
    finally:
        for state in active:
            handle = state["handle"]
            assert isinstance(handle, io.TextIOBase)
            handle.close()
    return out


def _validate_placeholders(template: str, allowed_keys: set[str]) -> None:
    formatter = string.Formatter()
    unknown_fields = sorted(
        {
            field_name
            for _, field_name, _, _ in formatter.parse(template)
            if field_name and field_name not in allowed_keys
        }
    )
    if unknown_fields:
        raise ValueError(
            "unknown template placeholder(s): "
            + ", ".join(unknown_fields)
            + ". allowed placeholders: "
            + ", ".join(sorted(allowed_keys))
        )


def run_roundrobin(args: argparse.Namespace) -> int:
    input_paths = [Path(path).expanduser() for path in args.input_file]
    bucket_specs = _resolve_bucket_specs(args)
    output_paths = [Path(bucket.output_file).expanduser() for bucket in bucket_specs]
    if not output_paths:
        raise ValueError("at least one output file is required")
    for input_path in input_paths:
        if not input_path.is_file():
            raise ValueError(f"input_file is not a file: {input_path}")

    template = _load_template(args)
    bucket_tags = [bucket.tag for bucket in bucket_specs]
    allowed_keys = {
        "line",
        "CONTENT",
        "source_index",
        "source_path",
        "source_name",
        "source_stem",
        "source_line_number",
        "global_index",
        "global_item_number",
        "bucket_index",
        "bucket_number",
        "bucket_item_index",
        "bucket_item_number",
        "bucket_path",
        "bucket_name",
        "bucket_stem",
        "bucket_tag",
        "BUCKET_TAG",
    }
    _validate_placeholders(template, allowed_keys)

    records = _iter_round_robin_lines(input_paths)
    bucket_item_counts = [0 for _ in output_paths]

    for output_path in output_paths:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    handles = [output_path.open("w", encoding="utf-8") for output_path in output_paths]
    try:
        for record in records:
            bucket_index = record.global_index % len(output_paths)
            bucket_item_index = bucket_item_counts[bucket_index]
            bucket_item_counts[bucket_index] += 1
            output_path = output_paths[bucket_index]
            bucket_tag = bucket_tags[bucket_index]
            rendered = template.format(
                line=record.line,
                CONTENT=record.line,
                source_index=record.source_index,
                source_path=record.source_path,
                source_name=record.source_name,
                source_stem=record.source_stem,
                source_line_number=record.source_line_number,
                global_index=record.global_index,
                global_item_number=record.global_item_number,
                bucket_index=bucket_index,
                bucket_number=bucket_index + 1,
                bucket_item_index=bucket_item_index,
                bucket_item_number=bucket_item_index + 1,
                bucket_path=str(output_path),
                bucket_name=output_path.name,
                bucket_stem=output_path.stem,
                bucket_tag=bucket_tag,
                BUCKET_TAG=bucket_tag,
            )
            if rendered.endswith("\n"):
                handles[bucket_index].write(rendered)
            else:
                handles[bucket_index].write(rendered + "\n")
    finally:
        for handle in handles:
            handle.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return run_roundrobin(args)


if __name__ == "__main__":
    raise SystemExit(main())
