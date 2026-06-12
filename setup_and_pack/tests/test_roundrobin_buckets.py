from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.roundrobin_buckets import BucketSpec, build_argument_parser, run_roundrobin


class RoundRobinBucketsTest(unittest.TestCase):
    def test_default_buckets_can_drive_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            in1 = root / "a.txt"
            out1 = root / "bucket_a.txt"
            out2 = root / "bucket_b.txt"
            in1.write_text("foo\nbar\n", encoding="utf-8")

            args = build_argument_parser().parse_args(
                [
                    "--input-file",
                    str(in1),
                    "--template",
                    "{{CONTENT}} -> {BUCKET_TAG}",
                ]
            )

            with mock.patch(
                "setup_and_pack.roundrobin_buckets.DEFAULT_BUCKETS",
                [
                    BucketSpec(tag="A", output_file=str(out1)),
                    BucketSpec(tag="B", output_file=str(out2)),
                ],
            ):
                rc = run_roundrobin(args)

            self.assertEqual(rc, 0)
            self.assertEqual(out1.read_text(encoding="utf-8").splitlines(), ["foo -> A"])
            self.assertEqual(out2.read_text(encoding="utf-8").splitlines(), ["bar -> B"])

    def test_custom_bucket_tag_and_content_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            in1 = root / "a.txt"
            out1 = root / "out1.txt"
            out2 = root / "out2.txt"

            in1.write_text("foo\nbar\n", encoding="utf-8")

            args = build_argument_parser().parse_args(
                [
                    "--input-file",
                    str(in1),
                    "--output-file",
                    str(out1),
                    str(out2),
                    "--bucket-tag",
                    "hostA",
                    "hostB",
                    "--template",
                    "/nvfile-heatstorage/nvfile-coldstorage/basemodel_data2/{{CONTENT}} {BUCKET_TAG}:/data/transfer_data/aigc/basemodel_data2/{{CONTENT}}",
                ]
            )

            rc = run_roundrobin(args)

            self.assertEqual(rc, 0)
            self.assertEqual(
                out1.read_text(encoding="utf-8").splitlines(),
                [
                    "/nvfile-heatstorage/nvfile-coldstorage/basemodel_data2/foo hostA:/data/transfer_data/aigc/basemodel_data2/foo",
                ],
            )
            self.assertEqual(
                out2.read_text(encoding="utf-8").splitlines(),
                [
                    "/nvfile-heatstorage/nvfile-coldstorage/basemodel_data2/bar hostB:/data/transfer_data/aigc/basemodel_data2/bar",
                ],
            )

    def test_roundrobin_input_and_output_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            in1 = root / "a.txt"
            in2 = root / "b.txt"
            in3 = root / "c.txt"
            out1 = root / "out1.txt"
            out2 = root / "out2.txt"

            in1.write_text("a1\na2\n", encoding="utf-8")
            in2.write_text("b1\n", encoding="utf-8")
            in3.write_text("c1\nc2\nc3\n", encoding="utf-8")

            args = build_argument_parser().parse_args(
                [
                    "--input-file",
                    str(in1),
                    str(in2),
                    str(in3),
                    "--output-file",
                    str(out1),
                    str(out2),
                    "--template",
                    "{bucket_number}:{bucket_item_number}:{source_name}:{source_line_number}:{line}",
                ]
            )

            rc = run_roundrobin(args)

            self.assertEqual(rc, 0)
            self.assertEqual(
                out1.read_text(encoding="utf-8").splitlines(),
                [
                    "1:1:a.txt:1:a1",
                    "1:2:c.txt:1:c1",
                    "1:3:c.txt:2:c2",
                ],
            )
            self.assertEqual(
                out2.read_text(encoding="utf-8").splitlines(),
                [
                    "2:1:b.txt:1:b1",
                    "2:2:a.txt:2:a2",
                    "2:3:c.txt:3:c3",
                ],
            )

    def test_template_file_can_render_multiline_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            in1 = root / "a.txt"
            out1 = root / "out1.txt"
            out2 = root / "out2.txt"
            template_file = root / "template.txt"

            in1.write_text("x1\nx2\n", encoding="utf-8")
            template_file.write_text("src={source_stem}\nvalue={line}\n---\n", encoding="utf-8")

            args = build_argument_parser().parse_args(
                [
                    "--input-file",
                    str(in1),
                    "--output-file",
                    str(out1),
                    str(out2),
                    "--template-file",
                    str(template_file),
                ]
            )

            rc = run_roundrobin(args)

            self.assertEqual(rc, 0)
            self.assertEqual(out1.read_text(encoding="utf-8"), "src=a\nvalue=x1\n---\n")
            self.assertEqual(out2.read_text(encoding="utf-8"), "src=a\nvalue=x2\n---\n")


if __name__ == "__main__":
    unittest.main()
