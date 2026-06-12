from __future__ import annotations

from pathlib import Path
import sys

THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from setup_and_pack.rclone_dist.client import main


if __name__ == "__main__":
    raise SystemExit(main())
