"""Capture the golden Silver/Gold outputs for tests/test_golden.py.

Run once from the repo root:  python tests/golden/make_golden.py

The reference outputs were produced by the former pandas stages (proven equal to
the Spark jobs by the archived `spark-engine-parity` change, max |diff| ~1e-13)
before they were replaced by the single Spark implementation (REQ-ORC-05). Re-run
only when a Silver/Gold business rule changes on purpose, and review the diff.
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))          # tests/ (for golden_data)

from golden_data import GOLDEN_GOLD, GOLDEN_SILVER, golden_cfg, land_golden_bronze  # noqa: E402

from idp.pipeline import to_gold, to_silver  # noqa: E402


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cfg = golden_cfg(Path(tmp))
        land_golden_bronze(cfg)
        to_silver(cfg)
        to_gold(cfg)
        for layer, tables in (("silver", GOLDEN_SILVER), ("gold", GOLDEN_GOLD)):
            for t in tables:
                src = Path(getattr(cfg.storage, layer)) / t
                dst = HERE / layer / t
                shutil.rmtree(dst, ignore_errors=True)
                # Parquet only: skip Spark's .crc checksums and _SUCCESS markers.
                shutil.copytree(src, dst, ignore=shutil.ignore_patterns(".*.crc", "_SUCCESS"))
                print(f"wrote {dst.relative_to(HERE.parent.parent)}")


if __name__ == "__main__":
    main()
