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
        # The lake tables are Iceberg (read through the catalog); the golden copies are
        # plain Parquet, partitioned by tenant, which tests/test_golden.py reads back.
        from idp.common import catalog
        for layer, tables in (("silver", GOLDEN_SILVER), ("gold", GOLDEN_GOLD)):
            for t in tables:
                df = catalog.read_table(cfg, layer, t)
                dst = HERE / layer / t
                shutil.rmtree(dst, ignore_errors=True)
                df.to_parquet(dst, partition_cols=["tenant_id"], index=False)
                print(f"wrote {dst.relative_to(HERE.parent.parent)} ({len(df)} rows)")


if __name__ == "__main__":
    main()
