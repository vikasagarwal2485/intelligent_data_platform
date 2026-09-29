"""Bounded, stratified training samples (REQ-ML-09).

``stratified_cap`` returns at most ``cap`` rows of a Spark DataFrame. When it has
more, it takes an exact quota from every stratum (e.g. tenant x label class),
proportional to the stratum's size, with largest-remainder rounding so the quotas
add up to exactly ``cap``. Within a stratum the rows are chosen in a random order
fixed by ``seed``. So the sample keeps each stratum's share: each tenant's
default/fraud rates stay within rounding of the full set.
"""
from __future__ import annotations

from typing import Any, Iterable


def quotas(counts: dict[Any, int], cap: int) -> dict[Any, int]:
    """Largest-remainder allocation of ``cap`` rows over strata of ``counts``."""
    total = sum(counts.values())
    if total <= cap:
        return dict(counts)
    exact = {k: n * cap / total for k, n in counts.items()}
    alloc = {k: int(v) for k, v in exact.items()}
    left = cap - sum(alloc.values())
    for k in sorted(exact, key=lambda k: (exact[k] - alloc[k], str(k)), reverse=True)[:left]:
        alloc[k] += 1
    return alloc


def stratified_cap(df: Any, strata: Iterable[str], cap: int, seed: int) -> tuple[Any, int]:
    """``df`` itself when it has at most ``cap`` rows, else an exact stratified sample
    of ``cap`` rows. Returns ``(DataFrame, rows)``."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    strata = list(strata)
    counts = {tuple(r[c] for c in strata): r["n"]
              for r in df.groupBy(*strata).agg(F.count("*").alias("n")).collect()}
    total = sum(counts.values())
    if total <= cap:
        return df, total
    alloc = quotas(counts, cap)
    spark = df.sparkSession
    quota_df = spark.createDataFrame([(*k, n) for k, n in alloc.items()],
                                     [*strata, "_quota"])
    rn = F.row_number().over(Window.partitionBy(*strata).orderBy(F.rand(seed)))
    sample = (df.join(F.broadcast(quota_df), on=strata)
                .withColumn("_rn", rn).where(F.col("_rn") <= F.col("_quota"))
                .drop("_rn", "_quota"))
    return sample, cap
