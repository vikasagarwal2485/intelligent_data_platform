"""Silver/Gold data transforms on Apache Spark — the single implementation
(REQ-ORC-05) shared by the CLI stages, the Kubernetes Jobs, Airflow and the tests.

Modules:

* ``session`` — SparkSession for a platform config (master, memory, S3 access)
* ``lake``    — layer locations and partitioned table read/write
* ``tokens``  — salted PII tokenization and id canonicalization as Spark columns
* ``silver``  — Bronze -> canonical Silver tables (pure DataFrame functions)
* ``features``— Silver -> Gold credit/fraud feature tables (pure DataFrame functions)

pyspark is imported inside functions, so importing this package (or anything that
imports it) never starts a JVM; only running a Silver/Gold stage does.
"""
