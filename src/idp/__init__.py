"""Intelligent Data Platform (IDP).

A telecom data-lakehouse prototype implementing the medallion architecture
(Bronze -> Silver -> Gold) plus two ML use cases:

* Alternative credit-limit scoring (``idp.ml.credit_model``)
* Fraud detection (``idp.ml.fraud_model``)

See ``docs/architecture_and_implementation_plan.md`` for the full design.

The prototype uses pandas as the compute engine so the full pipeline is unit
testable without a Spark/Kafka cluster. The production engines (Spark, Flink,
Kafka, Iceberg) are deployed via ``deploy/`` and documented in the plan; the
Python modules here encode the *same* canonical logic.
"""

__version__ = "0.1.0"
