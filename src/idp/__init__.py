"""Intelligent Data Platform (IDP).

A telecom data-lakehouse prototype implementing the medallion architecture
(Bronze -> Silver -> Gold) plus two ML use cases:

* Alternative credit-limit scoring (``idp.ml.credit_model``)
* Fraud detection (``idp.ml.fraud_model``)

See ``docs/architecture_and_implementation_plan.md`` for the full design.

Silver and Gold use Spark in local tests and in Kubernetes. Bronze, Silver and
Gold are Iceberg tables; pandas and scikit-learn serve the bounded ML workloads.
See ``docs/decisions/decision-log.md`` for the current architecture decisions.
"""

__version__ = "0.1.0"
