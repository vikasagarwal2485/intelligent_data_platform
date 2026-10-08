"""Intelligent Data Platform (IDP).

A multi-tenant telecom data lakehouse implementing the medallion architecture
(Bronze -> Silver -> Gold) plus two ML use cases:

* Alternative credit-limit scoring (``idp.ml.credit_model``)
* Fraud detection (``idp.ml.fraud_model``)

Silver and Gold run on Apache Spark (``idp.transforms``) over Iceberg tables;
scikit-learn trains and applies the models. The same stage functions
(``idp.pipeline``) run locally, in the tests, and in the cluster as Spark
Operator applications. See the README for the architecture and
``docs/decisions/decision-log.md`` for the decisions behind it.
"""

__version__ = "0.1.0"
