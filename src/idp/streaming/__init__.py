"""Kafka streaming bridge: synthetic data -> topics -> Bronze (plan section 3)."""
from idp.streaming.producer import produce
from idp.streaming.consumer import consume_to_bronze

__all__ = ["produce", "consume_to_bronze"]

# Map generator table name -> Kafka topic suffix (t.<tenant>.<suffix>).
TABLE_TO_SUFFIX = {
    "parties": "parties",
    "accounts": "accounts",
    "agreements": "agreements",
    "products": "products",
    "voice_events": "usage.voice",
    "data_events": "usage.data",
    "recharge_events": "recharge",
}
SUFFIX_TO_TABLE = {v: k for k, v in TABLE_TO_SUFFIX.items()}
