"""Kafka streaming bridge: TMF events -> topics -> canonical Bronze (REQ-STR-01/02)."""
from idp.streaming.producer import produce_to_kafka
from idp.streaming.consumer import consume_to_bronze
from idp.synthetic.generator import ENTITIES

__all__ = ["produce_to_kafka", "consume_to_bronze"]

# The canonical Bronze entities the consumer lands, in order.
BRONZE_ENTITIES = tuple(ENTITIES)

# TMF topic suffix (t.<tenant>.<suffix>) -> the TMF resources published on it, as
# create events (REQ-STR-01). Individuals and organizations share the party topic;
# voice and data usage share productUsage. Customers come first: the consumer
# resolves the others' references through them.
TOPIC_RESOURCES = {
    "customer": ("Customer",),
    "party": ("Individual", "Organization"),
    "billingAccount": ("BillingAccount",),
    "agreement": ("Agreement",),
    "product": ("Product",),
    "appliedCustomerBillingRate": ("AppliedCustomerBillingRate",),
    "topupBalance": ("TopupBalance",),
    "productUsage": ("ProductUsage",),
}
RESOURCE_TOPIC = {r: topic for topic, rs in TOPIC_RESOURCES.items() for r in rs}
CUSTOMER_SUFFIX = "customer"           # log-compacted; read in full by the consumer
DLQ_SUFFIX = "dlq"
