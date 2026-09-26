# Spec Delta

## ADDED Requirements

### Requirement: REQ-BRZ-04 — JSON:API source adapter
The ingestion layer SHALL provide an adapter that flattens TM Forum JSON:API usage
(voice and data) and recharge event envelopes into canonical records ready for
Bronze. The adapter SHALL:
- lift the `attributes` and `event-details` fields to top-level columns
- strip `UUID(...)` wrappers from the document id and relationship targets
- split charged amounts into `net_amount`, `tax_amount`, `gross_amount`
  (= net + tax) and `currency`

An absent money block SHALL yield zero amounts and no currency.

#### Scenario: SCN-BRZ-04.1 — Usage envelope flattened
- **WHEN** the data-usage sample in `data_samples/` is passed to the adapter
- **THEN** the record carries `service_id`, `event_type = data`, `qty` and `unit`
  from the used-service units, `gross_amount` = net + tax, and an `agreement_id`
  equal to the relationship target without its `UUID(...)` wrapper

#### Scenario: SCN-BRZ-04.2 — Voice details lifted
- **WHEN** the voice-usage sample is passed to the adapter
- **THEN** `traffic_case` and `other_party_number` from `event-details` appear as
  top-level fields

#### Scenario: SCN-BRZ-04.3 — Recharge envelope flattened
- **WHEN** the recharge sample is passed to the adapter
- **THEN** the record carries `service_id`, `amount`, `payment_method`, `status` and
  the unwrapped `agreement_id`

#### Scenario: SCN-BRZ-04.4 — Missing money block
- **WHEN** an envelope has no charged-amount block
- **THEN** net, tax and gross amounts are 0 and `currency` is empty
