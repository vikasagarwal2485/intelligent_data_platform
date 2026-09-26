# Spec Delta

## RENAMED Requirements

- FROM: `### Requirement: REQ-SLV-02 — JSON:API unwrap and normalization`
- TO: `### Requirement: REQ-SLV-02 — Id canonicalization and normalization`

## MODIFIED Requirements

### Requirement: REQ-SLV-02 — Id canonicalization and normalization
The Silver stage SHALL:
- strip `UUID(...)` wrappers from entity and relationship ids
- normalize money as `gross_amount = net_amount + tax_amount`
- normalize timestamps to UTC
- de-duplicate on natural keys

Silver curates canonical Bronze records; it does not parse JSON:API envelopes.
Unwrapping envelopes into canonical records is the ingestion adapter's job
(REQ-BRZ-04).

#### Scenario: SCN-SLV-02.1 — Money normalization
- **WHEN** a usage/recharge record with net + tax amounts is curated
- **THEN** `gross_amount` equals net + tax

#### Scenario: SCN-SLV-02.2 — UUID unwrap from real samples
- **WHEN** a Bronze record carries a relationship id wrapped as `"UUID(123...)"`,
  as in the real TM Forum documents in `data_samples/`, and is curated
- **THEN** the Silver id is the inner value without the wrapper, so the
  relationship joins its target (e.g. an agreement's `account_id` joins
  `accounts`)
