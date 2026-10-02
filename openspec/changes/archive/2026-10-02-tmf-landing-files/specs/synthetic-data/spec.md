# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-SYN-01 — Coherent multi-entity generation
The system SHALL generate referentially-consistent records across all seven TMF
entities (parties, accounts, agreements, products, voice/data usage, recharge)
for one or more tenants, with a subscriber's MSISDN/IMSI/IMEI consistent across
its agreement and events, and every record stamped with `tenant_id`.

The party of a company account (`is_company`) SHALL be an organization: its
`party_type` is `organization`, it carries an organization name and type, and its
person-only fields (`first_name`, `last_name`, `date_of_birth`) are null. Every
other party SHALL be an individual (`party_type = individual`). Generating
organizations SHALL NOT change any other generated value.

#### Scenario: SCN-SYN-01.1 — Referential integrity
- **WHEN** a dataset is generated
- **THEN** every usage/recharge `service_id` corresponds to a generated
  agreement MSISDN, and every agreement's `account_id` exists in accounts

#### Scenario: SCN-SYN-01.2 — Tenant stamping
- **WHEN** any entity/event table is generated
- **THEN** it contains a `tenant_id` column equal to the requested tenant

#### Scenario: SCN-SYN-01.3 — Company accounts have organization parties
- **WHEN** a dataset is generated
- **THEN** the party of every account with `is_company = true` has
  `party_type = organization`, a non-empty organization name, and null
  `first_name`, `last_name` and `date_of_birth`; every other party has
  `party_type = individual` and its person fields set; and every non-party value
  equals that of the same generation before organizations existed
