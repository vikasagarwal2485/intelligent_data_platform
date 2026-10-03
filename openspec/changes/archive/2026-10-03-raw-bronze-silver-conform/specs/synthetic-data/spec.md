# Synthetic data delta

## MODIFIED Requirements

### Requirement: REQ-SYN-01 — Coherent multi-entity generation
The system SHALL generate referentially-consistent records across the canonical
entities (parties, customers, billing accounts, agreements, products, voice/data
usage, recharge) for one or more tenants, with a subscriber's MSISDN/IMSI/IMEI
consistent across its agreement and events, and every record stamped with
`tenant_id`.

Each billing account SHALL reference its paying party (`payer_party_id`). Each
billing account SHALL have one customer: the party role through which the paying
party holds the account and its agreements, carrying the party (`party_id`), its
role, `segment`, status and since-date, and the billing account (`account_id`). The
segment belongs to the customer: billing accounts SHALL NOT carry one. Each agreement
SHALL carry its customer (`customer_id`) and that customer's billing account
(`account_id`). The parties SHALL NOT carry a role or an account: those belong to
the customer.

The payer party of a company billing account (`is_company`) SHALL be an
organization: its `party_type` is `organization`, it carries an organization name
and type, and its person-only fields (`first_name`, `last_name`, `date_of_birth`) are
null. Every other party SHALL be an individual (`party_type = individual`).
Introducing customers and renaming accounts to billing accounts SHALL NOT change
any generated value: they reorganize the same values.

#### Scenario: SCN-SYN-01.1 — Referential integrity
- **WHEN** a dataset is generated
- **THEN** every usage/recharge `service_id` corresponds to a generated agreement
  MSISDN; every agreement's `account_id` exists in billing accounts and its
  `customer_id` in customers; and every billing account's `payer_party_id` and every
  customer's `party_id` exist in parties

#### Scenario: SCN-SYN-01.2 — Tenant stamping
- **WHEN** any entity/event table is generated
- **THEN** it contains a `tenant_id` column equal to the requested tenant

#### Scenario: SCN-SYN-01.3 — Company accounts have organization parties
- **WHEN** a dataset is generated
- **THEN** the payer party of every billing account with `is_company = true` has
  `party_type = organization`, a non-empty organization name, and null
  `first_name`, `last_name` and `date_of_birth`; every other party has
  `party_type = individual` and its person fields set; and the values of the former
  model are unchanged: each billing account equals the former account (with
  `party_id` as `payer_party_id`) apart from its segment, which is its customer's
  `segment`, and each party's former role and account are its customer's


