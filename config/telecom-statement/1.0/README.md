# Telecom Statement 1.0 (draft)

A proposed standard for consented telecom data sharing with fintechs. Every operator
(Telecom Information Provider, TIP) produces the same statements; a fintech (Telecom
Information User, TIU) fetches them through a consent manager that can't read them,
as in India's Account Aggregator flow. Draft for discussion: not a regulator's
publication, and not produced by the platform yet.

| Schema | Statement type | Consent purpose | Fetch | Data life |
|---|---|---|---|---|
| `credit-profile.schema.json` | `CREDIT_PROFILE` | `CREDIT_UNDERWRITING` / `dpv:CreditChecking` | one-time, 6–12 months | days |
| `fraud-signals.schema.json` | `FRAUD_SIGNALS` | `IDENTITY_AND_FRAUD_CHECK` / `dpv:FraudPreventionAndDetection` or `dpv:IdentityVerification` | one-time, now | ≤ 24 h |
| `credit-monitoring.schema.json` | `CREDIT_MONITORING` | `CREDIT_MONITORING` / `dpv:CreditChecking` | periodic, loan term | per fetch |

`common.schema.json` holds the envelope (provider, consent, subject, signature) and the
shared types: bands, recency, invoices, collection stages, payment channels.

**Rules the schemas enforce**
- Closed objects: no field outside the standard; operator-specific data only in
  `extensions` as `x-<name>`.
- Minimisation:
  - no call records, counterparties, contacts, location or content;
  - the MSISDN only masked;
  - identity checked by match results, never returned;
  - fraud recency as bands, not timestamps;
  - payment channels, never payers or instruments.
- Purpose binding: each statement type accepts only its own purpose and fetch type;
  fraud signals live at most 24 hours; monitoring needs a frequency and an expiry.
- Explicit absence: `NOT_APPLICABLE` (prepaid has no invoices, postpaid no recharges)
  differs from `NOT_AVAILABLE` and from zero.

**Alignment.** The fraud fields follow GSMA CAMARA meanings (KYC Match, KYC Tenure,
SIM Swap, Device Swap, Call Forwarding Signal, Device Roaming Status). Invoices and
payments map to TM Forum TMF678/TMF670, recharges to TMF654, usage to TMF635, and
purposes to the W3C Data Privacy Vocabulary.

**Examples:** `examples/`. They're checked by `tests/test_telecom_statement.py`, with
cases showing that the rules above reject non-conforming statements.

**Open for the standards body**
- the band boundaries;
- the name-match algorithm;
- whether `connectionsUnderSameIdentity` and DoT's flags may be relayed;
- whether a circle or region field is ever allowed;
- the signature canonicalisation profile.
