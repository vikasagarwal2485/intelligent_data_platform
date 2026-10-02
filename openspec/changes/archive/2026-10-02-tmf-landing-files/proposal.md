# Proposal

## Why

The landing zone (`idp-landing/tenant=<t>/<entity>.json`) simulates the source
systems' file drop, but its files are flat, snake_case copies of the platform's own
canonical records. They don't look like what telco source systems actually expose:
TM Forum Open API resources. So the ingestion path is never exercised against a
realistic source format, and nothing shows how a real operator feed would be
adapted onto the canonical model.

The existing source adapter (`ingestion/jsonapi.py`, REQ-BRZ-04) targets an
operator-specific JSON:API flavour (kebab-case, `UUID(...)` wrappers), covers only
usage and recharge, and isn't used by the pipeline.

This change makes the landing files TM Forum Open API v5 resources. The adapter that
maps them back to the canonical records is driven by a configurable mapping profile,
and customers can extend the resources through TMF's own `@type` mechanism. Only the
landing files change here. Kafka, Bronze and Silver keep the canonical model, and a
second change will carry TMF resources through Kafka to the consumer.

## What Changes

- **Landing files become TMF v5 resources**, a JSON array per resource type, per
  tenant (`tenant=<t>/<resource>.json`):

  | File | TMF API | Resource |
  |---|---|---|
  | `individual.json`, `organization.json` | TMF632 Party | `Individual`, `Organization` |
  | `customer.json` | TMF629 Customer | `Customer` (a `PartyRole`) |
  | `billingAccount.json` | TMF666 Account | `BillingAccount` |
  | `agreement.json` | TMF651 Agreement | `Agreement` |
  | `product.json` | TMF637 Product Inventory | `Product` |
  | `appliedCustomerBillingRate.json` | TMF678 Customer Bill | `AppliedCustomerBillingRate` |
  | `topupBalance.json` | TMF654 Prepay Balance | `TopupBalance` |
  | `productUsage.json` | TMF635 Usage | `ProductUsage` (voice and data, by `usageType`) |

  They follow the v5 conventions throughout:
  - `id`/`href`/`@type`/`@baseType`/`@schemaLocation`;
  - polymorphic characteristics and contact media;
  - `RelatedPartyRefOrPartyRoleRef`.

  Agreement and TopupBalance have no public, stable v5 schema yet, so they use their
  latest stable shapes (TMF651 v4.0, TMF654 v4.0) with the v5 conventions. Each
  resource's source version is recorded so it can move to v5 when the schemas are
  published.
- **Customer role.** Accounts and agreements reference the `Customer` party role.
  The Customer's `engagedParty` is the Individual or Organization. The adapter
  resolves these references per tenant.
- **Organizations.** Parties of company accounts are `Organization`s. In the
  canonical records their person-only fields (`first_name`, `last_name`,
  `date_of_birth`) are null. The generator produces them that way, so every path
  (in-memory, landing, `idp run`) yields the same canonical data.
- **Mapping profiles and extensions.**
  - One mapping profile (`config/tmf/profiles/`) defines both directions:
    canonical → TMF (landing writer) and TMF → canonical (adapter).
  - The `standard` profile uses only standard fields and characteristics, plus the
    platform extension.
  - The platform extension adds `tenantId` to every resource, and carries the few
    canonical fields that have no standard home (e.g. TopupBalance's recharge type
    and agreement).
  - A customer profile can add `@type` subtypes with their own `@schemaLocation`
    schemas, without changing standard fields.
- **Validation.** Resources are validated against the vendored TMF schemas plus the
  extension schemas. A landing file is rejected when:
  - its `tenantId` doesn't match its folder;
  - it fails schema validation;
  - it holds a reference that doesn't resolve.
- **The producer still publishes canonical records** from the landing zone. The
  adapter runs when the landing files are read, so Kafka, Bronze and Silver are
  unchanged, and from-landing and in-memory production stay equivalent.
- **The JSON:API adapter and the `data_samples/` envelopes are replaced** by v5 TMF
  samples and the profile-driven adapter.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `bronze-ingestion`:
  - REQ-BRZ-03: the landing files are TMF v5 resources;
  - REQ-BRZ-04 (JSON:API adapter) is removed;
  - new REQ-BRZ-05: the TMF Open API adapter;
  - new REQ-BRZ-06: mapping profiles and `@type` extensions.
- `streaming-kafka`: REQ-STR-01's from-landing source reads TMF files, with the new
  rejection rules.
- `synthetic-data`: REQ-SYN-01 generates organization parties for company accounts,
  with null person fields.
- `silver-curation`: REQ-SLV-02 changes wording only, naming the TMF adapter
  instead of the JSON:API one.

## Impact

- **Code:**
  - `src/idp/ingestion/` gains a TMF module (profile loader, writer, adapter,
    reference resolution, validation), replacing `jsonapi.py`;
  - `landing.py` (writer and reader);
  - `synthetic/generator.py` (organizations);
  - `cli.py` (`idp generate`, `produce --from-landing`).
- **Config:** `config/tmf/` holds the vendored v5 schemas, the extension schemas and
  the `standard` profile.
- **Dependency:** `fastjsonschema` (compiled validation; see design §4).
- **Data:**
  - landing files change format, so existing landing files must be regenerated
    (`make stream-from-landing`);
  - the canonical `parties` records of organizations now have null person fields,
    so those `dim_subscriber` columns are null for business subscribers. No model
    feature uses them.
  - Kafka, Bronze and Silver schemas are unchanged.
- **Tests:** a round-trip test (canonical → TMF → canonical) for the `standard`
  profile, schema validation, reference resolution and rejection. The golden data is
  regenerated for organizations.
- **Docs:** README (landing zone), `deploy/README.md`, `make landing-show` (new file
  names).
- **Follow-up (change 2):** Kafka carries TMF resources, and the consumer runs the
  adapter.
