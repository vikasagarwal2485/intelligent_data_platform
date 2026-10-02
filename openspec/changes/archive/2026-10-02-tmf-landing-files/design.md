# Design

## Context

- **Landing today** (`ingestion/landing.py`): `write_landing` dumps each canonical
  entity frame per tenant to `tenant=<t>/<entity>.json`. `read_landing` checks each
  file (folder tenant, known entity, record `tenant_id`) and yields its records to
  `produce --from-landing`, which publishes them unchanged. In-memory and
  from-landing production are equivalent (SCN-STR-01.3).
- **Canonical model** (unchanged by this change), from `synthetic/generator.py`:
  - `parties`: first_name, last_name, date_of_birth, language, phone, email,
    consents, role, account_id
  - `accounts`: account_type, account_segment, lifecycle_status, valid_from,
    party_id, is_company
  - `agreements`: agreement_type, business_line, lifecycle_status, valid_from,
    msisdn, imsi, account_id
  - `products`: product_type, name, category, monthly_fee, lifecycle_status,
    agreement_id
  - `voice_events` / `data_events`: service_id, agreement_id, started_at,
    created_at, net and tax amounts, qty/unit, roaming, network tech, traffic case,
    other party, IMEI
  - `recharge_events`: service_id, agreement_id, created_at, amount, currency,
    payment_method, status, failure_reason_code, source, recharge_type
  - `fee_charges`: product_id, charge_type, amount, currency, charged_at
- **Silver** joins agreement → account (`account_id`) → party (`party_id`). It
  doesn't use `parties.account_id` or `parties.role`, and no model feature uses
  `date_of_birth`.
- **TMF v5** OAS files are published for TMF629, 632, 635, 637, 666 and 678.
  TMF651 v5 isn't in the public repo, and TMF654 v5 is only a preview. The stable
  versions are TMF651 v4.0 and TMF654 v4.0 (the public repo publishes no v4.1
  schema file, so v4.0.0 is vendored).

## Goals / Non-Goals

**Goals:**
- Landing files that are recognizable, schema-valid TMF v5 resources, linked the
  way TMF links them (through the Customer role).
- One configurable mapping, used in both directions, with a provable round trip.
- A foundation for customer-specific `@type` extensions, without code changes.

**Non-Goals:**
- Changing Kafka, Bronze or Silver. Change 2 moves the adapter to the consumer.
- Implementing TMF REST endpoints or notifications. These are files.
- Full TMF coverage (catalog references, bills, payments). Only what carries a
  canonical field.

## Decisions

### 1. A mapping profile drives both directions
`config/tmf/profiles/standard.yaml` holds one entry per canonical entity:
- the TMF resource and its spec version (e.g. `TMF635 v5.0.0`);
- the `@type` to emit and its extension schemas;
- for each canonical field, where it lives: a JSON path for a standard field, a
  typed characteristic (`name`, `@type`, `valueType`), or an extension attribute,
  plus an optional value map;
- for each reference field, how it resolves.

A small engine in `idp.ingestion.tmf` applies a profile:
- `to_tmf(canonical frames) -> resources`;
- `from_tmf(resources) -> canonical frames`.

Value maps are declared once and inverted for the way back, e.g. recharge status
`successful` ↔ `completed` (TMF654 `ActionStatusType`) and fee type `one_time` ↔
`oneTime`. Because the writer and the adapter use the same entries, they can't
drift.

- *Alternative:* hand-written mapping functions per entity. Rejected: no shared
  source of truth, and customer extensions would need code changes.

### 2. Resource mapping (standard profile)

| Canonical | TMF resource | Key mapping |
|---|---|---|
| parties (individual) | `Individual` | `givenName`, `familyName`, `birthDate`; `languageAbility[{languageCode, isFavouriteLanguage}]`; `contactMedium[{@type PhoneContactMedium, phoneNumber}, {@type EmailContactMedium, emailAddress}]`; `partyCharacteristic[BooleanCharacteristic consentMarketing / consentAnalytics]` |
| parties (organization) | `Organization` | `name`, `organizationType`, `isLegalEntity`; contact media and consents as above; language as `partyCharacteristic` `preferredLanguage` (Organization has no `languageAbility`) |
| (role) | `Customer` | id `<tenant>-cust-<n>`; `engagedParty {id, @referredType Individual or Organization}`; `account[{id}]`; `agreement[{id}]`; `validFor.startDateTime` (account valid_from); `characteristic[accountSegment]`; `status` |
| accounts | `BillingAccount` | `accountType`, `state` (lifecycle); `relatedParty[{role customer, partyOrPartyRole {id customer, @referredType Customer}}]` |
| agreements | `Agreement` (TMF651 v4.0 shape) | `agreementType`, `status`, `agreementPeriod.startDateTime`; `engagedParty[{role customer, partyOrPartyRole → Customer}]`; `characteristic[businessLine, msisdn, imsi]` |
| products | `Product` | `name`, `status`; `productCharacteristic[productType, category]`; `productPrice[{priceType recurring, recurringChargePeriod month, price.taxIncludedAmount}]`; `agreementItem[{agreementId}]` |
| fee_charges | `AppliedCustomerBillingRate` | `appliedBillingRateType` (charge type), `date`, `product {id}`, `taxIncludedAmount {unit, value}` |
| recharge_events | `TopupBalance` (TMF654 v4.0 shape) | `amount {amount, units}`, `requestedDate`, `status` (mapped), `reason`, `paymentMethod`, `channel`, `logicalResource[{id msisdn, @referredType MSISDN}]`; platform extension: `rechargeType`, `agreement {id}` |
| voice/data events | `ProductUsage` | `usageType` voice/data; `creationDate` (created_at); `status rated`; `usageCharacteristic[startDateTime, msisdn, agreementId, quantity, unit, isRoaming, networkTechnology, trafficCase, otherPartyNumber, imei]`; `usageProductPrice[{priceType usage, price {dutyFreeAmount net, taxIncludedAmount net+tax}}]` |

Notes:
- **Usage timestamps.** v5 `ProductUsage` has no `usageDate`. The event start is a
  `startDateTime` characteristic, and `creationDate` carries `created_at`.
- **Tax amount.** It is recovered as `taxIncludedAmount − dutyFreeAmount`, rounded to
  6 decimals (the generator's precision, finer than any canonical value), so the
  round trip is exact.
- **Agreement → account.** The agreement's account is the account of its Customer
  role. A customer with several accounts would be ambiguous. The generator creates
  one account per customer, so the adapter rejects an ambiguous agreement rather
  than guessing.

### 3. Platform extension: `tenantId`, plus what has no standard home
Every resource is emitted as `@type Idp<Resource>`, with `@baseType <Resource>` and
`@schemaLocation config/tmf/extensions/Idp<Resource>.schema.json`. The extension
schema is `allOf [base, {tenantId: string (required)}]`.

TopupBalance's extension adds `rechargeType` and `agreement`, because TMF654 has no
characteristics. Everything else uses characteristics, which are standard.

Customer profiles layer further subtypes the same way (`allOf` the platform
extension). The validator follows `@schemaLocation` only to local, vendored schema
files.

### 4. Vendored schemas, validated with `fastjsonschema`
`config/tmf/schemas/` holds:
- the v5 OAS component schemas for the six v5 APIs;
- JSON Schemas for the TMF651 v4.0 and TMF654 v4.0 resource shapes, rewritten with
  the v5 base types.

Each file states its source version. Schemas are vendored, never fetched at run time.

Validation runs on write and on read (before adapting). A schema failure
rejects the file (REQ-BRZ-06), with the first error's path and message.

`config/tmf/vendor_schemas.py` builds the bundles from upstream files pinned by
commit. OpenAPI discriminators become `anyOf` plus an explicit `if @type == T then
T` dispatch, because JSON Schema's `oneOf` fails on overlapping subtypes.

Validation uses `fastjsonschema`, not `jsonschema`. Each `@type`'s schema is merged
with everything it references into one self-contained schema and compiled once.
The interpreted `jsonschema` validated about 760 resources a second, which is
minutes for a tenant's `productUsage.json`; the compiled one validates about
10,700. `fastjsonschema` also enforces `format: date-time`, so the profile has an
`as: date` field option: `birthDate` is written as `YYYY-MM-DDT00:00:00Z` and read
back as the date.

### 5. Landing writer and reader
- **`write_landing`** builds each tenant's canonical frames into TMF resources with
  the profile, one file per resource type. `idp generate` keeps its
  one-tenant-at-a-time loop.
- **`read_landing`** reads a tenant folder's files and:
  1. validates each file;
  2. indexes the tenant's resources by type and id;
  3. adapts them to canonical entity records, resolving references through the
     index;
  4. yields one item per canonical entity, as now. A rejected file rejects the
     entities that need it: for example, an invalid `customer.json` rejects
     accounts, agreements and parties, because they can't resolve without it.

  The CLI reports "REJECTED n landing file(s) or entities", since a dependent
  entity can be rejected without its own file being bad.

  The producer is unchanged apart from the file and entity names, and one fix:
  records are normalized with `common/records.py:to_records`. Pandas gives a
  missing string as NaN, which isn't valid JSON, so missing values now go to Kafka
  as null, both in memory and from landing. So in-memory and
  from-landing production stay equivalent (SCN-STR-01.3), now with a TMF round trip
  in between.

### 6. Organizations in the generator
For `is_company` accounts the generator sets the party's `party_type` to
`organization`, `organization_name` to `Company <suffix>`, `organization_type` to
`company`, and `first_name`/`last_name`/`date_of_birth` to null. Other parties get
`party_type = individual`.

The organization values come from the subscriber suffix, not from random draws, so
no other value changes (SCN-SYN-01.3). The three new `parties` columns are additive,
so Bronze evolves by union-by-name and Silver ignores them. The golden data is
regenerated, because the person fields of company parties change.

### 7. Samples replace JSON:API
`data_samples/` gets one v5 TMF sample per resource type, taken from the generator
through the standard profile, so they validate by construction. The old JSON:API
envelopes and `jsonapi.py` (with `test_jsonapi.py`) are removed, and REQ-BRZ-04 is
retired in its favour. REQ-SLV-02 (silver-curation) only changes wording: it now
names the TMF adapter (REQ-BRZ-05) instead of the JSON:API one, and its scenario no
longer cites `data_samples/`.

## Risks / Trade-offs

- [TMF651 and TMF654 aren't true v5] → Each resource's source version is recorded in
  the profile and its schema file. Moving to v5 later means swapping the schema and
  the profile entry; the canonical side doesn't change.
- [Reference resolution needs a whole tenant folder in memory] → Reference resources
  (parties, customers, accounts, agreements, products) are small. The large event
  files (`productUsage.json`, about 9 MB per tenant today) only need lookups into
  them, and the reader already works one tenant at a time. Change 2 (the streaming
  consumer) must solve the same resolution per message, which is noted for it.
- [Characteristics are verbose] → Landing files grow by about 2–3×. They are
  simulated source drops, not the lake, so this doesn't affect Bronze or Silver.
- [Null person fields for B2B parties] → They're accepted (no feature uses them), and
  documented in the README.
- [A customer profile could break the round trip] → Profile loading checks that every
  canonical field has exactly one mapping. The round-trip test runs for every
  profile in `config/tmf/profiles/`.

## Migration Plan

1. Deploy the new image. Existing landing files are in the old format, so regenerate
   them: `make stream-from-landing` writes the new files (the `generate` step), then
   produces and consumes them. `generate` doesn't delete the old
   `tenant=<t>/<entity>.json` files, and the reader rejects them as unknown landing
   files, which stops the chain before consume. So first remove the old files
   (`parties`, `accounts`, `agreements`, `products`, `voice_events`, `data_events`,
   `recharge_events`, `fee_charges`) from each tenant folder, or empty the bucket
   with `make minio-reset`. If the chain already stopped, remove them and run
   `make stream-replay`.
2. Bronze and Silver are unaffected, except for the additive `parties` columns and
   the nulls for organizations on new data. Party ids are index-based, so Bronze
   keeps the older versions of the same parties (with person values), and
   `build_dim_subscriber` joins every version and keeps an arbitrary row per
   subscriber. A full Silver refresh reads the same Bronze, so it doesn't help: the
   organization nulls show in `dim_subscriber` only after Bronze is reset
   (`make minio-reset`, then a new stream). Silver choosing the latest party version
   is out of scope here (follow-up).
3. Rollback: revert the change. Old-format landing files must then be regenerated
   again, because the old reader can't read the new files.
