# Tasks

## 1. Schemas and profile foundation

- [x] 1.1 Vendor the TMF schemas into `config/tmf/schemas/`:
  - extract the v5 component schemas from the TMF629, 632, 635, 637, 666 and 678 OAS files;
  - write JSON Schemas for TMF651 v4.0 `Agreement` and TMF654 v4.0 `TopupBalance`, rewritten on the v5 base types;
  - record the source version and file in each.

  Verify with a test that every vendored schema loads and resolves its `$ref`s locally, with no network.
- [x] 1.2 Add the platform extension schemas, `config/tmf/extensions/Idp<Resource>.schema.json`: `allOf` base plus a required `tenantId`; `IdpTopupBalance` also has `rechargeType` and `agreement`. Pin `fastjsonschema` in `requirements.txt`/`pyproject.toml`. Verify that a sample resource validates with `tenantId` and fails without it.
- [x] 1.3 Create the profile engine in `src/idp/ingestion/tmf/`: load a profile YAML, check it, and apply it both ways (`to_tmf`, `from_tmf`):
  - field kinds: standard path, typed characteristic, extension attribute;
  - value maps, inverted automatically for the way back;
  - reference fields resolved through a per-tenant index.

  Loading rejects a profile in which a canonical field has zero or several mappings. Verify with unit tests on a toy profile.

## 2. Generator: organizations

- [x] 2.1 Change the generator so that `is_company` accounts get organization parties:
  - `party_type = organization`, `organization_name = Company <suffix>`, `organization_type = company`, and null `first_name`/`last_name`/`date_of_birth`;
  - other parties get `party_type = individual`;
  - no new random draws.

  Verify SCN-SYN-01.3: a dataset generated before and after is equal apart from these party fields. Regenerate the golden files (`tests/golden/make_golden.py`) and check the diff touches only company parties' person fields in `dim_subscriber`.

## 3. Standard profile

- [x] 3.1 Write `config/tmf/profiles/standard.yaml` for all canonical entities, following design §2 (Customer role, ProductUsage, TopupBalance extension, value maps). Make it the default profile in `config/platform.yaml` (`tmf.profile`, env `IDP_TMF_PROFILE`). Verify SCN-BRZ-05.1: a generated two-tenant dataset → `to_tmf` → `from_tmf` equals the original for every entity, money compared at 6 decimals. Also verify SCN-BRZ-06.1: every emitted field is a standard v5 field, a typed characteristic, or a platform-extension attribute.
- [x] 3.2 Verify the adapter behaviours:
  - SCN-BRZ-05.2: a Customer engaged with an Organization gives the account `party_id` and `is_company = true`;
  - SCN-BRZ-05.3: usage is split into voice and data, with net, tax and gross amounts;
  - SCN-BRZ-05.4: an unresolvable or ambiguous reference rejects the file and names the reference.

## 4. Landing writer and reader

- [x] 4.1 Rewrite `write_landing` to emit the TMF resource files per tenant with the active profile (`individual`, `organization`, `customer`, `billingAccount`, `agreement`, `product`, `appliedCustomerBillingRate`, `topupBalance`, `productUsage`), validating each resource. `idp generate` keeps its one-tenant-at-a-time loop. Verify SCN-BRZ-03.1, 03.3 and 03.5 (every resource valid, every reference resolves).
- [x] 4.2 Rewrite `read_landing`: validate each file, index the tenant's resources, adapt them to canonical entity records, and yield them per entity as now. Rejections cover: unknown tenant folder, unknown file, schema failure, `tenantId` ≠ folder, unresolvable reference. A rejected file also rejects the entities that depend on it. Keep `produce --from-landing`'s reporting and exit status. Verify:
  - SCN-STR-01.3: in-memory and from-landing publish the same messages;
  - SCN-STR-01.4 to 01.6 and SCN-BRZ-06.3: the rejection cases;
  - SCN-BRZ-03.4: generate → produce from landing → consume gives Bronze equal to the generated canonical records.

## 5. Replace JSON:API, samples and docs

- [x] 5.1 Remove `ingestion/jsonapi.py`, its exports and `tests/test_jsonapi.py`. Update `test_transforms_runtime.py` if it references them. Replace `data_samples/` with one TMF v5 sample per resource type, produced through the standard profile, with a test that every sample validates. Verify that no code references `jsonapi` and that the samples test passes.
- [x] 5.2 Update the docs:
  - `README.md`: the landing-zone section, with the TMF file list and examples, the profile and extension model, the organization nulls, and the `data_samples/` description;
  - `deploy/README.md`;
  - `make landing-show`, whose `ENTITY` values become the TMF file names;
  - the producer's and `idp generate`'s help texts.

  Verify by reading, and with `make landing-show ENTITY=productUsage LINES=40` on minikube after 6.2.

## 6. Verification

- [x] 6.1 `pytest -q` (default job) is green.
- [x] 6.2 On minikube:
  - `make image`, then `make stream-from-landing`;
  - the landing bucket holds the nine TMF files per tenant;
  - produce reports no rejections, consume lands Bronze, and `make silver` processes the new rows;
  - spot-check one resource of each type with `make landing-show`;
  - check that `dim_subscriber`'s person fields are null for organization parties only.
