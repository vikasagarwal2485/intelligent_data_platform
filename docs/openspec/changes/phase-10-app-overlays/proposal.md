# Phase 10 — App overlays: deploy the app separately from the platform base

## Why
Phase 9 split `deploy/` into `platform/` and `app/`, but the scoring-api and its
environment patch still rode inside the *platform* overlays, so `make
platform-base` deployed both a platform concern (namespaces/RBAC) and an app
concern (scoring-api). That blurs team ownership and rollout boundaries.

## What Changes
Each item references the spec Requirement id(s) it satisfies (traceability).
- `platform/overlays/{local,aws}` now reference only `../../base`
  (namespaces/RBAC/isolation) — no app, no patches — `REQ-PLT-01`, `REQ-PLT-04`.
- New `app/overlays/local` and `app/overlays/aws` reference `../../deployment`
  and carry the scoring-api env patch (local: `idp/scoring-api:dev`,
  `imagePullPolicy: Never`, 1 replica; aws: ECR image, IRSA, 3 replicas) —
  `REQ-PLT-01`, `REQ-PLT-04`.
- `Makefile`: new `app-deploy` target (`kubectl apply -k deploy/app/overlays/local`);
  `platform-base` help/`make image` hint updated — support `REQ-PLT-01/04`.
- `deploy/platform/argocd/application.yaml`: two Applications (sync-waves) —
  `idp-platform-base` (`deploy/platform/overlays/aws`) then `idp-app`
  (`deploy/app/overlays/aws`) — `REQ-PLT-04`.

## Impact
- Spec deltas: none — `platform-deployment` REQ-PLT-01/04 unchanged and still
  satisfied; this refines where the app patch lives.
- Code: `deploy/platform/overlays/**`, `deploy/app/overlays/**` (new),
  `deploy/app/deployment/kustomization.yaml`, `deploy/platform/argocd/application.yaml`,
  `Makefile`, `README.md`, `deploy/README.md`, plan tree.
- Decisions: ADR-0020.
- Operational: bring-up gains one step (`app-deploy`); no image or chart change.

## Spec Deltas
None. Re-verifies `specs/platform-deployment` REQ-PLT-01/04 against the split
overlays.
