# Tasks — Phase 10: App overlays

## 1. Split the overlays
- [x] 1.1 `platform/overlays/{local,aws}`: reference only `../../base` (drop
      app/deployment + scoring-api patch)
- [x] 1.2 New `app/overlays/local/kustomization.yaml`: `../../deployment` + local
      scoring-api patch (dev image, `imagePullPolicy: Never`, 1 replica)
- [x] 1.3 New `app/overlays/aws/kustomization.yaml`: `../../deployment` + aws
      scoring-api patch (ECR image, IRSA, 3 replicas)
- [x] 1.4 `app/deployment/kustomization.yaml`: keep it env-agnostic (comment update)

## 2. Wiring
- [x] 2.1 `Makefile`: add `app-deploy` (kubectl apply -k deploy/app/overlays/local);
      fix `platform-base` help + `make image` hint
- [x] 2.2 `deploy/platform/argocd/application.yaml`: two Applications with
      sync-waves (platform base → app)

## 3. Docs
- [x] 3.1 Root README (sequence `… platform-base → platform-services → app-deploy`,
      structure block, deployment bullets)
- [x] 3.2 `deploy/README.md` (tree, app section, Local + Production steps)
- [x] 3.3 Architecture plan repo-layout tree; ADR-0020

## 4. Verify
- [x] 4.1 All four overlays resolve; platform overlays have no scoring-api, app
      overlays carry exactly the scoring-api patch
- [x] 4.2 `make help` shows `app-deploy`; argocd manifest parses (2 Applications)

## Done when
`make platform-base` applies only platform base and `make app-deploy` applies the
scoring-api; both overlays build; no doc points at the old combined overlay.
