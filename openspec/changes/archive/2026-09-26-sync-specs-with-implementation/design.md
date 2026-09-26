# Design

## Context

The main specs were edited by hand in each phase instead of through delta specs,
and the 16 phase changes (`phase-0` … `phase-15`) remain unarchived with only
`proposal.md`/`tasks.md`. `openspec list` therefore shows 16 "active" changes that
are finished. `docs/gen_traceability.py` still resolves `specs/` and `changes/`
relative to `docs/`, which broke when the tree moved from `docs/openspec/` to
`openspec/`. See proposal.md — Why.

## Goals / Non-Goals

**Goals:**
- Main specs describe current behaviour exactly, with no requirement contradicting
  another.
- A delta-based baseline that the five follow-up changes can modify cleanly.
- Working traceability tooling.

**Non-Goals:**
- Any runtime behaviour change. Gaps between intended and implemented behaviour
  (duplicate re-runs, Spark parity, consent/tenant gateway, MLflow, OIDC) belong to
  their own changes. The requirements they rely on (REQ-SLV-01.2, REQ-ORC-02.2,
  REQ-FND-02/03, REQ-ML-04, REQ-SRV-03) are deliberately left as the intended
  behaviour.

## Decisions

- **Deltas, not direct edits.** Changes go through `specs/<capability>/spec.md`
  deltas and are merged by `openspec archive`. This gives a reviewable diff and
  fits the workflow the remaining changes use. The alternative was editing
  `openspec/specs/` in place; it was rejected because that is what caused the drift.
- **Keep REQ/SCN ids stable; never reuse a removed id.** REQ-GLD-03 is removed and
  its number retired. New requirements take the next free number (REQ-BRZ-03,
  REQ-SRV-04). The follow-up changes use REQ-GLD-04, REQ-SLV-06, REQ-ML-08,
  REQ-SRV-05, REQ-ORC-04 and REQ-FND-05/06 so no two changes collide.
- **Separate MLflow from ML-03.** The "MLflow in production" wording is dropped
  from ML-03 here. `mlflow-model-registry` reintroduces it as a normative change to
  REQ-ML-04, so the spec never claims unbuilt behaviour without an owning change.
- **Wire format documented as JSON.** STR-03 now states that payloads are JSON and
  that the Avro schemas are the registered contract. This matches the producer and
  consumer. Moving to Avro-encoded payloads would be a separate change.
- **Purpose line of `gold-features`.** Delta specs cannot change a Purpose, so the
  "(and optional scores)" wording is edited directly in the main spec as a task.
- **Phase changes are archived first, outside this change.** The 16 `phase-*`
  changes have no delta specs, so archiving them only moves them to
  `changes/archive/` and leaves the baseline this change's deltas target untouched.
  They are archived before this change is applied. The traceability generator
  reads their proposals from the archive.

## Risks / Trade-offs

- [Follow-up changes MODIFY the same blocks if archived before this one] → The
  follow-ups mostly ADD requirements. Where one MODIFIES a block this change also
  touches, its text is written against this change's version. Archive this change
  first.
- [Archiving 16 phase changes breaks proposal links in `docs/traceability.md`] →
  Update the generator to scan `changes/archive/*phase-*` and regenerate.
- [`openspec archive` on a change with no deltas may prompt or refuse] → Use
  `--skip-specs` (or the tool's equivalent) for the phase changes.

## Migration Plan

1. Validate: `openspec validate sync-specs-with-implementation --strict`.
2. Complete the tasks, then `openspec archive sync-specs-with-implementation`.
3. Confirm `openspec validate --specs --strict` passes on the merged main specs.
4. Rollback: `git revert` of the archive commit restores the previous specs.
