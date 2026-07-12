# Intelligent Data Platform — OpenSpec

This directory is an [OpenSpec](https://github.com/Fission-AI/OpenSpec)-format
specification of the Intelligent Data Platform, structured so an AI coding agent
(e.g. Claude Code) can implement the platform **phase by phase, sequentially and
autonomously**.

It complements the two prose documents:
- [`../architecture_and_implementation_plan.md`](../architecture_and_implementation_plan.md) — the reviewed architecture & plan
- [`../../README.md`](../../README.md) — the current step-by-step guide

## Layout

```
docs/openspec/
  project.md            # project context: mission, stack, conventions, commands
  roadmap.md            # the ordered list of phases (which change to implement next)
  specs/                # TARGET capability specs = the definition of done
    <capability>/spec.md    #   Requirements (SHALL) + Scenarios (WHEN/THEN)
  changes/              # one change proposal PER PHASE, implemented in order
    phase-N-<slug>/
      proposal.md            #   Why / What Changes / Impact / Spec Deltas
      tasks.md               #   the actionable, checkable implementation checklist
  decisions/
    decision-log.md     # Architecture Decision Records (ADR log)
```

## How an agent should use this (the loop)

1. Read [`project.md`](project.md) once for context and conventions.
2. Read [`roadmap.md`](roadmap.md); pick the **lowest-numbered phase not yet done**.
3. Open that phase's `changes/phase-N-<slug>/proposal.md` (why + what) and
   `tasks.md` (the checklist).
4. Implement the tasks in order. After each task, tick it in `tasks.md`
   (`- [ ]` → `- [x]`).
5. A phase is **done** when every scenario in the capability specs it touches
   (listed under the proposal's *Spec Deltas*) is satisfied and `make test`
   passes. Then move to the next phase.
6. Consult [`decisions/decision-log.md`](decisions/decision-log.md) before making
   any architectural choice — the ADRs are binding constraints.

## Conventions used in the specs

- **Requirements** use `SHALL` and are testable.
- Every requirement has ≥1 **Scenario** in `WHEN`/`THEN` (Given/When/Then) form.

### Traceability IDs

Every requirement and scenario carries a **globally unique id** for traceability
from specs → changes → code → tests.

- **Requirement id:** `REQ-<CAP>-NN` (e.g. `REQ-SLV-02`).
- **Scenario id:** `SCN-<CAP>-NN.M` (e.g. `SCN-SLV-02.1`), where `NN` is the
  owning requirement and `M` the scenario index.

Headings embed the id: `### Requirement: REQ-SLV-02 — <title>` /
`#### Scenario: SCN-SLV-02.1 — <title>`.

Capability codes (`<CAP>`):

| Code | Capability | Code | Capability |
|---|---|---|---|
| `FND` | foundation | `ML` | ml-models |
| `SYN` | synthetic-data | `SRV` | serving-api |
| `BRZ` | bronze-ingestion | `STR` | streaming-kafka |
| `SLV` | silver-curation | `PLT` | platform-deployment |
| `GLD` | gold-features | `ORC` | orchestration |

Each `changes/phase-N/proposal.md` **What Changes** item cites the `REQ-*` id(s)
it implements, so every implementation task traces back to a spec requirement and
its scenarios.

### Traceability matrix

[`traceability.md`](traceability.md) is a generated, at-a-glance view mapping
**Requirement → Scenarios → Phase → Code**. Regenerate it after editing any spec
or proposal:

```bash
python docs/openspec/gen_traceability.py
```
