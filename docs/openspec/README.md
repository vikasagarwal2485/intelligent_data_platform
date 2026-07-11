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
- Requirement headings are stable ids; a change's *Spec Deltas* reference them.
