# Documentation and OpenSpec guide

The repository keeps current requirements in `openspec/` and explanatory documents
in `docs/`. Start with the [product and deployment guide](../README.md),
[repository instructions](../AGENTS.md) and [operator memory](../internal/README.md).

## Sources

| Location | Purpose |
|---|---|
| [architecture_and_implementation_plan.md](architecture_and_implementation_plan.md) | Reviewed architecture and original plan; some target designs are superseded |
| [project.md](project.md) | Current project context and conventions |
| [decisions/decision-log.md](decisions/decision-log.md) | Accepted ADRs; later entries amend earlier choices |
| [../openspec/specs/](../openspec/specs/) | Current capability requirements and testable scenarios |
| [../openspec/changes/](../openspec/changes/) | Active changes; inspect before starting new work |
| [../openspec/changes/archive/](../openspec/changes/archive/) | Completed proposals and task history |
| [roadmap.md](roadmap.md) | Historical phases 0–15, already archived |
| [traceability.md](traceability.md) | Generated requirement → scenario → phase → code mapping |
| [triggering.md](triggering.md) | Current manual and Airflow Spark stage triggers |
| [../RELEASE_NOTES.md](../RELEASE_NOTES.md) | Unreleased work and development baseline notes |

## Change workflow

1. Read the relevant capability spec and current ADRs. Inspect the code and tests
   to establish the actual behavior. Use `openspec list --json` if the CLI is
   installed; the files under `openspec/changes/` remain directly readable.
2. For a substantial behavior change, create a proposal, spec deltas and tasks in
   `openspec/changes/<change>/` using the existing OpenSpec tooling. Do not restart
   archived phases. Housekeeping can follow the existing requirements directly.
3. Implement the scoped tasks and update their checkboxes with completed work.
   Preserve tenant isolation, privacy, stage boundaries and contract traceability.
4. Run `make check`, the relevant tests and required integrations. Every touched
   requirement's scenarios must be satisfied before claiming the change complete.
5. Update current specs and the ADR log when the behavior or decision changes,
   then archive the completed change under `openspec/changes/archive/`.
6. Regenerate traceability after spec/proposal edits; update release notes and the
   operator-memory handoff with evidence and remaining limits.

## Requirement conventions

Requirements use `SHALL` and have one or more `WHEN`/`THEN` scenarios. Each has a
stable ID: `REQ-<CAP>-NN` for requirements and `SCN-<CAP>-NN.M` for scenarios.
Headings embed those IDs; proposals cite the requirements they implement.

| Code | Capability | Code | Capability |
|---|---|---|---|
| FND | foundation | ML | ml-models |
| SYN | synthetic-data | SRV | serving-api |
| BRZ | bronze-ingestion | STR | streaming-kafka |
| SLV | silver-curation | PLT | platform-deployment |
| GLD | gold-features | ORC | orchestration |
| MCP | mcp-analytics | | |

Regenerate the matrix from the repository root:

```bash
python docs/gen_traceability.py
```

The generator retains historical code references and marks removed paths. It
includes active and archived phase proposals; it does not replace scenario tests.
