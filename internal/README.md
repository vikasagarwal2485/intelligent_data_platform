# Operator memory

This directory is the repository's persistent handoff for maintainers and coding
agents. It records the working map, operational lessons and verified state so the
next session can resume with context. It is tracked in Git and excluded from the
application Docker build context.

| File | Use |
|---|---|
| [repository.md](repository.md) | Current architecture, ownership and source map |
| [decisions.md](decisions.md) | Active constraints and links to authoritative ADRs |
| [runbook.md](runbook.md) | Workspace setup, checks, CI and operational pitfalls |
| [status.md](status.md) | Dated handoff, evidence and outstanding work |

Start with [../AGENTS.md](../AGENTS.md) and the status file. Read the runbook for
the commands involved in the task. Check the worktree and implementation before
acting on a remembered claim. Product requirements belong in
[OpenSpec](../openspec/specs/), architecture decisions in the
[ADR log](../docs/decisions/decision-log.md), and release-facing changes in
[release notes](../RELEASE_NOTES.md).

After material work, update the appropriate memory file and the status handoff in
the same change. Record what changed, why it matters, source paths and exactly
what was verified. Distinguish passed, skipped, unexecuted and failed checks.
Replace obsolete guidance when the source changes; keep useful history in Git.
Avoid transcripts and duplicate specifications. Link to the authoritative source.

Never put secrets, tokens, raw PII or real subscriber records here. Local `.env`
files are ignored, and the application does not automatically load them: pass
`IDP_*` environment variables explicitly. Repository dev credentials are examples
for disposable infrastructure, not production configuration.
