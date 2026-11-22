# Provider subagent completion journal

Status: superseded by [Durable helper observations and consumers](helper-completion-consumers.md)

## Decision

Persist terminal provider-subagent updates in an append-only JSON journal under `$PASEO_HOME`. Apply each update to the in-memory descriptor projection after the journal write, then acknowledge its sequence. On parent resume, replay journal entries to rebuild the projection. Reapplying an entry is safe because descriptor upserts are idempotent.

Completion evidence records the managed parent, provider, native helper ID, optional native parent, optional source time, outcome, provenance, and explicit gaps. Source event identity, native turn identity, and helper role remain unknown until a provider supplies them. The journal does not claim to detect completions the provider never reports.

## Consequences

The daemon can recover observed helper completions after a crash or restart. Delivery is at least once across the write/apply/ack boundary. The JSON journal grows with terminal updates and currently has no compaction policy.
