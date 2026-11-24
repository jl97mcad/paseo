# Adapter-inferred helper outcomes

Status: superseded by [Inferred outcome corrections](helper-completion-inferred-corrections.md). Extends [Durable helper observations and consumers](helper-completion-consumers.md) and supersedes its implicit rule that every upsert reaching the journal from a provider adapter is recorded as `provider-stream`. Identity, retention and replay rules there are unchanged.

## Decision

An adapter that concludes a helper's terminal outcome without receiving a terminal event for it marks that upsert with an internal `inferred` reason. The journal records it with provenance `adapter-inferred`, the outcome the adapter chose, a gap `outcome inferred by adapter: <reason>`, and no source event time. A consumer can tell it from a native terminal by provenance and gap.

Deduplication scope includes provenance, so a native terminal that arrives later is a separate entry and the inferred one is not rewritten. The descriptor projection is unchanged: status, `updatedAt` and `completionEvidence` follow the same path as before, with the new provenance and gap.

Marked today:

- Claude: `cancelRunningForegroundTasks` (turn canceled while a foreground task was running) and `failRunningTasks` (Claude process lost). A later `task_notification` can still correct either.
- OMP: `terminalizeRunning` on interrupt or reset. OMP lifecycle and progress statuses are native and stay `provider-stream`.
- OpenCode: a child found idle in a status snapshot at session start, and a child outcome read from session state on gap repair. Both are `snapshot` reads.

A `snapshot` upsert is a re-read of state, not a transition. It is journaled at most once per parent, provider and helper, and not at all once that helper has any completion on record. Without this, every session resume and every gap repair appended another entry with a fresh delivery ID.

## What is not decided

- The adapter's own timestamp is not recorded as `sourceEventTime`. No provider supplied one for these outcomes, and none is invented.
- No source or turn identity is fabricated. Inferred entries without a snapshot still get a fresh delivery per append, with the `deduplication identity unavailable` gap.
- Claude replay from the on-disk transcript derives a terminal from a stop reason or interrupt placeholder. That path applies events to descriptors without journaling, so it is not marked. If a recovery scan ever journals it, it must be marked the same way.
- An idle OpenCode child may have failed or stopped. `completed` stays the projected status; the journal now says it was inferred.
- This adds no no-loss claim. Abrupt exit, failed writes and provider history that no longer exists remain as described in [Helper completion coverage warning](helper-completion-coverage.md).
