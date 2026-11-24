# Codex helper completion history recovery

Status: accepted. Extends [Helper completion coverage warning](helper-completion-coverage.md) and supersedes the statement in [Durable helper observations and consumers](helper-completion-consumers.md) that source events which never arrive cannot be reconstructed.

## Decision

At daemon startup, scan retained Codex parent records in the background, one history session at a time. This does not load the agent runtime or change the durable projection. Each parent history, including child timelines and image attachments, is still projected in memory. For each child, accept a terminal outcome only when the legacy history binds the outcome and native turn ID in the same turn. Use that turn's completion time when present. The parent card and settled timeline activity are presentation, not completion evidence.

Persist native identities for live and recovered observations independently of delivery retention, in the same atomic journal write as each observation. Repeated scans do not duplicate retained identities, including after delivery compaction. The identity list holds the newest 10,000 identities. Consumer replay reports `recoveryIdentitiesDiscarded` when older identities are removed; a later scan can redeliver one. Live observations compacted before this identity tracking was added can also be redelivered. The existing journal retention gap still applies to deliveries.

## Limits

The scan marks host coverage uncertain before reading history. It cannot prove that Codex retained every source turn. Paginated `thread/items/list` rows bind item times to turn IDs but do not bind those times and outcomes to the same turn. The separate latest-turn status probe does not supply that association. Unbound statuses are not journaled as terminal observations. The scan is sequential and attempts at most 25 parents per startup, each for 30 seconds. A durable cursor rotates through sorted parent IDs on later starts; failed attempts advance too, so they are retried on a later rotation. A failed cursor write or interruption can repeat work. The scan logs incomplete reads and skipped parents. Codex child discovery visits at most 100 threads per parent including the root, leaving at most 99 children. History deletion, provider failures, abrupt exit, or a failed coverage write can still lose evidence. The atomic writer does not fsync.

A per-parent durable start marker requires an independent durable write and has its own failure window. A global marker can report uncertainty for unrelated parents. Neither proves no loss. Other providers have not been traced for this recovery path.

The focused regression in `agent-manager-stream-coalescing.test.ts` uses a fake Codex app server and reopened journal to check legacy turns, repeated scans, compaction and rotating batches. It does not exercise the production history connection, daemon restart, shutdown, API or browser, and is not a provider crash or power-loss experiment.

Before this change, `agent-manager.ts` set `historyPrimed` from durable timeline rows and skipped provider history during registration. Legacy hydration and rewind projected history without journaling it. Those paths are not the only history reads: `readArchivedHistory` stages history for `streamHistory`, and `readThreadHistory` also serves `ensureHistoryIndexed`. `dispatchSessionEvent` and `journalTerminalObservations` were the journal producers. This was a code trace, not a crash or no-loss test. The in-process append and flush failure cases are in `agent-manager-stream-coalescing.test.ts`.

Codex child status can come from the child's latest provider turn through `restoreCollabAgentCallFromChildHistory`; `settleHistoricalSubAgentActivity` instead settles presentation activity. The previous replay upsert did not forward the child turn ID. Claude, OpenCode, Pi, OMP, Copilot and ACP/Cursor history were not traced.
