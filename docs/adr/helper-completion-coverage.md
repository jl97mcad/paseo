# Helper completion coverage warning

Status: accepted. Extends [Durable helper observations and consumers](helper-completion-consumers.md) and supersedes its replay result shape, its process-lifetime overflow warning, and its statement that failure markers are visible only on a later observation.

Replay reports `coverageUncertain` for both parent and host consumers when an observation append has failed. The warning is host-wide and permanent: it does not identify a missing helper, time or parent, and does not block replay. The journal carries it on the next successful write, including a shutdown flush. In memory it is visible immediately to registered consumers. Overflow uncertainty is preserved across restarts after a successful write, even though per-parent details remain bounded and memory-only. A consumer must not treat `false` as proof of complete coverage.

Best-effort manager flush logs a failed journal write so deletion can still remove agent storage. A failed shutdown flush is logged and shutdown continues; it cannot persist the warning while storage remains unavailable. The runner intake must read and record `coverageUncertain` separately from the retention `gap`; this repository does not implement that intake. An abrupt exit, clean shutdown with a failed flush, or store recreation before a successful write can lose the warning. The warning also cannot detect events never delivered by a provider, failures before recording the marker, or power loss. The atomic writer does not fsync. A successful later write preserves uncertainty, not the lost observation. The existing per-parent failure details remain in memory until carried by a later observation, and can still be lost on restart.

## Provider history is not a replay source for lost observations

Status: accepted. Checked at 90b78cf against the Codex adapter. Claude, OpenCode, Pi and OMP history were not traced.

No traced code path rebuilds a failed observation from provider history after a restart, so the coverage warning stays the only signal. Only Codex was traced; the other providers are unverified, and this does not claim they cannot replay. For Codex, three facts rule it out:

- Nothing reads the history for this. A daemon restart with durable timeline rows does not re-read provider history (`historyPrimed`). Only legacy or imported chats and a rewind do, and both feed the in-memory projection, not the journal (`forceHydrateTimelineFromLegacyProviderHistory`).
- Codex history lacks the identity the journal needs. The replay upsert at `codex-app-server-agent.ts` (`loadPersistedSubAgentHistories`) carries the child thread ID, role, parent and a status taken from the parent tool call. It carries no native turn ID, event ID or source time. The journal records such an entry with those gaps and cannot deduplicate it (`completion-store.ts` `append`), so each hydrate would add a new delivery for the same completion.
- A recorded outcome would be inferred. The status comes from the parent's card, not from a terminal event for the helper's turn, so journaling it would present presentation state as provider evidence, which [Durable helper observations and consumers](helper-completion-consumers.md#retention-and-limits) rules out.

A journal-from-history path for Codex needs it to expose the helper's terminal turn ID and time first. Until then, do not add one, and do not add a coverage epoch: a start-time epoch needs the same write that failed, and a global one reports uncertainty for every parent after any unclean stop.
