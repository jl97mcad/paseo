# Helper completion current projection

Status: accepted. Supersedes the restoration ordering in [Inferred outcome corrections](helper-completion-inferred-corrections.md). Extends [Codex helper completion history recovery](helper-completion-codex-recovery.md).

## Decision

Keep delivery order and current state separate. Recovery retains each newly discovered terminal and its native identity in the delivery journal. A completed history read records the last observed terminal per helper as the current projection only when the previously projected terminal also appears in that history. If there is no prior projection, the history may establish one. A live state update during the read prevents that read from changing the projection. A running observation clears the terminal projection. A later read can try again.

An incomplete history cannot establish the relation between an existing terminal and an unobserved turn. Retain the last known projection and keep coverage uncertain. If the latest source observation is no longer retained, leave the current projection unknown. Neither delivery sequence nor opaque native IDs establish source order. A failed history read does not commit a projection.

A journal predating projection metadata restores a helper only when it has one retained terminal. Multiple terminals have unknown order; keep them available for replay and report uncertain coverage. Current-state markers are bounded to 10,000. When markers are discarded, `projectionDiscarded` reports the count and coverage becomes uncertain. Missing markers no longer establish a projection from history, since a discarded running or later terminal cannot be ruled out. Delivery retention and identity limits remain independent.

Read retained state before applying a forced provider history refresh. Explicit statuses from the fresh history replace the retained status and clear stale completion evidence when they supply none. Load the journal before publishing a newly registered session; only a missing journal starts empty. A malformed or unreadable journal fails registration and closes the unregistered session.

## Limits

A successful history scan cannot prove that the provider retained all turns. A stored projection describes the latest ordering established by observed history and live updates, not a guarantee of complete source history. A running marker does not preserve the running descriptor across restart; it prevents an older terminal from being presented as current. The bounded journal and identity window can remove the evidence needed to restore a projection. These limits do not change replay observations or their native identities.
