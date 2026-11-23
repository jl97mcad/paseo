# Host replay for helper observations

Status: accepted. Extends [Durable helper observations and consumers](helper-completion-consumers.md) and [Runner transport](helper-completion-runner-transport.md). Supersedes the consumers ADR's registration limit; see [Limits](#limits).

## Decision

A top-level MCP client uses `host_helper_completions` on the existing authenticated host route to replay retained observations across parents. The journal's global sequence supplies ordering and the parent ID comes from each recorded observation. This discovers parents with retained observations, including provider subagents that managed-agent inventory does not enumerate.

Replay registers a named host consumer, returns at most 100 unacknowledged events and reports the global retention gap. Acknowledge each delivery only after its effect is durable; replay after a crash can redeliver. Accept a gap only after recording the loss of coverage, then continue replay. Keep the consumer registered and poll again after an empty page: a quiet or archived parent can produce later observations. Release only when retiring the workflow, because release forgets its acknowledgements. Host consumers are a separate kind of registration from per-parent consumers, with the same name and per-name rules.

## Limits

The consumers ADR's single limit of 256 `(parent, consumer)` registrations no longer holds. Parent-scoped registrations share one journal-wide pool of 256 across all parents, with non-empty parent IDs of at most 128 characters. Host registrations have their own pool of 256, so the journal can hold 512 registrations. Neither pool can exhaust the other. Host scope is a distinct type in the store; the per-parent methods reject an empty parent ID, so only the `*Host` methods reach host registrations. Release retired host consumers to reclaim the pool.

A retained listing is not a historical inventory. Compaction can remove all observations of a parent; observations before journaling, events never delivered by providers, and failed appends with lost failure markers may never appear. A gap is conservative and does not identify what was lost. Neither an empty page nor an archived parent establishes source drain or no loss. Source IDs and times are present only when supplied by the source. The journal's write and crash limitations remain those in the consumers ADR.

Host replay is withheld from a connection that declares `?callerAgentId=`. The declaration is authenticated by the agent's per-agent token ([Caller identity](helper-completion-caller-identity.md)), so an agent cannot omit it to reach host replay. Holders of the daemon password or capability token are host clients. This adds no cross-host transport.
