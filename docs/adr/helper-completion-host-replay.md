# Host replay for helper observations

Status: accepted. Extends [Durable helper observations and consumers](helper-completion-consumers.md) and [Runner transport](helper-completion-runner-transport.md).

## Decision

A top-level MCP client uses `host_helper_completions` on the existing authenticated host route to replay retained observations across parents. The journal's global sequence supplies ordering and the parent ID comes from each recorded observation. This discovers parents with retained observations, including provider subagents that managed-agent inventory does not enumerate.

Replay registers a named host consumer, returns at most 100 unacknowledged events and reports the global retention gap. Acknowledge each delivery only after its effect is durable; replay after a crash can redeliver. Accept a gap only after recording the loss of coverage, then continue replay. Keep the consumer registered and poll again after an empty page: a quiet or archived parent can produce later observations. Release only when retiring the workflow, because release forgets its acknowledgements. Host consumers have a separate limit of 256 registrations from the existing per-parent limit.

## Limits

A retained listing is not a historical inventory. Compaction can remove all observations of a parent; observations before journaling, events never delivered by providers, and failed appends with lost failure markers may never appear. A gap is conservative and does not identify what was lost. Neither an empty page nor an archived parent establishes source drain or no loss. Source IDs and times are present only when supplied by the source. The journal's write and crash limitations remain those in the consumers ADR.

Agent-scoped MCP callers cannot invoke host replay. The existing shared bearer and caller identity limitations still apply; this adds no authentication or cross-host transport.
