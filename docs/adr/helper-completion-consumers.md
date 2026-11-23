# Durable helper observations and consumers

Status: accepted. Supersedes [Provider subagent completion journal](helper-completions.md).

## Decision

Use the existing atomic JSON storage and the daemon's single journal writer. Persist provider helper observations before applying the descriptor, and managed child turn terminals before publishing lifecycle settlement to finish subscribers. Closing a running runtime without a source terminal records an unknown observation. A running helper at parent turn end records an unknown observation, since it may still be working. A final assistant report is not required.

Native source identity and normalized turn routing identity are separate. Only provider-supplied event IDs or native turn IDs deduplicate observations, scoped to parent, provider, helper and provenance. Native turn deduplication also includes outcome. Missing native identity produces a new delivery with an explicit gap, even if timestamps match. The delivery UUID and sequence are journal identities; neither proves provider identity. `recordedAt` is daemon observation time, not source time. Providers may supply native event ID, native turn ID, source time and role. Codex forwards its native turn completion ID; adapter-generated turn IDs never stand in for it.

Completion evidence remains immutable. A verification is another journal entry linked by `verifiesSequence`, with its own outcome, evidence and observation time. It does not change the observed completion or the descriptor.

## Consumer API

The local MCP tool `helper_completions` takes `parentAgentId` and an operation:

| Operation     | Other arguments                   | Result                                         |
| ------------- | --------------------------------- | ---------------------------------------------- |
| `replay`      | `consumer`                        | Unacknowledged `events` and nullable `gap`     |
| `acknowledge` | `consumer`, `sequence`            | Acknowledges that individual retained delivery |
| `accept_gap`  | `consumer`, `discardedThrough`    | Accepts the exact reported retention boundary  |
| `release`     | `consumer`                        | Removes that durable consumer registration     |
| `verify`      | `sequence`, `outcome`, `evidence` | Adds a separately linked verification          |

Authority follows the MCP caller identity. An agent-scoped caller (the injected `?callerAgentId=` connection) may only pass its own agent ID as `parentAgentId`; any other parent is rejected for every operation, so agents cannot read, acknowledge, release, accept gaps for, or verify another parent's journal. The top-level MCP client has no caller identity and may act for any parent; this is the runner and CLI path, and it also performs verification. See [Runner transport](helper-completion-runner-transport.md) for the authentication that reaches it. Every connection shares one daemon bearer token and the caller ID is declared in the URL, so this separates cooperating agents and does not defend against a holder of the token who impersonates another agent. Per-agent credentials would be a separate change.

Use a stable, unique consumer name for each workflow. Replay registers it durably. Consumer names are scoped by parent; `projection` is reserved for legacy acknowledgement state. Acknowledging a later sequence does not acknowledge earlier deliveries. Replays may redeliver across a crash between apply and acknowledgement, so consumers must make effects idempotent using `deliveryId`. Only acknowledge after the effect is durable. Releasing a consumer intentionally forgets its acknowledgements.

The tool is additive and local to the contacted daemon. Existing WebSocket schemas are unchanged. Unknown observations and verification entries stay in this API; the existing descriptor protocol cannot express an unknown terminal outcome. Every session registration rebuilds the descriptor projection from retained known provider completions, regardless of delivery acknowledgements. Managed child observations are consumed through the journal API, not projected as provider subagents. The descriptor projection has no delivery cursor: replaying all retained known completions is idempotent. Exhausting external consumer registrations cannot block journal production or session registration.

## Retention and limits

Keep the newest 10,000 journal entries, subject also to a 16 MiB combined compact JSON event-payload limit. Each new entry is at most 64 KiB. Writes atomically compact the oldest entries, including unacknowledged entries; sequence numbers never restart. There are at most 256 durable `(parent, consumer)` registrations, with names and parent IDs bounded to 128 characters. Cursor metadata is additional bounded space: each registration can hold at most one acknowledgement per retained entry. Release retired consumers to reclaim registrations.

Replay reports `gap.discardedThrough` when a consumer's contiguous cursor predates compaction. This global boundary conservatively reports a possible gap even if the removed entries belonged to another parent. Acknowledgements alone cannot accept that gap. Verification of an expired completion fails explicitly. Retention also bounds deduplication and descriptor reconstruction: neither reaches entries already compacted away.

Snapshot rewriting remains linear in retained events and cursor metadata for each append or acknowledgement. This bounds growth, not latency. Move to segmented storage if this ceiling is too costly; no throughput or power-loss durability claim is made. The shared atomic writer uses rename without fsync. It protects daemon-process crash recovery after a successful write, not sudden power loss.

An observation append failure is logged with the parent and helper identity as not durably accepted. Provider descriptor projection and managed terminal processing continue. The daemon keeps in-memory failure markers for the 256 most recently failing parents. Each parent retains its newest 16 details, each limited to 256 UTF-16 code units plus an explicit truncation suffix. Older details become a count of further observations not durably accepted; the count saturates at the maximum safe integer and then reports a lower bound. These markers add less than 32 KiB of serialized gaps, so repeated failures cannot exhaust the 64 KiB entry limit on their own. The next successful append for that parent carries its markers in `gaps` and clears them from memory. Evicting a parent sets a process-wide overflow warning on every later new observation: failed observations for evicted parents may be missing. This warning stays set for the process lifetime, because later appends cannot establish full coverage for evicted parents. If no later append succeeds, the marker is not available through replay. A process crash before that later append loses the markers, omitted counts, overflow warning and failed observations, so consumers cannot detect that loss. A source event that never arrives cannot be reconstructed. An abrupt process death before any observation is persisted can leave no journal entry; unknown observations require an actual parent terminal or runtime-close observation. Older managed completions from before journaling was enabled are not inferred from presentation state.
