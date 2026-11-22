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

Use a stable, unique consumer name for each workflow. Replay registers it durably. Consumer names are scoped by parent; `projection` is reserved for daemon projection delivery. Acknowledging a later sequence does not acknowledge earlier deliveries. Replays may redeliver across a crash between apply and acknowledgement, so consumers must make effects idempotent using `deliveryId`. Only acknowledge after the effect is durable. Releasing a consumer intentionally forgets its acknowledgements.

The tool is additive and local to the contacted daemon. Existing WebSocket schemas are unchanged. Unknown observations and verification entries stay in this API; the existing descriptor protocol cannot express an unknown terminal outcome. Every session registration rebuilds the descriptor projection from retained known provider completions, regardless of delivery acknowledgements. Managed child observations are consumed through the journal API, not projected as provider subagents.

## Retention and limits

Keep the newest 10,000 journal entries, subject also to a 16 MiB combined compact JSON event-payload limit. Each new entry is at most 64 KiB. Writes atomically compact the oldest entries, including unacknowledged entries; sequence numbers never restart. There are at most 256 durable `(parent, consumer)` registrations, with names and parent IDs bounded to 128 characters. Cursor metadata is additional bounded space: each registration can hold at most one acknowledgement per retained entry. Release retired consumers to reclaim registrations.

Replay reports `gap.discardedThrough` when a consumer's contiguous cursor predates compaction. This global boundary conservatively reports a possible gap even if the removed entries belonged to another parent. Acknowledgements alone cannot accept that gap. Verification of an expired completion fails explicitly. Retention also bounds deduplication and descriptor reconstruction: neither reaches entries already compacted away.

Snapshot rewriting remains linear in retained events and cursor metadata for each append or acknowledgement. This bounds growth, not latency. Move to segmented storage if this ceiling is too costly; no throughput or power-loss durability claim is made. The shared atomic writer uses rename without fsync. It protects daemon-process crash recovery after a successful write, not sudden power loss.

A source event that never arrives, or whose journal write fails, has not been durably accepted. Recovery cannot reconstruct an unreported outcome. An abrupt process death before any observation is persisted can leave no journal entry; unknown observations require an actual parent terminal or runtime-close observation. Older managed completions from before journaling was enabled are not inferred from presentation state.
