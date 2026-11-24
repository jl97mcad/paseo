# Inferred outcome corrections

Status: accepted. Supersedes [Adapter-inferred helper outcomes](helper-completion-inferred-outcomes.md).

## Decision

Adapter-inferred terminals retain their provenance and inferred reason without a source event time. Restore them in the helper descriptor alongside provider-stream completions. Managed lifecycle records do not represent provider helpers.

Deduplicate an OpenCode snapshot against the latest retained completion for the same parent, provider and helper when that latest completion is an adapter-inferred snapshot with the same outcome. A native terminal or an inferred non-snapshot does not suppress the next snapshot, even with the same outcome. A changed outcome is a new observation. Compaction can discard the earlier snapshot, so a later read can be recorded again. Snapshot records carry a `snapshot observation` gap to distinguish them from other inferred outcomes.

Claude reports whether its foreground cleanup followed a canceled turn, failed turn or closed session. OMP reports a canceled or failed turn, process loss or provider idle deadline. These reasons describe the cleanup trigger, not a provider-reported helper outcome. OMP projects every such cleanup as `canceled`, including failed turns, process loss and provider idle deadlines. Consumers looking for failed OMP helpers must also inspect the inferred reason; filtering only for outcome `failed` misses these cases. Claude projects process loss as `failed`. OpenCode marks idle children found at startup and terminal child status read during gap repair as snapshots.

Codex interrupt-canceled subagents synthesized from tool-call status still enter as `provider-stream`. Claude transcript replay does not journal its inferred terminals. Neither has a fabricated native identity. Idle OpenCode children can have failed or stopped; the adapter currently projects them as completed. Retention, failed writes and missing provider history limit coverage as described in [Helper completion coverage warning](helper-completion-coverage.md).
