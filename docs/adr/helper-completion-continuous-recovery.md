# Continuous Codex helper recovery

Status: accepted. Supersedes the startup-only scheduling and limits in [Codex helper completion history recovery](helper-completion-codex-recovery.md). Its identity and evidence rules still apply.

## Decision

Scan retained Codex parents sequentially in the background. Pause for one second after every 25 parents, then refresh the parent list and repeat five minutes after each scan. Each attempt has a 30-second timeout. A durable cursor rotates through sorted parent IDs, including failed attempts, so later scans retry them without stalling other parents. Shutdown aborts the scan and the pause.

A cursor write failure ends the scan and is logged; the next scan retries after the five-minute pause. An interruption can repeat work. Each cursor advance rewrites the journal snapshot, up to 10,000 events or 16 MiB plus consumer metadata. Live and recovered identities share the newest 10,000 identity slots, so a busy host can exhaust this window quickly. The discarded count warns consumers that a later scan can redeliver an older observation.

The scan still cannot establish full coverage of source history. The original decision describes its source evidence and remaining limits.
