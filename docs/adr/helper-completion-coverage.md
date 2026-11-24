# Helper completion coverage warning

Status: accepted. Extends [Durable helper observations and consumers](helper-completion-consumers.md) and supersedes its replay result shape, its process-lifetime overflow warning, and its statement that failure markers are visible only on a later observation.

Replay reports `coverageUncertain` for both parent and host consumers when an observation append has failed. The warning is host-wide and permanent: it does not identify a missing helper, time or parent, and does not block replay. The journal carries it on the next successful write, including a shutdown flush. In memory it is visible immediately to registered consumers. Overflow uncertainty is preserved across restarts after a successful write, even though per-parent details remain bounded and memory-only. A consumer must not treat `false` as proof of complete coverage.

Best-effort manager flush logs a failed journal write so deletion can still remove agent storage. A failed shutdown flush is logged and shutdown continues; it cannot persist the warning while storage remains unavailable. The runner intake must read and record `coverageUncertain` separately from the retention `gap`; this repository does not implement that intake. An abrupt exit, clean shutdown with a failed flush, or store recreation before a successful write can lose the warning. The warning also cannot detect events never delivered by a provider, failures before recording the marker, or power loss. The atomic writer does not fsync. A successful later write preserves uncertainty, not the lost observation. The existing per-parent failure details remain in memory until carried by a later observation, and can still be lost on restart.

## Provider history and recovery limits

See [Codex helper completion history recovery](helper-completion-codex-recovery.md) for the startup scan and its limits.
