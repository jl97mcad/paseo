# Runner transport for helper completions

Status: accepted. Extends [Durable helper observations and consumers](helper-completion-consumers.md).

## Decision

A runner consumes `helper_completions` through the existing top-level Agent MCP endpoint, `POST /mcp/agents` without `callerAgentId`. It authenticates with the daemon password, the host identity the CLI already uses (`PASEO_PASSWORD`), sent as `Authorization: Bearer <password>`. The route already accepts that bearer, and the tool already gives the top-level client authority over any parent. No daemon code, API, schema or credential changes.

`packages/server/src/server/agent/agent-mcp.e2e.test.ts` characterizes this existing behavior (it passed without any source change, so it is not a red test) with a real password-protected daemon and an HTTP MCP client. It checks:

- Replay carries source evidence: native event and turn IDs, role, source time, provenance and `gaps`.
- Consumers acknowledge independently.
- A verification is a separate linked entry.
- A missing or wrong password, and the local credential, get 401.

## Limits of the seam

- **Local-credential-only hosts have no path.** The CLI also admits itself with `$PASEO_HOME/local-credential` on WebSocket and `/api/status`. `/mcp/agents` does not accept it, so a runner that holds only that file cannot reach the journal. Accepting it on this route would widen the route's authentication, so it needs an owner decision from whoever owns daemon auth. It was not done here.
- **No password configured:** the route is open and any local process may call it. That is the existing daemon trust model.
- **Agents no longer hold the capability token.** They hold a per-agent token that only works with their own `callerAgentId` ([Caller identity](helper-completion-caller-identity.md)). The capability token and the password remain host credentials.
- **Failed-write no-loss is unchanged.** The journal still cannot report an observation that was never persisted and whose marker was lost.
- **Daemon-local.** The runner must reach the daemon that owns the journal. There is no cross-host replay.
