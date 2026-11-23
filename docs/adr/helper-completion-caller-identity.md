# Caller identity for helper completions

Status: accepted. Narrows the shared-bearer limit in [Durable helper observations and consumers](helper-completion-consumers.md) and [Host replay](helper-completion-host-replay.md).

## Decision

Every agent's injected MCP connection carries its own bearer token, `HMAC-SHA256(capabilityToken, "paseo-agent-mcp:v1:" + agentId)`. `/mcp/agents` accepts that token only when `?callerAgentId=` names the same agent (`packages/server/src/server/auth.ts`, `packages/server/src/server/agent/agent-mcp-token.ts`).

| Presented bearer                    | Declared identity | Result                                                             |
| ----------------------------------- | ----------------- | ------------------------------------------------------------------ |
| Agent A's token                     | `callerAgentId=A` | Agent scope, own journal only                                      |
| Agent A's token                     | `callerAgentId=B` | 401                                                                |
| Agent A's token                     | none              | 401, so no `host_helper_completions` and no other parent's journal |
| Daemon password                     | none              | Host scope (the runner and CLI path, unchanged)                    |
| Daemon password or capability token | any               | Accepted as before                                                 |

The agent-scope checks inside `helper_completions` and `host_helper_completions` are unchanged. They now rest on a verified identity instead of a URL claim.

No new credential is stored or issued. The token is derived on demand from the per-daemon-run capability token, so it changes with each daemon start, as that token already did. HMAC is one-way: an agent cannot recover the capability token or a sibling's token from its own.

## Compatibility

- Protocol and tool schemas are unchanged. The only wire difference is the value of the `Authorization` header the daemon writes into an agent's launch config, which is regenerated at every launch.
- The capability token and the daemon password keep their current acceptance. Nothing is widened.
- A third-party client that sent the capability token with `callerAgentId` still works; only agents lose the shared token.

## Limits

- **No password configured:** the route is open and nothing is authenticated, so `callerAgentId` is a plain claim there. This is the existing local trust model.
- **Same-user processes:** an agent runs as the daemon's OS user. It can read what that user can read (the daemon password if exported in its environment, `$PASEO_HOME/local-credential`, the launch config of another agent). Per-agent tokens stop a token holder from crossing agents over MCP. They do not sandbox a hostile local process.
- **Bearer replay:** a token is a bearer secret. Anyone who reads agent A's launch config can act as A until the daemon restarts. Tokens are not revoked when an agent is archived.
- **Host authority is still one shared secret:** the password (or the capability token) grants every parent's journal. Separate runner credentials would be a separate change to daemon permissions ([permissions](../permissions.md)).
- **Local-credential-only hosts** still have no path to the journal, as recorded in [Runner transport](helper-completion-runner-transport.md).
