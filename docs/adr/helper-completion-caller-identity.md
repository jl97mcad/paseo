# Caller identity for helper completions

Status: accepted. Supersedes the shared-bearer limit statements in [Durable helper observations and consumers](helper-completion-consumers.md), [Host replay](helper-completion-host-replay.md) and [Runner transport](helper-completion-runner-transport.md). Those three keep their other decisions; read each shared-bearer or capability-token limit there as replaced by this ADR.

## Decision

Every agent's injected MCP connection carries its own bearer token, `HMAC-SHA256(capabilityToken, "paseo-agent-mcp:v1:" + agentId)`. `/mcp/agents` accepts that token only when `?callerAgentId=` names the same agent (`packages/server/src/server/auth.ts`, `packages/server/src/server/agent/agent-mcp-token.ts`).

| Presented bearer                    | Declared identity | Result                                                             |
| ----------------------------------- | ----------------- | ------------------------------------------------------------------ |
| Agent A's token                     | `callerAgentId=A` | Agent scope, own journal only                                      |
| Agent A's token                     | `callerAgentId=B` | 401                                                                |
| Agent A's token                     | none or empty     | 401, so no `host_helper_completions` and no other parent's journal |
| Daemon password                     | none              | Host scope (the runner and CLI path, unchanged)                    |
| Daemon password or capability token | any               | Accepted as before                                                 |

The agent-scope checks inside `helper_completions` and `host_helper_completions` are unchanged. They now rest on a verified identity instead of a URL claim.

`?callerAgentId=` with an empty value declares no identity. The route treats it as undeclared, so it needs a host credential and gets host scope, the same as omitting the parameter.

No new credential is stored. The token is derived per launch from the per-daemon-run capability token, so it changes with each daemon start, as that token already did. HMAC is one-way: an agent cannot recover the capability token or a sibling's token from its own.

## Host credentials stay out of child environments

The per-agent token only matters if the child cannot also present a host credential. The daemon took its password from `PASEO_PASSWORD` in deployment mode (Docker, a service manager), and every external child inherited it through `process.env`, so an agent, its MCP servers or a terminal could send it as the host bearer and ignore its scoped token. `createExternalProcessEnv` (`packages/server/src/server/paseo-env.ts`) now drops `PASEO_PASSWORD` from the inherited environment. It is the single constructor behind provider launches, the OMP MCP bridge, terminals, worktree setup commands and the self-node command.

- An explicit overlay still sets it: a provider profile's `env` that names `PASEO_PASSWORD`, for an agent that should reach another daemon, is the operator's choice and is passed through.
- The CLI inside an agent still connects. Without the password it falls back to `$PASEO_HOME/local-credential` for local targets. Remote targets need `password=` in the host URI.
- Managed launches already remove `PASEO_PASSWORD` from the daemon's own environment ([`daemonLaunchEnvironment`](../../packages/server/src/server/config-environment.ts)); the daemon then reads the hash from `config.json`.

## Compatibility

- Protocol and tool schemas are unchanged. The only wire difference is the value of the `Authorization` header the daemon writes into an agent's launch config, which is regenerated at every launch.
- The capability token and the daemon password keep their current acceptance. Nothing is widened.
- A third-party client that sent the capability token with `callerAgentId` still works; only agents lose the shared token.

## Limits

- **No password configured: no caller isolation.** The route accepts every request, so any process that can reach the listener can declare any `callerAgentId`, or none and get host scope, with or without a token. `isAgentMcpRequestAuthorized` returns true unconditionally there (`auth.test.ts` records this). Per-agent tokens are still injected but nothing checks them. Set a password to get the isolation in this ADR.
- **Same-user processes:** an agent runs as the daemon's OS user. It can read what that user can read: `$PASEO_HOME/local-credential`, the launch config of another agent, and the daemon's own `/proc/<pid>/environ`. Scrubbing the environment closes inheritance and leaves deliberate reads open. Per-agent tokens stop a token holder from crossing agents over MCP. They do not sandbox a hostile local process.
- **The terminal worker still holds the password in its own environment.** The daemon starts it with `process.env`; terminal children are scrubbed, the worker process is not.
- **Bearer replay:** a token is a bearer secret. Anyone who reads agent A's launch config can act as A until the daemon restarts. Tokens are not revoked when an agent is archived.
- **Host authority is still one shared secret:** the password (or the capability token) grants every parent's journal. Separate runner credentials would be a separate change to daemon permissions ([permissions](../permissions.md)).
- **Local-credential-only hosts** still have no path to the journal, as recorded in [Runner transport](helper-completion-runner-transport.md).
