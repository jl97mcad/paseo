import { createHmac } from "node:crypto";

/**
 * Per-agent credential for the Agent MCP endpoint, derived from the daemon-run
 * capability token. HMAC is one-way, so an agent holding its token cannot
 * recover the capability token or another agent's token.
 */
export function deriveAgentMcpToken(capabilityToken: string, agentId: string): string {
  return createHmac("sha256", capabilityToken)
    .update(`paseo-agent-mcp:v1:${agentId}`)
    .digest("hex");
}
