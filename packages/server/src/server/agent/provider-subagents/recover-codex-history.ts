import type { Logger } from "pino";
import type { AgentClient, AgentSession } from "../agent-sdk-types.js";
import type { StoredAgentRecord } from "../agent-storage.js";
import { buildConfigOverrides } from "../../persistence-hooks.js";
import type { ProviderSubagentCompletionStore } from "./completion-store.js";

/** Read native history without loading agent runtimes or changing timeline projections. */
export async function recoverCodexHistory(
  records: readonly StoredAgentRecord[],
  client: AgentClient | undefined,
  store: ProviderSubagentCompletionStore,
  logger: Logger,
  signal: AbortSignal,
): Promise<void> {
  const candidates = records.filter(
    (record) => record.provider === "codex" && record.persistence?.sessionId,
  );
  if (!candidates.length) return;
  // Paginated items do not bind outcomes to the source turn, and history may have
  // been pruned before this scan. A successful scan cannot establish completeness.
  await store.markCoverageUncertain();
  if (!client) {
    logger.warn({ skipped: candidates.length }, "Codex helper history recovery unavailable");
    return;
  }
  let remaining = candidates.length;
  for (const record of candidates) {
    if (signal.aborted) break;
    const handle = record.persistence;
    if (!handle) continue;
    const timeout = AbortSignal.timeout(30_000);
    const attempt = AbortSignal.any([signal, timeout]);
    try {
      const session = await client.resumeSession(
        { provider: "codex", sessionId: handle.sessionId, metadata: handle.metadata },
        { provider: "codex", cwd: record.cwd, ...buildConfigOverrides(record) },
        { agentId: record.id, env: record.launchEnv },
        { purpose: "history", signal: attempt },
      );
      try {
        await appendHistory(session, record.id, store, attempt);
      } finally {
        await session.close();
      }
    } catch (err) {
      logger.warn({ err, agentId: record.id }, "Codex helper history recovery incomplete");
    }
    remaining--;
  }
  if (remaining) logger.warn({ skipped: remaining }, "Codex helper history recovery stopped early");
}

async function appendHistory(
  session: AgentSession,
  parentId: string,
  store: ProviderSubagentCompletionStore,
  signal: AbortSignal,
): Promise<void> {
  for await (const event of session.streamHistory()) {
    signal.throwIfAborted();
    if (
      event.type !== "provider_subagent" ||
      event.event.type !== "upsert" ||
      !event.event.nativeTurnId ||
      !event.event.status ||
      event.event.status === "running"
    )
      continue;
    await store.append(parentId, event.provider, event.event, "provider-stream", [], true);
  }
}
