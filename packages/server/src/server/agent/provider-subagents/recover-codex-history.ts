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
  // Bound each cycle; the next cycle refreshes the list and resumes at the cursor.
  candidates.sort((a, b) => {
    if (a.id < b.id) return -1;
    if (a.id > b.id) return 1;
    return 0;
  });
  const cursor = await store.recoveryCursor();
  const start = cursor ? candidates.findIndex((record) => record.id > cursor) : 0;
  const ordered =
    start < 0 ? candidates : [...candidates.slice(start), ...candidates.slice(0, start)];
  let attempted = 0;
  let lastAttempted: string | undefined;
  for (const record of ordered.slice(0, 25)) {
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
      if (!signal.aborted)
        logger.warn({ err, agentId: record.id }, "Codex helper history recovery incomplete");
    }
    if (signal.aborted) break;
    lastAttempted = record.id;
    attempted++;
  }
  if (lastAttempted) {
    try {
      await store.advanceRecoveryCursor(lastAttempted);
    } catch (err) {
      logger.warn(
        { err, agentId: lastAttempted, skipped: ordered.length - attempted },
        "Codex helper history recovery cursor write failed",
      );
      throw err;
    }
  }
  if (signal.aborted && attempted < ordered.length)
    logger.warn(
      { skipped: ordered.length - attempted },
      "Codex helper history recovery stopped early",
    );
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
    await store.append(parentId, event.provider, event.event, "provider-stream");
  }
}
