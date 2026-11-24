import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pino from "pino";
import { afterEach, describe, expect, test, vi } from "vitest";

import { PARENT_AGENT_ID_LABEL } from "@getpaseo/protocol/agent-labels";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createAgentMcpServer } from "./mcp-server.js";
import { ProviderSnapshotManager } from "./provider-snapshot-manager.js";
import { writeJsonFileAtomic } from "../atomic-file.js";
import { createTestLogger } from "../../test-utils/test-logger.js";
import { AgentManager, type AgentManagerEvent } from "./agent-manager.js";
import { AgentStorage, parseStoredAgentRecord } from "./agent-storage.js";
import { recoverCodexHistory } from "./provider-subagents/recover-codex-history.js";
import { ProviderSubagentCompletionStore } from "./provider-subagents/completion-store.js";
import { ensureAgentLoaded } from "./agent-loading.js";
import { AGENT_STREAM_COALESCE_DEFAULT_WINDOW_MS } from "./agent-stream-coalescer.js";
import { limitAgentTimelineItemContent } from "./agent-timeline-content.js";
import type {
  AgentTimelineRow,
  AgentTimelineStore,
  AgentTimelineFetchOptions,
} from "./agent-timeline-store-types.js";
import { InMemoryAgentTimelineStore } from "./agent-timeline-store.js";
import { CodexAppServerAgentSession } from "./providers/codex-app-server-agent.js";
import {
  createFakeCodexAppServer,
  waitForNextEvent,
} from "./providers/codex/test-utils/fake-app-server.js";
import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { ClaudeTaskProtocolSource } from "./providers/claude/subagents/live-source.js";
import {
  foldSubagentObservations,
  type SubagentObservation,
} from "./providers/claude/subagents/observation.js";
import { OpenCodeAgentClient } from "./providers/opencode-agent.js";
import {
  TestOpenCodeClient,
  TestOpenCodeHarness,
} from "./providers/opencode/test-utils/test-opencode-harness.js";
import { OmpAgentClient } from "./providers/omp/agent.js";
import { FakeOmp } from "./providers/omp/test-utils/fake-omp.js";
import { PiRpcAgentClient } from "./providers/pi/agent.js";
import { FakePi } from "./providers/pi/test-utils/fake-pi.js";
import { projectTimelineRows } from "./timeline-projection.js";
import type {
  AgentCapabilityFlags,
  AgentClient,
  AgentLaunchContext,
  AgentPersistenceHandle,
  AgentPromptInput,
  AgentProvider,
  AgentRunOptions,
  AgentRunResult,
  AgentSession,
  AgentSessionConfig,
  AgentStreamEvent,
  AgentTimelineItem,
  ProviderCatalog,
} from "./agent-sdk-types.js";

/**
 * Contract for AgentManager pre-record stream coalescing.
 * Assistant/reasoning chunks coalesce before recordTimeline() assigns canonical seqs.
 */

const COALESCE_WINDOW_MS = AGENT_STREAM_COALESCE_DEFAULT_WINDOW_MS;
const BEFORE_COALESCE_WINDOW_MS = Math.max(COALESCE_WINDOW_MS - 1, 0);

const TEST_CAPABILITIES: AgentCapabilityFlags = {
  supportsStreaming: false,
  supportsSessionPersistence: false,
  supportsDynamicModes: false,
  supportsMcpServers: false,
  supportsReasoningStream: false,
  supportsToolInvocations: false,
};

const AGENT_IDS = [
  "00000000-0000-4000-8000-000000000001",
  "00000000-0000-4000-8000-000000000002",
  "00000000-0000-4000-8000-000000000003",
  "00000000-0000-4000-8000-000000000004",
] as const;

const TOOL_CALL: AgentTimelineItem = {
  type: "tool_call",
  callId: "tool-1",
  name: "shell",
  status: "completed",
  error: null,
  detail: {
    type: "shell",
    command: "printf ok",
    output: "ok",
    exitCode: 0,
  },
};

function toolCall(options?: {
  callId?: string;
  status?: "running" | "completed" | "failed" | "canceled";
  output?: string;
  error?: unknown;
}): Extract<AgentTimelineItem, { type: "tool_call" }> {
  const status = options?.status ?? "running";
  return {
    type: "tool_call",
    callId: options?.callId ?? "tool-1",
    name: "shell",
    status,
    error: status === "failed" ? (options?.error ?? "failed") : null,
    detail: {
      type: "shell",
      command: "printf ok",
      output: options?.output ?? "",
      exitCode: status === "completed" ? 0 : null,
    },
  };
}

class FileTimelineStore implements AgentTimelineStore {
  private memory = new InMemoryAgentTimelineStore();
  constructor(
    private file: string,
    private agentId: string,
  ) {
    let rows: AgentTimelineRow[] = [];
    try {
      rows = JSON.parse(readFileSync(file, "utf8"));
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
    }
    this.memory.initialize(agentId, { rows });
  }
  private save() {
    writeFileSync(this.file, JSON.stringify(this.memory.getRows(this.agentId)));
  }
  async appendCommitted(
    id: string,
    item: AgentTimelineItem,
    options?: { timestamp?: string; turnId?: string },
  ) {
    const row = this.memory.append(id, item, options);
    this.save();
    return row;
  }
  async fetchCommitted(id: string, options?: AgentTimelineFetchOptions) {
    return this.memory.fetch(id, options);
  }
  async getLatestCommittedSeq(id: string) {
    return this.memory.getRows(id).at(-1)?.seq ?? 0;
  }
  async getCommittedRows(id: string) {
    return this.memory.getRows(id);
  }
  async getLastItem(id: string) {
    return this.memory.getLastItem(id);
  }
  async getLastAssistantMessage(id: string) {
    return this.memory.getLastAssistantMessage(id);
  }
  async deleteAgent(id: string) {
    this.memory.delete(id);
    this.memory.initialize(id);
    this.save();
  }
  async bulkInsert(id: string, rows: readonly AgentTimelineRow[]) {
    for (const row of rows)
      this.memory.append(id, row.item, { timestamp: row.timestamp, turnId: row.turnId });
    this.save();
  }
  async updateCommittedRow(id: string, row: AgentTimelineRow) {
    const rows = this.memory.getRows(id);
    const index = rows.findIndex((entry) => entry.seq === row.seq);
    if (index >= 0) rows[index] = row;
    this.memory.initialize(id, { rows });
    this.save();
  }
}

class TestAgentSession implements AgentSession {
  readonly capabilities = TEST_CAPABILITIES;
  readonly id: string;
  private historyEvents: AgentStreamEvent[] = [];
  private readonly subscribers = new Set<(event: AgentStreamEvent) => void>();
  private turnIdCounter = 0;

  constructor(
    readonly provider: AgentProvider,
    private readonly config: AgentSessionConfig,
    sessionId: string,
  ) {
    this.id = sessionId;
  }

  async run(): Promise<AgentRunResult> {
    return {
      sessionId: this.id,
      finalText: "",
      timeline: [],
    };
  }

  async startTurn(
    _prompt: AgentPromptInput,
    _options?: AgentRunOptions,
  ): Promise<{ turnId: string }> {
    return { turnId: `turn-${++this.turnIdCounter}` };
  }

  subscribe(callback: (event: AgentStreamEvent) => void): () => void {
    this.subscribers.add(callback);
    return () => {
      this.subscribers.delete(callback);
    };
  }

  pushEvent(event: AgentStreamEvent): void {
    for (const callback of this.subscribers) {
      callback(event);
    }
  }

  setHistory(events: AgentStreamEvent[]): void {
    this.historyEvents = [...events];
  }

  async *streamHistory(): AsyncGenerator<AgentStreamEvent> {
    for (const event of this.historyEvents) {
      yield event;
    }
  }

  async getRuntimeInfo() {
    return {
      provider: this.provider,
      sessionId: this.id,
      model: this.config.model ?? null,
      modeId: this.config.modeId ?? null,
    };
  }

  async getAvailableModes() {
    return [];
  }

  async getCurrentMode() {
    return null;
  }

  async setMode(): Promise<void> {}

  getPendingPermissions() {
    return [];
  }

  async respondToPermission(): Promise<void> {}

  describePersistence(): AgentPersistenceHandle {
    return {
      provider: this.provider,
      sessionId: this.id,
    };
  }

  async interrupt(): Promise<void> {}

  async close(): Promise<void> {}
}

class TestAgentClient implements AgentClient {
  readonly capabilities = TEST_CAPABILITIES;
  private sessionCounter = 0;
  readonly sessions = new Map<string, TestAgentSession>();

  constructor(readonly provider: AgentProvider = "codex") {}

  async createSession(
    config: AgentSessionConfig,
    _launchContext?: AgentLaunchContext,
  ): Promise<AgentSession> {
    const session = new TestAgentSession(
      config.provider,
      config,
      `${config.provider}-session-${++this.sessionCounter}`,
    );
    this.sessions.set(config.cwd, session);
    return session;
  }

  async resumeSession(
    _handle: AgentPersistenceHandle,
    config?: Partial<AgentSessionConfig>,
    _launchContext?: AgentLaunchContext,
  ): Promise<AgentSession> {
    const resolvedConfig: AgentSessionConfig = {
      provider: this.provider,
      cwd: config?.cwd ?? process.cwd(),
      ...config,
    };
    return this.createSession(resolvedConfig);
  }

  async fetchCatalog(): Promise<ProviderCatalog> {
    return {
      models: [
        {
          provider: this.provider,
          id: "test-model",
          label: "Test Model",
          isDefault: true,
        },
      ],
      modes: [],
    };
  }

  async isAvailable(): Promise<boolean> {
    return true;
  }

  getSession(cwd: string): TestAgentSession {
    const session = this.sessions.get(cwd);
    if (!session) {
      throw new Error(`No test session for cwd ${cwd}`);
    }
    return session;
  }
}

interface Harness {
  manager: AgentManager;
  client: TestAgentClient;
  events: AgentManagerEvent[];
  workdir: string;
  cleanup: () => void;
}

function createHarness(options?: { provider?: AgentProvider }): Harness {
  const workdir = mkdtempSync(join(tmpdir(), "agent-manager-stream-coalescing-"));
  const client = new TestAgentClient(options?.provider ?? "codex");
  const manager = new AgentManager({
    clients: { [client.provider]: client },
    idFactory: createIdFactory(),
    logger: createTestLogger(),
  });
  const events: AgentManagerEvent[] = [];
  manager.subscribe((event) => events.push(event), { replayState: false });

  return {
    manager,
    client,
    events,
    workdir,
    cleanup: () => rmSync(workdir, { recursive: true, force: true }),
  };
}

function createIdFactory(): () => string {
  let index = 0;
  return () => AGENT_IDS[index++] ?? `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
}

async function createManagedSession(
  harness: Harness,
  options?: { agentId?: string; provider?: AgentProvider; workdir?: string },
): Promise<{ agentId: string; session: TestAgentSession; workdir: string }> {
  const agentId = options?.agentId ?? AGENT_IDS[0];
  const workdir = options?.workdir ?? harness.workdir;
  await harness.manager.createAgent(
    {
      provider: options?.provider ?? harness.client.provider,
      cwd: workdir,
    },
    agentId,
    { workspaceId: undefined },
  );
  return {
    agentId,
    session: harness.client.getSession(workdir),
    workdir,
  };
}

function assistant(
  text: string,
  provider: AgentProvider = "codex",
  turnId?: string,
): AgentStreamEvent {
  return {
    type: "timeline",
    provider,
    turnId,
    item: { type: "assistant_message", text },
  };
}

function reasoning(
  text: string,
  provider: AgentProvider = "codex",
  turnId?: string,
): AgentStreamEvent {
  return {
    type: "timeline",
    provider,
    turnId,
    item: { type: "reasoning", text },
  };
}

function timelineEvent(
  item: AgentTimelineItem,
  provider: AgentProvider = "codex",
  turnId?: string,
): AgentStreamEvent {
  return {
    type: "timeline",
    provider,
    turnId,
    item,
  };
}

function terminalEvent(
  type: "turn_completed" | "turn_failed" | "turn_canceled",
  turnId: string,
): AgentStreamEvent {
  if (type === "turn_completed") {
    return { type, provider: "codex", turnId };
  }
  if (type === "turn_failed") {
    return { type, provider: "codex", turnId, error: "failed" };
  }
  return { type, provider: "codex", turnId, reason: "canceled" };
}

function getStreamEvents(events: AgentManagerEvent[], agentId: string): AgentManagerEvent[] {
  return events.filter((event) => event.type === "agent_stream" && event.agentId === agentId);
}

function getTimelineStreamEvents(
  events: AgentManagerEvent[],
  agentId: string,
): AgentManagerEvent[] {
  return getStreamEvents(events, agentId).filter(
    (event) => event.type === "agent_stream" && event.event.type === "timeline",
  );
}

function getTimelineItems(rows: AgentTimelineRow[]): AgentTimelineItem[] {
  return rows.map((row) => row.item);
}

// The leading-edge flush emits the first chunk of a burst as its own row. Clients
// read the projected timeline, where mergeAssistantChunks/mergeReasoningChunks
// join those contiguous rows back together, so history reads the same either way.
function getProjectedTimelineItems(rows: AgentTimelineRow[]): AgentTimelineItem[] {
  return projectTimelineRows({ rows, mode: "projected" }).map((entry) => entry.item);
}

function expectContiguousRowSeqs(rows: AgentTimelineRow[], expected: number[]): void {
  const projected = projectTimelineRows({ rows, mode: "projected" });
  const covered = projected.flatMap((row) =>
    row.sourceSeqRanges.flatMap((range) =>
      Array.from(
        { length: range.endSeq - range.startSeq + 1 },
        (_, index) => range.startSeq + index,
      ),
    ),
  );
  expect([...new Set(covered)].sort((a, b) => a - b)).toEqual(expected);
}

function expectContiguousLiveSeqs(events: AgentManagerEvent[], expected: number[]): void {
  expect(events.map((event) => (event.type === "agent_stream" ? event.seq : undefined))).toEqual(
    expected,
  );
}

async function waitForSessionEventQueue(): Promise<void> {
  for (let i = 0; i < 10_000; i++) {
    await Promise.resolve();
  }
}

afterEach(() => {
  vi.useRealTimers();
});

describe("live notification turns", () => {
  test.each([
    { sequence: "while idle", agentStarted: false, emptyCycle: false },
    { sequence: "between agent_start and turn_start", agentStarted: true, emptyCycle: false },
    { sequence: "after an empty cycle", agentStarted: true, emptyCycle: true },
  ])(
    "keeps OMP replies in separate turns with a notice $sequence",
    async ({ agentStarted, emptyCycle }) => {
      const runtime = new FakeOmp();
      const provider = "omp";
      const logger = createTestLogger();
      const manager = new AgentManager({
        clients: {
          omp: new OmpAgentClient({
            runtime,
            logger,
            runtimeSettings: { command: { mode: "replace", argv: [process.execPath] } },
          }),
        },
        logger,
      });
      const workdir = mkdtempSync(join(tmpdir(), "omp-live-turns-"));
      const events: AgentManagerEvent[] = [];
      manager.subscribe((event) => events.push(event), { replayState: false });
      const { id } = await manager.createAgent({ provider, cwd: workdir }, undefined, {
        workspaceId: undefined,
      });
      try {
        const session = runtime.latestSession();
        const finishReply = async (text: string) => {
          const completed = () =>
            getStreamEvents(events, id).filter(
              (event) => event.type === "agent_stream" && event.event.type === "turn_completed",
            ).length;
          const before = completed();
          session.streamAssistantText(text, text);
          session.finishTurn({ role: "assistant", content: [] });
          await vi.waitFor(() => {
            expect(completed()).toBe(before + 1);
          });
        };
        const notice = (message: string) => {
          session.emit({ type: "extension_ui_request", id: message, method: "notify", message });
        };
        session.beginTurn();
        await finishReply("First reply");

        if (agentStarted) session.emit({ type: "agent_start" });
        if (emptyCycle) session.emit({ type: "agent_end", messages: [] });
        notice("Idle notice");
        session.beginTurn();
        session.acceptCustomMessage("<system-notice>Completion</system-notice>");
        notice("Running notice");
        await finishReply("Second reply");

        const page = await manager.fetchTimeline(id, { wholeTurns: true, limit: 0 });
        const ids = page.rows.map((row) => row.turnId);
        expect(
          page.rows.map(({ item }) => {
            if (item.type === "assistant_message") return item.text;
            if (item.type === "notification") return item.message;
            return item.type;
          }),
        ).toEqual(["First reply", "Idle notice", "Completion", "Running notice", "Second reply"]);
        const [first, idle, wake] = ids;
        expect(ids).toEqual([
          expect.any(String),
          expect.any(String),
          expect.any(String),
          wake,
          wake,
        ]);
        expect(new Set([first, idle, wake]).size).toBe(3);
        expect(
          getTimelineStreamEvents(events, id).map((event) =>
            event.type === "agent_stream" && event.event.type === "timeline"
              ? event.event.turnId
              : null,
          ),
        ).toEqual(ids);
        expect(
          (await manager.fetchTimeline(id, { wholeTurns: true, limit: 1 })).rows.map(
            (row) => row.turnId,
          ),
        ).toEqual([wake, wake, wake]);
      } finally {
        await manager.closeAgent(id);
        rmSync(workdir, { recursive: true, force: true });
      }
    },
  );

  test("keeps a Claude completion with the run it wakes after an independent notice", async () => {
    const harness = createHarness({ provider: "claude" });
    try {
      const { agentId, session } = await createManagedSession(harness);
      session.pushEvent(assistant("First reply", "claude", "first"));
      session.pushEvent(
        timelineEvent({ type: "notification", level: "info", message: "Idle" }, "claude"),
      );
      const completion = { ...toolCall({ status: "completed" }), name: "task_notification" };
      session.pushEvent(
        timelineEvent({ ...completion, callId: "stopped-task" }, "claude", "stopped"),
      );
      session.pushEvent(timelineEvent(completion, "claude", "wake"));
      session.pushEvent({ type: "turn_started", provider: "claude", turnId: "wake" });
      session.pushEvent(assistant("Second reply", "claude", "wake"));
      session.pushEvent({ type: "turn_completed", provider: "claude", turnId: "wake" });
      await vi.waitFor(() => {
        expect(getStreamEvents(harness.events, agentId).at(-1)).toMatchObject({
          event: { type: "turn_completed", turnId: "wake" },
        });
      });
      const page = await harness.manager.fetchTimeline(agentId, { wholeTurns: true, limit: 0 });
      expect(page.rows.map((row) => row.turnId)).toEqual([
        "first",
        expect.any(String),
        "stopped",
        "wake",
        "wake",
      ]);
      expect(page.rows[1]?.turnId).not.toBe("first");
      expect(page.rows[1]?.turnId).not.toBe("wake");
      expect(
        getStreamEvents(harness.events, agentId).filter(
          (event) => event.type === "agent_stream" && event.event.type === "turn_started",
        ),
      ).toEqual([
        expect.objectContaining({
          event: { type: "turn_started", provider: "claude", turnId: "wake" },
        }),
      ]);
      expect(
        (await harness.manager.fetchTimeline(agentId, { wholeTurns: true, limit: 1 })).rows.map(
          (row) => row.item,
        ),
      ).toEqual([completion, { type: "assistant_message", text: "Second reply" }]);
    } finally {
      harness.cleanup();
    }
  });

  test("an OMP runtime notice during a foreground turn reaches the run caller", async () => {
    const runtime = new FakeOmp();
    const logger = createTestLogger();
    const manager = new AgentManager({
      clients: {
        omp: new OmpAgentClient({
          runtime,
          logger,
          runtimeSettings: { command: { mode: "replace", argv: [process.execPath] } },
        }),
      },
      logger,
    });
    const workdir = mkdtempSync(join(tmpdir(), "omp-foreground-notice-"));
    const agent = await manager.createAgent({ provider: "omp", cwd: workdir }, undefined, {
      workspaceId: undefined,
    });
    const session = runtime.latestSession();
    try {
      const result = manager.runAgent(agent.id, "Work");
      await vi.waitFor(() => expect(session.prompts).toHaveLength(1));
      session.beginTurn();
      session.emit({
        type: "notice",
        level: "info",
        message: "Working",
      });
      session.streamAssistantText("Done");
      session.finishTurn({ role: "assistant", content: [] });
      const item = { type: "notification", level: "info", message: "Working" };
      expect((await result).timeline).toContainEqual(item);
      const rows = (await manager.fetchTimeline(agent.id, { wholeTurns: true, limit: 0 })).rows;
      expect(rows.find((row) => row.item.type === "notification")?.turnId).toEqual(
        expect.any(String),
      );
      expect(rows.find((row) => row.item.type === "notification")?.turnId).toEqual(
        rows.find((row) => row.item.type === "assistant_message")?.turnId,
      );
    } finally {
      await manager.closeAgent(agent.id);
      rmSync(workdir, { recursive: true, force: true });
    }
  });

  test("an idle Pi UI notice gets its own turn without starting a run", async () => {
    const runtime = new FakePi();
    const logger = createTestLogger();
    const manager = new AgentManager({
      clients: {
        pi: new PiRpcAgentClient({
          runtime,
          logger,
          runtimeSettings: { command: { mode: "replace", argv: [process.execPath] } },
        }),
      },
      logger,
    });
    const workdir = mkdtempSync(join(tmpdir(), "pi-idle-notice-"));
    const events: AgentManagerEvent[] = [];
    manager.subscribe((event) => events.push(event), { replayState: false });
    const { id } = await manager.createAgent({ provider: "pi", cwd: workdir }, undefined, {
      workspaceId: undefined,
    });
    try {
      runtime.latestSession().emit({
        type: "extension_ui_request",
        id: "notice",
        method: "notify",
        message: "Idle",
      });
      await vi.waitFor(() => expect(getTimelineStreamEvents(events, id)).toHaveLength(1));
      const { rows } = await manager.fetchTimeline(id, { wholeTurns: true, limit: 1 });
      const item = { type: "notification", level: "info", message: "Idle" };
      expect(rows).toEqual([expect.objectContaining({ item, turnId: expect.any(String) })]);
      expect(getStreamEvents(events, id)).toEqual([
        expect.objectContaining({
          event: { type: "timeline", provider: "pi", item, turnId: rows[0]?.turnId },
        }),
      ]);
      expect(manager.getAgent(id)?.lifecycle).toBe("idle");
    } finally {
      await manager.closeAgent(id);
      rmSync(workdir, { recursive: true, force: true });
    }
  });
});

describe("target coalesced behavior", () => {
  test("bounds tool output before persisting and streaming it", async () => {
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      const output = `${"a".repeat(512 * 1024)}${"z".repeat(512 * 1024)}`;
      const expectedItem = limitAgentTimelineItemContent(toolCall({ status: "completed", output }));

      session.pushEvent(timelineEvent(toolCall({ status: "completed", output })));
      await waitForSessionEventQueue();

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([expectedItem]);
      expect(
        events.map((event) => (event.type === "agent_stream" ? event.event.item : null)),
      ).toEqual([expectedItem]);
    } finally {
      harness.cleanup();
    }
  });

  test("bounds appended tool output before persisting and streaming it", async () => {
    const harness = createHarness();
    try {
      const { agentId } = await createManagedSession(harness);
      const output = `${"a".repeat(512 * 1024)}${"z".repeat(512 * 1024)}`;
      const expectedItem = limitAgentTimelineItemContent(toolCall({ status: "completed", output }));

      await harness.manager.appendTimelineItem(agentId, toolCall({ status: "completed", output }));

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([expectedItem]);
      expect(
        events.map((event) => (event.type === "agent_stream" ? event.event.item : null)),
      ).toEqual([expectedItem]);
    } finally {
      harness.cleanup();
    }
  });

  test("bounds tool output while hydrating provider history", async () => {
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      const output = `${"a".repeat(512 * 1024)}${"z".repeat(512 * 1024)}`;
      const expectedItem = limitAgentTimelineItemContent(toolCall({ status: "completed", output }));
      session.setHistory([timelineEvent(toolCall({ status: "completed", output }))]);

      await harness.manager.hydrateTimelineFromProvider(agentId, { force: true });

      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        expectedItem,
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("bounds tool output emitted only to the live stream", async () => {
    const harness = createHarness();
    try {
      const { agentId } = await createManagedSession(harness);
      const output = `${"a".repeat(512 * 1024)}${"z".repeat(512 * 1024)}`;
      const expectedItem = limitAgentTimelineItemContent(toolCall({ status: "completed", output }));

      await harness.manager.emitLiveTimelineItem(
        agentId,
        toolCall({ status: "completed", output }),
      );

      const events = getTimelineStreamEvents(harness.events, agentId);
      expect(await harness.manager.getTimelineRows(agentId)).toEqual([]);
      expect(
        events.map((event) => (event.type === "agent_stream" ? event.event.item : null)),
      ).toEqual([expectedItem]);
    } finally {
      harness.cleanup();
    }
  });

  test("bounds failed shell output carried in the error", async () => {
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      const content = `${"a".repeat(512 * 1024)}${"z".repeat(512 * 1024)}`;
      const expectedItem = limitAgentTimelineItemContent(
        toolCall({ status: "failed", error: { content } }),
      );

      session.pushEvent(timelineEvent(toolCall({ status: "failed", error: { content } })));
      await waitForSessionEventQueue();

      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        expectedItem,
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test(`coalesces a same-tick assistant burst after the ${COALESCE_WINDOW_MS}ms window`, async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      for (let i = 0; i < 1000; i++) {
        session.pushEvent(assistant("x"));
      }
      await waitForSessionEventQueue();

      // Leading edge: the first chunk is already out, the other 999 are buffered.
      await vi.advanceTimersByTimeAsync(BEFORE_COALESCE_WINDOW_MS);
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "x" },
      ]);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(1);
      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(rows).toHaveLength(1);
      expect(events).toHaveLength(2);
      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "x".repeat(1000) },
      ]);
      expect(events.map((event) => (event.type === "agent_stream" ? event.event : null))).toEqual([
        {
          type: "timeline",
          provider: "codex",
          item: { type: "assistant_message", text: "x" },
        },
        {
          type: "timeline",
          provider: "codex",
          item: { type: "assistant_message", text: "x".repeat(999) },
        },
      ]);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "x".repeat(1000) },
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test(`coalesces same-tick reasoning chunks after the ${COALESCE_WINDOW_MS}ms window`, async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      for (let i = 0; i < 100; i++) {
        session.pushEvent(reasoning("r"));
      }
      await waitForSessionEventQueue();

      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);
      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([{ type: "reasoning", text: "r".repeat(100) }]);
      expect(events).toHaveLength(2);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "reasoning", text: "r".repeat(100) },
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("coalesces running tool calls without flushing buffered text", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      const runningToolCall = toolCall({ output: "running" });

      for (let i = 0; i < 500; i++) {
        session.pushEvent(assistant("a"));
      }
      session.pushEvent(timelineEvent(runningToolCall));
      await waitForSessionEventQueue();

      // Only the leading chunk is out; the running tool call stays buffered with
      // the rest of the text rather than forcing an early flush.
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "a" },
      ]);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "a".repeat(500) },
        runningToolCall,
      ]);
      expectContiguousLiveSeqs(getTimelineStreamEvents(harness.events, agentId), [1, 2, 3]);

      for (let i = 0; i < 500; i++) {
        session.pushEvent(assistant("b"));
      }
      await waitForSessionEventQueue();
      // The "b" burst arrives inside the window that just flushed, so it gets no
      // leading flush of its own and coalesces whole.
      await vi.advanceTimersByTimeAsync(BEFORE_COALESCE_WINDOW_MS);
      expect(await harness.manager.getTimelineRows(agentId)).toHaveLength(2);

      await vi.advanceTimersByTimeAsync(1);
      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a".repeat(500) },
        runningToolCall,
        { type: "assistant_message", text: "b".repeat(500) },
      ]);
      expectContiguousRowSeqs(rows, [1, 2, 3, 4]);
      expectContiguousLiveSeqs(events, [1, 2, 3, 4]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a".repeat(500) },
        runningToolCall,
        { type: "assistant_message", text: "b".repeat(500) },
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("coalesces same-window tool call updates to the latest snapshot", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      for (let i = 0; i < 200; i++) {
        session.pushEvent(timelineEvent(toolCall({ output: `chunk-${i}` })));
      }
      await waitForSessionEventQueue();

      // The leading edge flushes the first snapshot; the remaining 199 collapse to
      // the latest one. Both carry the same callId, so the projection folds them
      // into a single tool call row.
      await vi.advanceTimersByTimeAsync(BEFORE_COALESCE_WINDOW_MS);
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        toolCall({ output: "chunk-0" }),
      ]);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(1);
      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([toolCall({ output: "chunk-199" })]);
      expect(events).toHaveLength(2);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
      expect(getProjectedTimelineItems(rows)).toEqual([toolCall({ output: "chunk-199" })]);
    } finally {
      harness.cleanup();
    }
  });

  test("coalesces interleaved tool calls independently and preserves first arrival order", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(timelineEvent(toolCall({ callId: "tool-1", output: "one-a" })));
      session.pushEvent(timelineEvent(toolCall({ callId: "tool-2", output: "two-a" })));
      session.pushEvent(timelineEvent(toolCall({ callId: "tool-1", output: "one-b" })));
      session.pushEvent(timelineEvent(toolCall({ callId: "tool-2", output: "two-b" })));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        toolCall({ callId: "tool-1", output: "one-b" }),
        toolCall({ callId: "tool-2", output: "two-b" }),
      ]);
      expect(events).toHaveLength(3);
      expectContiguousRowSeqs(rows, [1, 2, 3]);
      expectContiguousLiveSeqs(events, [1, 2, 3]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        toolCall({ callId: "tool-1", output: "one-b" }),
        toolCall({ callId: "tool-2", output: "two-b" }),
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("terminal tool call statuses flush immediately with pending text", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("before"));
      session.pushEvent(timelineEvent(toolCall({ output: "running" })));
      await waitForSessionEventQueue();
      // "before" led the burst; the running tool call is still buffered.
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "before" },
      ]);

      session.pushEvent(timelineEvent(toolCall({ status: "completed", output: "done" })));
      await waitForSessionEventQueue();

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "before" },
        toolCall({ status: "completed", output: "done" }),
      ]);
      expect(events).toHaveLength(2);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);

      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);
      expect(await harness.manager.getTimelineRows(agentId)).toHaveLength(2);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(2);
    } finally {
      harness.cleanup();
    }
  });

  test("preserves mixed text and tool call order after coalescing", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("a"));
      session.pushEvent(timelineEvent(toolCall({ output: "first" })));
      session.pushEvent(reasoning("r"));
      session.pushEvent(timelineEvent(toolCall({ output: "latest" })));
      session.pushEvent(assistant("b"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a" },
        toolCall({ output: "latest" }),
        { type: "reasoning", text: "r" },
        { type: "assistant_message", text: "b" },
      ]);
      expect(events).toHaveLength(4);
      expectContiguousRowSeqs(rows, [1, 2, 3, 4]);
      expectContiguousLiveSeqs(events, [1, 2, 3, 4]);
    } finally {
      harness.cleanup();
    }
  });

  test("preserves assistant and reasoning interleave", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("a1"));
      session.pushEvent(reasoning("r1"));
      session.pushEvent(reasoning("r2"));
      session.pushEvent(assistant("a2"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a1" },
        { type: "reasoning", text: "r1r2" },
        { type: "assistant_message", text: "a2" },
      ]);
      expect(events).toHaveLength(3);
      expectContiguousRowSeqs(rows, [1, 2, 3]);
      expectContiguousLiveSeqs(events, [1, 2, 3]);
    } finally {
      harness.cleanup();
    }
  });

  test("preserves strict alternating assistant/reasoning order", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      for (let i = 1; i <= 5; i++) {
        session.pushEvent(assistant(`a${i}`));
        session.pushEvent(reasoning(`r${i}`));
      }
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a1" },
        { type: "reasoning", text: "r1" },
        { type: "assistant_message", text: "a2" },
        { type: "reasoning", text: "r2" },
        { type: "assistant_message", text: "a3" },
        { type: "reasoning", text: "r3" },
        { type: "assistant_message", text: "a4" },
        { type: "reasoning", text: "r4" },
        { type: "assistant_message", text: "a5" },
        { type: "reasoning", text: "r5" },
      ]);
      expect(rows).toHaveLength(10);
      expect(events).toHaveLength(10);
      expectContiguousRowSeqs(
        rows,
        Array.from({ length: 10 }, (_, index) => index + 1),
      );
      expectContiguousLiveSeqs(
        events,
        Array.from({ length: 10 }, (_, index) => index + 1),
      );
    } finally {
      harness.cleanup();
    }
  });

  test("isolates buffers per agent", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const first = await createManagedSession(harness, {
        agentId: AGENT_IDS[0],
        workdir: harness.workdir,
      });
      const secondWorkdir = mkdtempSync(join(tmpdir(), "agent-manager-stream-coalescing-"));
      const second = await createManagedSession(harness, {
        agentId: AGENT_IDS[1],
        workdir: secondWorkdir,
      });

      first.session.pushEvent(assistant("a"));
      second.session.pushEvent(assistant("b"));
      first.session.pushEvent(assistant("a"));
      second.session.pushEvent(assistant("b"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const firstRows = await harness.manager.getTimelineRows(first.agentId);
      const secondRows = await harness.manager.getTimelineRows(second.agentId);
      const firstEvents = getTimelineStreamEvents(harness.events, first.agentId);
      const secondEvents = getTimelineStreamEvents(harness.events, second.agentId);

      expect(getTimelineItems(firstRows)).toEqual([{ type: "assistant_message", text: "aa" }]);
      expect(getTimelineItems(secondRows)).toEqual([{ type: "assistant_message", text: "bb" }]);
      expect(firstEvents).toHaveLength(2);
      expect(secondEvents).toHaveLength(2);
      expectContiguousRowSeqs(firstRows, [1, 2]);
      expectContiguousRowSeqs(secondRows, [1, 2]);
      expectContiguousLiveSeqs(firstEvents, [1, 2]);
      expectContiguousLiveSeqs(secondEvents, [1, 2]);
      expect(getProjectedTimelineItems(firstRows)).toEqual([
        { type: "assistant_message", text: "aa" },
      ]);
      expect(getProjectedTimelineItems(secondRows)).toEqual([
        { type: "assistant_message", text: "bb" },
      ]);
      rmSync(secondWorkdir, { recursive: true, force: true });
    } finally {
      harness.cleanup();
    }
  });

  test("provider boundaries break collapse", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("codex", "codex"));
      session.pushEvent(assistant("claude", "claude"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([{ type: "assistant_message", text: "codexclaude" }]);
      expect(events.map((event) => (event.type === "agent_stream" ? event.event : null))).toEqual([
        { type: "timeline", provider: "codex", item: { type: "assistant_message", text: "codex" } },
        {
          type: "timeline",
          provider: "claude",
          item: { type: "assistant_message", text: "claude" },
        },
      ]);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
    } finally {
      harness.cleanup();
    }
  });

  test("turnId boundaries break collapse", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("one", "codex", "turn-1"));
      session.pushEvent(assistant("two", "codex", "turn-2"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "one" },
        { type: "assistant_message", text: "two" },
      ]);
      expect(events.map((event) => (event.type === "agent_stream" ? event.event : null))).toEqual([
        {
          type: "timeline",
          provider: "codex",
          turnId: "turn-1",
          item: { type: "assistant_message", text: "one" },
        },
        {
          type: "timeline",
          provider: "codex",
          turnId: "turn-2",
          item: { type: "assistant_message", text: "two" },
        },
      ]);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
    } finally {
      harness.cleanup();
    }
  });

  test("drops empty text but preserves whitespace byte-exactly", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant(""));
      session.pushEvent(assistant(" "));
      session.pushEvent(assistant("\n\t"));
      session.pushEvent(assistant("done"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);

      // The empty chunk is dropped before the coalescer, so " " leads the burst.
      expect(getTimelineItems(rows)).toEqual([{ type: "assistant_message", text: " \n\tdone" }]);
      expect(events).toHaveLength(2);
      expectContiguousRowSeqs(rows, [1, 2]);
      expectContiguousLiveSeqs(events, [1, 2]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: " \n\tdone" },
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("flushes pending chunks before terminal lifecycle events by dispatch order", async () => {
    vi.useFakeTimers();

    for (const terminalType of ["turn_completed", "turn_failed", "turn_canceled"] as const) {
      const harness = createHarness();
      try {
        const { agentId, session } = await createManagedSession(harness);

        session.pushEvent(assistant(`${terminalType}-text`, "codex", terminalType));
        session.pushEvent(terminalEvent(terminalType, terminalType));
        await waitForSessionEventQueue();

        const streamEvents = getStreamEvents(harness.events, agentId);
        const timelineEvents = getTimelineStreamEvents(harness.events, agentId);
        const rows = await harness.manager.getTimelineRows(agentId);

        expect(getTimelineItems(rows)[0]).toEqual({
          type: "assistant_message",
          text: `${terminalType}-text`,
        });
        const expectedTimelineEventCount = terminalType === "turn_failed" ? 2 : 1;
        expect(timelineEvents).toHaveLength(expectedTimelineEventCount);
        expect(streamEvents[0]).toMatchObject({
          type: "agent_stream",
          event: {
            type: "timeline",
            item: { type: "assistant_message", text: `${terminalType}-text` },
          },
          seq: 1,
        });
        const terminalEventIndex = streamEvents.findIndex(
          (event) => event.type === "agent_stream" && event.event.type === terminalType,
        );
        expect(terminalEventIndex).toBeGreaterThan(0);
        expect(streamEvents[terminalEventIndex]).toMatchObject({
          type: "agent_stream",
          event: { type: terminalType },
        });
        expect(
          streamEvents[terminalEventIndex]?.type === "agent_stream"
            ? streamEvents[terminalEventIndex].seq
            : undefined,
        ).toBe(undefined);

        await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);
        expect(await harness.manager.getTimelineRows(agentId)).toHaveLength(
          expectedTimelineEventCount,
        );
        expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(
          expectedTimelineEventCount,
        );
      } finally {
        harness.cleanup();
      }
    }
  });

  test("lifecycle paths are timer-safe", async () => {
    vi.useFakeTimers();

    const closeHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(closeHarness);
      session.pushEvent(assistant("close"));
      await waitForSessionEventQueue();
      await closeHarness.manager.closeAgent(agentId);
      await vi.advanceTimersByTimeAsync(100);

      expect(getTimelineStreamEvents(closeHarness.events, agentId)).toHaveLength(1);
      expect(getTimelineStreamEvents(closeHarness.events, agentId)[0]).toMatchObject({
        type: "agent_stream",
        event: { type: "timeline", item: { type: "assistant_message", text: "close" } },
        seq: 1,
      });
    } finally {
      closeHarness.cleanup();
    }

    const reloadHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(reloadHarness);
      session.pushEvent(assistant("reload"));
      await waitForSessionEventQueue();
      await reloadHarness.manager.reloadAgentSession(agentId);
      await vi.advanceTimersByTimeAsync(100);

      const rows = await reloadHarness.manager.getTimelineRows(agentId);
      expect(getTimelineItems(rows)).toEqual([{ type: "assistant_message", text: "reload" }]);
      expect(getTimelineStreamEvents(reloadHarness.events, agentId)).toHaveLength(1);
    } finally {
      reloadHarness.cleanup();
    }

    const deletionHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(deletionHarness);
      session.pushEvent(assistant("delete"));
      await waitForSessionEventQueue();
      await deletionHarness.manager.closeAgent(agentId);
      await vi.advanceTimersByTimeAsync(100);

      expect(deletionHarness.manager.getAgent(agentId)).toBeNull();
      expect(getTimelineStreamEvents(deletionHarness.events, agentId)).toHaveLength(1);
    } finally {
      deletionHarness.cleanup();
    }
  });

  test("old timers cannot write into reused agent ids", async () => {
    vi.useFakeTimers();

    const reloadHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(reloadHarness);
      session.pushEvent(assistant("before-reload"));
      await waitForSessionEventQueue();
      await reloadHarness.manager.reloadAgentSession(agentId);
      await vi.advanceTimersByTimeAsync(100);

      expect(getTimelineItems(await reloadHarness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "before-reload" },
      ]);
      expect(getTimelineStreamEvents(reloadHarness.events, agentId)).toHaveLength(1);
    } finally {
      reloadHarness.cleanup();
    }

    const reuseHarness = createHarness();
    try {
      const first = await createManagedSession(reuseHarness, {
        agentId: AGENT_IDS[0],
        workdir: reuseHarness.workdir,
      });
      first.session.pushEvent(assistant("old"));
      await waitForSessionEventQueue();
      await reuseHarness.manager.closeAgent(first.agentId);

      const nextWorkdir = mkdtempSync(join(tmpdir(), "agent-manager-stream-coalescing-"));
      const second = await createManagedSession(reuseHarness, {
        agentId: AGENT_IDS[0],
        workdir: nextWorkdir,
      });
      second.session.pushEvent(assistant("new"));
      await waitForSessionEventQueue();
      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);

      expect(getTimelineItems(await reuseHarness.manager.getTimelineRows(second.agentId))).toEqual([
        { type: "assistant_message", text: "new" },
      ]);
      expect(getTimelineStreamEvents(reuseHarness.events, first.agentId)).toHaveLength(2);
      expect(
        getTimelineStreamEvents(reuseHarness.events, first.agentId).map((event) =>
          event.type === "agent_stream" && event.event.type === "timeline"
            ? event.event.item
            : null,
        ),
      ).toEqual([
        { type: "assistant_message", text: "old" },
        { type: "assistant_message", text: "new" },
      ]);
      rmSync(nextWorkdir, { recursive: true, force: true });
    } finally {
      reuseHarness.cleanup();
    }
  });

  test("flush helper is idempotent and re-entry safe", async () => {
    vi.useFakeTimers();
    const idempotencyHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(idempotencyHarness);

      session.pushEvent(assistant("a"));
      session.pushEvent(assistant("b"));
      session.pushEvent(assistant("c"));
      session.pushEvent(timelineEvent(TOOL_CALL));
      session.pushEvent(timelineEvent(TOOL_CALL));
      await waitForSessionEventQueue();

      const rows = await idempotencyHarness.manager.getTimelineRows(agentId);
      const timelineEvents = getTimelineStreamEvents(idempotencyHarness.events, agentId);

      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "abc" },
        TOOL_CALL,
      ]);
      expect(timelineEvents).toHaveLength(4);
      expectContiguousRowSeqs(rows, [1, 2, 3, 4]);
      expectContiguousLiveSeqs(timelineEvents, [1, 2, 3, 4]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "abc" },
        TOOL_CALL,
      ]);
    } finally {
      idempotencyHarness.cleanup();
    }

    const reentryHarness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(reentryHarness);
      let didPushDuringFlush = false;
      reentryHarness.manager.subscribe(
        (event) => {
          if (
            didPushDuringFlush ||
            event.type !== "agent_stream" ||
            event.agentId !== agentId ||
            event.event.type !== "timeline" ||
            event.event.item.type !== "assistant_message"
          ) {
            return;
          }
          didPushDuringFlush = true;
          session.pushEvent(assistant("B"));
        },
        { replayState: false },
      );

      session.pushEvent(assistant("A"));
      await waitForSessionEventQueue();

      // "A" flushes on the leading edge; the chunk the subscriber pushes during
      // that flush must land in the next window, not corrupt this one.
      const rowsAfterFirstFlush = await reentryHarness.manager.getTimelineRows(agentId);
      expect(getTimelineItems(rowsAfterFirstFlush)).toEqual([
        { type: "assistant_message", text: "A" },
      ]);
      expect(getTimelineStreamEvents(reentryHarness.events, agentId)).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(COALESCE_WINDOW_MS);
      await waitForSessionEventQueue();

      const rowsAfterSecondFlush = await reentryHarness.manager.getTimelineRows(agentId);
      const timelineEvents = getTimelineStreamEvents(reentryHarness.events, agentId);
      expect(getTimelineItems(rowsAfterSecondFlush)).toEqual([
        { type: "assistant_message", text: "AB" },
      ]);
      expect(timelineEvents).toHaveLength(2);
      expectContiguousRowSeqs(rowsAfterSecondFlush, [1, 2]);
      expectContiguousLiveSeqs(timelineEvents, [1, 2]);
      expect(didPushDuringFlush).toBe(true);
    } finally {
      reentryHarness.cleanup();
    }
  });

  test("manager flush drains pending chunks before background tasks", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("flush"));
      session.pushEvent(assistant("-pending"));
      await waitForSessionEventQueue();

      // "flush" left on the leading edge; "-pending" is what manager.flush() drains.
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "flush" },
      ]);
      await harness.manager.flush();

      const rows = await harness.manager.getTimelineRows(agentId);
      const events = getTimelineStreamEvents(harness.events, agentId);
      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "flush-pending" },
      ]);
      expect(events).toHaveLength(2);

      await vi.advanceTimersByTimeAsync(100);
      expect(await harness.manager.getTimelineRows(agentId)).toHaveLength(1);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(2);
    } finally {
      harness.cleanup();
    }
  });

  test("history hydration projects immediately without waiting for a streaming window", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      session.setHistory([assistant("a1"), assistant("a2"), reasoning("r1"), reasoning("r2")]);

      await harness.manager.hydrateTimelineFromProvider(agentId, { force: true });

      const rows = await harness.manager.getTimelineRows(agentId);
      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a1a2" },
        { type: "reasoning", text: "r1r2" },
      ]);
      expectContiguousRowSeqs(rows, [1, 2, 3, 4]);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(100);
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "a1a2" },
        { type: "reasoning", text: "r1r2" },
      ]);
    } finally {
      harness.cleanup();
    }
  });

  test("foreground stream yields coalesced timeline events before terminal events", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);
      const stream = harness.manager.streamAgent(agentId, "prompt");
      const firstEvent = stream.next();
      await harness.manager.waitForAgentRunStart(agentId);

      session.pushEvent(assistant("hello ", "codex", "turn-1"));
      session.pushEvent(assistant("world", "codex", "turn-1"));
      session.pushEvent(terminalEvent("turn_completed", "turn-1"));
      await waitForSessionEventQueue();

      await expect(firstEvent).resolves.toEqual({
        done: false,
        value: { type: "turn_started", provider: "codex", turnId: "turn-1" },
      });
      await expect(stream.next()).resolves.toEqual({
        done: false,
        value: {
          type: "timeline",
          provider: "codex",
          turnId: "turn-1",
          item: { type: "assistant_message", text: "hello " },
        },
      });
      await expect(stream.next()).resolves.toEqual({
        done: false,
        value: {
          type: "timeline",
          provider: "codex",
          turnId: "turn-1",
          item: { type: "assistant_message", text: "world" },
        },
      });
      await expect(stream.next()).resolves.toEqual({
        done: false,
        value: { type: "turn_completed", provider: "codex", turnId: "turn-1" },
      });
      await expect(stream.next()).resolves.toEqual({ done: true, value: undefined });

      const rows = await harness.manager.getTimelineRows(agentId);
      const streamEvents = getStreamEvents(harness.events, agentId);
      expect(getTimelineItems(rows)).toEqual([{ type: "assistant_message", text: "hello world" }]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "hello world" },
      ]);
      expect(streamEvents[0]).toMatchObject({
        type: "agent_stream",
        event: { type: "turn_started", provider: "codex", turnId: "turn-1" },
      });
      expect(streamEvents[1]).toMatchObject({
        type: "agent_stream",
        event: {
          type: "timeline",
          provider: "codex",
          turnId: "turn-1",
          item: { type: "assistant_message", text: "hello " },
        },
      });
      expect(streamEvents[2]).toMatchObject({
        type: "agent_stream",
        event: {
          type: "timeline",
          provider: "codex",
          turnId: "turn-1",
          item: { type: "assistant_message", text: "world" },
        },
      });
      expect(streamEvents[3]).toMatchObject({
        type: "agent_stream",
        event: { type: "turn_completed", provider: "codex", turnId: "turn-1" },
      });
    } finally {
      harness.cleanup();
    }
  });

  test("every collapsed item is one timeline row with monotonic seqs", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    try {
      const { agentId, session } = await createManagedSession(harness);

      session.pushEvent(assistant("a1", "codex", "turn-1"));
      session.pushEvent(assistant("a2", "codex", "turn-1"));
      session.pushEvent(reasoning("r1", "codex", "turn-1"));
      session.pushEvent(reasoning("r2", "codex", "turn-1"));
      session.pushEvent(assistant("b1", "codex", "turn-1"));
      session.pushEvent(assistant("b2", "codex", "turn-1"));
      session.pushEvent(terminalEvent("turn_completed", "turn-1"));
      await waitForSessionEventQueue();

      const rows = await harness.manager.getTimelineRows(agentId);
      expect(getTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a1a2" },
        { type: "reasoning", text: "r1r2" },
        { type: "assistant_message", text: "b1b2" },
      ]);
      expectContiguousRowSeqs(rows, [1, 2, 3, 4]);
      const timelineEvents = getTimelineStreamEvents(harness.events, agentId);
      expect(timelineEvents).toHaveLength(4);
      expectContiguousLiveSeqs(timelineEvents, [1, 2, 3, 4]);
      expect(getProjectedTimelineItems(rows)).toEqual([
        { type: "assistant_message", text: "a1a2" },
        { type: "reasoning", text: "r1r2" },
        { type: "assistant_message", text: "b1b2" },
      ]);

      await vi.advanceTimersByTimeAsync(100);
      expect(getTimelineItems(await harness.manager.getTimelineRows(agentId))).toEqual([
        { type: "assistant_message", text: "a1a2" },
        { type: "reasoning", text: "r1r2" },
        { type: "assistant_message", text: "b1b2" },
      ]);
      expect(getTimelineStreamEvents(harness.events, agentId)).toHaveLength(4);
    } finally {
      harness.cleanup();
    }
  });
});

test("replays a silent provider subagent completion after manager restart", async () => {
  const root = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), "completion-replay-"));
  const logger = createTestLogger();
  const client = new TestAgentClient();
  const storage = new AgentStorage(join(root, "agents"), logger);
  const completions = new ProviderSubagentCompletionStore(root);
  const manager = new AgentManager({
    clients: { codex: client },
    registry: storage,
    providerSubagentCompletionStore: completions,
    logger,
  });
  const agentId = AGENT_IDS[0];
  await manager.createAgent({ provider: "codex", cwd: root }, agentId, { workspaceId: undefined });
  client.getSession(root).pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: {
      type: "upsert",
      id: "helper-1",
      sourceEventId: "native-completion-1",
      status: "completed",
      timestamp: "2026-01-02T03:04:05.000Z",
    },
  });
  client.getSession(root).pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: {
      type: "upsert",
      id: "helper-1",
      sourceEventId: "native-completion-1",
      status: "completed",
      timestamp: "2026-01-02T03:04:05.000Z",
    },
  });
  client.getSession(root).pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: { type: "upsert", id: "helper-2", status: "failed" },
  });
  await waitForSessionEventQueue();
  await manager.flush();
  await storage.flush();
  expect(manager.listProviderSubagents(agentId)).toMatchObject([
    { id: "helper-1", status: "completed" },
    { id: "helper-2", status: "failed", completionEvidence: { sourceEventTime: null } },
  ]);
  const journal = await completions.readAll(agentId);
  expect(journal).toMatchObject([
    {
      sequence: 1,
      helperId: "helper-1",
      nativeTurnId: null,
      role: null,
      sourceEventTime: "2026-01-02T03:04:05.000Z",
      provenance: "provider-stream",
      outcome: "completed",
      gaps: ["native turn identity unavailable", "helper role unavailable"],
    },
    {
      helperId: "helper-2",
      sourceEventTime: null,
      outcome: "failed",
      gaps: [
        "native event identity unavailable",
        "native turn identity unavailable",
        "helper role unavailable",
        "source event time unavailable",
        "deduplication identity unavailable",
      ],
    },
  ]);
  expect(journal).toHaveLength(2);
  for (const event of journal)
    await completions.acknowledge(agentId, event.sequence, "test-consumer");
  expect(await completions.replay(agentId, "test-consumer")).toHaveLength(0);
  await expect(completions.acknowledge(agentId, 99)).rejects.toThrow(
    "Cannot acknowledge unknown completion",
  );
  await manager.closeAgent(agentId);
  await manager.flush();
  const crashEvent = {
    type: "upsert" as const,
    id: "helper-3",
    sourceEventId: "native-completion-3",
    status: "canceled" as const,
    timestamp: "2026-01-02T03:05:00.000Z",
  };
  await completions.append(agentId, "codex", crashEvent);
  await completions.append(agentId, "codex", crashEvent);
  expect(await completions.replay(agentId, "test-consumer")).toMatchObject([
    { helperId: "helper-3" },
  ]);
  // Model a crash after the durable append and before projection/ack.
  const restartedStorage = new AgentStorage(join(root, "agents"), logger);
  const restarted = new AgentManager({
    clients: { codex: client },
    registry: restartedStorage,
    providerSubagentCompletionStore: new ProviderSubagentCompletionStore(root),
    logger,
  });
  await ensureAgentLoaded(agentId, {
    agentManager: restarted,
    agentStorage: restartedStorage,
    logger,
  });
  const recovered = restarted.listProviderSubagents(agentId);
  expect(recovered.map(({ id }) => id)).toEqual(
    expect.arrayContaining(["helper-1", "helper-2", "helper-3"]),
  );
  expect(recovered.find(({ id }) => id === "helper-1")?.status).toBe("completed");
  expect(recovered.find(({ id }) => id === "helper-2")).toMatchObject({
    status: "failed",
    completionEvidence: { sourceEventTime: null },
  });
  expect(recovered.find(({ id }) => id === "helper-3")?.status).toBe("canceled");
  await expect(restartedStorage.get(agentId)).resolves.toMatchObject({ id: agentId });
  await restarted.closeAgent(agentId);
  await restarted.flush();
  await restartedStorage.flush();
  // Loading again in the same manager must rebuild even already acknowledged events.
  await ensureAgentLoaded(agentId, {
    agentManager: restarted,
    agentStorage: restartedStorage,
    logger,
  });
  expect(restarted.listProviderSubagents(agentId).map(({ id }) => id)).toEqual(
    expect.arrayContaining(["helper-1", "helper-2", "helper-3"]),
  );
  await restarted.closeAgent(agentId);
  await restarted.flush();
  await restartedStorage.flush();
});

test("durable consumers keep independent acknowledgements and uncertain deliveries", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-consumers-"));
  const store = new ProviderSubagentCompletionStore(root);
  const first = await store.append("parent", "codex", {
    type: "upsert",
    id: "helper",
    status: "completed",
  });
  const second = await store.append("parent", "codex", {
    type: "upsert",
    id: "helper",
    status: "completed",
  });
  expect(first?.sequence).not.toBe(second?.sequence);
  expect(first?.gaps).toContain("deduplication identity unavailable");
  const native = {
    type: "upsert",
    id: "helper",
    status: "failed",
    sourceEventId: "event-1",
    nativeTurnId: "turn-2",
    role: "worker",
  } as const;
  const third = await store.append("parent", "codex", native);
  expect(await store.append("parent", "codex", native)).toEqual(third);
  expect(third).toMatchObject({ sourceEventId: "event-1", nativeTurnId: "turn-2", role: "worker" });
  if (!first || !second || !third) throw new Error("missing observations");
  await store.acknowledge("parent", second.sequence, "consumer-a");
  expect((await store.replay("parent", "consumer-a")).map((e) => e.sequence)).toEqual([
    first.sequence,
    third.sequence,
  ]);
  const restarted = new ProviderSubagentCompletionStore(root);
  expect(await restarted.replay("parent", "consumer-b")).toHaveLength(3);
  expect(await restarted.replay("parent", "consumer-a")).toHaveLength(2);
  const unknown = await restarted.append("parent", "codex", { type: "upsert", id: "silent" });
  expect(unknown).toMatchObject({ outcome: "unknown" });
  const verification = await restarted.verify("parent", first.sequence, {
    outcome: "failed",
    evidence: "independent check",
  });
  expect(verification).toMatchObject({
    kind: "verification",
    verifiesSequence: first.sequence,
    outcome: "failed",
  });
  expect((await restarted.readAll("parent"))[0]).toEqual(first);
  const timed = {
    type: "upsert",
    id: "timed",
    status: "completed",
    timestamp: "2026-01-01T00:00:00Z",
  } as const;
  const timedFirst = await restarted.append("parent", "codex", timed);
  const timedSecond = await restarted.append("parent", "codex", timed);
  expect(timedFirst?.deliveryId).not.toBe(timedSecond?.deliveryId);
});

test("managed child terminal settlement is durable without a final report", async () => {
  const root = mkdtempSync(join(tmpdir(), "managed-completions-"));
  const logger = createTestLogger();
  const client = new TestAgentClient();
  const storage = new AgentStorage(join(root, "agents"), logger);
  const store = new ProviderSubagentCompletionStore(root);
  const manager = new AgentManager({
    clients: { codex: client },
    registry: storage,
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
    labels: { [PARENT_AGENT_ID_LABEL]: AGENT_IDS[1] },
  });
  const session = client.getSession(root);
  session.pushEvent({ type: "turn_started", provider: "codex", turnId: "native-1" });
  session.pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: { type: "upsert", id: "silent-helper", status: "running" },
  });
  session.pushEvent({
    type: "turn_completed",
    provider: "codex",
    turnId: "native-1",
    completionSource: { nativeTurnId: "native-1" },
  });
  await waitForSessionEventQueue();
  await manager.flush();
  expect(await store.readAll(AGENT_IDS[1])).toMatchObject([
    {
      helperId: AGENT_IDS[0],
      nativeTurnId: "native-1",
      provenance: "managed-lifecycle",
      outcome: "completed",
    },
  ]);
  expect(await store.readAll(AGENT_IDS[0])).toMatchObject([
    {
      helperId: "silent-helper",
      outcome: "unknown",
      gaps: expect.arrayContaining(["parent turn ended without helper terminal report"]),
    },
  ]);
  const restarted = new ProviderSubagentCompletionStore(root);
  expect(await restarted.replay(AGENT_IDS[1], "automation")).toHaveLength(1);
  // Repeated source delivery is one observation, even after the turn was applied.
  session.pushEvent({
    type: "turn_completed",
    provider: "codex",
    turnId: "native-1",
    completionSource: { nativeTurnId: "native-1" },
  });
  await waitForSessionEventQueue();
  await manager.flush();
  expect(await store.readAll(AGENT_IDS[1])).toHaveLength(1);
  for (const [index, type] of ["turn_failed", "turn_canceled"].entries()) {
    const turnId = `native-${index + 2}`;
    session.pushEvent({ type: "turn_started", provider: "codex", turnId });
    session.pushEvent(
      type === "turn_failed"
        ? {
            type: "turn_failed",
            provider: "codex",
            turnId,
            error: "failed",
            completionSource: { nativeTurnId: turnId },
          }
        : {
            type: "turn_canceled",
            provider: "codex",
            turnId,
            reason: "canceled",
            completionSource: { nativeTurnId: turnId },
          },
    );
    await waitForSessionEventQueue();
  }
  await manager.flush();
  expect((await store.readAll(AGENT_IDS[1])).map((e) => e.outcome)).toEqual([
    "completed",
    "failed",
    "canceled",
  ]);
  session.pushEvent({ type: "turn_started", provider: "codex", turnId: "adapter-generated" });
  session.pushEvent({ type: "turn_completed", provider: "codex", turnId: "adapter-generated" });
  await waitForSessionEventQueue();
  await manager.flush();
  expect((await store.readAll(AGENT_IDS[1])).at(-1)).toMatchObject({
    nativeTurnId: null,
    gaps: expect.arrayContaining([
      "native turn identity unavailable",
      "deduplication identity unavailable",
    ]),
  });
  session.pushEvent({ type: "turn_started", provider: "codex", turnId: "interrupted-runtime" });
  await waitForSessionEventQueue();
  await manager.closeAgent(AGENT_IDS[0]);
  await manager.flush();
  expect((await store.readAll(AGENT_IDS[1])).at(-1)).toMatchObject({
    outcome: "unknown",
    gaps: expect.arrayContaining(["runtime closed without source terminal report"]),
  });
});

test("managed terminal settles when its journal append fails", async () => {
  const root = mkdtempSync(join(tmpdir(), "managed-append-failure-"));
  const logs: string[] = [];
  const logger = pino({ level: "error" }, { write: (line) => logs.push(line) });
  const client = new TestAgentClient();
  let fail = true;
  const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
    if (fail) throw new Error("journal unavailable");
    await writeJsonFileAtomic(file, data);
  });
  const manager = new AgentManager({
    clients: { codex: client },
    registry: new AgentStorage(join(root, "agents"), logger),
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
    labels: { [PARENT_AGENT_ID_LABEL]: AGENT_IDS[1] },
  });
  const session = client.getSession(root);
  const stream = manager.streamAgent(AGENT_IDS[0], "work");
  const started = stream.next();
  await manager.waitForAgentRunStart(AGENT_IDS[0]);
  await started;
  session.pushEvent({ type: "turn_completed", provider: "codex", turnId: "turn-1" });
  await waitForSessionEventQueue();
  await expect(stream.next()).resolves.toMatchObject({ value: { type: "turn_completed" } });
  await expect(stream.next()).resolves.toEqual({ done: true, value: undefined });
  expect(await store.readAll(AGENT_IDS[1])).toEqual([]);
  expect(logs.join("")).toContain("Helper observation not durably accepted");
  expect(logs.join("")).toContain(AGENT_IDS[0]);
  fail = false;
  session.pushEvent({ type: "turn_started", provider: "codex", turnId: "later" });
  session.pushEvent({
    type: "turn_completed",
    provider: "codex",
    turnId: "later",
    completionSource: { nativeTurnId: "later" },
  });
  await waitForSessionEventQueue();
  await manager.flush();
  expect(await new ProviderSubagentCompletionStore(root).readAll(AGENT_IDS[1])).toMatchObject([
    {
      nativeTurnId: "later",
      outcome: "completed",
      gaps: expect.arrayContaining([
        `observation not durably accepted: ${AGENT_IDS[0]} (managed-lifecycle)`,
      ]),
    },
  ]);
  session.pushEvent({ type: "turn_started", provider: "codex", turnId: "closing" });
  await waitForSessionEventQueue();
  fail = true;
  await expect(manager.closeAgent(AGENT_IDS[0])).resolves.toBeUndefined();
  expect(logs).toHaveLength(2);
  fail = false;
  expect(await new ProviderSubagentCompletionStore(root).readAll(AGENT_IDS[1])).toHaveLength(1);
});

test("failed observation coverage remains visible after a clean store flush", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-coverage-"));
  let fail = true;
  const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
    if (fail) throw new Error("journal unavailable");
    await writeJsonFileAtomic(file, data);
  });
  await expect(
    store.append(AGENT_IDS[0], "codex", {
      type: "upsert",
      id: "helper-failed",
      status: "completed",
    }),
  ).rejects.toThrow("journal unavailable");
  store.recordFailedAppend(AGENT_IDS[0], "helper-failed", "provider-stream");
  await expect(store.flush()).rejects.toThrow("journal unavailable");
  // A new instance cannot see memory that was never written, as after an abrupt exit.
  expect(
    (await new ProviderSubagentCompletionStore(root).consume(AGENT_IDS[0], "before-recovery"))
      .coverageUncertain,
  ).toBe(false);
  fail = false;
  await store.flush();
  const replay = await new ProviderSubagentCompletionStore(root).consume(AGENT_IDS[0], "recovery");
  expect(replay.events).toEqual([]);
  expect(replay.coverageUncertain).toBe(true);
});

test("shutdown continues when completion coverage persistence fails", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-shutdown-"));
  const logs: string[] = [];
  const logger = pino({ level: "warn" }, { write: (line) => logs.push(line) });
  const store = new ProviderSubagentCompletionStore(root, 10000, async () => {
    throw new Error("journal unavailable");
  });
  store.recordFailedAppend("parent", "helper", "provider-stream");
  const manager = new AgentManager({ clients: {}, providerSubagentCompletionStore: store, logger });
  await expect(manager.flushForShutdown()).resolves.toBeUndefined();
  expect(logs.join("")).toContain("Failed to persist helper completion coverage at shutdown");
});

test("provider subagent projection survives a failed journal append", async () => {
  const root = mkdtempSync(join(tmpdir(), "provider-append-failure-"));
  const logger = createTestLogger();
  const client = new TestAgentClient();
  let fail = true;
  const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
    if (fail) throw new Error("journal unavailable");
    await writeJsonFileAtomic(file, data);
  });
  const manager = new AgentManager({
    clients: { codex: client },
    registry: new AgentStorage(join(root, "agents"), logger),
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
  });
  client.getSession(root).pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: { type: "upsert", id: "helper-failed", status: "completed" },
  });
  await waitForSessionEventQueue();
  expect(manager.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
    { id: "helper-failed", status: "completed" },
  ]);
  fail = false;
  await store.append(AGENT_IDS[0], "codex", {
    type: "upsert",
    id: "helper-later",
    status: "failed",
  });
  expect(await store.replay(AGENT_IDS[0], "gap-consumer")).toMatchObject([
    {
      helperId: "helper-later",
      gaps: expect.arrayContaining([
        "observation not durably accepted: helper-failed (provider-stream)",
      ]),
    },
  ]);
  await manager.closeAgent(AGENT_IDS[0]);
  await manager.flush();
});

test("many failed observations stay bounded and allow durable recovery", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-failure-bound-"));
  let fail = true;
  const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
    if (fail) throw new Error("journal unavailable");
    await writeJsonFileAtomic(file, data);
  });
  for (let index = 0; index < 1000; index++) {
    const id = `helper-${index}`;
    await expect(
      store.append("parent", "codex", { type: "upsert", id, status: "completed" }),
    ).rejects.toThrow("journal unavailable");
    store.recordFailedAppend("parent", id, "provider-stream");
  }
  fail = false;
  await store.append("parent", "codex", { type: "upsert", id: "recovered", status: "completed" });
  const [recovered] = await new ProviderSubagentCompletionStore(root).replay(
    "parent",
    "automation",
  );
  expect(recovered.helperId).toBe("recovered");
  expect(recovered.gaps).toContain("984 further observations not durably accepted");
  expect(recovered.gaps).toContain(
    "observation not durably accepted: helper-999 (provider-stream)",
  );
  expect(
    recovered.gaps.filter((gap) => gap.startsWith("observation not durably accepted:")),
  ).toHaveLength(16);
  expect(recovered.gaps.join("")).not.toContain("helper-0 (");
  expect(Buffer.byteLength(JSON.stringify(recovered))).toBeLessThan(64 * 1024);
  await store.append("parent", "codex", { type: "upsert", id: "next", status: "completed" });
  expect((await store.readAll("parent"))[1].gaps).not.toContain(
    "984 further observations not durably accepted",
  );
});

test("failure detail and parent overflow remain visible without blocking appends", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-failure-overflow-"));
  const store = new ProviderSubagentCompletionStore(root);
  for (let index = 0; index < 257; index++) {
    store.recordFailedAppend(`parent-${index}`, `helper-${index}`, "provider-stream");
  }
  for (let index = 0; index < 16; index++) {
    store.recordFailedAppend("parent-256", "\u0000".repeat(10_000), "provider-stream");
  }
  await store.append("parent-256", "codex", {
    type: "upsert",
    id: "recovered",
    status: "completed",
  });
  await store.append("parent-0", "codex", { type: "upsert", id: "evicted", status: "completed" });
  const restarted = new ProviderSubagentCompletionStore(root);
  const [recovered] = await restarted.replay("parent-256", "automation");
  const overflow =
    "failure tracking overflow: failed observations for evicted parents may be missing";
  expect(recovered.gaps).toContain(overflow);
  expect(recovered.gaps).toContain("1 further observations not durably accepted");
  const details = recovered.gaps.filter((gap) =>
    gap.startsWith("observation not durably accepted:"),
  );
  expect(details).toHaveLength(16);
  expect(details.every((gap) => gap.endsWith(" [truncated]"))).toBe(true);
  expect(Buffer.byteLength(JSON.stringify(recovered))).toBeLessThan(32 * 1024);
  const [evicted] = await restarted.replay("parent-0", "automation");
  expect(evicted.gaps).toContain(overflow);
  expect(evicted.gaps).not.toContain(
    "observation not durably accepted: helper-0 (provider-stream)",
  );
});

test.each(["turn-end", "close", "reload"])(
  "running helper observation append failure preserves %s and is replayed as a later gap",
  async (action) => {
    const root = mkdtempSync(join(tmpdir(), "running-helper-append-failure-"));
    const logs: string[] = [];
    const logger = pino({ level: "error" }, { write: (line) => logs.push(line) });
    const client = new TestAgentClient();
    let fail = true;
    const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
      if (fail) throw new Error("journal unavailable");
      await writeJsonFileAtomic(file, data);
    });
    const manager = new AgentManager({
      clients: { codex: client },
      registry: new AgentStorage(join(root, "agents"), logger),
      providerSubagentCompletionStore: store,
      logger,
    });
    try {
      await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
        workspaceId: undefined,
      });
      const session = client.getSession(root);
      session.pushEvent({ type: "turn_started", provider: "codex", turnId: "parent-turn" });
      session.pushEvent({
        type: "provider_subagent",
        provider: "codex",
        event: { type: "upsert", id: "running-helper", status: "running" },
      });
      await waitForSessionEventQueue();
      if (action === "turn-end") {
        session.pushEvent({ type: "turn_completed", provider: "codex", turnId: "parent-turn" });
        await waitForSessionEventQueue();
        expect(manager.getAgent(AGENT_IDS[0])?.lifecycle).toBe("idle");
      } else if (action === "close") {
        await manager.closeAgent(AGENT_IDS[0]);
        expect(manager.getAgent(AGENT_IDS[0])).toBeNull();
      } else {
        await manager.reloadAgentSession(AGENT_IDS[0]);
        expect(manager.getAgent(AGENT_IDS[0])?.lifecycle).toBe("idle");
      }
      expect(logs.join("")).toContain("Helper observation not durably accepted");
      expect(logs.join("")).toContain("running-helper");
      expect(await store.readAll(AGENT_IDS[0])).toEqual([]);
      fail = false;
      await store.append(AGENT_IDS[0], "codex", {
        type: "upsert",
        id: "later",
        status: "completed",
      });
      expect(
        await new ProviderSubagentCompletionStore(root).replay(AGENT_IDS[0], "automation"),
      ).toMatchObject([
        {
          helperId: "later",
          gaps: expect.arrayContaining([
            "observation not durably accepted: running-helper (daemon-observation)",
          ]),
        },
      ]);
    } finally {
      fail = false;
      await manager.closeAgent(AGENT_IDS[0]);
      await manager.flush();
    }
  },
);

test("MCP consumers replay and ack independently across daemon reload", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-api-"));
  const logger = createTestLogger();
  const store = new ProviderSubagentCompletionStore(root);
  const storage = new AgentStorage(join(root, "agents"), logger);
  const manager = new AgentManager({ clients: {}, providerSubagentCompletionStore: store, logger });
  await store.append("parent", "codex", { type: "upsert", id: "helper", status: "completed" });
  const providers = new ProviderSnapshotManager({ logger });
  const server = await createAgentMcpServer({
    agentManager: manager,
    agentStorage: storage,
    providerSnapshotManager: providers,
    logger,
  });
  const client = new Client({ name: "completion-consumer-test", version: "1" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  try {
    const replay = await client.callTool({
      name: "helper_completions",
      arguments: { operation: "replay", parentAgentId: "parent", consumer: "automation" },
    });
    expect(replay.isError).not.toBe(true);
    expect(replay.structuredContent).toMatchObject({
      events: [{ sequence: 1, helperId: "helper" }],
      gap: null,
    });
    expect(
      (
        await client.callTool({
          name: "helper_completions",
          arguments: {
            operation: "acknowledge",
            parentAgentId: "parent",
            consumer: "automation",
            sequence: 1,
          },
        })
      ).isError,
    ).not.toBe(true);
    expect(
      await new ProviderSubagentCompletionStore(root).replay("parent", "automation"),
    ).toHaveLength(0);
    expect(await store.replay("parent", "another-consumer")).toHaveLength(1);
    const verified = await client.callTool({
      name: "helper_completions",
      arguments: {
        operation: "verify",
        parentAgentId: "parent",
        sequence: 1,
        outcome: "unknown",
        evidence: "No independent result available",
      },
    });
    expect(verified.structuredContent).toMatchObject({
      kind: "verification",
      verifiesSequence: 1,
      outcome: "unknown",
    });
    const invalid = await client.callTool({
      name: "helper_completions",
      arguments: {
        operation: "acknowledge",
        parentAgentId: "parent",
        consumer: "automation",
        sequence: 999,
      },
    });
    expect(invalid.isError).toBe(true);
  } finally {
    await client.close();
    await server.close();
    providers.destroy();
    await manager.flush();
    await storage.flush();
  }
});

test("helper_completions limits agent callers to their own parent and leaves the host client unrestricted", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-authority-"));
  const logger = createTestLogger();
  const store = new ProviderSubagentCompletionStore(root);
  const storage = new AgentStorage(join(root, "agents"), logger);
  await store.append("parent-a", "codex", { type: "upsert", id: "helper-a", status: "completed" });
  await store.append("parent-b", "codex", { type: "upsert", id: "helper-b", status: "completed" });
  store.recordFailedAppend("parent-a", "unwritten-helper", "provider-stream");
  await store.flush();
  const manager = new AgentManager({
    clients: {},
    providerSubagentCompletionStore: new ProviderSubagentCompletionStore(root),
    logger,
  });
  const providers = new ProviderSnapshotManager({ logger });
  const connect = async (callerAgentId?: string) => {
    const server = await createAgentMcpServer({
      agentManager: manager,
      agentStorage: storage,
      providerSnapshotManager: providers,
      callerAgentId,
      logger,
    });
    const client = new Client({
      name: `completion-authority-${callerAgentId ?? "host"}`,
      version: "1",
    });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    await client.connect(clientTransport);
    return { server, client };
  };
  const a = await connect("parent-a");
  const b = await connect("parent-b");
  const host = await connect();
  const call = (client: Client, args: Record<string, unknown>) =>
    client.callTool({ name: "helper_completions", arguments: args });
  try {
    const hostReplay = {
      name: "host_helper_completions",
      arguments: { operation: "replay", consumer: "host", limit: 1 },
    };
    expect((await a.client.callTool(hostReplay)).isError).toBe(true);
    expect((await host.client.callTool(hostReplay)).structuredContent).toMatchObject({
      coverageUncertain: true,
      events: [{ parentAgentId: "parent-a" }],
      gap: null,
    });
    // Each agent can use the journal for its own helpers.
    expect(
      (await call(a.client, { operation: "replay", parentAgentId: "parent-a", consumer: "mine" }))
        .structuredContent,
    ).toMatchObject({ events: [{ helperId: "helper-a" }], gap: null });

    // Neither agent can read, ack, release, accept gaps for, or verify the other's journal.
    const crossAttempts: Record<string, unknown>[] = [
      { operation: "replay", parentAgentId: "parent-b", consumer: "mine" },
      { operation: "acknowledge", parentAgentId: "parent-b", consumer: "mine", sequence: 2 },
      { operation: "accept_gap", parentAgentId: "parent-b", consumer: "mine", discardedThrough: 0 },
      { operation: "release", parentAgentId: "parent-b", consumer: "mine" },
      {
        operation: "verify",
        parentAgentId: "parent-b",
        sequence: 2,
        outcome: "failed",
        evidence: "forged",
      },
    ];
    for (const args of crossAttempts) {
      const result = await call(a.client, args);
      expect(result.isError, String(args.operation)).toBe(true);
    }
    // The rejected calls left parent-b untouched: no consumer registered, no verification added.
    expect(await store.replay("parent-b", "mine")).toHaveLength(1);
    expect(await store.replay("parent-b", "other")).toMatchObject([{ helperId: "helper-b" }]);
    expect(
      (await call(b.client, { operation: "replay", parentAgentId: "parent-b", consumer: "mine" }))
        .structuredContent,
    ).toMatchObject({ events: [{ helperId: "helper-b" }] });
    expect(
      (await call(b.client, { operation: "replay", parentAgentId: "parent-a", consumer: "mine" }))
        .isError,
    ).toBe(true);

    // The host (top-level) client keeps access to any parent, including verification.
    expect(
      (
        await call(host.client, {
          operation: "replay",
          parentAgentId: "parent-a",
          consumer: "runner",
        })
      ).structuredContent,
    ).toMatchObject({ events: [{ helperId: "helper-a" }] });
    expect(
      (
        await call(host.client, {
          operation: "verify",
          parentAgentId: "parent-b",
          sequence: 2,
          outcome: "completed",
          evidence: "checked by runner",
        })
      ).isError,
    ).not.toBe(true);
  } finally {
    for (const { client, server } of [a, b, host]) {
      await client.close();
      await server.close();
    }
    providers.destroy();
    await manager.flush();
    await storage.flush();
  }
});

test("retention reports replay gaps and never reuses delivery sequences", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-retention-"));
  const store = new ProviderSubagentCompletionStore(root, 2);
  for (let i = 0; i < 3; i++)
    await store.append("parent", "codex", {
      type: "upsert",
      id: `helper-${i}`,
      status: "completed",
    });
  expect(await store.consume("parent", "consumer")).toMatchObject({
    events: [{ sequence: 2 }, { sequence: 3 }],
    gap: { discardedThrough: 1 },
  });
  await expect(store.replay("parent", "consumer")).rejects.toThrow("replay gap");
  await store.acknowledge("parent", 2, "consumer");
  expect((await store.consume("parent", "consumer")).gap).toEqual({ discardedThrough: 1 });
  await store.acceptGap("parent", "consumer", 1);
  expect(await store.replay("parent", "consumer")).toMatchObject([{ sequence: 3 }]);
  const restarted = new ProviderSubagentCompletionStore(root, 2);
  expect(
    await restarted.append("parent", "codex", { type: "upsert", id: "next", status: "completed" }),
  ).toMatchObject({ sequence: 4 });
  expect(await restarted.readAll("parent")).toHaveLength(2);
  const large = await restarted.append("parent", "codex", {
    type: "upsert",
    id: "large",
    status: "completed",
    role: "x".repeat(60_000),
  });
  if (!large) throw new Error("missing observation");
  await expect(
    restarted.verify("parent", large.sequence, { outcome: "unknown", evidence: "x".repeat(8192) }),
  ).rejects.toThrow("Verification exceeds 64 KiB");
});

test("apply before ack failure redelivers and rebuilds the projection", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-ack-crash-"));
  const logger = createTestLogger();
  const client = new TestAgentClient();
  const storage = new AgentStorage(join(root, "agents"), logger);
  let failAck = false;
  const store = new ProviderSubagentCompletionStore(root, 10000, async (file, data) => {
    if (failAck) throw new Error("simulated crash before ack persistence");
    await writeJsonFileAtomic(file, data);
  });
  const manager = new AgentManager({
    clients: { codex: client },
    registry: storage,
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
  });
  client.getSession(root).pushEvent({
    type: "provider_subagent",
    provider: "codex",
    event: { type: "upsert", id: "helper", status: "completed", sourceEventId: "terminal-1" },
  });
  await waitForSessionEventQueue();
  await manager.flush();
  expect(manager.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
    { id: "helper", status: "completed" },
  ]);
  failAck = true;
  await expect(store.acknowledge(AGENT_IDS[0], 1, "automation")).rejects.toThrow("simulated crash");
  failAck = false;
  const recoveredStore = new ProviderSubagentCompletionStore(root);
  expect(await recoveredStore.replay(AGENT_IDS[0], "automation")).toMatchObject([
    { helperId: "helper" },
  ]);
  await manager.closeAgent(AGENT_IDS[0]);
  await manager.flush();
  await storage.flush();
  const recoveredStorage = new AgentStorage(join(root, "agents"), logger);
  const recovered = new AgentManager({
    clients: { codex: client },
    registry: recoveredStorage,
    providerSubagentCompletionStore: recoveredStore,
    logger,
  });
  await ensureAgentLoaded(AGENT_IDS[0], {
    agentManager: recovered,
    agentStorage: recoveredStorage,
    logger,
  });
  expect(recovered.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
    { id: "helper", status: "completed" },
  ]);
  await recoveredStore.acknowledge(AGENT_IDS[0], 1, "automation");
  expect(await recoveredStore.replay(AGENT_IDS[0], "automation")).toHaveLength(0);
  await recovered.closeAgent(AGENT_IDS[0]);
  await recovered.flush();
  await recoveredStorage.flush();
});

test("Codex preserves native terminal identity separately from normalized routing identity", async () => {
  const appServer = createFakeCodexAppServer();
  const session = new CodexAppServerAgentSession(
    { provider: "codex", cwd: process.cwd(), model: "gpt-5.4" },
    null,
    createTestLogger(),
    async () => appServer.child,
  );
  try {
    const started = await session.startTurn("first");
    await appServer.waitForTurnStart();
    appServer.startsTurn({ threadId: "thread-1", turnId: "provider-native-turn" });
    const terminal = waitForNextEvent(session, "turn_completed");
    appServer.child.stdout.write(
      `${JSON.stringify({ method: "turn/completed", params: { threadId: "thread-1", turn: { id: "provider-native-turn", status: "completed" } } })}\n`,
    );
    const event = await terminal;
    expect(event.turnId).toBe(started.turnId);
    expect(event.completionSource?.nativeTurnId).toBe("provider-native-turn");
    expect(event.turnId).not.toBe(event.completionSource?.nativeTurnId);
    appServer.assertNoErrors();
  } finally {
    await session.close();
    appServer.disconnect();
  }
});

test("Codex history retains each source terminal with its native turn and time", async () => {
  const appServer = createFakeCodexAppServer({
    "thread/read": (params) => {
      const { threadId } = params as { threadId: string };
      if (threadId === "thread-1")
        return {
          thread: {
            turns: [
              {
                items: [
                  {
                    type: "collabAgentToolCall",
                    id: "spawn",
                    tool: "spawnAgent",
                    status: "inProgress",
                    prompt: "child",
                    receiverThreadIds: ["child"],
                    agentsStates: { child: { status: "running" } },
                  },
                ],
              },
            ],
          },
        };
      return {
        thread: {
          turns: [
            {
              id: "child-turn-1",
              status: "completed",
              completedAt: 1767323045,
              items: [{ type: "agentMessage", id: "message-1", text: "first" }],
            },
            {
              id: "child-turn-2",
              status: "failed",
              completedAt: 1767323105,
              items: [{ type: "agentMessage", id: "message-2", text: "second" }],
            },
          ],
        },
      };
    },
  });
  const session = new CodexAppServerAgentSession(
    { provider: "codex", cwd: process.cwd(), model: "gpt-5.4" },
    { sessionId: "thread-1" },
    createTestLogger(),
    async () => appServer.child,
  );
  try {
    await session.getRuntimeInfo();
    const observations: unknown[] = [];
    for await (const event of session.streamHistory()) {
      if (event.type === "provider_subagent" && event.event.type === "upsert")
        observations.push(event.event);
    }
    expect(observations).toMatchObject([
      {
        id: "child",
        status: "completed",
        nativeTurnId: "child-turn-1",
        timestamp: "2026-01-02T03:04:05.000Z",
      },
      {
        id: "child",
        status: "failed",
        nativeTurnId: "child-turn-2",
        timestamp: "2026-01-02T03:05:05.000Z",
      },
    ]);
    appServer.assertNoErrors();
  } finally {
    await session.close();
    appServer.disconnect();
  }
});

test("startup recovery journals source turns for unloaded agents without duplicate replay", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-recovery-"));
  const client = new TestAgentClient();
  let paginated = false;
  const servers: ReturnType<typeof createFakeCodexAppServer>[] = [];
  vi.spyOn(client, "resumeSession").mockImplementation(async () => {
    const server = createFakeCodexAppServer({
      "thread/read": (params) => {
        const { threadId } = params as { threadId: string };
        if (threadId !== "parent-thread") {
          if (paginated) return { thread: { historyMode: "paginated" } };
          return {
            thread: {
              turns: [
                { id: "turn-one", status: "completed", completedAt: 1767323045, items: [] },
                { id: "turn-two", status: "failed", completedAt: 1767323105, items: [] },
              ],
            },
          };
        }
        return {
          thread: {
            turns: [
              {
                items: [
                  {
                    type: "collabAgentToolCall",
                    id: "spawn",
                    tool: "spawnAgent",
                    status: "inProgress",
                    prompt: "child",
                    receiverThreadIds: ["child"],
                    agentsStates: { child: { status: "running" } },
                  },
                ],
              },
            ],
          },
        };
      },
      "thread/items/list": () => ({
        data: [
          {
            turnId: "unbound",
            item: { type: "agentMessage", id: "message", text: "output" },
            completedAtMs: 1767323045000,
          },
        ],
      }),
      "thread/turns/list": () => ({ data: [{ id: "unbound", status: "failed" }] }),
    });
    servers.push(server);
    const session = new CodexAppServerAgentSession(
      { provider: "codex", cwd: root },
      { sessionId: "parent-thread" },
      createTestLogger(),
      async () => server.child,
    );
    await session.getRuntimeInfo();
    return session;
  });
  const record = parseStoredAgentRecord({
    id: AGENT_IDS[0],
    provider: "codex",
    cwd: root,
    createdAt: "2026-01-02T00:00:00.000Z",
    updatedAt: "2026-01-02T00:00:00.000Z",
    persistence: { provider: "codex", sessionId: "parent-thread" },
  });
  try {
    const signal = new AbortController().signal;
    await recoverCodexHistory(
      [record],
      client,
      new ProviderSubagentCompletionStore(root),
      createTestLogger(),
      signal,
    );
    const restarted = new ProviderSubagentCompletionStore(root);
    expect(await restarted.readAll(AGENT_IDS[0])).toMatchObject([
      {
        helperId: "child",
        nativeTurnId: "turn-one",
        outcome: "completed",
        sourceEventTime: "2026-01-02T03:04:05.000Z",
      },
      {
        helperId: "child",
        nativeTurnId: "turn-two",
        outcome: "failed",
        sourceEventTime: "2026-01-02T03:05:05.000Z",
      },
    ]);
    const interrupted = new ProviderSubagentCompletionStore(root);
    vi.spyOn(interrupted, "advanceRecoveryCursor").mockRejectedValueOnce(new Error("cursor lost"));
    await expect(
      recoverCodexHistory([record], client, interrupted, createTestLogger(), signal),
    ).rejects.toThrow("cursor lost");
    await recoverCodexHistory([record], client, restarted, createTestLogger(), signal);
    expect(await new ProviderSubagentCompletionStore(root).readAll(AGENT_IDS[0])).toHaveLength(2);
    expect((await restarted.consume(AGENT_IDS[0], "recovery")).coverageUncertain).toBe(true);
    const liveRoot = join(root, "live-compact");
    const liveStore = new ProviderSubagentCompletionStore(liveRoot, 1);
    await liveStore.append(AGENT_IDS[0], "codex", {
      type: "upsert",
      id: "child",
      status: "completed",
      nativeTurnId: "turn-one",
    });
    await liveStore.append(AGENT_IDS[0], "codex", {
      type: "upsert",
      id: "other",
      status: "completed",
      nativeTurnId: "other-turn",
    });
    const liveRestarted = new ProviderSubagentCompletionStore(liveRoot, 1);
    await recoverCodexHistory([record], client, liveRestarted, createTestLogger(), signal);
    expect(await liveRestarted.readAll(AGENT_IDS[0])).toMatchObject([
      { sequence: 3, nativeTurnId: "turn-two" },
    ]);

    const compactRoot = join(root, "compact");
    await recoverCodexHistory(
      [record],
      client,
      new ProviderSubagentCompletionStore(compactRoot, 1),
      createTestLogger(),
      signal,
    );
    const compactRestarted = new ProviderSubagentCompletionStore(compactRoot, 1);
    await recoverCodexHistory([record], client, compactRestarted, createTestLogger(), signal);
    expect(await compactRestarted.readAll(AGENT_IDS[0])).toMatchObject([
      { sequence: 2, nativeTurnId: "turn-two" },
    ]);

    const orderedRoot = join(root, "out-of-order");
    mkdirSync(orderedRoot);
    const orderedStore = new ProviderSubagentCompletionStore(orderedRoot);
    const orderedClient = new TestAgentClient();
    const orderedLogger = createTestLogger();
    const orderedStorage = new AgentStorage(join(orderedRoot, "agents"), orderedLogger);
    const timelineFile = join(root, "timeline.json");
    const original = new AgentManager({
      clients: { codex: orderedClient },
      registry: orderedStorage,
      durableTimelineStore: new FileTimelineStore(timelineFile, AGENT_IDS[0]),
      providerSubagentCompletionStore: orderedStore,
      logger: orderedLogger,
    });
    await original.createAgent({ provider: "codex", cwd: orderedRoot }, AGENT_IDS[0], {
      workspaceId: undefined,
    });
    orderedClient.getSession(orderedRoot).pushEvent({
      type: "provider_subagent",
      provider: "codex",
      event: { type: "upsert", id: "child", status: "failed", nativeTurnId: "turn-two" },
    });
    await waitForSessionEventQueue();
    await original.appendTimelineItem(AGENT_IDS[0], {
      type: "assistant_message",
      text: "persisted",
    });
    await original.flush();
    await original.closeAgent(AGENT_IDS[0]);
    await original.flush();
    await orderedStorage.flush();
    await recoverCodexHistory([record], client, orderedStore, createTestLogger(), signal);
    const reopened = new ProviderSubagentCompletionStore(orderedRoot);
    expect(await reopened.readAll(AGENT_IDS[0])).toMatchObject([
      { nativeTurnId: "turn-two" },
      { nativeTurnId: "turn-one" },
    ]);
    const restoredStorage = new AgentStorage(join(orderedRoot, "agents"), orderedLogger);
    const restored = new AgentManager({
      clients: { codex: new TestAgentClient() },
      registry: restoredStorage,
      durableTimelineStore: new FileTimelineStore(timelineFile, AGENT_IDS[0]),
      providerSubagentCompletionStore: reopened,
      logger: orderedLogger,
    });
    await ensureAgentLoaded(AGENT_IDS[0], {
      agentManager: restored,
      agentStorage: restoredStorage,
      logger: orderedLogger,
    });
    expect(await restored.getTimelineRows(AGENT_IDS[0])).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ item: expect.objectContaining({ text: "persisted" }) }),
      ]),
    );
    expect(restored.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
      { id: "child", status: "failed", completionEvidence: { nativeTurnId: "turn-two" } },
    ]);
    await restored.closeAgent(AGENT_IDS[0]);
    await restored.flush();
    await restoredStorage.flush();

    paginated = true;
    const unbound = new ProviderSubagentCompletionStore(join(root, "paginated"));
    await recoverCodexHistory([record], client, unbound, createTestLogger(), signal);
    expect(await unbound.readAll(AGENT_IDS[0])).toEqual([]);
    expect((await unbound.consume(AGENT_IDS[0], "recovery")).coverageUncertain).toBe(true);

    for (const server of servers) server.assertNoErrors();
  } finally {
    for (const server of servers) server.disconnect();
  }
});

test("recovery continues through bounded batches and wraps after journal restart", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-scan-"));
  const client = new TestAgentClient();
  const seen: string[] = [];
  vi.spyOn(client, "resumeSession").mockImplementation(async (_handle, _config, context) => {
    seen.push(context.agentId);
    throw new Error("synthetic unavailable history");
  });
  const records = Array.from({ length: 27 }, (_, index) =>
    parseStoredAgentRecord({
      id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      provider: "codex",
      cwd: root,
      createdAt: "2026-01-02T00:00:00.000Z",
      updatedAt: "2026-01-02T00:00:00.000Z",
      persistence: { provider: "codex", sessionId: `thread-${index}` },
    }),
  );
  const signal = new AbortController().signal;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    createTestLogger(),
    signal,
  );
  expect(seen).toEqual(records.slice(0, 25).map((record) => record.id));
  seen.length = 0;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    createTestLogger(),
    signal,
  );
  expect(seen).toEqual([...records.slice(25), ...records.slice(0, 23)].map((record) => record.id));
});

test("new parents wait for their position after a persisted recovery cursor", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-new-parent-"));
  const client = new TestAgentClient();
  const seen: string[] = [];
  vi.spyOn(client, "resumeSession").mockImplementation(async (_handle, _config, context) => {
    seen.push(context.agentId);
    throw new Error("synthetic unavailable history");
  });
  function record(index: number) {
    return parseStoredAgentRecord({
      id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      provider: "codex",
      cwd: root,
      createdAt: "2026-01-02T00:00:00.000Z",
      updatedAt: "2026-01-02T00:00:00.000Z",
      persistence: { provider: "codex", sessionId: `thread-${index}` },
    });
  }
  const records = Array.from({ length: 50 }, (_, index) => record(index * 2));
  const signal = new AbortController().signal;
  const logger = createTestLogger();
  const info = vi.spyOn(logger, "info");
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    logger,
    signal,
  );
  expect(seen).toEqual(records.slice(0, 25).map((parent) => parent.id));
  const before = record(1);
  const after = record(49);
  records.push(before, after);
  seen.length = 0;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    logger,
    signal,
  );
  expect(seen).toEqual([after, ...records.slice(25, 49)].map((parent) => parent.id));
  expect(info).toHaveBeenLastCalledWith(
    { attempted: 25, deferred: 27, parentCount: 52, nextCursor: records[48].id },
    "Codex helper history recovery cycle finished",
  );
  seen.length = 0;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    logger,
    signal,
  );
  expect(seen).toEqual(
    [records[49], records[0], before, ...records.slice(1, 23)].map((parent) => parent.id),
  );
});

test("recovery resumes after a cursor write failure and reports it", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-cursor-failure-"));
  const client = new TestAgentClient();
  const seen: string[] = [];
  vi.spyOn(client, "resumeSession").mockImplementation(async (_handle, _config, context) => {
    seen.push(context.agentId);
    throw new Error("synthetic unavailable history");
  });
  const records = Array.from({ length: 3 }, (_, index) =>
    parseStoredAgentRecord({
      id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      provider: "codex",
      cwd: root,
      createdAt: "2026-01-02T00:00:00.000Z",
      updatedAt: "2026-01-02T00:00:00.000Z",
      persistence: { provider: "codex", sessionId: `thread-${index}` },
    }),
  );
  const store = new ProviderSubagentCompletionStore(root);
  const advance = store.advanceRecoveryCursor.bind(store);
  vi.spyOn(store, "advanceRecoveryCursor").mockImplementation(async (id) => {
    if (id === records[2].id) throw new Error("synthetic cursor failure");
    await advance(id);
  });
  const logger = createTestLogger();
  const warning = vi.spyOn(logger, "warn");
  await expect(
    recoverCodexHistory(records, client, store, logger, new AbortController().signal),
  ).rejects.toThrow("synthetic cursor failure");
  expect(warning).toHaveBeenCalledWith(
    expect.objectContaining({ skipped: 0 }),
    "Codex helper history recovery cursor write failed",
  );
  seen.length = 0;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    logger,
    new AbortController().signal,
  );
  expect(seen).toEqual(records.map((record) => record.id));
});

test("recovery stops on abort and resumes from its durable cursor", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-abort-"));
  const client = new TestAgentClient();
  const controller = new AbortController();
  const seen: string[] = [];
  vi.spyOn(client, "resumeSession").mockImplementation(async (_handle, _config, context) => {
    seen.push(context.agentId);
    if (seen.length === 2) controller.abort();
    throw new Error("synthetic unavailable history");
  });
  const records = Array.from({ length: 200 }, (_, index) =>
    parseStoredAgentRecord({
      id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      provider: "codex",
      cwd: root,
      createdAt: "2026-01-02T00:00:00.000Z",
      updatedAt: "2026-01-02T00:00:00.000Z",
      persistence: { provider: "codex", sessionId: `thread-${index}` },
    }),
  );
  const logger = createTestLogger();
  const warning = vi.spyOn(logger, "warn");
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    logger,
    controller.signal,
  );
  expect(warning).not.toHaveBeenCalledWith(
    expect.objectContaining({ agentId: records[1].id }),
    "Codex helper history recovery incomplete",
  );
  expect(seen).toEqual([records[0].id, records[1].id]);
  expect(warning).toHaveBeenCalledWith(
    { skipped: 24 },
    "Codex helper history recovery stopped early",
  );
  seen.length = 0;
  await recoverCodexHistory(
    records,
    client,
    new ProviderSubagentCompletionStore(root),
    createTestLogger(),
    new AbortController().signal,
  );
  expect(seen).toEqual(records.slice(1, 26).map((record) => record.id));
});

test("failed journal write persists neither identity nor event", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-atomic-"));
  const event = {
    type: "upsert" as const,
    id: "child",
    status: "completed" as const,
    nativeTurnId: "turn",
  };
  const failing = new ProviderSubagentCompletionStore(root, 10000, async () => {
    throw new Error("write failed");
  });
  await expect(failing.append("parent", "codex", event)).rejects.toThrow("write failed");
  const restarted = new ProviderSubagentCompletionStore(root);
  expect(await restarted.readAll("parent")).toEqual([]);
  await restarted.append("parent", "codex", event);
  expect(await new ProviderSubagentCompletionStore(root).readAll("parent")).toHaveLength(1);
});

test("identity eviction reports the deduplication limit after restart", async () => {
  const root = mkdtempSync(join(tmpdir(), "codex-identities-"));
  await writeJsonFileAtomic(join(root, "provider-subagent-completions.json"), {
    events: [],
    recoveryIdentities: Array.from({ length: 10000 }, (_, i) => `old-${i}`),
  });
  const store = new ProviderSubagentCompletionStore(root);
  await store.append("parent", "codex", {
    type: "upsert",
    id: "child",
    status: "completed",
    nativeTurnId: "turn",
  });
  expect(
    (await new ProviderSubagentCompletionStore(root).consume("parent", "reader"))
      .recoveryIdentitiesDiscarded,
  ).toBe(1);
});

test("external consumer capacity cannot block lifecycle recovery", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-capacity-"));
  const logger = createTestLogger();
  const client = new TestAgentClient();
  const store = new ProviderSubagentCompletionStore(root);
  await store.append(AGENT_IDS[0], "codex", {
    type: "upsert",
    id: "helper",
    status: "completed",
    sourceEventId: "native-1",
  });
  for (let index = 0; index < 256; index++) await store.consume(AGENT_IDS[0], `consumer-${index}`);
  await expect(store.consume(AGENT_IDS[0], "overflow")).rejects.toThrow("consumer limit");
  const manager = new AgentManager({
    clients: { codex: client },
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
  });
  expect(manager.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
    { id: "helper", status: "completed" },
  ]);
  await manager.closeAgent(AGENT_IDS[0]);
  await manager.flush();
});

test("host completion replay survives restart, retention and activity across parents", async () => {
  const root = mkdtempSync(join(tmpdir(), "completion-host-"));
  let store = new ProviderSubagentCompletionStore(root, 3);
  for (let i = 0; i < 4; i++)
    await store.append(`parent-${i}`, "codex", {
      type: "upsert",
      id: `helper-${i}`,
      status: "completed",
    });
  for (let i = 0; i < 256; i++) await store.consume(`registered-${i}`, "old");
  expect(await store.consumeHost("runner", 2)).toMatchObject({
    events: [
      { sequence: 2, parentAgentId: "parent-1" },
      { sequence: 3, parentAgentId: "parent-2" },
    ],
    gap: { discardedThrough: 1 },
  });
  await store.acceptHostGap("runner", 1);
  await store.acknowledgeHost("runner", 2);
  await store.acknowledgeHost("runner", 3);
  store = new ProviderSubagentCompletionStore(root, 3);
  expect(await store.consumeHost("runner", 2)).toMatchObject({
    events: [{ sequence: 4 }],
    gap: null,
  });
  await store.append("parent-1", "codex", { type: "upsert", id: "late", status: "completed" });
  expect(await store.consumeHost("runner", 2)).toMatchObject({
    events: [{ sequence: 4 }, { sequence: 5 }],
  });
  await store.releaseHost("runner");
  expect(await store.consumeHost("runner", 2)).toMatchObject({
    events: [{ sequence: 3 }, { sequence: 4 }],
    gap: { discardedThrough: 2 },
  });
});

test("host consumers replay and acknowledge independently of each other and of parent consumers", async () => {
  const store = new ProviderSubagentCompletionStore(
    mkdtempSync(join(tmpdir(), "completion-host-")),
  );
  for (const parent of ["parent-a", "parent-b"])
    await store.append(parent, "codex", {
      type: "upsert",
      id: `helper-${parent}`,
      status: "completed",
    });
  await store.consumeHost("first", 100);
  await store.consumeHost("second", 100);
  await store.consume("parent-a", "first");
  await store.acknowledgeHost("first", 1);
  await store.acknowledgeHost("first", 2);
  expect((await store.consumeHost("first", 100)).events).toEqual([]);
  expect((await store.consumeHost("second", 100)).events.map((e) => e.sequence)).toEqual([1, 2]);
  // A parent consumer with the same name is a separate registration.
  expect((await store.consume("parent-a", "first")).events.map((e) => e.sequence)).toEqual([1]);
  await store.releaseHost("first");
  expect((await store.consumeHost("first", 100)).events).toHaveLength(2);
  expect((await store.consumeHost("second", 100)).events.map((e) => e.sequence)).toEqual([1, 2]);
});

test("host consumer capacity is separate from parent capacity and trips at 256", async () => {
  const store = new ProviderSubagentCompletionStore(
    mkdtempSync(join(tmpdir(), "completion-host-")),
  );
  await store.append("parent", "codex", { type: "upsert", id: "helper", status: "completed" });
  for (let index = 0; index < 256; index++) await store.consumeHost(`host-${index}`, 1);
  await expect(store.consumeHost("overflow", 1)).rejects.toThrow("consumer limit");
  await expect(store.acknowledgeHost("overflow", 1)).rejects.toThrow("consumer limit");
  await expect(store.acceptHostGap("overflow", 0)).rejects.toThrow("consumer limit");
  // Existing host consumers and the parent pool are unaffected.
  expect((await store.consumeHost("host-0", 1)).events).toHaveLength(1);
  await store.consume("parent", "parent-consumer");
  for (let index = 1; index < 256; index++) await store.consume("parent", `consumer-${index}`);
  await expect(store.consume("parent", "overflow")).rejects.toThrow("consumer limit");
  await store.releaseHost("host-0");
  await store.consumeHost("overflow", 1);
});

test("an empty parent ID cannot reach host replay through the parent API", async () => {
  const store = new ProviderSubagentCompletionStore(
    mkdtempSync(join(tmpdir(), "completion-host-")),
  );
  await store.append("parent", "codex", { type: "upsert", id: "helper", status: "completed" });
  await expect(store.consume("", "consumer")).rejects.toThrow();
  await expect(store.acknowledge("", 1, "consumer")).rejects.toThrow();
  await expect(store.acceptGap("", "consumer", 0)).rejects.toThrow();
  await expect(store.releaseConsumer("", "consumer")).rejects.toThrow();
  await expect(store.replay("", "consumer")).rejects.toThrow();
});

describe("adapter-inferred helper terminals", () => {
  async function setup(provider: "codex" | "omp", clients: Partial<Record<string, AgentClient>>) {
    const root = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), "completion-inferred-"));
    const logger = createTestLogger();
    const completions = new ProviderSubagentCompletionStore(root);
    const manager = new AgentManager({
      clients,
      registry: new AgentStorage(join(root, "agents"), logger),
      providerSubagentCompletionStore: completions,
      logger,
    });
    await manager.createAgent({ provider, cwd: root }, AGENT_IDS[0], { workspaceId: undefined });
    return { root, completions, manager };
  }

  test.each(["disk rehydration", "history replacement"] as const)(
    "restores Claude native and inferred terminals from an empty descriptor store after %s",
    async (path) => {
      const client = new TestAgentClient();
      const { root, completions, manager } = await setup("codex", { codex: client });
      const source = new ClaudeTaskProtocolSource();
      const started = (id: string, task: string) =>
        ({
          type: "system",
          subtype: "task_started",
          task_id: task,
          tool_use_id: id,
          description: id,
          subagent_type: "general-purpose",
          task_type: "local_agent",
        }) as unknown as SDKMessage;
      const session = client.getSession(root);
      const push = (observations: SubagentObservation[]) => {
        for (const event of foldSubagentObservations(observations))
          session.pushEvent({ type: "provider_subagent", provider: "claude", event });
      };
      push(source.observe(started("toolu_native", "task-native")));
      push(
        source.observe({
          type: "system",
          subtype: "task_notification",
          task_id: "task-native",
          tool_use_id: "toolu_native",
          status: "completed",
        } as unknown as SDKMessage),
      );
      push(source.observe(started("toolu_canceled", "task-canceled")));
      push(source.cancelRunningForegroundTasks());
      push(source.observe(started("toolu_failed", "task-failed")));
      push(source.failRunningTasks());
      await waitForSessionEventQueue();
      await manager.flush();

      const journal = await completions.readAll(AGENT_IDS[0]);
      expect(
        journal.map(({ helperId, outcome, provenance, sourceEventTime }) => ({
          helperId,
          outcome,
          provenance,
          sourceEventTime,
        })),
      ).toEqual([
        {
          helperId: "toolu_native",
          outcome: "completed",
          provenance: "provider-stream",
          sourceEventTime: null,
        },
        {
          helperId: "toolu_canceled",
          outcome: "canceled",
          provenance: "adapter-inferred",
          sourceEventTime: null,
        },
        {
          helperId: "toolu_failed",
          outcome: "failed",
          provenance: "adapter-inferred",
          sourceEventTime: null,
        },
      ]);
      expect(journal[0]?.gaps).not.toContain("outcome inferred by adapter");
      expect(journal[1]?.gaps).toContain(
        "outcome inferred by adapter: foreground task still running: turn canceled",
      );
      expect(journal[2]?.gaps).toContain(
        "outcome inferred by adapter: Claude process lost while the task was running",
      );
      const removedHelpers: string[] = [];
      const unsubscribe = manager.subscribe(
        (event) => {
          if (event.type === "provider_subagent" && event.event.type === "remove") {
            removedHelpers.push(event.event.subagentId);
          }
        },
        { replayState: false },
      );
      // Neither replacement supplies helper history; only the retained journal can restore them.
      session.setHistory([]);
      try {
        if (path === "disk rehydration") {
          await manager.reloadAgentSession(AGENT_IDS[0], undefined, { rehydrateFromDisk: true });
        } else {
          await manager.hydrateTimelineFromProvider(AGENT_IDS[0], { force: true, broadcast: true });
        }
      } finally {
        unsubscribe();
      }
      expect(removedHelpers).toEqual(["toolu_native", "toolu_canceled", "toolu_failed"]);
      expect(manager.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
        { id: "toolu_native", status: "completed" },
        {
          id: "toolu_canceled",
          status: "canceled",
          completionEvidence: {
            provenance: "adapter-inferred",
            outcome: "canceled",
            sourceEventId: null,
            sourceEventTime: null,
          },
        },
        {
          id: "toolu_failed",
          status: "failed",
          completionEvidence: {
            provenance: "adapter-inferred",
            outcome: "failed",
            sourceEventId: null,
            sourceEventTime: null,
          },
        },
      ]);
    },
  );

  test("journals OMP's interrupt-synthesized cancel apart from a native lifecycle outcome", async () => {
    const runtime = new FakeOmp();
    const logger = createTestLogger();
    const { manager, completions } = await setup("omp", {
      omp: new OmpAgentClient({
        runtime,
        logger,
        runtimeSettings: { command: { mode: "replace", argv: [process.execPath] } },
      }),
    });
    const session = runtime.latestSession();
    const run = manager.runAgent(AGENT_IDS[0], "Work").catch(() => undefined);
    await vi.waitFor(() => expect(session.prompts).toHaveLength(1));
    session.beginTurn();
    const lifecycle = (id: string, status: "started" | "completed") =>
      session.emit({
        type: "subagent_lifecycle",
        payload: { id, agent: "worker", status, index: 0 },
      });
    lifecycle("native-done", "started");
    lifecycle("native-done", "completed");
    lifecycle("still-running", "started");
    await vi.waitFor(async () => {
      expect(manager.listProviderSubagents(AGENT_IDS[0])).toHaveLength(2);
    });
    await manager.cancelAgentRun(AGENT_IDS[0]);
    await run;
    await manager.flush();

    const journal = await completions.readAll(AGENT_IDS[0]);
    expect(
      journal.map(({ helperId, outcome, provenance }) => ({ helperId, outcome, provenance })),
    ).toEqual([
      { helperId: "native-done", outcome: "completed", provenance: "provider-stream" },
      { helperId: "still-running", outcome: "canceled", provenance: "adapter-inferred" },
    ]);
    expect(journal[1]).toMatchObject({
      sourceEventTime: null,
      sourceEventId: null,
      nativeTurnId: null,
      gaps: expect.arrayContaining([
        "outcome inferred by adapter: helper still running: turn canceled",
      ]),
    });
    expect(manager.listProviderSubagents(AGENT_IDS[0])).toMatchObject([
      { id: "native-done", status: "completed" },
      { id: "still-running", status: "canceled" },
    ]);
  });

  test("OpenCode marks startup and recovered child state as snapshots", async () => {
    const runtime = new TestOpenCodeHarness();
    const sdk = new TestOpenCodeClient();
    sdk.sessionCreateResponse = { data: { id: "parent" } };
    sdk.sessionChildrenResponses = [
      { data: [{ id: "startup", parentID: "parent" }] },
      { data: [] },
    ];
    runtime.enqueueClient(sdk);
    const client = new OpenCodeAgentClient(createTestLogger(), undefined, {
      serverManager: runtime,
      createClient: runtime.createClient,
    });
    const session = await client.createSession({ provider: "opencode", cwd: "/workspace/repo" });
    const events: AgentStreamEvent[] = [];
    session.subscribe((event) => events.push(event));
    try {
      await vi.waitFor(() =>
        expect(events).toContainEqual(
          expect.objectContaining({
            type: "provider_subagent",
            event: expect.objectContaining({
              id: "startup",
              status: "completed",
              inferred: { reason: "child not busy when the session was read", snapshot: true },
            }),
          }),
        ),
      );
      sdk.sessionChildrenResponses = [
        { data: [{ id: "recovered", parentID: "parent", directory: "/workspace/repo" }] },
        { data: [] },
      ];
      sdk.sessionMessagesResponse = {
        data: [
          {
            info: {
              id: "message",
              sessionID: "recovered",
              role: "assistant",
              error: { name: "ProviderError", data: { message: "failed" } },
            },
            parts: [],
          },
        ],
      };
      sdk.emitEvent({ type: "server.connected", properties: {} });
      await vi.waitFor(() =>
        expect(events).toContainEqual(
          expect.objectContaining({
            type: "provider_subagent",
            event: expect.objectContaining({
              id: "recovered",
              status: "failed",
              inferred: {
                reason: "child outcome read from session state on recovery",
                snapshot: true,
              },
            }),
          }),
        ),
      );
    } finally {
      await session.close();
    }
  });

  test("snapshots deduplicate by latest inferred snapshot outcome and preserve native terminals", async () => {
    const client = new TestAgentClient();
    const { root, completions, manager } = await setup("codex", { codex: client });
    const session = client.getSession(root);
    const snapshot = (id: string, status: "completed" | "failed" = "completed") => ({
      type: "provider_subagent" as const,
      provider: "opencode" as const,
      event: {
        type: "upsert" as const,
        id,
        status,
        inferred: { reason: "child not busy when the session was read", snapshot: true as const },
      },
    });
    // Each resume or gap repair re-reads the same child state.
    for (let read = 0; read < 3; read += 1) session.pushEvent(snapshot("seen-twice"));
    // A terminal the provider reported live is already on record.
    session.pushEvent({
      type: "provider_subagent",
      provider: "opencode",
      event: { type: "upsert", id: "native-first", status: "completed" },
    });
    session.pushEvent(snapshot("native-first"));
    session.pushEvent(snapshot("seen-twice", "failed"));
    await waitForSessionEventQueue();
    await manager.flush();

    expect(
      (await completions.readAll(AGENT_IDS[0])).map(({ helperId, provenance }) => ({
        helperId,
        provenance,
      })),
    ).toEqual([
      { helperId: "seen-twice", provenance: "adapter-inferred" },
      { helperId: "native-first", provenance: "provider-stream" },
      { helperId: "native-first", provenance: "adapter-inferred" },
      { helperId: "seen-twice", provenance: "adapter-inferred" },
    ]);
    expect(manager.listProviderSubagents(AGENT_IDS[0])).toContainEqual(
      expect.objectContaining({
        id: "seen-twice",
        status: "failed",
        completionEvidence: expect.objectContaining({ outcome: "failed" }),
      }),
    );
    session.pushEvent(snapshot("seen-twice"));
    await waitForSessionEventQueue();
    await manager.flush();
    expect((await completions.readAll(AGENT_IDS[0])).at(-1)).toMatchObject({
      helperId: "seen-twice",
      outcome: "completed",
      provenance: "adapter-inferred",
    });
    expect(manager.listProviderSubagents(AGENT_IDS[0])).toContainEqual(
      expect.objectContaining({
        id: "seen-twice",
        status: "completed",
        completionEvidence: expect.objectContaining({ outcome: "completed" }),
      }),
    );
  });
});

test.each(["timeout", "shutdown"])(
  "recovery closes a stalled history read on %s",
  async (reason) => {
    const root = mkdtempSync(join(tmpdir(), "codex-stalled-"));
    const client = new TestAgentClient();
    const timeout = new AbortController();
    const shutdown = new AbortController();
    const timeoutSpy = vi.spyOn(AbortSignal, "timeout").mockReturnValueOnce(timeout.signal);
    const servers: ReturnType<typeof createFakeCodexAppServer>[] = [];
    const sessions: CodexAppServerAgentSession[] = [];
    const seen: string[] = [];
    vi.spyOn(client, "resumeSession").mockImplementation(async (_handle, _config, context) => {
      seen.push(context.agentId);
      const server = createFakeCodexAppServer(
        seen.length === 1 ? { "thread/read": () => new Promise(() => {}) } : {},
      );
      servers.push(server);
      const session = new CodexAppServerAgentSession(
        { provider: "codex", cwd: root },
        { sessionId: "parent-thread" },
        createTestLogger(),
        async () => server.child,
      );
      sessions.push(session);
      await session.getRuntimeInfo();
      return session;
    });
    const records = [0, 1].map((index) =>
      parseStoredAgentRecord({
        id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
        provider: "codex",
        cwd: root,
        createdAt: "2026-01-02T00:00:00.000Z",
        updatedAt: "2026-01-02T00:00:00.000Z",
        persistence: { provider: "codex", sessionId: "parent-thread" },
      }),
    );
    const store = new ProviderSubagentCompletionStore(root);
    let cycle: Promise<void> | undefined;
    try {
      cycle = recoverCodexHistory(records, client, store, createTestLogger(), shutdown.signal);
      while (!servers.length) await new Promise((resolve) => setTimeout(resolve, 1));
      await servers[0].waitForRequest("thread/read");
      (reason === "timeout" ? timeout : shutdown).abort();
      await Promise.race([
        cycle,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("recovery stalled")), 200),
        ),
      ]);
      expect(seen).toEqual(
        reason === "timeout" ? records.map((record) => record.id) : [records[0].id],
      );
      expect(await store.recoveryCursor()).toBe(reason === "timeout" ? records[1].id : null);
      if (reason === "shutdown") {
        await recoverCodexHistory(
          records,
          client,
          store,
          createTestLogger(),
          new AbortController().signal,
        );
        expect(seen.slice(1)).toEqual(records.map((record) => record.id));
      }
    } finally {
      timeoutSpy.mockRestore();
      for (const session of sessions) await session.close();
      await cycle;
      for (const server of servers) server.disconnect();
    }
  },
);

test("forced helper history supersedes retained terminals", async () => {
  const root = mkdtempSync(join(tmpdir(), "helper-refresh-"));
  const client = new TestAgentClient();
  const logger = createTestLogger();
  const store = new ProviderSubagentCompletionStore(root);
  const manager = new AgentManager({
    clients: { codex: client },
    registry: new AgentStorage(join(root, "agents"), logger),
    providerSubagentCompletionStore: store,
    logger,
  });
  await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
    workspaceId: undefined,
  });
  const session = client.getSession(root);
  for (const id of ["running", "finished"])
    session.pushEvent({
      type: "provider_subagent",
      provider: "codex",
      event: { type: "upsert", id, status: "completed", nativeTurnId: "old" },
    });
  await waitForSessionEventQueue();
  session.setHistory([
    {
      type: "provider_subagent",
      provider: "codex",
      event: { type: "upsert", id: "running", status: "running" },
    },
    {
      type: "provider_subagent",
      provider: "codex",
      event: { type: "upsert", id: "finished", status: "failed", nativeTurnId: "new" },
    },
  ]);
  await manager.hydrateTimelineFromProvider(AGENT_IDS[0], { force: true });
  expect(manager.listProviderSubagents(AGENT_IDS[0])).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ id: "running", status: "running" }),
      expect.objectContaining({ id: "finished", status: "failed" }),
    ]),
  );
  expect(
    manager.listProviderSubagents(AGENT_IDS[0]).find((helper) => helper.id === "running")
      ?.completionEvidence,
  ).toBeUndefined();
  expect(
    manager.listProviderSubagents(AGENT_IDS[0]).find((helper) => helper.id === "finished")
      ?.completionEvidence,
  ).toBeUndefined();
  await manager.closeAgent(AGENT_IDS[0]);
  await manager.flush();
});

test("malformed completion journal closes failed registration and permits retry", async () => {
  const root = mkdtempSync(join(tmpdir(), "helper-invalid-"));
  const client = new TestAgentClient();
  const logger = createTestLogger();
  const journal = join(root, "provider-subagent-completions.json");
  writeFileSync(journal, "{");
  const manager = new AgentManager({
    clients: { codex: client },
    registry: new AgentStorage(join(root, "agents"), logger),
    providerSubagentCompletionStore: new ProviderSubagentCompletionStore(root),
    logger,
  });
  const close = vi.spyOn(TestAgentSession.prototype, "close");
  try {
    await expect(
      manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
        workspaceId: undefined,
      }),
    ).rejects.toThrow();
    expect(manager.getAgent(AGENT_IDS[0])).toBeNull();
    expect(close).toHaveBeenCalledOnce();
    writeFileSync(journal, JSON.stringify({ events: [] }));
    await manager.createAgent({ provider: "codex", cwd: root }, AGENT_IDS[0], {
      workspaceId: undefined,
    });
    expect(manager.getAgent(AGENT_IDS[0])).not.toBeNull();
    await manager.closeAgent(AGENT_IDS[0]);
    await manager.flush();
  } finally {
    close.mockRestore();
  }
});

test("recovery does not replace a newer live helper with older history", async () => {
  const root = mkdtempSync(join(tmpdir(), "helper-recovery-race-"));
  const store = new ProviderSubagentCompletionStore(root);
  const watermark = await store.recoveryWatermark();
  await store.append("parent", "codex", { type: "upsert", id: "child", status: "running" });
  const older = await store.append(
    "parent",
    "codex",
    { type: "upsert", id: "child", status: "completed", nativeTurnId: "opaque" },
    "provider-stream",
    [],
    true,
  );
  await store.recordRecoveryProjection("parent", watermark, [
    { provider: "codex", helperId: "child", sequence: older?.sequence ?? null },
  ]);
  expect(await new ProviderSubagentCompletionStore(root).readProjection("parent")).toEqual([]);
});

test("incomplete history and legacy ordering do not invent a current terminal", async () => {
  const root = mkdtempSync(join(tmpdir(), "helper-uncertain-"));
  const store = new ProviderSubagentCompletionStore(root);
  await store.append("parent", "codex", {
    type: "upsert",
    id: "child",
    status: "failed",
    nativeTurnId: "later",
  });
  const watermark = await store.recoveryWatermark();
  const old = await store.append(
    "parent",
    "codex",
    { type: "upsert", id: "child", status: "completed", nativeTurnId: "older" },
    "provider-stream",
    [],
    true,
  );
  await store.recordRecoveryProjection("parent", watermark, [
    { provider: "codex", helperId: "child", sequence: old?.sequence ?? null },
  ]);
  expect(await store.readProjection("parent")).toMatchObject([
    { nativeTurnId: "later", outcome: "failed" },
  ]);
  const file = join(root, "provider-subagent-completions.json");
  const raw = JSON.parse(readFileSync(file, "utf8"));
  delete raw.projectionVersion;
  delete raw.projection;
  writeFileSync(file, JSON.stringify(raw));
  const legacy = new ProviderSubagentCompletionStore(root);
  expect(await legacy.readAll("parent")).toHaveLength(2);
  expect(await legacy.readProjection("parent")).toEqual([]);
  expect((await legacy.consume("parent", "test")).coverageUncertain).toBe(true);
  const legacyWatermark = await legacy.recoveryWatermark();
  await legacy.recordRecoveryProjection("parent", legacyWatermark, [
    { provider: "codex", helperId: "child", sequence: old?.sequence ?? null },
  ]);
  expect(await legacy.readProjection("parent")).toEqual([]);
});

test("projection retention reports discarded current-state markers", async () => {
  const root = mkdtempSync(join(tmpdir(), "helper-projection-limit-"));
  const file = join(root, "provider-subagent-completions.json");
  const projection = Object.fromEntries(
    Array.from({ length: 10000 }, (_, i) => [
      JSON.stringify(["parent", "codex", `helper-${i}`]),
      { sequence: null, revision: i },
    ]),
  );
  writeFileSync(
    file,
    JSON.stringify({ events: [], projection, projectionVersion: 1, projectionRevision: 10000 }),
  );
  const store = new ProviderSubagentCompletionStore(root);
  await store.append("parent", "codex", { type: "upsert", id: "new", status: "running" });
  expect(await store.consume("parent", "test")).toMatchObject({
    projectionDiscarded: 1,
    coverageUncertain: true,
  });
});
