import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test, vi } from "vitest";

import { PARENT_AGENT_ID_LABEL } from "@getpaseo/protocol/agent-labels";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createAgentMcpServer } from "./mcp-server.js";
import { ProviderSnapshotManager } from "./provider-snapshot-manager.js";
import { writeJsonFileAtomic } from "../atomic-file.js";
import { createTestLogger } from "../../test-utils/test-logger.js";
import { AgentManager, type AgentManagerEvent } from "./agent-manager.js";
import { AgentStorage } from "./agent-storage.js";
import { ProviderSubagentCompletionStore } from "./provider-subagents/completion-store.js";
import { ensureAgentLoaded } from "./agent-loading.js";
import { AGENT_STREAM_COALESCE_DEFAULT_WINDOW_MS } from "./agent-stream-coalescer.js";
import { limitAgentTimelineItemContent } from "./agent-timeline-content.js";
import type { AgentTimelineRow } from "./agent-timeline-store-types.js";
import { CodexAppServerAgentSession } from "./providers/codex-app-server-agent.js";
import {
  createFakeCodexAppServer,
  waitForNextEvent,
} from "./providers/codex/test-utils/fake-app-server.js";
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
