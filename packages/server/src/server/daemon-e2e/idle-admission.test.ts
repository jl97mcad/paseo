import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test } from "vitest";
import { createTestLogger } from "../../test-utils/test-logger.js";
import { OpenCodeV2AgentClient } from "../agent/providers/opencode/v2/agent.js";
import { V2Harness } from "../agent/providers/opencode/test-utils/v2-harness.js";
import { createTestAgentClients } from "../test-utils/fake-agent-client.js";
import { createTestPaseoDaemon } from "../test-utils/paseo-daemon.js";
import { DaemonClient } from "../test-utils/daemon-client.js";

test("idle-only SDK sends reserve one turn and retain delivery receipts", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "idle-admission-"));
  const runtime = new V2Harness();
  runtime.autoComplete = false;
  runtime.api.model.list = async () => ({ location: runtime.info.location, data: [] });
  runtime.api.provider.list = async () => ({
    location: runtime.info.location,
    data: [{ id: "test", name: "Test", activation: "enabled", package: "test" }],
  });
  let interrupts = 0;
  const interrupt = runtime.interrupt;
  runtime.interrupt = async (...args) => {
    interrupts++;
    return interrupt(...args);
  };
  const provider = new OpenCodeV2AgentClient({
    logger: createTestLogger(),
    runtime: runtime.runtime,
  });
  const daemon = await createTestPaseoDaemon({
    agentClients: { ...createTestAgentClients(), opencode: provider },
    paseoHomeRoot: path.join(root, "daemon"),
    staticDir: path.join(root, "static"),
    cleanup: false,
  });
  const client = new DaemonClient({ url: `ws://127.0.0.1:${daemon.port}/ws` });
  try {
    await client.connect();
    await client.fetchAgents({ subscribe: {} });
    const recipient = await client.createAgent({ provider: "opencode", cwd: root });
    const sender = await client.createAgent({ provider: "claude", cwd: root });
    const sends = await Promise.allSettled([
      client.sendAgentMessage(recipient.id, "first", {
        idleOnly: true,
        messageId: "first-id",
        sourceAgentId: sender.id,
      }),
      client.sendAgentMessage(recipient.id, "second", { idleOnly: true, messageId: "second-id" }),
    ]);
    expect(sends.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(sends.filter((result) => result.status === "rejected")).toEqual([
      expect.objectContaining({ reason: expect.objectContaining({ message: "agent_busy" }) }),
    ]);
    await expect.poll(() => runtime.prompts.length).toBe(1);
    expect(interrupts).toBe(0);
    const winner = sends[0].status === "fulfilled" ? "first" : "second";
    const winningOptions =
      winner === "first"
        ? { idleOnly: true, messageId: "first-id", sourceAgentId: sender.id }
        : { idleOnly: true, messageId: "second-id" };
    await client.sendAgentMessage(recipient.id, winner, winningOptions);
    expect(runtime.prompts).toHaveLength(1);
    await expect(client.sendAgentMessage(recipient.id, "changed", winningOptions)).rejects.toThrow(
      "agent_request_key_conflict",
    );
    runtime.finishExecution();
    await client.waitForAgentUpsert(recipient.id, (agent) => agent.status === "idle");
    const loser = winner === "first" ? "second" : "first";
    const losingOptions =
      loser === "first"
        ? { idleOnly: true, messageId: "first-id", sourceAgentId: sender.id }
        : { idleOnly: true, messageId: "second-id" };
    await client.sendAgentMessage(recipient.id, loser, losingOptions);
    await expect.poll(() => runtime.prompts.length).toBe(2);
    const attributed = runtime.prompts.find((prompt) => prompt.includes("first"));
    expect(attributed).toContain(`source-agent-id="${sender.id}"`);
    expect(attributed).toContain('message-id="first-id"');
    expect(runtime.prompts).toContain("second");
    const key = createHash("sha256")
      .update(JSON.stringify(["send", recipient.id, losingOptions.messageId]))
      .digest("hex");
    const receiptPath = path.join(daemon.paseoHome, "agent-requests", `${key}.json`);
    const receipt = JSON.parse(await readFile(receiptPath, "utf8"));
    await writeFile(receiptPath, JSON.stringify({ ...receipt, state: "pending" }));
    await expect(client.sendAgentMessage(recipient.id, loser, losingOptions)).rejects.toThrow(
      "agent_request_outcome_unknown",
    );
    expect(runtime.prompts).toHaveLength(2);
    expect(interrupts).toBe(0);
  } finally {
    await client.close();
    await daemon.close();
  }
});
