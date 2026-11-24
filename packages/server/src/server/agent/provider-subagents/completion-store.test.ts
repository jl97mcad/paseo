import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "vitest";
import { writeJsonFileAtomic } from "../../atomic-file.js";
import { ProviderSubagentCompletionStore } from "./completion-store.js";

function open(root: string, maxEvents = 10_000) {
  const counter = { writes: 0 };
  const store = new ProviderSubagentCompletionStore(root, maxEvents, async (file, data) => {
    counter.writes += 1;
    await writeJsonFileAtomic(file, data);
  });
  return { store, counter };
}

const running = { type: "upsert", id: "helper", status: "running" } as const;
const terminal = (id: string, sourceEventId: string) =>
  ({
    type: "upsert",
    id,
    status: "completed",
    sourceEventId,
    nativeTurnId: sourceEventId,
  }) as const;
const projected = async (store: ProviderSubagentCompletionStore) =>
  (await store.readProjection("parent")).map((event) => event.helperId);

describe("running observations write the journal only when the marker changes", () => {
  test("first marker persists once and repeats stay in memory", async () => {
    const { store, counter } = open(mkdtempSync(join(tmpdir(), "completion-running-")));
    for (let i = 0; i < 50; i += 1) await store.append("parent", "codex", running);
    expect(counter.writes).toBe(1);
  });

  test("every running update still advances the recovery watermark", async () => {
    const { store } = open(mkdtempSync(join(tmpdir(), "completion-watermark-")));
    await store.append("parent", "codex", running);
    const before = await store.recoveryWatermark();
    await store.append("parent", "codex", running);
    expect(await store.recoveryWatermark()).toBe(before + 1);
  });

  test("a terminal then running persists the transition once and never presents the terminal", async () => {
    const { store, counter } = open(mkdtempSync(join(tmpdir(), "completion-transition-")));
    await store.append("parent", "codex", terminal("helper", "e1"));
    expect(await projected(store)).toEqual(["helper"]);
    const base = counter.writes;
    for (let i = 0; i < 20; i += 1) await store.append("parent", "codex", running);
    expect(counter.writes - base).toBe(1);
    expect(await projected(store)).toEqual([]);
    await store.append("parent", "codex", terminal("helper", "e2"));
    expect(await projected(store)).toEqual(["helper"]);
  });

  test("a stale recovery read cannot restore a terminal over a later running update", async () => {
    const { store } = open(mkdtempSync(join(tmpdir(), "completion-interleave-")));
    const first = await store.append("parent", "codex", terminal("helper", "e1"));
    await store.append("parent", "codex", running);
    const watermark = await store.recoveryWatermark();
    await store.append("parent", "codex", running);
    await store.recordRecoveryProjection("parent", watermark, [
      { provider: "codex", helperId: "helper", sequence: first?.sequence ?? null },
    ]);
    expect(await projected(store)).toEqual([]);
  });

  test("a recovery read at the current watermark may not replace a running marker either", async () => {
    const { store } = open(mkdtempSync(join(tmpdir(), "completion-current-")));
    const first = await store.append("parent", "codex", terminal("helper", "e1"));
    await store.append("parent", "codex", running);
    await store.append("parent", "codex", running);
    await store.recordRecoveryProjection("parent", await store.recoveryWatermark(), [
      { provider: "codex", helperId: "helper", sequence: first?.sequence ?? null },
    ]);
    expect(await projected(store)).toEqual([]);
  });

  test("reopening keeps the running marker so history cannot present the old terminal", async () => {
    const root = mkdtempSync(join(tmpdir(), "completion-reopen-"));
    const first = open(root);
    const done = await first.store.append("parent", "codex", terminal("helper", "e1"));
    for (let i = 0; i < 5; i += 1) await first.store.append("parent", "codex", running);
    const reopened = open(root).store;
    await reopened.recordRecoveryProjection("parent", await reopened.recoveryWatermark(), [
      { provider: "codex", helperId: "helper", sequence: done?.sequence ?? null },
    ]);
    expect(await projected(reopened)).toEqual([]);
  });

  test("an evicted terminal stays uncertain and repeated running adds no writes", async () => {
    const { store, counter } = open(mkdtempSync(join(tmpdir(), "completion-evict-")), 2);
    await store.append("parent", "codex", terminal("a", "e1"));
    await store.append("parent", "codex", terminal("b", "e2"));
    await store.append("parent", "codex", terminal("c", "e3"));
    const base = counter.writes;
    for (let i = 0; i < 10; i += 1)
      await store.append("parent", "codex", { type: "upsert", id: "a", status: "running" });
    expect(counter.writes - base).toBeLessThanOrEqual(1);
    expect((await store.consume("parent", "probe")).coverageUncertain).toBe(true);
    expect(await projected(store)).toEqual(["b", "c"]);
  });

  test("a failed write on the marker transition surfaces and leaves state unchanged", async () => {
    const root = mkdtempSync(join(tmpdir(), "completion-fail-"));
    let fail = true;
    const store = new ProviderSubagentCompletionStore(root, 10_000, async (file, data) => {
      if (fail) throw new Error("journal unavailable");
      await writeJsonFileAtomic(file, data);
    });
    await expect(store.append("parent", "codex", running)).rejects.toThrow("journal unavailable");
    fail = false;
    await store.append("parent", "codex", running);
    expect(await store.recoveryWatermark()).toBe(1);
  });
});
