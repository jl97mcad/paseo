import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { z } from "zod";
import { AgentProviderSchema } from "@getpaseo/protocol/provider-manifest";
import { writeJsonFileAtomic } from "../../atomic-file.js";
import type { AgentProvider } from "../agent-sdk-types.js";
import type { ProviderSubagentInputEvent } from "./store.js";

const completionSchema = z.object({
  sequence: z.number().int().positive(),
  deliveryId: z.string().default(() => randomUUID()),
  recordedAt: z.string().nullable().default(null),
  eventKey: z.string(),
  kind: z.enum(["completion", "verification"]).default("completion"),
  verifiesSequence: z.number().int().positive().nullable().default(null),
  evidence: z.string().nullable().default(null),
  parentAgentId: z.string(),
  provider: AgentProviderSchema,
  helperId: z.string(),
  sourceEventId: z.string().nullable(),
  nativeTurnId: z.string().nullable(),
  nativeParentId: z.string().nullable(),
  role: z.string().nullable(),
  sourceEventTime: z.string().nullable(),
  provenance: z.string(),
  outcome: z.enum(["completed", "failed", "canceled", "unknown"]),
  gaps: z.array(z.string()),
});
export type ProviderSubagentCompletion = z.infer<typeof completionSchema>;
const cursorSchema = z.object({
  through: z.number().int().nonnegative(),
  acknowledged: z.array(z.number().int().positive()),
});
const journalSchema = z.object({
  // COMPAT(completion-journal): added in v0.17.1, remove after 2027-03-01.
  acknowledgedThrough: z.record(z.string(), z.number().int().nonnegative()).default({}),
  consumers: z.record(z.string(), cursorSchema).default({}),
  nextSequence: z.number().int().positive().optional(),
  discardedThrough: z.number().int().nonnegative().default(0),
  events: z.array(completionSchema),
});
type Journal = z.infer<typeof journalSchema>;

/** Single daemon writer. Consumers call the daemon API, never open this file independently. */
export class ProviderSubagentCompletionStore {
  private journal: Journal | undefined;
  private pending: Promise<unknown> = Promise.resolve();
  private readonly file: string;

  constructor(
    directory: string,
    private readonly maxEvents = 10_000,
    private readonly persist: typeof writeJsonFileAtomic = writeJsonFileAtomic,
  ) {
    if (!Number.isInteger(maxEvents) || maxEvents < 1) throw new Error("Invalid retention limit");
    this.file = join(directory, "provider-subagent-completions.json");
  }

  private async load(): Promise<Journal> {
    if (this.journal) return this.journal;
    try {
      this.journal = journalSchema.parse(JSON.parse(await readFile(this.file, "utf8")));
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
      this.journal = { acknowledgedThrough: {}, consumers: {}, discardedThrough: 0, events: [] };
    }
    return this.journal;
  }

  private serialize<T>(action: () => Promise<T>): Promise<T> {
    const result = this.pending.then(action);
    this.pending = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async save(next: Journal): Promise<void> {
    // ponytail: bounded snapshot rewrite; move to segmented storage if 10k records are too costly.
    let bytes = next.events.reduce(
      (total, event) => total + Buffer.byteLength(JSON.stringify(event)),
      0,
    );
    while (next.events.length > this.maxEvents || bytes > 16 * 1024 * 1024) {
      const removed = next.events.shift();
      if (!removed) break;
      bytes -= Buffer.byteLength(JSON.stringify(removed));
      next.discardedThrough = removed.sequence;
    }
    next.consumers = Object.fromEntries(
      Object.entries(next.consumers).map(([key, cursor]) => [
        key,
        {
          ...cursor,
          acknowledged: cursor.acknowledged.filter((sequence) => sequence > next.discardedThrough),
        },
      ]),
    );
    await this.persist(this.file, next);
    this.journal = next;
  }

  append(
    parentAgentId: string,
    provider: AgentProvider,
    event: Extract<ProviderSubagentInputEvent, { type: "upsert" }>,
    provenance = "provider-stream",
    gaps: string[] = [],
  ): Promise<ProviderSubagentCompletion | null> {
    if (event.status === "running") return Promise.resolve(null);
    return this.serialize(async () => {
      const journal = await this.load();
      const outcome = event.status ?? "unknown";
      let identity: string[] | null = null;
      if (event.sourceEventId) identity = ["event", event.sourceEventId];
      else if (event.nativeTurnId) identity = ["turn", event.nativeTurnId, outcome];
      const missing = Object.entries({
        "native event identity unavailable": event.sourceEventId,
        "native turn identity unavailable": event.nativeTurnId,
        "helper role unavailable": event.role,
        "source event time unavailable": event.timestamp,
      })
        .filter(([, value]) => !value)
        .map(([gap]) => gap);
      const deliveryId = randomUUID();
      const eventKey = JSON.stringify([
        parentAgentId,
        provider,
        event.id,
        provenance,
        identity ?? deliveryId,
      ]);
      const existing = identity && journal.events.find((entry) => entry.eventKey === eventKey);
      if (existing) return existing;
      const completion = completionSchema.parse({
        sequence: journal.nextSequence ?? (journal.events.at(-1)?.sequence ?? 0) + 1,
        deliveryId,
        recordedAt: new Date().toISOString(),
        eventKey,
        parentAgentId,
        provider,
        helperId: event.id,
        sourceEventId: event.sourceEventId ?? null,
        nativeTurnId: event.nativeTurnId ?? null,
        nativeParentId: event.parentSubagentId ?? null,
        role: event.role ?? null,
        sourceEventTime: event.timestamp ?? null,
        provenance,
        outcome,
        gaps: [
          ...missing,
          ...(!identity ? ["deduplication identity unavailable"] : []),
          ...(outcome === "unknown" ? ["terminal outcome unavailable"] : []),
          ...gaps,
        ],
      });
      if (Buffer.byteLength(JSON.stringify(completion)) > 64 * 1024)
        throw new Error("Completion exceeds 64 KiB");
      await this.save({
        ...journal,
        nextSequence: completion.sequence + 1,
        events: [...journal.events, completion],
      });
      return completion;
    });
  }

  readAll(parentAgentId: string): Promise<ProviderSubagentCompletion[]> {
    return this.serialize(async () =>
      (await this.load()).events.filter((e) => e.parentAgentId === parentAgentId),
    );
  }

  private cursor(journal: Journal, parentAgentId: string, consumer: string) {
    z.string().min(1).max(128).parse(consumer);
    z.string().min(1).max(128).parse(parentAgentId);
    return (
      journal.consumers[JSON.stringify([parentAgentId, consumer])] ?? {
        through: consumer === "projection" ? (journal.acknowledgedThrough[parentAgentId] ?? 0) : 0,
        acknowledged: [],
      }
    );
  }

  consume(parentAgentId: string, consumer: string) {
    return this.serialize(async () => {
      let journal = await this.load();
      const cursor = this.cursor(journal, parentAgentId, consumer);
      const key = JSON.stringify([parentAgentId, consumer]);
      if (!(key in journal.consumers)) {
        if (Object.keys(journal.consumers).length >= 256)
          throw new Error("Completion consumer limit reached");
        await this.save({
          ...journal,
          events: [...journal.events],
          consumers: { ...journal.consumers, [key]: cursor },
        });
        journal = await this.load();
      }
      return {
        events: journal.events.filter(
          (e) =>
            e.parentAgentId === parentAgentId &&
            e.sequence > cursor.through &&
            !cursor.acknowledged.includes(e.sequence),
        ),
        gap:
          cursor.through < journal.discardedThrough
            ? { discardedThrough: journal.discardedThrough }
            : null,
      };
    });
  }

  async replay(parentAgentId: string, consumer = "projection") {
    const batch = await this.consume(parentAgentId, consumer);
    if (batch.gap) throw new Error(`Completion replay gap through ${batch.gap.discardedThrough}`);
    return batch.events;
  }

  acknowledge(parentAgentId: string, sequence: number, consumer = "projection"): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      const cursor = this.cursor(journal, parentAgentId, consumer);
      if (sequence <= cursor.through || cursor.acknowledged.includes(sequence)) return;
      if (!journal.events.some((e) => e.parentAgentId === parentAgentId && e.sequence === sequence))
        throw new Error(`Cannot acknowledge unknown completion ${sequence}`);
      const key = JSON.stringify([parentAgentId, consumer]);
      if (!(key in journal.consumers) && Object.keys(journal.consumers).length >= 256)
        throw new Error("Completion consumer limit reached");
      const acknowledged = [...cursor.acknowledged, sequence];
      let through = cursor.through;
      for (const event of journal.events.filter(
        (e) =>
          cursor.through >= journal.discardedThrough &&
          e.parentAgentId === parentAgentId &&
          e.sequence > through,
      )) {
        if (!acknowledged.includes(event.sequence)) break;
        through = event.sequence;
      }
      const next = {
        through,
        acknowledged: acknowledged.filter((seq) => seq > through && seq > journal.discardedThrough),
      };
      await this.save({
        ...journal,
        events: [...journal.events],
        consumers: { ...journal.consumers, [key]: next },
      });
    });
  }

  acceptGap(parentAgentId: string, consumer: string, discardedThrough: number): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      if (discardedThrough !== journal.discardedThrough)
        throw new Error("Retention boundary changed; replay again");
      const cursor = this.cursor(journal, parentAgentId, consumer);
      const key = JSON.stringify([parentAgentId, consumer]);
      if (!(key in journal.consumers) && Object.keys(journal.consumers).length >= 256)
        throw new Error("Completion consumer limit reached");
      await this.save({
        ...journal,
        events: [...journal.events],
        consumers: {
          ...journal.consumers,
          [key]: {
            through: Math.max(cursor.through, discardedThrough),
            acknowledged: cursor.acknowledged.filter((seq) => seq > discardedThrough),
          },
        },
      });
    });
  }

  releaseConsumer(parentAgentId: string, consumer: string): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      const consumers = { ...journal.consumers };
      delete consumers[JSON.stringify([parentAgentId, consumer])];
      await this.save({ ...journal, events: [...journal.events], consumers });
    });
  }

  verify(
    parentAgentId: string,
    sequence: number,
    input: { outcome: ProviderSubagentCompletion["outcome"]; evidence: string },
  ) {
    return this.serialize(async () => {
      const journal = await this.load();
      const observed = journal.events.find(
        (e) =>
          e.parentAgentId === parentAgentId && e.sequence === sequence && e.kind === "completion",
      );
      if (!observed) throw new Error("Unknown completion to verify");
      const entry = completionSchema.parse({
        ...observed,
        sequence: journal.nextSequence ?? (journal.events.at(-1)?.sequence ?? 0) + 1,
        deliveryId: randomUUID(),
        recordedAt: new Date().toISOString(),
        eventKey: randomUUID(),
        sourceEventId: null,
        nativeTurnId: null,
        sourceEventTime: null,
        gaps: ["verification source time unavailable"],
        kind: "verification",
        verifiesSequence: sequence,
        evidence: z.string().min(1).max(8192).parse(input.evidence),
        outcome: input.outcome,
        provenance: "consumer-verification",
      });
      if (Buffer.byteLength(JSON.stringify(entry)) > 64 * 1024)
        throw new Error("Verification exceeds 64 KiB");
      await this.save({
        ...journal,
        nextSequence: entry.sequence + 1,
        events: [...journal.events, entry],
      });
      return entry;
    });
  }

  flush(): Promise<void> {
    return this.pending.then(() => undefined);
  }
}
