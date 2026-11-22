import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { z } from "zod";
import { writeJsonFileAtomic } from "../../atomic-file.js";
import type { AgentProvider } from "../agent-sdk-types.js";
import type { ProviderSubagentInputEvent } from "./store.js";

const completionSchema = z.object({
  sequence: z.number().int().positive(),
  eventKey: z.string(),
  parentAgentId: z.string(),
  provider: z.string(),
  helperId: z.string(),
  sourceEventId: z.string().nullable(),
  nativeTurnId: z.string().nullable(),
  nativeParentId: z.string().nullable(),
  role: z.string().nullable(),
  sourceEventTime: z.string().nullable(),
  provenance: z.string(),
  outcome: z.enum(["completed", "failed", "canceled"]),
  gaps: z.array(z.string()),
});

export type ProviderSubagentCompletion = z.infer<typeof completionSchema>;
const journalSchema = z.object({
  acknowledgedThrough: z.record(z.string(), z.number().int().nonnegative()),
  events: z.array(completionSchema),
});

/** Durable at-least-once completion journal; replay is idempotent at the descriptor projection. */
export class ProviderSubagentCompletionStore {
  private journal: z.infer<typeof journalSchema> | undefined;
  private pending: Promise<unknown> = Promise.resolve();
  private readonly file: string;

  constructor(directory: string) {
    this.file = join(directory, "provider-subagent-completions.json");
  }

  private async load() {
    if (this.journal) return this.journal;
    try {
      this.journal = journalSchema.parse(JSON.parse(await readFile(this.file, "utf8")));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      this.journal = { acknowledgedThrough: {}, events: [] };
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

  append(
    parentAgentId: string,
    provider: AgentProvider,
    event: Extract<ProviderSubagentInputEvent, { type: "upsert" }>,
  ): Promise<ProviderSubagentCompletion | null> {
    const outcome = event.status;
    if (!outcome || outcome === "running") return Promise.resolve(null);
    return this.serialize(async () => {
      const journal = await this.load();
      const eventKey = `${parentAgentId}\0${provider}\0${event.id}\0${outcome}\0${event.timestamp ?? "unknown"}`;
      const existing = journal.events.find((entry) => entry.eventKey === eventKey);
      if (existing) return existing;
      const completion: ProviderSubagentCompletion = {
        sequence: journal.events.length + 1,
        eventKey,
        parentAgentId,
        provider,
        helperId: event.id,
        sourceEventId: null,
        nativeTurnId: null,
        nativeParentId: event.parentSubagentId ?? null,
        role: null,
        sourceEventTime: event.timestamp ?? null,
        provenance: "provider-stream",
        outcome,
        gaps: [
          "native event identity unavailable",
          "native turn identity unavailable",
          "helper role unavailable",
          ...(event.timestamp ? [] : ["source event time unavailable"]),
        ],
      };
      const next = { ...journal, events: [...journal.events, completion] };
      await writeJsonFileAtomic(this.file, next);
      this.journal = next;
      return completion;
    });
  }

  readAll(parentAgentId: string): Promise<ProviderSubagentCompletion[]> {
    return this.serialize(async () =>
      (await this.load()).events.filter((event) => event.parentAgentId === parentAgentId),
    );
  }

  replay(parentAgentId: string): Promise<ProviderSubagentCompletion[]> {
    return this.serialize(async () => {
      const journal = await this.load();
      const cursor = journal.acknowledgedThrough[parentAgentId] ?? 0;
      return journal.events.filter(
        (event) => event.parentAgentId === parentAgentId && event.sequence > cursor,
      );
    });
  }

  acknowledge(parentAgentId: string, sequence: number): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      const current = journal.acknowledgedThrough[parentAgentId] ?? 0;
      if (sequence <= current) return;
      if (
        !journal.events.some(
          (event) => event.parentAgentId === parentAgentId && event.sequence === sequence,
        )
      )
        throw new Error(`Cannot acknowledge unknown completion ${sequence}`);
      const next = {
        ...journal,
        acknowledgedThrough: { ...journal.acknowledgedThrough, [parentAgentId]: sequence },
      };
      await writeJsonFileAtomic(this.file, next);
      this.journal = next;
    });
  }

  flush(): Promise<void> {
    return this.pending.then(() => undefined);
  }
}
