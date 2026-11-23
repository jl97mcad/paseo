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
  coverageUncertain: z.boolean().default(false),
});
type Journal = z.infer<typeof journalSchema>;

// Host scope is a distinct type, not an empty parent ID: parent IDs must be non-empty, so only the
// *Host methods can reach a host registration. The serialized key keeps the empty-parent shape.
type ConsumerScope = { kind: "host" } | { kind: "parent"; parentAgentId: string };
const HOST_SCOPE: ConsumerScope = { kind: "host" };
const HOST_KEY_PREFIX = '["",';
const parentIdSchema = z.string().min(1).max(128);

function parentScope(parentAgentId: string): ConsumerScope {
  return { kind: "parent", parentAgentId: parentIdSchema.parse(parentAgentId) };
}

function consumerKey(scope: ConsumerScope, consumer: string): string {
  return JSON.stringify([scope.kind === "host" ? "" : scope.parentAgentId, consumer]);
}

function inScope(scope: ConsumerScope, event: { parentAgentId: string }): boolean {
  return scope.kind === "host" || event.parentAgentId === scope.parentAgentId;
}

interface FailedAppends {
  gaps: string[];
  omitted: number;
}

/** Single daemon writer. Consumers call the daemon API, never open this file independently. */
export class ProviderSubagentCompletionStore {
  private journal: Journal | undefined;
  private pending: Promise<unknown> = Promise.resolve();
  private readonly failedAppends = new Map<string, FailedAppends>();
  private failureTrackingOverflowed = false;
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
      this.journal = {
        acknowledgedThrough: {},
        consumers: {},
        discardedThrough: 0,
        events: [],
        coverageUncertain: false,
      };
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
    next.coverageUncertain ||= this.failedAppends.size > 0 || this.failureTrackingOverflowed;
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
          ...this.failedObservationGaps(parentAgentId),
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
      this.failedAppends.delete(parentAgentId);
      return completion;
    });
  }

  private failedObservationGaps(parentAgentId: string): string[] {
    const failures = this.failedAppends.get(parentAgentId);
    const failureGaps = [...(failures?.gaps ?? [])];
    if (failures?.omitted) {
      const count =
        failures.omitted === Number.MAX_SAFE_INTEGER
          ? `at least ${failures.omitted}`
          : `${failures.omitted}`;
      failureGaps.push(`${count} further observations not durably accepted`);
    }
    if (this.failureTrackingOverflowed)
      failureGaps.push(
        "failure tracking overflow: failed observations for evicted parents may be missing",
      );
    return failureGaps;
  }

  recordFailedAppend(parentAgentId: string, helperId: string, provenance: string): void {
    const failures = this.failedAppends.get(parentAgentId) ?? { gaps: [], omitted: 0 };
    const detail = `observation not durably accepted: ${helperId} (${provenance})`;
    // Bound serialized gaps too: JSON escaping can expand each code unit to six bytes.
    failures.gaps.push(detail.length > 256 ? `${detail.slice(0, 256)} [truncated]` : detail);
    if (failures.gaps.length > 16) {
      failures.gaps.shift();
      failures.omitted = Math.min(failures.omitted + 1, Number.MAX_SAFE_INTEGER);
    }
    this.failedAppends.delete(parentAgentId);
    this.failedAppends.set(parentAgentId, failures);
    if (this.failedAppends.size > 256) {
      const oldest = this.failedAppends.keys().next().value;
      if (oldest !== undefined) this.failedAppends.delete(oldest);
      this.failureTrackingOverflowed = true;
    }
  }

  readAll(parentAgentId: string): Promise<ProviderSubagentCompletion[]> {
    return this.serialize(async () =>
      (await this.load()).events.filter((e) => e.parentAgentId === parentAgentId),
    );
  }

  private cursor(journal: Journal, scope: ConsumerScope, consumer: string) {
    z.string().min(1).max(128).parse(consumer);
    return (
      journal.consumers[consumerKey(scope, consumer)] ?? {
        through:
          consumer === "projection" && scope.kind === "parent"
            ? (journal.acknowledgedThrough[scope.parentAgentId] ?? 0)
            : 0,
        acknowledged: [],
      }
    );
  }

  // Parent and host registrations have separate pools of 256, so host replay cannot be starved by
  // parent consumers or the reverse.
  private consumerLimitReached(journal: Journal, scope: ConsumerScope): boolean {
    const host = scope.kind === "host";
    return (
      Object.keys(journal.consumers).filter((key) => key.startsWith(HOST_KEY_PREFIX) === host)
        .length >= 256
    );
  }

  async consume(parentAgentId: string, consumer: string) {
    return this.consumeScope(parentScope(parentAgentId), consumer, 10_000);
  }

  consumeHost(consumer: string, limit: number) {
    return this.consumeScope(HOST_SCOPE, consumer, z.number().int().min(1).max(100).parse(limit));
  }

  acknowledgeHost(consumer: string, sequence: number) {
    return this.acknowledgeScope(HOST_SCOPE, sequence, consumer);
  }

  acceptHostGap(consumer: string, discardedThrough: number) {
    return this.acceptGapScope(HOST_SCOPE, consumer, discardedThrough);
  }

  releaseHost(consumer: string) {
    return this.releaseScope(HOST_SCOPE, consumer);
  }

  private consumeScope(scope: ConsumerScope, consumer: string, limit: number) {
    return this.serialize(async () => {
      let journal = await this.load();
      const cursor = this.cursor(journal, scope, consumer);
      const key = consumerKey(scope, consumer);
      if (!(key in journal.consumers)) {
        if (this.consumerLimitReached(journal, scope))
          throw new Error("Completion consumer limit reached");
        await this.save({
          ...journal,
          events: [...journal.events],
          consumers: { ...journal.consumers, [key]: cursor },
        });
        journal = await this.load();
      }
      return {
        coverageUncertain:
          journal.coverageUncertain ||
          this.failedAppends.size > 0 ||
          this.failureTrackingOverflowed,
        events: journal.events
          .filter(
            (e) =>
              inScope(scope, e) &&
              e.sequence > cursor.through &&
              !cursor.acknowledged.includes(e.sequence),
          )
          .slice(0, limit),
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

  async acknowledge(
    parentAgentId: string,
    sequence: number,
    consumer = "projection",
  ): Promise<void> {
    return this.acknowledgeScope(parentScope(parentAgentId), sequence, consumer);
  }

  private acknowledgeScope(scope: ConsumerScope, sequence: number, consumer: string) {
    return this.serialize(async () => {
      const journal = await this.load();
      const cursor = this.cursor(journal, scope, consumer);
      if (sequence <= cursor.through || cursor.acknowledged.includes(sequence)) return;
      if (!journal.events.some((e) => inScope(scope, e) && e.sequence === sequence))
        throw new Error(`Cannot acknowledge unknown completion ${sequence}`);
      const key = consumerKey(scope, consumer);
      if (!(key in journal.consumers) && this.consumerLimitReached(journal, scope))
        throw new Error("Completion consumer limit reached");
      const acknowledged = [...cursor.acknowledged, sequence];
      let through = cursor.through;
      for (const event of journal.events.filter(
        (e) =>
          cursor.through >= journal.discardedThrough && inScope(scope, e) && e.sequence > through,
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

  async acceptGap(
    parentAgentId: string,
    consumer: string,
    discardedThrough: number,
  ): Promise<void> {
    return this.acceptGapScope(parentScope(parentAgentId), consumer, discardedThrough);
  }

  private acceptGapScope(
    scope: ConsumerScope,
    consumer: string,
    discardedThrough: number,
  ): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      if (discardedThrough !== journal.discardedThrough)
        throw new Error("Retention boundary changed; replay again");
      const cursor = this.cursor(journal, scope, consumer);
      const key = consumerKey(scope, consumer);
      if (!(key in journal.consumers) && this.consumerLimitReached(journal, scope))
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

  async releaseConsumer(parentAgentId: string, consumer: string): Promise<void> {
    return this.releaseScope(parentScope(parentAgentId), consumer);
  }

  private releaseScope(scope: ConsumerScope, consumer: string): Promise<void> {
    return this.serialize(async () => {
      const journal = await this.load();
      const consumers = { ...journal.consumers };
      delete consumers[consumerKey(scope, consumer)];
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
    return this.serialize(async () => {
      if (this.failedAppends.size === 0 && !this.failureTrackingOverflowed) return;
      const journal = await this.load();
      await this.save({ ...journal, events: [...journal.events] });
    });
  }
}
