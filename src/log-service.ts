import { AsyncLocalStorage } from "node:async_hooks";

export type LogLevel = "info" | "success" | "warn" | "error";

/** Who may see this line in the Market log UI. */
export type LogAudience = "user" | "recorder" | "ops";

/** 5m and 15m markets keep this many UTC clock windows in the Market log. */
export const LOG_KEPT_WINDOWS = 5;

export interface LogEntry {
  tMs: number;
  level: LogLevel;
  source: string;
  message: string;
  /** Market window start (unix sec) when the line was emitted. */
  windowStart?: number;
  /** Present on user-scoped lines; stripped before SSE. */
  userId?: string;
  audience: LogAudience;
}

export type LogMeta = {
  userId?: string;
};

/** Capture / recorder plumbing — Market log on dest only. */
const RECORDER_SOURCES = new Set([
  "recorder",
  "replay",
  "clob",
  "retention",
  "series-hub",
]);

/** Bot / order / sim activity — Market log for that account only. */
const USER_SOURCES = new Set(["trading", "sim"]);

type LogListener = (entry: LogEntry) => void;

const logUserStore = new AsyncLocalStorage<{ userId: string }>();

export function runWithLogUser<T>(userId: string, fn: () => T): T {
  const id = String(userId ?? "").trim();
  if (!id) return fn();
  return logUserStore.run({ userId: id }, fn);
}

export function runWithLogUserAsync<T>(userId: string, fn: () => Promise<T>): Promise<T> {
  const id = String(userId ?? "").trim();
  if (!id) return fn();
  return logUserStore.run({ userId: id }, fn);
}

export function currentLogUserId(): string | undefined {
  return logUserStore.getStore()?.userId;
}

export function inferLogAudience(source: string, userId?: string): LogAudience {
  if (RECORDER_SOURCES.has(source)) return "recorder";
  if (USER_SOURCES.has(source) && userId) return "user";
  return "ops";
}

/** SSE / UI payload — never include account ids. */
export function publicLogEntry(entry: LogEntry): Omit<LogEntry, "userId" | "audience"> {
  return {
    tMs: entry.tMs,
    level: entry.level,
    source: entry.source,
    message: entry.message,
    windowStart: entry.windowStart,
  };
}

export function isLogVisibleToClient(
  entry: LogEntry,
  clientUserId: string | undefined,
  recorderProcess: boolean,
): boolean {
  if (entry.audience === "recorder") return recorderProcess;
  if (entry.audience === "user") {
    return Boolean(clientUserId && entry.userId && entry.userId === clientUserId);
  }
  return false;
}

class LogService {
  private readonly buffer: LogEntry[] = [];
  private readonly listeners = new Set<LogListener>();
  private currentWindowStart: number | null = null;
  private windowDurationSec = 300;
  /** Sources silenced while Replay/Open re-simulates (avoids flooding SSE + UI). */
  private readonly mutedSources = new Set<string>();

  info(source: string, message: string, meta?: LogMeta): void {
    this.emit("info", source, message, meta);
  }

  success(source: string, message: string, meta?: LogMeta): void {
    this.emit("success", source, message, meta);
  }

  warn(source: string, message: string, meta?: LogMeta): void {
    this.emit("warn", source, message, meta);
  }

  error(source: string, message: string, meta?: LogMeta): void {
    this.emit("error", source, message, meta);
  }

  /** Run `fn` with one or more log sources fully suppressed. */
  runWithMutedSources<T>(sources: string[], fn: () => T): T {
    for (const source of sources) this.mutedSources.add(source);
    try {
      return fn();
    } finally {
      for (const source of sources) this.mutedSources.delete(source);
    }
  }

  async runWithMutedSourcesAsync<T>(sources: string[], fn: () => Promise<T>): Promise<T> {
    for (const source of sources) this.mutedSources.add(source);
    try {
      return await fn();
    } finally {
      for (const source of sources) this.mutedSources.delete(source);
    }
  }

  /** Track display window rolls — buffer keeps the last 5 UTC clock windows. */
  setActiveWindow(windowStart: number, windowEnd?: number): void {
    if (!Number.isFinite(windowStart) || windowStart <= 0) return;
    const durRaw = Number(windowEnd) - windowStart;
    const durationSec =
      durRaw === 900 || durRaw === 300
        ? durRaw
        : durRaw > 0
          ? durRaw
          : this.windowDurationSec;
    if (this.currentWindowStart === windowStart && this.windowDurationSec === durationSec) {
      return;
    }
    this.currentWindowStart = windowStart;
    this.windowDurationSec = durationSec > 0 ? durationSec : 300;
    this.pruneBuffer();
  }

  getRecent(): LogEntry[] {
    return [...this.buffer];
  }

  getRecentForClient(
    clientUserId: string | undefined,
    recorderProcess: boolean,
  ): ReturnType<typeof publicLogEntry>[] {
    return this.buffer
      .filter((entry) => isLogVisibleToClient(entry, clientUserId, recorderProcess))
      .map(publicLogEntry);
  }

  onEntry(listener: LogListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private keptWindowStarts(): number[] {
    const newest = this.currentWindowStart;
    const dur = this.windowDurationSec;
    if (newest == null || !(dur > 0)) return [];
    const starts: number[] = [];
    for (let i = 0; i < LOG_KEPT_WINDOWS; i += 1) {
      starts.push(newest - i * dur);
    }
    return starts;
  }

  private isWindowKept(windowStart?: number): boolean {
    if (this.currentWindowStart == null) return true;
    if (windowStart == null) return false;
    if (this.keptWindowStarts().includes(windowStart)) return true;
    // UTC clock can be one window ahead of a late REST roll (Buy GTD).
    return windowStart === this.currentWindowStart + this.windowDurationSec;
  }

  private pruneBuffer(): void {
    if (this.buffer.length === 0) return;
    const kept = this.buffer.filter((entry) => this.isWindowKept(entry.windowStart));
    this.buffer.length = 0;
    this.buffer.push(...kept);
  }

  private emit(level: LogLevel, source: string, message: string, meta?: LogMeta): void {
    if (this.mutedSources.has(source)) return;

    const userId = String(meta?.userId ?? currentLogUserId() ?? "").trim() || undefined;
    const audience = inferLogAudience(source, userId);
    const entry: LogEntry = {
      tMs: Date.now(),
      level,
      source,
      message,
      windowStart: this.currentWindowStart ?? undefined,
      userId,
      audience,
    };

    const line = `[${source}] ${message}`;
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else console.log(line); // success + info

    if (audience === "ops") return;

    this.buffer.push(entry);
    this.pruneBuffer();

    for (const listener of this.listeners) {
      listener(entry);
    }
  }
}

export const logService = new LogService();
