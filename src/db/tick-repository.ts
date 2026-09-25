import type {
  ChainlinkTickDocument,
  ClobBookTickDocument,
  ClobRawTickDocument,
  MarketDocument,
} from "../types.js";
import {
  chainlinkTicksPath,
  chainlinkTicksZstPath,
  clobBookTicksPath,
  clobRawTicksPath,
  clobRawTicksZstPath,
  marketTicksDir,
  parseWindowStartFromFilename,
  windowTicksDir,
} from "./data-dir.js";
import { readJsonlZstLines } from "./tick-zst.js";
import { OFFICIAL_RESOLVE_MAX_WAIT_MS } from "../official-window-resolution.js";
import { appendJsonlLines, readJsonlFile, writeJsonlFile } from "./file-store.js";
import {
  fromStoredBookTick,
  fromStoredChainlinkTick,
  roundTo4,
  toStoredChainlinkTick,
  type StoredTickDocument,
} from "../tick-compact.js";
import { slimClobRawTick, slimChainlinkTick } from "../tick-slim.js";
import { isUnusablePricePath } from "../window-dynamics.js";
import fs from "fs/promises";
import path from "path";

/** Only cache usable=true. A premature false (pre-flush) must be allowed to flip. */
const usableWindowCache = new Set<string>();
/** Wait for the last tick flush before scoring a just-ended window. */
export const COVERAGE_FLUSH_GRACE_SEC = 3;

/** Per-file `_id`s already on disk — skip appends that would duplicate a retry. */
const writtenTickIdsByFile = new Map<string, Set<string>>();

async function loadWrittenTickIds(filePath: string): Promise<Set<string>> {
  const cached = writtenTickIdsByFile.get(filePath);
  if (cached) return cached;
  const known = new Set<string>();
  const rows = await readJsonlFile<{ _id?: unknown }>(filePath);
  for (const row of rows) {
    if (row._id != null && String(row._id).length > 0) known.add(String(row._id));
  }
  writtenTickIdsByFile.set(filePath, known);
  return known;
}

export function forgetWrittenTickIds(filePath: string): void {
  writtenTickIdsByFile.delete(filePath);
}

async function appendTicks<T extends { _id?: unknown }>(
  filePath: string,
  docs: T[],
  present: (doc: T) => unknown | null = (doc) => doc,
): Promise<void> {
  if (docs.length === 0) return;
  const known = await loadWrittenTickIds(filePath);
  const fresh = docs.filter((doc) => {
    const id = doc._id != null ? String(doc._id) : "";
    return !id || !known.has(id);
  });
  if (fresh.length === 0) return;
  const stored = fresh.map(present).filter((row) => row != null);
  if (stored.length > 0) await appendJsonlLines(filePath, stored);
  for (const doc of fresh) {
    if (doc._id != null && String(doc._id).length > 0) known.add(String(doc._id));
  }
}

export async function insertClobRawTicks(
  market: MarketDocument,
  ticks: ClobRawTickDocument[],
): Promise<void> {
  if (ticks.length === 0) return;
  const byWindow = new Map<number, ClobRawTickDocument[]>();
  for (const tick of ticks) {
    const batch = byWindow.get(tick.windowStart) ?? [];
    batch.push(tick);
    byWindow.set(tick.windowStart, batch);
  }
  await Promise.all(
    [...byWindow.entries()].map(([windowStart, batch]) =>
      appendTicks(clobRawTicksPath(market._id, windowStart), batch, (tick) =>
        slimClobRawTick(tick as unknown as Record<string, unknown>),
      ),
    ),
  );
}

export async function insertClobBookTicks(
  market: MarketDocument,
  ticks: ClobBookTickDocument[],
): Promise<void> {
  if (ticks.length === 0) return;
  const byWindow = new Map<number, ClobBookTickDocument[]>();
  for (const tick of ticks) {
    const batch = byWindow.get(tick.windowStart) ?? [];
    batch.push(tick);
    byWindow.set(tick.windowStart, batch);
  }
  await Promise.all(
    [...byWindow.entries()].map(([windowStart, batch]) =>
      appendTicks(clobBookTicksPath(market._id, windowStart), batch),
    ),
  );
}

export async function insertChainlinkTicks(
  market: MarketDocument,
  ticks: ChainlinkTickDocument[],
): Promise<void> {
  if (ticks.length === 0) return;
  const byWindow = new Map<number, ChainlinkTickDocument[]>();
  for (const tick of ticks) {
    const batch = byWindow.get(tick.windowStart) ?? [];
    batch.push(tick);
    byWindow.set(tick.windowStart, batch);
  }
  await Promise.all(
    [...byWindow.entries()].map(([windowStart, batch]) =>
      appendTicks(chainlinkTicksPath(market._id, windowStart), batch, (tick) =>
        slimChainlinkTick(tick as unknown as Record<string, unknown>),
      ),
    ),
  );
}

export async function listClobRawTicks(
  market: MarketDocument,
  windowStart: number,
  limit = 10_000,
): Promise<ClobRawTickDocument[]> {
  return readJsonlFile<ClobRawTickDocument>(clobRawTicksPath(market._id, windowStart), limit);
}

/** Dest Replay: zst only. Missing file = empty. */
export async function listReplayClobRawTicks(
  market: MarketDocument,
  windowStart: number,
): Promise<ClobRawTickDocument[]> {
  const rows = await readJsonlZstLines(clobRawTicksZstPath(market._id, windowStart));
  return rows as ClobRawTickDocument[];
}

/** Dest Replay: zst only. Missing file = empty. */
export async function listReplayChainlinkTicks(
  market: MarketDocument,
  windowStart: number,
): Promise<ChainlinkTickDocument[]> {
  const rows = await readJsonlZstLines<StoredTickDocument>(
    chainlinkTicksZstPath(market._id, windowStart),
  );
  return rows.map((row) => fromStoredChainlinkTick(row));
}

export async function listClobBookTicks(
  market: MarketDocument,
  windowStart: number,
  limit = 10_000,
): Promise<ClobBookTickDocument[]> {
  const ticks = await readJsonlFile<StoredTickDocument>(
    clobBookTicksPath(market._id, windowStart),
    limit,
  );
  // Expand compact / top-of-book storage into full book docs for the sim engine.
  return ticks
    .map((doc) => fromStoredBookTick(doc))
    .sort((a, b) => a.tMs - b.tMs);
}

export async function listChainlinkTicks(
  market: MarketDocument,
  windowStart: number,
  limit = 10_000,
): Promise<ChainlinkTickDocument[]> {
  const ticks = await readJsonlFile<StoredTickDocument>(
    chainlinkTicksPath(market._id, windowStart),
    limit,
  );
  // Critical: disk ticks omit derived fields (assetGap, range*). Without expand,
  // Replay gap filters see no gap and never buy → all-zero schedule stats.
  return ticks
    .map((doc) => fromStoredChainlinkTick(doc))
    .sort((a, b) => a.tMs - b.tMs);
}

/**
 * Stamp Polymarket Gamma settlement close onto the Chainlink JSONL tip at windowEnd.
 * Mid-window Chainlink samples are kept as recorded; ticks at/after windowEnd are replaced
 * by one tip priced from Gamma (not invented Chainlink). Skips if there is no mid-window path.
 */
export async function stampOfficialChainlinkCloseTip(
  market: MarketDocument,
  windowStart: number,
  windowEnd: number,
  opts: { closePrice: number; priceToBeat: number },
): Promise<"updated" | "unchanged" | "skipped-no-ticks"> {
  if (
    !Number.isFinite(windowStart) ||
    !Number.isFinite(windowEnd) ||
    !Number.isFinite(opts.closePrice) ||
    !Number.isFinite(opts.priceToBeat)
  ) {
    return "skipped-no-ticks";
  }

  const filePath = chainlinkTicksPath(market._id, windowStart);
  const raw = await readJsonlFile<StoredTickDocument>(filePath, Number.MAX_SAFE_INTEGER);
  if (raw.length === 0) return "skipped-no-ticks";

  const endMs = Math.round(windowEnd * 1000);
  const tipEpsMs = 2;
  const expanded = raw
    .map((doc) => fromStoredChainlinkTick(doc))
    .sort((a, b) => a.tMs - b.tMs);
  const mid = expanded.filter((t) => t.tMs < endMs - tipEpsMs);
  if (mid.length === 0) return "skipped-no-ticks";

  const close = roundTo4(opts.closePrice);
  const ptb = roundTo4(opts.priceToBeat);
  const existingTips = expanded.filter((t) => t.tMs >= endMs - tipEpsMs);
  const tipMatch =
    existingTips.length === 1 &&
    existingTips[0]!.tMs === endMs &&
    Number(existingTips[0]!.assetPrice) === close &&
    Number(existingTips[0]!.prevCloseAsset) === ptb;
  if (tipMatch && mid.length + existingTips.length === expanded.length) {
    return "unchanged";
  }

  const lastMid = mid[mid.length - 1]!;
  const tip: ChainlinkTickDocument = {
    _id: `${windowStart}:gamma-close`,
    windowStart,
    windowEnd,
    tMs: endMs,
    assetPrice: close,
    prevCloseAsset: ptb,
    priceToBeatSource: "gamma",
    ptbCrossings: lastMid.ptbCrossings,
    minAssetPrice: lastMid.minAssetPrice,
    maxAssetPrice: lastMid.maxAssetPrice,
  };

  await writeJsonlFile(filePath, [
    ...mid.map((t) => toStoredChainlinkTick(t)),
    toStoredChainlinkTick(tip),
  ]);
  forgetWrittenTickIds(filePath);
  return "updated";
}

async function windowHasNonEmptyTickFile(
  filePath: string,
): Promise<boolean> {
  try {
    const st = await fs.stat(filePath);
    return st.isFile() && st.size > 0;
  } catch {
    return false;
  }
}

/** Window-start folder names under the market ticks dir (not file-quality filtered). */
export async function listTickWindowStarts(series: string): Promise<number[]> {
  const ticksRoot = marketTicksDir(series);
  try {
    const entries = await fs.readdir(ticksRoot, { withFileTypes: true });
    const starts: number[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const windowStart = parseWindowStartFromFilename(entry.name);
      if (windowStart != null) starts.push(windowStart);
    }
    return starts.sort((a, b) => a - b);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return [];
    throw err;
  }
}

/** Which window starts have a non-empty Chainlink tick file (cheap disk check). */
export async function windowsHavingChainlinkTicks(
  market: MarketDocument,
  windowStarts: number[],
): Promise<number[]> {
  const present: number[] = [];
  await Promise.all(
    windowStarts.map(async (windowStart) => {
      if (!Number.isFinite(windowStart)) return;
      if (await windowHasNonEmptyTickFile(chainlinkTicksPath(market._id, windowStart))) {
        present.push(windowStart);
      }
    }),
  );
  return present.sort((a, b) => a - b);
}

/** Which window starts have a non-empty CLOB book tick file (cheap disk check). */
export async function windowsHavingClobBookTicks(
  market: MarketDocument,
  windowStarts: number[],
): Promise<number[]> {
  const present: number[] = [];
  await Promise.all(
    windowStarts.map(async (windowStart) => {
      if (!Number.isFinite(windowStart)) return;
      if (await windowHasNonEmptyTickFile(clobBookTicksPath(market._id, windowStart))) {
        present.push(windowStart);
      }
    }),
  );
  return present.sort((a, b) => a - b);
}

export type WindowChipState = "missing" | "pending" | "recorded";

async function windowHasUsableZstPricePath(
  market: MarketDocument,
  windowStart: number,
): Promise<boolean> {
  const key = `${market._id}:${windowStart}:zst`;
  if (usableWindowCache.has(key)) return true;
  const winSec = (market.timeframeMinutes === 15 ? 15 : 5) * 60;
  const endedAt = windowStart + winSec;
  const rows = await readJsonlZstLines<StoredTickDocument>(
    chainlinkTicksZstPath(market._id, windowStart),
  );
  const ticks = rows.map((doc) => fromStoredChainlinkTick(doc));
  const usable = !isUnusablePricePath(
    ticks.map((tick) => ({
      tMs: tick.tMs,
      assetPrice: tick.assetPrice,
    })),
    windowStart,
    endedAt,
  );
  if (usable) usableWindowCache.add(key);
  return usable;
}

/** Green chip: both zst files exist and the raw Chainlink path is usable. */
export async function classifyWindowChip(
  market: MarketDocument,
  windowStart: number,
  nowSec = Math.floor(Date.now() / 1000),
): Promise<WindowChipState> {
  const winSec = (market.timeframeMinutes === 15 ? 15 : 5) * 60;
  const windowEnd = windowStart + winSec;
  const [hasRawZst, hasChainZst] = await Promise.all([
    windowHasNonEmptyTickFile(clobRawTicksZstPath(market._id, windowStart)),
    windowHasNonEmptyTickFile(chainlinkTicksZstPath(market._id, windowStart)),
  ]);
  if (hasRawZst && hasChainZst) {
    return (await windowHasUsableZstPricePath(market, windowStart))
      ? "recorded"
      : "missing";
  }
  if (nowSec < windowEnd) return "missing";
  if (nowSec < windowEnd + OFFICIAL_RESOLVE_MAX_WAIT_MS / 1000) {
    return "pending";
  }
  return "missing";
}

export async function classifyWindowChips(
  market: MarketDocument,
  windowStarts: number[],
  nowSec = Math.floor(Date.now() / 1000),
): Promise<Map<number, WindowChipState>> {
  const out = new Map<number, WindowChipState>();
  await Promise.all(
    windowStarts.map(async (windowStart) => {
      if (!Number.isFinite(windowStart)) return;
      out.set(windowStart, await classifyWindowChip(market, windowStart, nowSec));
    }),
  );
  return out;
}

/**
 * Replay-ready windows: both `.jsonl.zst` files exist and Chainlink path is usable.
 * Live JSONL is not Replay-ready.
 */
export async function windowsHavingBookAndChainlinkTicks(
  market: MarketDocument,
  windowStarts: number[],
): Promise<number[]> {
  const present: number[] = [];
  await Promise.all(
    windowStarts.map(async (windowStart) => {
      if (!Number.isFinite(windowStart)) return;
      if ((await classifyWindowChip(market, windowStart)) === "recorded") {
        present.push(windowStart);
      }
    }),
  );
  return present.sort((a, b) => a - b);
}

/** Replay-usable windows: zst only. */
export async function windowsHavingReplayTickFiles(
  market: MarketDocument,
  windowStarts: number[],
): Promise<number[]> {
  return windowsHavingBookAndChainlinkTicks(market, windowStarts);
}

export async function countClobRawTicksForWindow(
  market: MarketDocument,
  windowStart: number,
): Promise<number> {
  const raw = await readJsonlFile<ClobRawTickDocument>(
    clobRawTicksPath(market._id, windowStart),
    Number.MAX_SAFE_INTEGER,
  );
  return raw.length;
}

export async function countClobBookTicksForWindow(
  market: MarketDocument,
  windowStart: number,
): Promise<number> {
  const raw = await readJsonlFile<ClobBookTickDocument>(
    clobBookTicksPath(market._id, windowStart),
    Number.MAX_SAFE_INTEGER,
  );
  return raw.length;
}

export async function countChainlinkTicksForWindow(
  market: MarketDocument,
  windowStart: number,
): Promise<number> {
  const raw = await readJsonlFile<ChainlinkTickDocument>(
    chainlinkTicksPath(market._id, windowStart),
    Number.MAX_SAFE_INTEGER,
  );
  return raw.length;
}

/** @deprecated Use listClobBookTicks */
export const listBookTicks = listClobBookTicks;

export async function pruneTicks(market: MarketDocument, cutoff: number): Promise<number> {
  const ticksRoot = marketTicksDir(market._id);
  let deleted = 0;
  try {
    const entries = await fs.readdir(ticksRoot, { withFileTypes: true });
    for (const entry of entries) {
      const windowStart = parseWindowStartFromFilename(entry.name);
      if (windowStart == null || windowStart >= cutoff) continue;
      const target = path.join(ticksRoot, entry.name);
      if (entry.isDirectory()) {
        await fs.rm(target, { recursive: true, force: true });
      } else {
        await fs.unlink(target);
      }
      deleted += 1;
    }
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== "ENOENT") throw err;
  }
  return deleted;
}

export async function ensureWindowTickDir(series: string, windowStart: number): Promise<void> {
  await fs.mkdir(windowTicksDir(series, windowStart), { recursive: true });
}
