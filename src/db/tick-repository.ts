import type {
  ChainlinkTickDocument,
  ClobBookTickDocument,
  ClobRawTickDocument,
  MarketDocument,
} from "../types.js";
import {
  chainlinkTicksPath,
  clobBookTicksPath,
  clobRawTicksPath,
  marketTicksDir,
  parseWindowStartFromFilename,
  windowTicksDir,
} from "./data-dir.js";
import { appendJsonlLines, readJsonlFile, writeJsonlFile } from "./file-store.js";
import {
  fromStoredBookTick,
  fromStoredChainlinkTick,
  roundTo4,
  toStoredChainlinkTick,
  type StoredTickDocument,
} from "../tick-compact.js";
import fs from "fs/promises";
import path from "path";

async function appendTicks<T>(filePath: string, docs: T[]): Promise<void> {
  await appendJsonlLines(filePath, docs);
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
      appendTicks(clobRawTicksPath(market._id, windowStart), batch),
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
      appendTicks(chainlinkTicksPath(market._id, windowStart), batch),
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

/**
 * Replay-usable windows: non-empty CLOB book **and** Chainlink tick files.
 * Missing either side is treated as no recording for Schedule Replay.
 */
export async function windowsHavingReplayTickFiles(
  market: MarketDocument,
  windowStarts: number[],
): Promise<number[]> {
  const present: number[] = [];
  await Promise.all(
    windowStarts.map(async (windowStart) => {
      if (!Number.isFinite(windowStart)) return;
      const [hasBook, hasChainlink] = await Promise.all([
        windowHasNonEmptyTickFile(clobBookTicksPath(market._id, windowStart)),
        windowHasNonEmptyTickFile(chainlinkTicksPath(market._id, windowStart)),
      ]);
      if (hasBook && hasChainlink) present.push(windowStart);
    }),
  );
  return present.sort((a, b) => a - b);
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
