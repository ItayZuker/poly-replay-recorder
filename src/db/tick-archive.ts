import fsp from "fs/promises";
import path from "path";
import { hotCutoffSec } from "../retention.js";
import { logService } from "../log-service.js";
import type { MarketDocument } from "../types.js";
import {
  marketArchiveDir,
  marketTicksDir,
  marketWindowsDir,
  parseWindowStartFromFilename,
} from "./data-dir.js";
import { pruneRecordedWindows } from "./recorded-window-repository.js";
import { deleteRecordedWindowsBefore } from "./recorded-window-mongo-repository.js";
import { deleteWindowTradersBefore } from "./window-trader-repository.js";

export interface PruneMarketResult {
  series: string;
  cutoffSec: number;
  windowsRemoved: number;
  tickDirsRemoved: number;
  archiveRemoved: boolean;
}

async function listColdTickDirs(series: string, cutoffSec: number): Promise<number[]> {
  const ticksDir = marketTicksDir(series);
  try {
    const entries = await fsp.readdir(ticksDir, { withFileTypes: true });
    const cold: number[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const windowStart = parseWindowStartFromFilename(entry.name);
      if (windowStart == null || windowStart >= cutoffSec) continue;
      cold.push(windowStart);
    }
    return cold;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return [];
    throw err;
  }
}

async function removeTickDir(series: string, windowStart: number): Promise<void> {
  const tickDir = path.join(marketTicksDir(series), String(windowStart));
  await fsp.rm(tickDir, { recursive: true, force: true });
}

/** Drop any legacy zip archive folder for this series. */
async function removeArchiveDir(series: string): Promise<boolean> {
  const archiveDir = marketArchiveDir(series);
  try {
    await fsp.access(archiveDir);
  } catch {
    return false;
  }
  await fsp.rm(archiveDir, { recursive: true, force: true });
  return true;
}

/**
 * Permanently delete tick/window data older than the hot retention window (default 7 days).
 * No zip archive — cold data is discarded.
 */
export async function pruneColdMarketData(
  market: MarketDocument,
): Promise<PruneMarketResult> {
  const series = market._id;
  const cutoffSec = hotCutoffSec(Math.floor(Date.now() / 1000), market.retentionDays);

  const coldTicks = await listColdTickDirs(series, cutoffSec);
  for (const windowStart of coldTicks) {
    await removeTickDir(series, windowStart);
  }

  const windowsRemoved = await pruneRecordedWindows(market, cutoffSec);
  await deleteRecordedWindowsBefore(cutoffSec, series).catch((err) => {
    logService.warn(
      "retention",
      `Mongo recorded_windows prune failed (${series}): ${String(err)}`,
    );
  });
  await deleteWindowTradersBefore(cutoffSec, series).catch((err) => {
    logService.warn(
      "retention",
      `Mongo window_traders prune failed (${series}): ${String(err)}`,
    );
  });

  const archiveRemoved = await removeArchiveDir(series);

  if (coldTicks.length > 0 || windowsRemoved > 0 || archiveRemoved) {
    logService.info(
      "retention",
      `Pruned ${series}: ${coldTicks.length} tick dirs, ${windowsRemoved} window files` +
        (archiveRemoved ? ", removed legacy archive/" : ""),
    );
  }

  return {
    series,
    cutoffSec,
    windowsRemoved,
    tickDirsRemoved: coldTicks.length,
    archiveRemoved,
  };
}

/** @deprecated Use pruneColdMarketData — no longer zips cold data. */
export async function archiveColdMarketData(
  market: MarketDocument,
): Promise<PruneMarketResult> {
  return pruneColdMarketData(market);
}
