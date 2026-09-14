import type { MarketDocument, RecordedWindowDocument } from "../types.js";
import { getWeekHistoryCutoffUtcSec } from "../day-hour-slots.js";
import {
  fromStoredRecordedWindow,
  type StoredWindowDocument,
} from "../window-compact.js";
import { marketWindowsDir } from "./data-dir.js";
import { listWindowFiles, readJsonFile } from "./file-store.js";
import {
  deleteRecordedWindowSummary,
  getRecordedWindowSummary,
  listRecordedWindowStarts as listMongoWindowStarts,
  listRecordedWindowsSince,
  summaryToRecordedWindow,
  upsertRecordedWindowSummary,
} from "./recorded-window-mongo-repository.js";
import path from "path";

/**
 * Window headers live in Mongo `recorded_windows`.
 * Local `windows/*.json` is read only for one-time backfill.
 */

export async function saveRecordedWindow(
  market: MarketDocument,
  doc: Omit<RecordedWindowDocument, "_id" | "updatedAt">,
): Promise<void> {
  await upsertRecordedWindowSummary(market._id, {
    ...doc,
    updatedAt: new Date().toISOString(),
  });
}

export async function getRecordedWindow(
  market: MarketDocument,
  windowStart: number,
): Promise<RecordedWindowDocument | null> {
  const summary = await getRecordedWindowSummary(market._id, windowStart);
  return summary ? summaryToRecordedWindow(summary) : null;
}

export async function listRecordedWindowStarts(series: string): Promise<number[]> {
  return listMongoWindowStarts(series, getWeekHistoryCutoffUtcSec());
}

export async function listRecordedWindows(
  market: MarketDocument,
): Promise<RecordedWindowDocument[]> {
  const summaries = await listRecordedWindowsSince(getWeekHistoryCutoffUtcSec(), market._id);
  return summaries.map(summaryToRecordedWindow);
}

export async function getWindowDataVersion(
  market: MarketDocument,
  windows?: RecordedWindowDocument[],
): Promise<string> {
  const list = windows ?? (await listRecordedWindows(market));
  if (list.length === 0) return "0";
  const latest = list.reduce((best, current) =>
    current.windowStart > best.windowStart ? current : best,
  );
  return `${latest.windowStart}:${latest.savedAt}`;
}

/** Local JSON prune is a no-op — Mongo retention is `deleteRecordedWindowsBefore`. */
export async function pruneRecordedWindows(
  _market: MarketDocument,
  _cutoff: number,
): Promise<number> {
  return 0;
}

export async function deleteRecordedWindowFile(
  series: string,
  windowStart: number,
): Promise<void> {
  await deleteRecordedWindowSummary(series, windowStart);
}

/** Disk headers for migrating into Mongo. */
export async function listLocalRecordedWindows(
  market: MarketDocument,
): Promise<RecordedWindowDocument[]> {
  const dir = marketWindowsDir(market._id);
  const files = await listWindowFiles(dir);
  const windows = await Promise.all(
    files.map(async (filename) => {
      try {
        const doc = await readJsonFile<StoredWindowDocument>(path.join(dir, filename));
        return doc ? fromStoredRecordedWindow(doc) : null;
      } catch {
        return null;
      }
    }),
  );
  return windows
    .filter((window): window is RecordedWindowDocument => window != null)
    .sort((a, b) => a.windowStart - b.windowStart);
}
