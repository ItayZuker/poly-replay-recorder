import type { PtbHistoryEntry, WindowOutcome } from "../types.js";
import { decodePtbHistory, encodePtbHistory } from "../ptb-history.js";
import { getMongoClient, getMongoDbName } from "./mongo-client.js";

const COLLECTION = "recorded_windows";

/** Slim window summary used by Replay slot counts and Open Replay (Mongo). */
export interface RecordedWindowSummary {
  series: string;
  windowStart: number;
  windowEnd: number;
  savedAt: string;
  ptbCrossings?: number;
  rangeTop?: number;
  rangeBottom?: number;
  uniqueTraders?: number;
  newWallets?: number;
  windowOutcome?: WindowOutcome;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  /** Window open / PTB (for Open Replay metrics). */
  prevCloseAsset?: number;
  /** Last asset mark for the window (Open Replay close). */
  assetPrice?: number;
  ptbHistory?: PtbHistoryEntry[];
  gammaPtb?: number;
}

type MongoRecordedWindowDoc = {
  _id?: string | undefined;
  series?: string;
  marketSeries?: string;
  windowStart?: number;
  windowEnd?: number;
  savedAt?: string | Date;
  ptbCrossings?: number;
  rangeTop?: number;
  rangeBottom?: number;
  uniqueTraders?: number;
  newWallets?: number;
  windowOutcome?: WindowOutcome | null;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  prevCloseAsset?: number;
  assetPrice?: number;
  ptbHistory?: unknown;
  gammaPtb?: number;
  /** Legacy nested payload from older sim writers. */
  window?: {
    windowStart?: number;
    windowEnd?: number;
    savedAt?: string;
    ptbCrossings?: number;
    rangeTop?: number;
    rangeBottom?: number;
    uniqueTraders?: number;
    newWallets?: number;
    windowOutcome?: WindowOutcome | null;
    minAssetPrice?: number;
    maxAssetPrice?: number;
    assetRange?: number;
    prevCloseAsset?: number;
    assetPrice?: number;
  };
};

const WINDOW_SUMMARY_PROJECTION = {
  _id: 1,
  series: 1,
  marketSeries: 1,
  windowStart: 1,
  windowEnd: 1,
  savedAt: 1,
  ptbCrossings: 1,
  rangeTop: 1,
  rangeBottom: 1,
  uniqueTraders: 1,
  newWallets: 1,
  windowOutcome: 1,
  minAssetPrice: 1,
  maxAssetPrice: 1,
  assetRange: 1,
  prevCloseAsset: 1,
  assetPrice: 1,
  ptbHistory: 1,
  gammaPtb: 1,
  "window.windowStart": 1,
  "window.windowEnd": 1,
  "window.savedAt": 1,
  "window.ptbCrossings": 1,
  "window.rangeTop": 1,
  "window.rangeBottom": 1,
  "window.uniqueTraders": 1,
  "window.newWallets": 1,
  "window.windowOutcome": 1,
  "window.minAssetPrice": 1,
  "window.maxAssetPrice": 1,
  "window.assetRange": 1,
  "window.prevCloseAsset": 1,
  "window.assetPrice": 1,
} as const;

function seriesFromDoc(doc: MongoRecordedWindowDoc): string | null {
  if (typeof doc.series === "string" && doc.series.length > 0) return doc.series;
  if (typeof doc.marketSeries === "string" && doc.marketSeries.length > 0) return doc.marketSeries;
  if (typeof doc._id === "string" && doc._id.includes(":")) {
    return doc._id.slice(0, doc._id.lastIndexOf(":"));
  }
  return null;
}

function savedAtToString(value: string | Date | undefined, fallback: number): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string" && value.length > 0) return value;
  return String(fallback);
}

function normalizeDoc(doc: MongoRecordedWindowDoc): RecordedWindowSummary | null {
  const nested = doc.window;
  const series = seriesFromDoc(doc);
  const windowStart = doc.windowStart ?? nested?.windowStart;
  if (!series || windowStart == null || !Number.isFinite(windowStart)) return null;

  const windowEnd = doc.windowEnd ?? nested?.windowEnd ?? windowStart;
  const savedAt = savedAtToString(doc.savedAt ?? nested?.savedAt, windowStart);
  const windowOutcome = doc.windowOutcome ?? nested?.windowOutcome;

  const out: RecordedWindowSummary = {
    series,
    windowStart,
    windowEnd,
    savedAt,
  };

  const ptbCrossings = doc.ptbCrossings ?? nested?.ptbCrossings;
  const rangeTop = doc.rangeTop ?? nested?.rangeTop;
  const rangeBottom = doc.rangeBottom ?? nested?.rangeBottom;
  const uniqueTraders = doc.uniqueTraders ?? nested?.uniqueTraders;
  const newWallets = doc.newWallets ?? nested?.newWallets;
  const minAssetPrice = doc.minAssetPrice ?? nested?.minAssetPrice;
  const maxAssetPrice = doc.maxAssetPrice ?? nested?.maxAssetPrice;
  const assetRange = doc.assetRange ?? nested?.assetRange;
  const prevCloseAsset = doc.prevCloseAsset ?? nested?.prevCloseAsset;
  const assetPrice = doc.assetPrice ?? nested?.assetPrice;

  if (ptbCrossings != null) out.ptbCrossings = ptbCrossings;
  if (rangeTop != null) out.rangeTop = rangeTop;
  if (rangeBottom != null) out.rangeBottom = rangeBottom;
  if (uniqueTraders != null) out.uniqueTraders = uniqueTraders;
  if (newWallets != null) out.newWallets = newWallets;
  if (minAssetPrice != null && Number.isFinite(minAssetPrice)) out.minAssetPrice = minAssetPrice;
  if (maxAssetPrice != null && Number.isFinite(maxAssetPrice)) out.maxAssetPrice = maxAssetPrice;
  if (assetRange != null && Number.isFinite(assetRange)) out.assetRange = assetRange;
  if (prevCloseAsset != null && Number.isFinite(prevCloseAsset)) out.prevCloseAsset = prevCloseAsset;
  if (assetPrice != null && Number.isFinite(assetPrice)) out.assetPrice = assetPrice;
  const ptbHistory = decodePtbHistory(doc.ptbHistory);
  if (ptbHistory) out.ptbHistory = ptbHistory;
  if (doc.gammaPtb != null && Number.isFinite(doc.gammaPtb)) out.gammaPtb = doc.gammaPtb;
  if (windowOutcome === "up" || windowOutcome === "down") out.windowOutcome = windowOutcome;

  return out;
}

/** Upsert slim recorded-window fields for one finished window (recorder role). */
export async function upsertRecordedWindowSummary(
  series: string,
  window: {
    windowStart: number;
    windowEnd: number;
    savedAt: string;
    ptbCrossings?: number;
    rangeTop?: number;
    rangeBottom?: number;
    uniqueTraders?: number;
    newWallets?: number;
    windowOutcome?: WindowOutcome;
    minAssetPrice?: number;
    maxAssetPrice?: number;
    assetRange?: number;
    prevCloseAsset?: number;
    assetPrice?: number;
    ptbHistory?: PtbHistoryEntry[];
    gammaPtb?: number;
  },
): Promise<void> {
  const mongo = await getMongoClient();
  const _id = `${series}:${window.windowStart}`;
  const $set: MongoRecordedWindowDoc = {
    series,
    windowStart: window.windowStart,
    windowEnd: window.windowEnd,
    savedAt: window.savedAt,
  };
  if (window.ptbCrossings != null) $set.ptbCrossings = window.ptbCrossings;
  if (window.rangeTop != null) $set.rangeTop = window.rangeTop;
  if (window.rangeBottom != null) $set.rangeBottom = window.rangeBottom;
  if (window.uniqueTraders != null) $set.uniqueTraders = window.uniqueTraders;
  if (window.newWallets != null) $set.newWallets = window.newWallets;
  if (window.minAssetPrice != null && Number.isFinite(window.minAssetPrice)) {
    $set.minAssetPrice = window.minAssetPrice;
  }
  if (window.maxAssetPrice != null && Number.isFinite(window.maxAssetPrice)) {
    $set.maxAssetPrice = window.maxAssetPrice;
  }
  if (window.assetRange != null && Number.isFinite(window.assetRange)) {
    $set.assetRange = window.assetRange;
  }
  if (window.prevCloseAsset != null && Number.isFinite(window.prevCloseAsset)) {
    $set.prevCloseAsset = window.prevCloseAsset;
  }
  if (window.assetPrice != null && Number.isFinite(window.assetPrice)) {
    $set.assetPrice = window.assetPrice;
  }
  const ptbHistory = encodePtbHistory(window.ptbHistory);
  if (ptbHistory) $set.ptbHistory = ptbHistory;
  if (window.gammaPtb != null && Number.isFinite(window.gammaPtb)) {
    $set.gammaPtb = window.gammaPtb;
  }
  if (window.windowOutcome === "up" || window.windowOutcome === "down") {
    $set.windowOutcome = window.windowOutcome;
  }

  // Prefer flat fields; clear legacy nested outcome so reads cannot diverge.
  const update: { $set: MongoRecordedWindowDoc; $unset?: Record<string, ""> } = { $set };
  if (window.windowOutcome === "up" || window.windowOutcome === "down") {
    update.$unset = { "window.windowOutcome": "" };
  }

  await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .updateOne({ _id }, update, { upsert: true });
}

/** Delete one Mongo recorded_windows summary. */
export async function deleteRecordedWindowSummary(
  series: string,
  windowStart: number,
): Promise<void> {
  const mongo = await getMongoClient();
  const _id = `${series}:${windowStart}`;
  await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .deleteOne({ _id });
}

/** Delete Mongo recorded_windows summaries older than cutoff (optionally one series). */
export async function deleteRecordedWindowsBefore(
  cutoffUtc: number,
  series?: string,
): Promise<number> {
  const mongo = await getMongoClient();
  const filter: { windowStart: { $lt: number }; series?: string } = {
    windowStart: { $lt: cutoffUtc },
  };
  if (series) filter.series = series;
  const result = await mongo
    .db(getMongoDbName())
    .collection(COLLECTION)
    .deleteMany(filter);
  return result.deletedCount ?? 0;
}

/**
 * Fetch rolling-window summaries for Replay slot counts.
 * Projects only summary fields — never ticks.
 */
export async function listRecordedWindowsSince(
  cutoffUtc: number,
  series?: string,
): Promise<RecordedWindowSummary[]> {
  const mongo = await getMongoClient();
  const filter: { windowStart: { $gte: number }; series?: string } = {
    windowStart: { $gte: cutoffUtc },
  };
  if (series) filter.series = series;
  const docs = await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .find(filter, { projection: WINDOW_SUMMARY_PROJECTION })
    .sort({ windowStart: 1 })
    .batchSize(5_000)
    .toArray();

  const out: RecordedWindowSummary[] = [];
  for (const doc of docs) {
    const normalized = normalizeDoc(doc);
    if (!normalized) continue;
    // Legacy docs may lack `series` on the filter field — keep series-wide scans intact.
    if (series && normalized.series !== series) continue;
    out.push(normalized);
  }
  return out;
}

/** One window summary from Mongo. */
export async function getRecordedWindowSummary(
  series: string,
  windowStart: number,
): Promise<RecordedWindowSummary | null> {
  const ser = String(series || "").trim();
  const ws = Math.floor(Number(windowStart));
  if (!ser || !Number.isFinite(ws) || ws <= 0) return null;
  const mongo = await getMongoClient();
  const doc = await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .findOne({ _id: `${ser}:${ws}` }, { projection: WINDOW_SUMMARY_PROJECTION });
  if (!doc) return null;
  const normalized = normalizeDoc(doc);
  if (!normalized || normalized.series !== ser) return null;
  return normalized;
}
