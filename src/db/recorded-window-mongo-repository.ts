import type { PtbHistoryEntry, RecordedWindowDocument, WindowOutcome } from "../types.js";
import { decodePtbHistory, encodePtbHistory } from "../ptb-history.js";
import { getMongoClient, getMongoDbName } from "./mongo-client.js";

const COLLECTION = "recorded_windows";

/** Window summary in Mongo — dest Replay reads the original slim fields; extras are ignored. */
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
  knownWallets?: number;
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
  ptbChainlink?: number;
  ptbTwap30?: number;
  ptbTwap60?: number;
  slug?: string;
  question?: string;
  conditionId?: string;
  yesPrice?: number;
  noPrice?: number;
  assetGap?: number;
  tickCount?: number;
  clobRawCount?: number;
  clobBookCount?: number;
  chainlinkCount?: number;
}

export type RecordedWindowWrite = Omit<RecordedWindowDocument, "_id" | "updatedAt"> & {
  updatedAt?: string;
};

type MongoRecordedWindowDoc = {
  _id?: string | undefined;
  series?: string;
  marketSeries?: string;
  windowStart?: number;
  windowEnd?: number;
  savedAt?: string | Date;
  updatedAt?: string | Date;
  ptbCrossings?: number;
  rangeTop?: number;
  rangeBottom?: number;
  uniqueTraders?: number;
  newWallets?: number;
  knownWallets?: number;
  windowOutcome?: WindowOutcome | null;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  prevCloseAsset?: number;
  assetPrice?: number;
  ptbHistory?: unknown;
  gammaPtb?: number;
  ptbChainlink?: number;
  ptbTwap30?: number;
  ptbTwap60?: number;
  slug?: string;
  question?: string;
  conditionId?: string;
  yesPrice?: number;
  noPrice?: number;
  assetGap?: number;
  tickCount?: number;
  clobRawCount?: number;
  clobBookCount?: number;
  chainlinkCount?: number;
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
  ptbChainlink: 1,
  ptbTwap30: 1,
  ptbTwap60: 1,
  slug: 1,
  question: 1,
  conditionId: 1,
  yesPrice: 1,
  noPrice: 1,
  assetGap: 1,
  tickCount: 1,
  clobRawCount: 1,
  clobBookCount: 1,
  chainlinkCount: 1,
  knownWallets: 1,
  updatedAt: 1,
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
  if (doc.ptbChainlink != null && Number.isFinite(doc.ptbChainlink)) {
    out.ptbChainlink = doc.ptbChainlink;
  }
  if (doc.ptbTwap30 != null && Number.isFinite(doc.ptbTwap30)) out.ptbTwap30 = doc.ptbTwap30;
  if (doc.ptbTwap60 != null && Number.isFinite(doc.ptbTwap60)) out.ptbTwap60 = doc.ptbTwap60;
  if (windowOutcome === "up" || windowOutcome === "down") out.windowOutcome = windowOutcome;
  if (typeof doc.slug === "string" && doc.slug.trim()) out.slug = doc.slug.trim();
  if (typeof doc.question === "string" && doc.question.trim()) out.question = doc.question.trim();
  if (typeof doc.conditionId === "string" && doc.conditionId.trim()) {
    out.conditionId = doc.conditionId.trim();
  }
  if (doc.yesPrice != null && Number.isFinite(doc.yesPrice)) out.yesPrice = doc.yesPrice;
  if (doc.noPrice != null && Number.isFinite(doc.noPrice)) out.noPrice = doc.noPrice;
  if (doc.assetGap != null && Number.isFinite(doc.assetGap)) out.assetGap = doc.assetGap;
  if (doc.tickCount != null && Number.isFinite(doc.tickCount)) out.tickCount = doc.tickCount;
  if (doc.clobRawCount != null && Number.isFinite(doc.clobRawCount)) out.clobRawCount = doc.clobRawCount;
  if (doc.clobBookCount != null && Number.isFinite(doc.clobBookCount)) out.clobBookCount = doc.clobBookCount;
  if (doc.chainlinkCount != null && Number.isFinite(doc.chainlinkCount)) {
    out.chainlinkCount = doc.chainlinkCount;
  }
  if (doc.knownWallets != null && Number.isFinite(doc.knownWallets)) out.knownWallets = doc.knownWallets;

  return out;
}

export function summaryToRecordedWindow(summary: RecordedWindowSummary): RecordedWindowDocument {
  return {
    _id: `${summary.series}:${summary.windowStart}`,
    windowStart: summary.windowStart,
    windowEnd: summary.windowEnd,
    savedAt: summary.savedAt,
    updatedAt: summary.savedAt,
    slug: summary.slug,
    question: summary.question,
    conditionId: summary.conditionId,
    assetPrice: summary.assetPrice,
    prevCloseAsset: summary.prevCloseAsset,
    ptbHistory: summary.ptbHistory,
    gammaPtb: summary.gammaPtb,
    ptbChainlink: summary.ptbChainlink,
    ptbTwap30: summary.ptbTwap30,
    ptbTwap60: summary.ptbTwap60,
    assetGap: summary.assetGap,
    windowOutcome: summary.windowOutcome,
    yesPrice: summary.yesPrice,
    noPrice: summary.noPrice,
    ptbCrossings: summary.ptbCrossings,
    minAssetPrice: summary.minAssetPrice,
    maxAssetPrice: summary.maxAssetPrice,
    assetRange: summary.assetRange,
    rangeTop: summary.rangeTop,
    rangeBottom: summary.rangeBottom,
    uniqueTraders: summary.uniqueTraders,
    newWallets: summary.newWallets,
    knownWallets: summary.knownWallets,
    tickCount: summary.tickCount ?? 0,
    clobRawCount: summary.clobRawCount,
    clobBookCount: summary.clobBookCount,
    chainlinkCount: summary.chainlinkCount,
  };
}

function setNum(target: MongoRecordedWindowDoc, key: keyof MongoRecordedWindowDoc, value: unknown): void {
  const n = Number(value);
  if (Number.isFinite(n)) (target as Record<string, unknown>)[key as string] = n;
}

function setStr(target: MongoRecordedWindowDoc, key: keyof MongoRecordedWindowDoc, value: unknown): void {
  if (typeof value === "string" && value.trim()) {
    (target as Record<string, unknown>)[key as string] = value.trim();
  }
}

function buildWindowSet(
  series: string,
  window: RecordedWindowWrite,
): { $set: MongoRecordedWindowDoc; $unset?: Record<string, ""> } {
  const $set: MongoRecordedWindowDoc = {
    series,
    windowStart: window.windowStart,
    windowEnd: window.windowEnd,
    savedAt: window.savedAt,
  };
  if (window.updatedAt) $set.updatedAt = window.updatedAt;
  setStr($set, "slug", window.slug);
  setStr($set, "question", window.question);
  setStr($set, "conditionId", window.conditionId);
  setNum($set, "ptbCrossings", window.ptbCrossings);
  setNum($set, "rangeTop", window.rangeTop);
  setNum($set, "rangeBottom", window.rangeBottom);
  setNum($set, "uniqueTraders", window.uniqueTraders);
  setNum($set, "newWallets", window.newWallets);
  setNum($set, "knownWallets", window.knownWallets);
  setNum($set, "minAssetPrice", window.minAssetPrice);
  setNum($set, "maxAssetPrice", window.maxAssetPrice);
  setNum($set, "assetRange", window.assetRange);
  setNum($set, "prevCloseAsset", window.prevCloseAsset);
  setNum($set, "assetPrice", window.assetPrice);
  setNum($set, "yesPrice", window.yesPrice);
  setNum($set, "noPrice", window.noPrice);
  setNum($set, "assetGap", window.assetGap);
  setNum($set, "tickCount", window.tickCount);
  setNum($set, "clobRawCount", window.clobRawCount);
  setNum($set, "clobBookCount", window.clobBookCount);
  setNum($set, "chainlinkCount", window.chainlinkCount);
  const ptbHistory = encodePtbHistory(window.ptbHistory);
  if (ptbHistory) $set.ptbHistory = ptbHistory;
  if (window.gammaPtb != null && Number.isFinite(window.gammaPtb)) {
    $set.gammaPtb = window.gammaPtb;
  }
  setNum($set, "ptbChainlink", window.ptbChainlink);
  setNum($set, "ptbTwap30", window.ptbTwap30);
  setNum($set, "ptbTwap60", window.ptbTwap60);
  if (window.windowOutcome === "up" || window.windowOutcome === "down") {
    $set.windowOutcome = window.windowOutcome;
  }

  // Prefer flat fields; clear legacy nested outcome so reads cannot diverge.
  const update: { $set: MongoRecordedWindowDoc; $unset?: Record<string, ""> } = { $set };
  if (window.windowOutcome === "up" || window.windowOutcome === "down") {
    update.$unset = { "window.windowOutcome": "" };
  }
  return update;
}

/** Upsert one window header (full local-JSON field set; dest ignores unknown keys). */
export async function upsertRecordedWindowSummary(
  series: string,
  window: RecordedWindowWrite,
): Promise<void> {
  const mongo = await getMongoClient();
  const _id = `${series}:${window.windowStart}`;
  await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .updateOne({ _id }, buildWindowSet(series, window), { upsert: true });
}

export async function upsertRecordedWindowSummaries(
  series: string,
  windows: RecordedWindowWrite[],
): Promise<number> {
  if (windows.length === 0) return 0;
  const mongo = await getMongoClient();
  const ops = windows.map((window) => ({
    updateOne: {
      filter: { _id: `${series}:${window.windowStart}` },
      update: buildWindowSet(series, window),
      upsert: true,
    },
  }));
  const result = await mongo
    .db(getMongoDbName())
    .collection<MongoRecordedWindowDoc>(COLLECTION)
    .bulkWrite(ops, { ordered: false });
  return (result.upsertedCount ?? 0) + (result.modifiedCount ?? 0) + (result.matchedCount ?? 0);
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

export async function listRecordedWindowStarts(
  series: string,
  cutoffUtc = 0,
): Promise<number[]> {
  const windows = await listRecordedWindowsSince(cutoffUtc, series);
  return windows.map((window) => window.windowStart);
}
