import { SEED_MARKETS } from "../collections.js";
import { clampRetentionDays, HOT_RETENTION_DAYS } from "../retention.js";
import type { MarketDocument } from "../types.js";
import {
  ensureMarketDirs,
  getDataDir,
  initStorage,
  marketsFilePath,
} from "./data-dir.js";
import { readJsonFile } from "./file-store.js";
import { getMongoClient, getMongoDbName } from "./mongo-client.js";

const COLLECTION = "markets";
const SEED_SERIES_IDS = SEED_MARKETS.map((m) => m.series);

type MarketsFile = Record<string, MarketDocument>;

export type MarketAdminPatch = Partial<
  Pick<MarketDocument, "available" | "recordingEnabled" | "retentionDays">
>;

async function collection() {
  const mongo = await getMongoClient();
  return mongo.db(getMongoDbName()).collection<MarketDocument>(COLLECTION);
}

function normalizeMarket(doc: MarketDocument): MarketDocument {
  return {
    ...doc,
    available: doc.available !== false,
    recordingEnabled: doc.recordingEnabled === true,
    retentionDays: clampRetentionDays(
      doc.retentionDays != null ? doc.retentionDays : HOT_RETENTION_DAYS,
    ),
  };
}

export async function ensureMarketIndexes(): Promise<void> {
  const col = await collection();
  await col.createIndex({ timeframeMinutes: 1 });
}

/** One-time import from legacy data/markets.json when Mongo is empty. */
async function migrateMarketsFromDiskIfNeeded(): Promise<void> {
  const col = await collection();
  const existing = await col.estimatedDocumentCount();
  if (existing > 0) return;

  const disk = await readJsonFile<MarketsFile>(marketsFilePath());
  if (!disk || Object.keys(disk).length === 0) return;

  const docs = Object.values(disk).filter((m) => m?._id);
  if (docs.length === 0) return;

  await col.insertMany(docs, { ordered: false });
}

/** Backfill available / retentionDays on existing seed markets. */
async function migrateMarketAdminFields(): Promise<void> {
  const col = await collection();
  const now = new Date().toISOString();
  await col.updateMany(
    { available: { $exists: false } },
    { $set: { available: true, updatedAt: now } },
  );
  await col.updateMany(
    { retentionDays: { $exists: false } },
    { $set: { retentionDays: HOT_RETENTION_DAYS, updatedAt: now } },
  );
}

export async function initStorageAndSeed(): Promise<void> {
  await initStorage();
  await ensureMarketIndexes();
  await migrateMarketsFromDiskIfNeeded();
  await seedMarkets();
  await migrateMarketAdminFields();
  await ensureAllMarketDirs();
}

export async function seedMarkets(): Promise<void> {
  const col = await collection();
  const now = new Date().toISOString();

  for (const seed of SEED_MARKETS) {
    const existing = await col.findOne({ _id: seed.series });
    await col.updateOne(
      { _id: seed.series },
      {
        $set: {
          label: seed.label,
          timeframeMinutes: seed.timeframeMinutes,
          updatedAt: now,
        },
        $setOnInsert: {
          _id: seed.series,
          available: true,
          recordingEnabled: false,
          retentionDays: HOT_RETENTION_DAYS,
          createdAt: existing?.createdAt ?? now,
        },
      },
      { upsert: true },
    );
  }
}

export async function ensureAllMarketDirs(): Promise<void> {
  await Promise.all(SEED_SERIES_IDS.map((series) => ensureMarketDirs(series)));
}

export async function ensureAllMarketIndexes(): Promise<void> {
  await ensureMarketIndexes();
  await ensureAllMarketDirs();
}

export async function listMarkets(): Promise<MarketDocument[]> {
  const col = await collection();
  const docs = await col.find({ _id: { $in: [...SEED_SERIES_IDS] } }).toArray();
  const byId = new Map(docs.map((d) => [d._id, normalizeMarket(d)]));
  return SEED_SERIES_IDS.map((series) => byId.get(series)).filter(
    (market): market is MarketDocument => market != null,
  );
}

/** Markets visible/tradable in the trader app. */
export async function listAvailableMarkets(): Promise<MarketDocument[]> {
  const markets = await listMarkets();
  return markets.filter((m) => m.available);
}

export async function getMarket(series: string): Promise<MarketDocument | null> {
  const col = await collection();
  const doc = await col.findOne({ _id: series });
  return doc ? normalizeMarket(doc) : null;
}

export async function requireAvailableMarket(
  series: string,
): Promise<MarketDocument> {
  const market = await getMarket(series);
  if (!market) {
    throw new Error(`Unknown series: ${series}`);
  }
  if (!market.available) {
    throw new Error(`Market unavailable: ${series}`);
  }
  return market;
}

export async function updateMarket(
  series: string,
  patch: MarketAdminPatch,
): Promise<MarketDocument | null> {
  const col = await collection();
  const existing = await col.findOne({ _id: series });
  if (!existing) return null;

  const $set: Partial<MarketDocument> = {
    updatedAt: new Date().toISOString(),
  };
  if (typeof patch.available === "boolean") {
    $set.available = patch.available;
  }
  if (typeof patch.recordingEnabled === "boolean") {
    $set.recordingEnabled = patch.recordingEnabled;
  }
  if (patch.retentionDays !== undefined) {
    $set.retentionDays = clampRetentionDays(patch.retentionDays);
  }

  await col.updateOne({ _id: series }, { $set });
  const updated = await col.findOne({ _id: series });
  return updated ? normalizeMarket(updated) : null;
}

export { getDataDir };
