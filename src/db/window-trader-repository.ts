import { getMongoClient, getMongoDbName } from "./mongo-client.js";

const COLLECTION = "window_traders";

export interface WindowTradersDocument {
  _id: string;
  series: string;
  windowStart: number;
  addresses: string[];
  updatedAt: string;
}

async function collection() {
  const mongo = await getMongoClient();
  return mongo.db(getMongoDbName()).collection<WindowTradersDocument>(COLLECTION);
}

function docId(series: string, windowStart: number): string {
  return `${series}:${windowStart}`;
}

function normalizeAddress(address: string): string {
  return address.trim().toLowerCase();
}

export async function ensureWindowTraderIndexes(): Promise<void> {
  const col = await collection();
  await col.createIndex({ series: 1, windowStart: 1 });
}

/** Replace the trader address list for one market window. */
export async function replaceWindowTraders(
  series: string,
  windowStart: number,
  addresses: string[],
): Promise<void> {
  const marketSeries = String(series ?? "").trim();
  const start = Math.floor(Number(windowStart));
  if (!marketSeries || !Number.isFinite(start) || start <= 0) return;

  const unique = [
    ...new Set(addresses.map(normalizeAddress).filter((a) => a.startsWith("0x"))),
  ];
  const col = await collection();
  await col.updateOne(
    { _id: docId(marketSeries, start) },
    {
      $set: {
        series: marketSeries,
        windowStart: start,
        addresses: unique,
        updatedAt: new Date().toISOString(),
      },
    },
    { upsert: true },
  );
}

/** Map windowStart → addresses for the given series windows. */
export async function listWindowTradersByStarts(
  series: string,
  windowStarts: number[],
): Promise<Map<number, string[]>> {
  const marketSeries = String(series ?? "").trim();
  const starts = [
    ...new Set(
      windowStarts
        .map((n) => Math.floor(Number(n)))
        .filter((n) => Number.isFinite(n) && n > 0),
    ),
  ];
  const out = new Map<number, string[]>();
  if (!marketSeries || starts.length === 0) return out;

  const col = await collection();
  const docs = await col
    .find({ series: marketSeries, windowStart: { $in: starts } })
    .project({ windowStart: 1, addresses: 1 })
    .toArray();
  for (const doc of docs) {
    const start = Number(doc.windowStart);
    if (!Number.isFinite(start)) continue;
    out.set(
      start,
      Array.isArray(doc.addresses) ? doc.addresses.map(normalizeAddress) : [],
    );
  }
  return out;
}

export async function deleteWindowTradersBefore(
  cutoffSec: number,
  series?: string,
): Promise<number> {
  const cutoff = Math.floor(Number(cutoffSec));
  if (!Number.isFinite(cutoff) || cutoff <= 0) return 0;
  const col = await collection();
  const filter: { windowStart: { $lt: number }; series?: string } = {
    windowStart: { $lt: cutoff },
  };
  if (series) filter.series = series;
  const result = await col.deleteMany(filter);
  return result.deletedCount ?? 0;
}
