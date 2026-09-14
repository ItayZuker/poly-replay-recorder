import { SEED_MARKETS } from "../collections.js";
import { logService } from "../log-service.js";
import type { MarketDocument } from "../types.js";
import { listLocalRecordedWindows } from "./recorded-window-repository.js";
import { upsertRecordedWindowSummaries } from "./recorded-window-mongo-repository.js";

/** Copy local `windows/*.json` headers into Mongo `recorded_windows` (full field set). */
export async function backfillLocalWindowsToMongo(): Promise<number> {
  let upserted = 0;
  for (const seed of SEED_MARKETS) {
    const market = { _id: seed.series } as MarketDocument;
    const local = await listLocalRecordedWindows(market).catch(() => []);
    if (local.length === 0) continue;
    await upsertRecordedWindowSummaries(
      market._id,
      local.map((window) => ({
        ...window,
        updatedAt: window.updatedAt || new Date().toISOString(),
      })),
    );
    upserted += local.length;
    logService.info("recorder", `Backfilled ${local.length} window header(s) to Mongo (${market._id})`);
  }
  return upserted;
}
