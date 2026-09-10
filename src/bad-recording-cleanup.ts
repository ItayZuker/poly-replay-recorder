import fs from "fs/promises";
import { listMarkets } from "./db/market-repository.js";
import { windowTicksDir } from "./db/data-dir.js";
import {
  deleteRecordedWindowSummary,
  listRecordedWindowsSince,
} from "./db/recorded-window-mongo-repository.js";
import {
  deleteRecordedWindowFile,
  listRecordedWindows,
} from "./db/recorded-window-repository.js";
import { getWeekHistoryCutoffUtcSec } from "./day-hour-slots.js";
import { logService } from "./log-service.js";
import { isFlatPriceWindow } from "./window-dynamics.js";
import type { MarketDocument } from "./types.js";

/** Remove a bad window from Mongo, local files, and ticks. */
export async function discardBadRecording(
  series: string,
  windowStart: number,
  reason: string,
): Promise<void> {
  await deleteRecordedWindowSummary(series, windowStart).catch(() => undefined);
  await deleteRecordedWindowFile(series, windowStart).catch(() => undefined);
  const ticksDir = windowTicksDir(series, windowStart);
  await fs.rm(ticksDir, { recursive: true, force: true }).catch(() => undefined);
  logService.warn(
    "recorder",
    `Discarded bad recording ${series} @ ${windowStart}: ${reason}`,
  );
}

/** Scan local + Mongo history and delete flat-price windows. */
export async function purgeFlatPriceRecordings(
  markets?: MarketDocument[],
): Promise<number> {
  const list = markets ?? (await listMarkets());
  const cutoffUtc = getWeekHistoryCutoffUtcSec();
  let removed = 0;
  const seen = new Set<string>();

  for (const market of list) {
    const series = market._id;
    const localWindows = await listRecordedWindows(market).catch(() => []);
    for (const window of localWindows) {
      if (!isFlatPriceWindow(window)) continue;
      const key = `${series}:${window.windowStart}`;
      if (seen.has(key)) continue;
      seen.add(key);
      await discardBadRecording(series, window.windowStart, "flat asset price (local)");
      removed += 1;
    }

    const mongoWindows = await listRecordedWindowsSince(cutoffUtc, series).catch(() => []);
    for (const window of mongoWindows) {
      if (!isFlatPriceWindow(window)) continue;
      const key = `${series}:${window.windowStart}`;
      if (seen.has(key)) continue;
      seen.add(key);
      await discardBadRecording(series, window.windowStart, "flat asset price (mongo)");
      removed += 1;
    }
  }

  if (removed > 0) {
    logService.info("recorder", `Purged ${removed} flat-price recording(s)`);
  }
  return removed;
}
