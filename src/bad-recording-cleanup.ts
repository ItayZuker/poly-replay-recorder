import fs from "fs/promises";
import { listMarkets } from "./db/market-repository.js";
import { windowTicksDir } from "./db/data-dir.js";
import {
  deleteRecordedWindowFile,
  listRecordedWindows,
} from "./db/recorded-window-repository.js";
import { logService } from "./log-service.js";
import { isFlatPriceWindow } from "./window-dynamics.js";
import type { MarketDocument } from "./types.js";

/** Remove a bad window from Mongo and ticks. Local `windows/*.json` is left in place. */
export async function discardBadRecording(
  series: string,
  windowStart: number,
  reason: string,
): Promise<void> {
  await deleteRecordedWindowFile(series, windowStart).catch(() => undefined);
  const ticksDir = windowTicksDir(series, windowStart);
  await fs.rm(ticksDir, { recursive: true, force: true }).catch(() => undefined);
  logService.warn(
    "recorder",
    `Discarded bad recording ${series} @ ${windowStart}: ${reason}`,
  );
}

/** Scan Mongo history and delete flat-price windows. */
export async function purgeFlatPriceRecordings(
  markets?: MarketDocument[],
): Promise<number> {
  const list = markets ?? (await listMarkets());
  let removed = 0;

  for (const market of list) {
    const windows = await listRecordedWindows(market).catch(() => []);
    for (const window of windows) {
      if (!isFlatPriceWindow(window)) continue;
      await discardBadRecording(market._id, window.windowStart, "flat asset price");
      removed += 1;
    }
  }

  if (removed > 0) {
    logService.info("recorder", `Purged ${removed} flat-price recording(s)`);
  }
  return removed;
}
