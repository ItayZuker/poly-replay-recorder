import { pruneColdMarketData } from "./db/tick-archive.js";
import { listMarkets } from "./db/market-repository.js";
import { logService } from "./log-service.js";
import { HOT_RETENTION_DAYS } from "./retention.js";

const RETENTION_INTERVAL_MS = 60 * 60 * 1000;

let retentionTimer: ReturnType<typeof setInterval> | null = null;
let retentionInFlight = false;

export async function runRetentionForAllMarkets(): Promise<void> {
  if (retentionInFlight) return;
  retentionInFlight = true;
  try {
    const markets = await listMarkets();
    for (const market of markets) {
      try {
        await pruneColdMarketData(market);
      } catch (err) {
        logService.error("retention", `Failed for ${market._id}: ${String(err)}`);
      }
    }
  } finally {
    retentionInFlight = false;
  }
}

export function startArchiveScheduler(): void {
  if (retentionTimer) return;
  logService.info(
    "retention",
    `Scheduler started (delete tick/window data older than ${HOT_RETENTION_DAYS} days)`,
  );
  void runRetentionForAllMarkets();
  retentionTimer = setInterval(() => {
    void runRetentionForAllMarkets();
  }, RETENTION_INTERVAL_MS);
}

export function stopArchiveScheduler(): void {
  if (retentionTimer) {
    clearInterval(retentionTimer);
    retentionTimer = null;
  }
}
