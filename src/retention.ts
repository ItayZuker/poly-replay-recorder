/**
 * Tick/window data kept on disk + Mongo summaries.
 * Default ~7 days so previous weekday hours remain until the same hour is re-recorded.
 * Per-market override via MarketDocument.retentionDays.
 */
export const HOT_RETENTION_DAYS = 7;
export const MIN_RETENTION_DAYS = 1;
export const MAX_RETENTION_DAYS = 365;

const SECONDS_PER_DAY = 86_400;

export function clampRetentionDays(raw: unknown): number {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n)) return HOT_RETENTION_DAYS;
  return Math.max(MIN_RETENTION_DAYS, Math.min(MAX_RETENTION_DAYS, n));
}

export function hotCutoffSec(
  nowSec = Math.floor(Date.now() / 1000),
  retentionDays: number = HOT_RETENTION_DAYS,
): number {
  const days = clampRetentionDays(retentionDays);
  return nowSec - days * SECONDS_PER_DAY;
}

/** UTC calendar day for a window start (YYYY-MM-DD). */
export function utcDayKey(windowStartSec: number): string {
  const date = new Date(windowStartSec * 1000);
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
