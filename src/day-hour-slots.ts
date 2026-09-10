/**
 * Week grid slots are UTC weekday × hour.
 * Replay uses the last occurrence of each weekday×hour (this week once that
 * hour arrives; otherwise last week). A missing/unusable last occurrence is
 * No Recordings — it does not fall back to an older good week.
 */

export type WeekDayId = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";

const UTC_DAY_TO_ID: WeekDayId[] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/** Floor for loading Replay history (~2 weeks of weekday hours). */
export function getWeekHistoryCutoffUtcSec(now = new Date()): number {
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  return Math.floor(Date.UTC(y, m, d - 13) / 1000);
}

export function utcDayKeyFromWindowStart(windowStart: number): string {
  return new Date(windowStart * 1000).toISOString().slice(0, 10);
}

export function dayHourFromWindowStart(windowStart: number): {
  day: WeekDayId;
  hour: number;
  slotKey: string;
  dayKey: string;
} {
  const date = new Date(windowStart * 1000);
  const day = UTC_DAY_TO_ID[date.getUTCDay()] ?? "sun";
  const hour = date.getUTCHours();
  return {
    day,
    hour,
    slotKey: `${day}:${hour}`,
    dayKey: date.toISOString().slice(0, 10),
  };
}

/**
 * For each UTC weekday×hour slot, keep windows from the latest calendar day
 * that has data in that slot (newer week overrides only that hour).
 * Pass `eligible` so stubs / unfinished days cannot replace last week's usable day.
 */
export function selectLatestDayHourWindows<T extends { windowStart: number }>(
  windows: T[],
  eligible?: (window: T) => boolean,
): T[] {
  const list = eligible ? windows.filter(eligible) : windows;
  if (list.length === 0) return [];

  const latestDayBySlot = new Map<string, string>();
  for (const window of list) {
    const { slotKey, dayKey } = dayHourFromWindowStart(window.windowStart);
    const prev = latestDayBySlot.get(slotKey);
    if (!prev || dayKey > prev) latestDayBySlot.set(slotKey, dayKey);
  }

  return list.filter((window) => {
    const { slotKey, dayKey } = dayHourFromWindowStart(window.windowStart);
    return latestDayBySlot.get(slotKey) === dayKey;
  });
}
