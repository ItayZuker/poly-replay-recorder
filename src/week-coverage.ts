import {
  dayHourFromWindowStart,
  getWeekHistoryCutoffUtcSec,
  selectLatestDayHourWindows,
} from "./day-hour-slots.js";
import { getMarket } from "./db/market-repository.js";
import { listRecordedWindowStarts } from "./db/recorded-window-repository.js";

export const WEEK_DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;
export type WeekDayId = (typeof WEEK_DAYS)[number];

export interface HourSlotCoverage {
  day: WeekDayId;
  hour: number;
  recorded: number;
  expected: number;
  /** Index 0 is :00 in the hour; length is 12 (5m) or 4 (15m). */
  windows: boolean[];
}

export interface WeekCoverage {
  series: string;
  timeframeMinutes: 5 | 15;
  expectedPerHour: number;
  weekStart: number;
  slots: HourSlotCoverage[];
}

/** Monday 00:00 UTC of the current UTC week, unix seconds. */
export function utcWeekMondaySec(nowMs = Date.now()): number {
  const d = new Date(nowMs);
  const fromMon = (d.getUTCDay() + 6) % 7;
  return Math.floor(
    Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - fromMon) / 1000,
  );
}

export function expectedWindowsPerHour(timeframeMinutes: number): 4 | 12 {
  return timeframeMinutes === 15 ? 4 : 12;
}

function utcHourStartSec(dayKey: string, hour: number): number {
  return Math.floor(
    Date.parse(`${dayKey}T${String(hour).padStart(2, "0")}:00:00.000Z`) / 1000,
  );
}

function windowFlagsForSlot(
  starts: Set<number> | undefined,
  expected: number,
  winSec: number,
): boolean[] {
  const flags = Array.from({ length: expected }, () => false);
  if (!starts || starts.size === 0) return flags;
  const first = starts.values().next().value;
  if (first == null) return flags;
  const { dayKey, hour } = dayHourFromWindowStart(first);
  const hourStart = utcHourStartSec(dayKey, hour);
  for (let i = 0; i < expected; i += 1) {
    flags[i] = starts.has(hourStart + i * winSec);
  }
  return flags;
}

/** This week's hour uses this week's files only once that hour starts; earlier hours stay last week. */
export async function getWeekCoverage(series: string): Promise<WeekCoverage> {
  const market = await getMarket(series);
  if (!market) {
    throw new Error(`Unknown series: ${series}`);
  }
  const timeframeMinutes: 5 | 15 = market.timeframeMinutes === 15 ? 15 : 5;
  const expected = expectedWindowsPerHour(timeframeMinutes);
  const winSec = timeframeMinutes * 60;
  const weekStart = utcWeekMondaySec();
  const nowSec = Math.floor(Date.now() / 1000);
  const cutoff = getWeekHistoryCutoffUtcSec();

  const present = new Set(
    (await listRecordedWindowStarts(market._id)).filter(
      (windowStart) => windowStart >= cutoff && windowStart + winSec <= nowSec,
    ),
  );
  const prior = selectLatestDayHourWindows(
    [...present]
      .filter((windowStart) => windowStart < weekStart)
      .map((windowStart) => ({ windowStart })),
  );
  const priorBySlot = new Map<string, Set<number>>();
  for (const { windowStart } of prior) {
    const { slotKey } = dayHourFromWindowStart(windowStart);
    let set = priorBySlot.get(slotKey);
    if (!set) {
      set = new Set();
      priorBySlot.set(slotKey, set);
    }
    set.add(windowStart);
  }

  const slots: HourSlotCoverage[] = [];
  for (let dayIndex = 0; dayIndex < WEEK_DAYS.length; dayIndex += 1) {
    const day = WEEK_DAYS[dayIndex];
    for (let hour = 0; hour < 24; hour += 1) {
      const thisWeekHour = weekStart + dayIndex * 86_400 + hour * 3_600;
      const windows =
        nowSec >= thisWeekHour
          ? Array.from({ length: expected }, (_, i) => present.has(thisWeekHour + i * winSec))
          : windowFlagsForSlot(priorBySlot.get(`${day}:${hour}`), expected, winSec);
      slots.push({
        day,
        hour,
        recorded: windows.filter(Boolean).length,
        expected,
        windows,
      });
    }
  }

  return {
    series: market._id,
    timeframeMinutes,
    expectedPerHour: expected,
    weekStart,
    slots,
  };
}
