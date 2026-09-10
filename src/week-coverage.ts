import { getMarket } from "./db/market-repository.js";
import { windowsHavingReplayTickFiles } from "./db/tick-repository.js";

export const WEEK_DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;
export type WeekDayId = (typeof WEEK_DAYS)[number];

export interface HourSlotCoverage {
  day: WeekDayId;
  hour: number;
  recorded: number;
  expected: number;
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

export async function getWeekCoverage(series: string): Promise<WeekCoverage> {
  const market = await getMarket(series);
  if (!market) {
    throw new Error(`Unknown series: ${series}`);
  }
  const timeframeMinutes: 5 | 15 = market.timeframeMinutes === 15 ? 15 : 5;
  const expected = expectedWindowsPerHour(timeframeMinutes);
  const winSec = timeframeMinutes * 60;
  const weekStart = utcWeekMondaySec();

  const starts: number[] = [];
  for (let day = 0; day < 7; day += 1) {
    for (let hour = 0; hour < 24; hour += 1) {
      const slot = weekStart + day * 86400 + hour * 3600;
      for (let i = 0; i < expected; i += 1) {
        starts.push(slot + i * winSec);
      }
    }
  }

  const present = new Set(await windowsHavingReplayTickFiles(market, starts));
  const slots: HourSlotCoverage[] = [];
  for (let day = 0; day < 7; day += 1) {
    for (let hour = 0; hour < 24; hour += 1) {
      const slot = weekStart + day * 86400 + hour * 3600;
      let recorded = 0;
      for (let i = 0; i < expected; i += 1) {
        if (present.has(slot + i * winSec)) recorded += 1;
      }
      slots.push({
        day: WEEK_DAYS[day],
        hour,
        recorded,
        expected,
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
