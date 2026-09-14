import {
  dayHourFromWindowStart,
  getWeekHistoryCutoffUtcSec,
  selectLatestDayHourWindows,
} from "./day-hour-slots.js";
import { getMarket } from "./db/market-repository.js";
import {
  classifyWindowChips,
  listTickWindowStarts,
  type WindowChipState,
} from "./db/tick-repository.js";
import { recordingManager } from "./recording-manager.js";

export const WEEK_DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;
export type WeekDayId = (typeof WEEK_DAYS)[number];

export interface HourSlotCoverage {
  day: WeekDayId;
  hour: number;
  recorded: number;
  expected: number;
  /** Index 0 is :00 in the hour; length is 12 (5m) or 4 (15m). */
  windows: WindowChipState[];
}

export interface WeekCoverage {
  series: string;
  timeframeMinutes: 5 | 15;
  expectedPerHour: number;
  weekStart: number;
  slots: HourSlotCoverage[];
  /** Current window is receiving both raw CLOB and Chainlink. */
  liveBothSockets: boolean;
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

function windowStatesForHourStart(
  byStart: Map<number, WindowChipState> | Set<number> | undefined,
  expected: number,
  winSec: number,
  hourStart: number,
): WindowChipState[] {
  return Array.from({ length: expected }, (_, i) => {
    const start = hourStart + i * winSec;
    if (byStart instanceof Map) return byStart.get(start) ?? "missing";
    if (byStart instanceof Set) return byStart.has(start) ? "recorded" : "missing";
    return "missing";
  });
}

function priorRecordedBySlot(recordedStarts: number[]): Map<string, Set<number>> {
  const prior = selectLatestDayHourWindows(
    recordedStarts.map((windowStart) => ({ windowStart })),
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
  return priorBySlot;
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

  const tickStarts = (await listTickWindowStarts(market._id)).filter(
    (windowStart) => windowStart >= cutoff,
  );
  const chipByStart = await classifyWindowChips(market, tickStarts, nowSec);
  const recordedStarts = [...chipByStart.entries()]
    .filter(([, state]) => state === "recorded")
    .map(([windowStart]) => windowStart);
  const priorBySlot = priorRecordedBySlot(
    recordedStarts.filter((windowStart) => windowStart < weekStart),
  );

  const slots: HourSlotCoverage[] = [];
  for (let dayIndex = 0; dayIndex < WEEK_DAYS.length; dayIndex += 1) {
    const day = WEEK_DAYS[dayIndex];
    for (let hour = 0; hour < 24; hour += 1) {
      const thisWeekHour = weekStart + dayIndex * 86_400 + hour * 3_600;
      let windows: WindowChipState[];
      if (nowSec >= thisWeekHour) {
        windows = windowStatesForHourStart(chipByStart, expected, winSec, thisWeekHour);
      } else {
        const priorStarts = priorBySlot.get(`${day}:${hour}`);
        const first = priorStarts?.values().next().value;
        const hourStart =
          first != null
            ? utcHourStartSec(dayHourFromWindowStart(first).dayKey, hour)
            : thisWeekHour;
        windows = windowStatesForHourStart(priorStarts, expected, winSec, hourStart);
      }
      slots.push({
        day,
        hour,
        recorded: windows.filter((state) => state === "recorded").length,
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
    liveBothSockets: recordingManager.getRecorder(market._id)?.isLiveBothSockets() === true,
  };
}
