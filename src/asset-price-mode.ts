import {
  assetGapOrUnset,
  roundPolymarketAssetPrice,
  roundPolymarketAssetPriceMaybe,
} from "./polymarket-display-price.js";
import { parseMarketSeries } from "./market-pair.js";
import type { LiveWindowState } from "./types.js";

export type AssetPriceMode = "raw" | "twap30" | "twap60";
export type PtbPriceMode = "chainlink" | "twap30" | "twap60" | "chainlink-rest";

/** Default: last raw Chainlink tick on every market. */
export const DEFAULT_ASSET_PRICE_MODE: AssetPriceMode = "raw";
/** Default: first in-window Chainlink, then follow REST openPrice. */
export const DEFAULT_PTB_PRICE_MODE: PtbPriceMode = "chainlink-rest";

const modeByUser = new Map<string, AssetPriceMode>();
const ptbModeByUser = new Map<string, PtbPriceMode>();

export function normalizeAssetPriceMode(raw: unknown): AssetPriceMode {
  const value = String(raw ?? "").trim().toLowerCase();
  if (value === "twap30" || value === "30" || value === "twap_30") return "twap30";
  if (value === "twap60" || value === "60" || value === "twap_60") return "twap60";
  // Legacy Match Polymarket (series-based 30s/60s) → keep an average; 30s is global.
  if (value === "twap") return "twap30";
  return "raw";
}

export function twapLookbackSecondsForMode(mode: AssetPriceMode): 30 | 60 | null {
  if (mode === "twap30") return 30;
  if (mode === "twap60") return 60;
  return null;
}

export function rememberAssetPriceMode(userId: string, mode: AssetPriceMode): void {
  const id = String(userId || "").trim();
  if (!id) return;
  modeByUser.set(id, mode);
}

export function peekAssetPriceMode(userId?: string | null): AssetPriceMode {
  const id = String(userId || "").trim();
  if (!id) return DEFAULT_ASSET_PRICE_MODE;
  return modeByUser.get(id) ?? DEFAULT_ASSET_PRICE_MODE;
}

export function normalizePtbPriceMode(raw: unknown): PtbPriceMode {
  const value = String(raw ?? "").trim().toLowerCase();
  if (value === "chainlink" || value === "chainlink-rtds") return "chainlink";
  if (value === "twap30" || value === "30" || value === "twap_30") return "twap30";
  if (value === "twap60" || value === "60" || value === "twap_60") return "twap60";
  return "chainlink-rest";
}

export function rememberPtbPriceMode(userId: string, mode: PtbPriceMode): void {
  const id = String(userId || "").trim();
  if (!id) return;
  ptbModeByUser.set(id, mode);
}

export function peekPtbPriceMode(userId?: string | null): PtbPriceMode {
  const id = String(userId || "").trim();
  if (!id) return DEFAULT_PTB_PRICE_MODE;
  return ptbModeByUser.get(id) ?? DEFAULT_PTB_PRICE_MODE;
}

export function twapLookbackSecondsForTimeframe(timeframe: string): 30 | 60 {
  return String(timeframe || "").toLowerCase() === "15m" ? 60 : 30;
}

export function twapLookbackSecondsForSeries(series: string): 30 | 60 {
  try {
    return twapLookbackSecondsForTimeframe(parseMarketSeries(series).timeframe);
  } catch {
    return 30;
  }
}

export interface PriceSample {
  tMs: number;
  price: number;
}

/** Time-weighted average of a step-held price over [atMs − windowMs, atMs]. */
export function computeTwapAt(
  samples: PriceSample[],
  atMs: number,
  windowMs: number,
): number | undefined {
  if (!Number.isFinite(atMs) || !Number.isFinite(windowMs) || windowMs <= 0) {
    return undefined;
  }
  const pts = samples
    .filter(
      (s) =>
        s != null &&
        Number.isFinite(s.tMs) &&
        Number.isFinite(s.price),
    )
    .sort((a, b) => a.tMs - b.tMs);
  if (pts.length === 0) return undefined;

  const startMs = atMs - windowMs;
  let carry: number | undefined;
  for (const p of pts) {
    if (p.tMs <= startMs) carry = p.price;
    else break;
  }

  const segs: PriceSample[] = [];
  if (carry != null) segs.push({ tMs: startMs, price: carry });
  for (const p of pts) {
    if (p.tMs <= startMs) continue;
    if (p.tMs > atMs) break;
    segs.push(p);
  }
  if (segs.length === 0) {
    const last = pts[pts.length - 1];
    return last.tMs <= atMs ? last.price : undefined;
  }

  let area = 0;
  let covered = 0;
  for (let i = 0; i < segs.length; i += 1) {
    const from = Math.max(segs[i].tMs, startMs);
    const to = i + 1 < segs.length ? segs[i + 1].tMs : atMs;
    const dt = to - from;
    if (dt <= 0) continue;
    area += segs[i].price * dt;
    covered += dt;
  }
  if (covered <= 0) return segs[segs.length - 1].price;
  return area / covered;
}

export function applyTwapToReplayTicks<
  T extends {
    tMs: number;
    assetPrice?: number;
    prevCloseAsset?: number;
    assetGap?: number;
  },
>(ticks: T[], lookbackSec: number): T[] {
  const windowMs = Math.max(1, lookbackSec) * 1000;
  const samples: PriceSample[] = [];
  for (const tick of ticks) {
    if (tick.assetPrice == null || !Number.isFinite(tick.assetPrice)) continue;
    samples.push({ tMs: tick.tMs, price: tick.assetPrice });
  }
  if (samples.length === 0) return ticks;

  return ticks.map((tick) => {
    if (tick.assetPrice == null || !Number.isFinite(tick.assetPrice)) return tick;
    const twap = roundPolymarketAssetPriceMaybe(
      computeTwapAt(samples, tick.tMs, windowMs),
    );
    if (twap == null) return tick;
    return {
      ...tick,
      assetPrice: twap,
      assetGap: assetGapOrUnset(twap, tick.prevCloseAsset),
    };
  });
}

export function applyTwapToPriceHistory(
  history: Array<{ t: number; price: number }>,
  lookbackSec: number,
): Array<{ t: number; price: number }> {
  const windowMs = Math.max(1, lookbackSec) * 1000;
  const samples: PriceSample[] = history
    .filter((p) => Number.isFinite(p.t) && Number.isFinite(p.price))
    .map((p) => ({ tMs: p.t * 1000, price: p.price }));
  if (samples.length === 0) return history;
  return history.map((point) => {
    const twap = roundPolymarketAssetPriceMaybe(
      computeTwapAt(samples, point.t * 1000, windowMs),
    );
    if (twap == null) return point;
    return { t: point.t, price: twap };
  });
}

function twapValueForMode(state: LiveWindowState, lookback: 30 | 60): number | undefined {
  if (lookback === 60) {
    return state.assetPriceTwap60 ?? state.assetPriceTwap;
  }
  return state.assetPriceTwap30 ?? state.assetPriceTwap;
}

/** Shared live state stores raw Current; overlay TWAP for a user who selected it. */
export function applyLiveStatePriceMode(
  state: LiveWindowState,
  mode: AssetPriceMode,
): LiveWindowState {
  const lookback = twapLookbackSecondsForMode(mode);
  if (lookback == null) return state;
  const twap = roundPolymarketAssetPriceMaybe(twapValueForMode(state, lookback));
  const history = applyTwapToPriceHistory(state.priceHistory ?? [], lookback);
  const price = twap ?? history[history.length - 1]?.price;
  if (price == null || !Number.isFinite(price)) return state;
  return {
    ...state,
    assetPrice: price,
    assetGap: assetGapOrUnset(price, state.prevCloseAsset),
    priceHistory: history,
  };
}

export function resetLivePtbAnchors(state: LiveWindowState): void {
  state.ptbChainlink = undefined;
  state.ptbTwap30 = undefined;
  state.ptbTwap60 = undefined;
}

/** Latch first in-window Chainlink tick and first 30s/60s TWAP as frozen PTB anchors. */
export function latchLivePtbAnchors(
  state: LiveWindowState,
  rawCurrent?: number,
  atSec = Date.now() / 1000,
): void {
  const inWindow =
    Number.isFinite(state.windowStart) &&
    Number.isFinite(state.windowEnd) &&
    atSec >= state.windowStart &&
    atSec < state.windowEnd;
  if (!inWindow) return;
  if (state.ptbChainlink == null && rawCurrent != null && Number.isFinite(rawCurrent)) {
    state.ptbChainlink = rawCurrent;
  }
  if (
    state.ptbTwap30 == null &&
    state.assetPriceTwap30 != null &&
    Number.isFinite(state.assetPriceTwap30)
  ) {
    state.ptbTwap30 = state.assetPriceTwap30;
  }
  if (
    state.ptbTwap60 == null &&
    state.assetPriceTwap60 != null &&
    Number.isFinite(state.assetPriceTwap60)
  ) {
    state.ptbTwap60 = state.assetPriceTwap60;
  }
}

export function resolvePtbForMode(
  state: Pick<
    LiveWindowState,
    | "prevCloseAsset"
    | "priceToBeatSource"
    | "ptbChainlink"
    | "ptbTwap30"
    | "ptbTwap60"
  >,
  mode: PtbPriceMode,
): { ptb: number; source: LiveWindowState["priceToBeatSource"] } | undefined {
  const normalized = normalizePtbPriceMode(mode);
  if (normalized === "chainlink-rest") {
    if (state.prevCloseAsset != null && Number.isFinite(state.prevCloseAsset)) {
      return { ptb: state.prevCloseAsset, source: state.priceToBeatSource };
    }
    return undefined;
  }
  if (normalized === "chainlink") {
    const ptb =
      state.ptbChainlink ??
      (state.priceToBeatSource === "chainlink-rtds" ? state.prevCloseAsset : undefined);
    if (ptb != null && Number.isFinite(ptb)) {
      return { ptb, source: "chainlink-rtds" };
    }
    return undefined;
  }
  const latched = normalized === "twap60" ? state.ptbTwap60 : state.ptbTwap30;
  if (latched != null && Number.isFinite(latched)) {
    return { ptb: latched, source: "chainlink-rtds" };
  }
  return undefined;
}

/** Overlay the user's PTB choice onto shared live state (canonical prevClose stays chainlink→REST). */
export function applyLiveStatePtbMode(
  state: LiveWindowState,
  mode: PtbPriceMode,
): LiveWindowState {
  const normalized = normalizePtbPriceMode(mode);
  if (normalized === "chainlink-rest") return state;
  const resolved = resolvePtbForMode(state, normalized);
  if (!resolved) {
    return {
      ...state,
      prevCloseAsset: undefined,
      priceToBeatSource: undefined,
      assetGap: undefined,
    };
  }
  return {
    ...state,
    prevCloseAsset: resolved.ptb,
    priceToBeatSource: resolved.source,
    assetGap: assetGapOrUnset(state.assetPrice, resolved.ptb),
  };
}

export function applyLiveStateDisplayModes(
  state: LiveWindowState,
  assetMode: AssetPriceMode,
  ptbMode: PtbPriceMode,
): LiveWindowState {
  return applyLiveStatePriceMode(applyLiveStatePtbMode(state, ptbMode), assetMode);
}

export function resolveReplayPtb<
  T extends {
    tMs: number;
    assetPrice?: number;
    prevCloseAsset?: number;
  },
>(ticks: T[], mode: PtbPriceMode): { ptb: number; source: string } | undefined {
  const normalized = normalizePtbPriceMode(mode);
  if (normalized === "chainlink-rest") return undefined;
  if (normalized === "chainlink") {
    for (const tick of ticks) {
      if (tick.prevCloseAsset != null && Number.isFinite(tick.prevCloseAsset)) {
        return { ptb: tick.prevCloseAsset, source: "chainlink" };
      }
    }
    for (const tick of ticks) {
      if (tick.assetPrice != null && Number.isFinite(tick.assetPrice)) {
        return { ptb: tick.assetPrice, source: "chainlink" };
      }
    }
    return undefined;
  }
  const windowMs = (normalized === "twap60" ? 60 : 30) * 1000;
  const samples: PriceSample[] = [];
  for (const tick of ticks) {
    if (tick.assetPrice == null || !Number.isFinite(tick.assetPrice)) continue;
    samples.push({ tMs: tick.tMs, price: tick.assetPrice });
  }
  for (const sample of samples) {
    const twap = roundPolymarketAssetPriceMaybe(computeTwapAt(samples, sample.tMs, windowMs));
    if (twap != null) return { ptb: twap, source: normalized };
  }
  return undefined;
}

export function applyPtbModeToReplayTicks<
  T extends {
    tMs: number;
    assetPrice?: number;
    prevCloseAsset?: number;
    assetGap?: number;
    priceToBeatSource?: string;
  },
>(ticks: T[], mode: PtbPriceMode): T[] {
  const normalized = normalizePtbPriceMode(mode);
  if (normalized === "chainlink-rest") return ticks;
  const resolved = resolveReplayPtb(ticks, normalized);
  if (!resolved) {
    return ticks.map((tick) => ({
      ...tick,
      prevCloseAsset: undefined,
      assetGap: undefined,
    }));
  }
  return ticks.map((tick) => ({
    ...tick,
    prevCloseAsset: resolved.ptb,
    priceToBeatSource: resolved.source,
    assetGap: assetGapOrUnset(tick.assetPrice, resolved.ptb),
  }));
}

export function appendCappedHistory(
  history: Array<{ t: number; price: number }>,
  t: number,
  price: number,
  max = 2000,
): void {
  const last = history[history.length - 1];
  if (!last || last.t !== t || last.price !== price) {
    history.push({ t, price });
    if (history.length > max) history.splice(0, history.length - max);
  }
}

export function roundedTwapOrUndefined(value: number | undefined): number | undefined {
  if (value == null || !Number.isFinite(value)) return undefined;
  return roundPolymarketAssetPrice(value);
}
