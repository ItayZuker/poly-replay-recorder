import type { WindowHitRecord } from "./types.js";

export const PTB_CROSSING_EPSILON = 0.01;

export type PtbSide = "above" | "below";

export interface WindowDynamicsTracker {
  lastPtbSide: PtbSide | null;
}

export function createWindowDynamicsTracker(): WindowDynamicsTracker {
  return { lastPtbSide: null };
}

export function getPtbSide(
  assetPrice: number,
  prevCloseAsset: number,
  epsilon = PTB_CROSSING_EPSILON,
): PtbSide | null {
  const diff = assetPrice - prevCloseAsset;
  if (Math.abs(diff) <= epsilon) return null;
  return diff > 0 ? "above" : "below";
}

export function syncWindowAssetRange(window: WindowHitRecord): void {
  if (
    window.minAssetPrice != null &&
    window.maxAssetPrice != null &&
    Number.isFinite(window.minAssetPrice) &&
    Number.isFinite(window.maxAssetPrice)
  ) {
    window.assetRange = Math.max(0, window.maxAssetPrice - window.minAssetPrice);
  }
}

export function syncWindowRangeFromPtb(window: WindowHitRecord): void {
  syncWindowAssetRange(window);
  if (window.prevCloseAsset == null || !Number.isFinite(window.prevCloseAsset)) {
    return;
  }
  if (window.maxAssetPrice != null && Number.isFinite(window.maxAssetPrice)) {
    window.rangeTop = Math.max(0, window.maxAssetPrice - window.prevCloseAsset);
  }
  if (window.minAssetPrice != null && Number.isFinite(window.minAssetPrice)) {
    window.rangeBottom = Math.max(0, window.prevCloseAsset - window.minAssetPrice);
  }
}

export function finalizeWindowDynamics(window: WindowHitRecord): void {
  syncWindowRangeFromPtb(window);
}

export function updateWindowDynamics(
  window: WindowHitRecord,
  tracker: WindowDynamicsTracker,
  assetPrice?: number,
  prevCloseAsset?: number,
): void {
  if (
    assetPrice == null ||
    prevCloseAsset == null ||
    !Number.isFinite(assetPrice) ||
    !Number.isFinite(prevCloseAsset)
  ) {
    return;
  }

  if (window.minAssetPrice == null || assetPrice < window.minAssetPrice) {
    window.minAssetPrice = assetPrice;
  }
  if (window.maxAssetPrice == null || assetPrice > window.maxAssetPrice) {
    window.maxAssetPrice = assetPrice;
  }

  window.prevCloseAsset = prevCloseAsset;
  syncWindowRangeFromPtb(window);

  const side = getPtbSide(assetPrice, prevCloseAsset);
  if (side == null) return;

  if (tracker.lastPtbSide != null && tracker.lastPtbSide !== side) {
    window.ptbCrossings = (window.ptbCrossings ?? 0) + 1;
  }
  tracker.lastPtbSide = side;
}

export function getWindowRangeFromPtb(
  window: WindowHitRecord,
  ptbFallback?: number,
): { rangeTop?: number; rangeBottom?: number } {
  const ptb = window.prevCloseAsset ?? ptbFallback;
  if (ptb == null || !Number.isFinite(ptb)) {
    return {
      rangeTop: window.rangeTop,
      rangeBottom: window.rangeBottom,
    };
  }

  const rangeTop =
    window.maxAssetPrice != null && Number.isFinite(window.maxAssetPrice)
      ? Math.max(0, window.maxAssetPrice - ptb)
      : undefined;
  const rangeBottom =
    window.minAssetPrice != null && Number.isFinite(window.minAssetPrice)
      ? Math.max(0, ptb - window.minAssetPrice)
      : undefined;

  return { rangeTop, rangeBottom };
}

export function getWindowAssetRange(window: WindowHitRecord): number | null {
  if (window.assetRange != null && Number.isFinite(window.assetRange)) {
    return window.assetRange;
  }
  if (
    window.minAssetPrice != null &&
    window.maxAssetPrice != null &&
    Number.isFinite(window.minAssetPrice) &&
    Number.isFinite(window.maxAssetPrice)
  ) {
    return Math.max(0, window.maxAssetPrice - window.minAssetPrice);
  }
  return null;
}

/** True when the window’s asset price never moved (bad / stuck Chainlink recording). */
export function isFlatPriceWindow(window: {
  minAssetPrice?: number | null;
  maxAssetPrice?: number | null;
  assetRange?: number | null;
}): boolean {
  if (
    window.minAssetPrice != null &&
    window.maxAssetPrice != null &&
    Number.isFinite(window.minAssetPrice) &&
    Number.isFinite(window.maxAssetPrice)
  ) {
    return window.maxAssetPrice === window.minAssetPrice;
  }
  if (window.assetRange != null && Number.isFinite(window.assetRange)) {
    return window.assetRange === 0;
  }
  return false;
}

/** Consecutive price silence that makes a Replay path unusable. */
export const UNUSABLE_PRICE_GAP_MS = 30_000;

/** Flat when every Chainlink sample in the tick stream shares one price. */
export function isFlatPriceFromTicks(
  ticks: Array<{ source?: string; assetPrice?: number | null }>,
): boolean {
  let min: number | null = null;
  let max: number | null = null;
  let count = 0;
  for (const tick of ticks) {
    if (tick.source !== "chainlink-tick") continue;
    const price = tick.assetPrice;
    if (price == null || !Number.isFinite(price)) continue;
    count += 1;
    min = min == null ? price : Math.min(min, price);
    max = max == null ? price : Math.max(max, price);
  }
  if (count === 0 || min == null || max == null) return false;
  return min === max;
}

/**
 * Replay-unusable price path: fully flat, a ≥30s hole, or the same print for
 * at least half the window. Missing Chainlink samples are unusable.
 */
export function isUnusablePricePath(
  ticks: Array<{ tMs?: number; source?: string; assetPrice?: number | null }>,
  windowStartSec: number,
  windowEndSec: number,
): boolean {
  const startMs = windowStartSec * 1000;
  const endMs = windowEndSec * 1000;
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return true;
  }
  const durationMs = endMs - startMs;
  const samples: Array<{ tMs: number; price: number }> = [];
  for (const tick of ticks) {
    if (tick.source != null && tick.source !== "chainlink-tick") continue;
    const price = tick.assetPrice;
    const tMs = tick.tMs;
    if (price == null || !Number.isFinite(price) || tMs == null || !Number.isFinite(tMs)) {
      continue;
    }
    if (tMs < startMs || tMs >= endMs) continue;
    samples.push({ tMs, price });
  }
  if (samples.length === 0) return true;
  samples.sort((a, b) => a.tMs - b.tMs);

  let min = samples[0].price;
  let max = samples[0].price;
  let longestGap = samples[0].tMs - startMs;
  let longestFlat = 0;
  let flatFrom = samples[0].tMs;
  let flatPrice = samples[0].price;
  for (let i = 0; i < samples.length; i++) {
    const cur = samples[i];
    min = Math.min(min, cur.price);
    max = Math.max(max, cur.price);
    if (i > 0) {
      longestGap = Math.max(longestGap, cur.tMs - samples[i - 1].tMs);
    }
    if (cur.price === flatPrice) {
      longestFlat = Math.max(longestFlat, cur.tMs - flatFrom);
    } else {
      flatPrice = cur.price;
      flatFrom = cur.tMs;
    }
  }
  longestGap = Math.max(longestGap, endMs - samples[samples.length - 1].tMs);
  longestFlat = Math.max(longestFlat, endMs - flatFrom);

  if (min === max) return true;
  if (longestGap >= UNUSABLE_PRICE_GAP_MS) return true;
  if (longestFlat >= durationMs / 2) return true;
  return false;
}
