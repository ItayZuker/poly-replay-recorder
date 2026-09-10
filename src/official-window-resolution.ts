import { sleepMs } from "./fetch-timeout.js";
import { fetchGammaWindowResolution } from "./gamma-window-resolution.js";
import {
  assetGapOrUnset,
  roundPolymarketAssetPriceMaybe,
} from "./polymarket-display-price.js";
import type { WindowOutcome } from "./types.js";

export type OfficialDisplayState = {
  prevCloseAsset?: number;
  assetPrice?: number;
  assetGap?: number;
  priceToBeatSource?: "chainlink-rtds" | "polymarket-openPrice" | "gamma";
  officialSettled?: boolean;
};

export type OfficialOutcomeSource = "crypto-price" | "gamma";

export interface OfficialWindowResolution {
  outcome: WindowOutcome;
  source: OfficialOutcomeSource;
  finalPrice?: number;
  priceToBeat?: number;
  yesPrice?: number;
  noPrice?: number;
}

/** True when a recording carries an explicit Gamma Up/Down settlement. */
export function hasOfficialWindowOutcome(
  outcome: unknown,
): outcome is WindowOutcome {
  return outcome === "up" || outcome === "down";
}

/** Overlay Gamma eventMetadata PTB/close onto live or recorded display fields. */
export function applyOfficialDisplayToState(
  state: OfficialDisplayState,
  official: OfficialWindowResolution,
): void {
  const ptb = roundPolymarketAssetPriceMaybe(official.priceToBeat);
  const current = roundPolymarketAssetPriceMaybe(official.finalPrice);
  if (ptb != null) {
    state.prevCloseAsset = ptb;
    state.priceToBeatSource = "gamma";
  }
  if (current != null) {
    state.assetPrice = current;
  }
  state.assetGap = assetGapOrUnset(state.assetPrice, state.prevCloseAsset);
  state.officialSettled = true;
}

/**
 * Official Up/Down for a finished window — same source Polymarket uses to pay tokens:
 * explicit Gamma resolve with ~1/~0 payout prices.
 *
 * Crypto-price open/close is intentionally not used here: it can disagree with Gamma
 * and previously caused false held wins in Live trading / Schedule stats.
 * Never uses live Chainlink or mid-book marks.
 */
export async function fetchOfficialWindowResolution(
  slug: string,
  signal?: AbortSignal,
): Promise<OfficialWindowResolution | null> {
  const trimmed = slug.trim();
  if (!trimmed) return null;

  try {
    const gamma = await fetchGammaWindowResolution(trimmed, signal);
    if (gamma?.outcome === "up" || gamma?.outcome === "down") {
      return {
        outcome: gamma.outcome,
        source: "gamma",
        finalPrice: gamma.finalPrice,
        priceToBeat: gamma.priceToBeat,
        yesPrice: gamma.yesPrice,
        noPrice: gamma.noPrice,
      };
    }
  } catch {
    // caller / wait loop retries
  }

  return null;
}

/**
 * Poll until Gamma explicitly resolves with settled ~1/~0 prices.
 * Used at recorder finalize and held-trade settlement.
 */
export async function waitForOfficialWindowResolution(
  slug: string,
  options: { maxWaitMs?: number; intervalMs?: number } = {},
): Promise<OfficialWindowResolution | null> {
  const maxWaitMs = options.maxWaitMs ?? 90_000;
  const intervalMs = options.intervalMs ?? 1000;
  const startedAt = Date.now();

  while (Date.now() - startedAt <= maxWaitMs) {
    try {
      const resolution = await fetchOfficialWindowResolution(slug);
      if (resolution) return resolution;
    } catch {
      // keep polling until maxWaitMs
    }
    await sleepMs(intervalMs);
  }

  return null;
}
