import { fetchWithTimeout, sleepMs } from "./fetch-timeout.js";
import { roundPolymarketAssetPriceMaybe } from "./polymarket-display-price.js";
import type { WindowOutcome } from "./types.js";

const GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events";

/** Settled payout prices once Gamma has explicitly resolved (~1 / ~0). */
const SETTLED_WIN_MIN = 0.99;
const SETTLED_LOSS_MAX = 0.01;

export interface GammaWindowResolution {
  outcome: WindowOutcome;
  finalPrice?: number;
  priceToBeat?: number;
  yesPrice?: number;
  noPrice?: number;
}

function parseJsonArray<T>(value: unknown): T[] {
  if (Array.isArray(value)) return value as T[];
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? (parsed as T[]) : [];
    } catch {
      return [];
    }
  }
  return [];
}

function normalizeOutcomeLabel(value: unknown): string {
  return String(value ?? "")
    .trim()
    .toLowerCase();
}

/** Map outcomePrices by Up/Down (or Yes/No) labels; fall back to [0]=Up, [1]=Down. */
export function pricesFromGammaMarket(market: Record<string, unknown>): {
  yesPrice?: number;
  noPrice?: number;
} {
  const prices = parseJsonArray<string>(market.outcomePrices).map(Number);
  const outcomes = parseJsonArray<string>(market.outcomes).map(normalizeOutcomeLabel);

  const upIdx = outcomes.findIndex((o) => o === "up" || o === "yes");
  const downIdx = outcomes.findIndex((o) => o === "down" || o === "no");

  if (upIdx >= 0 && downIdx >= 0) {
    return {
      yesPrice: Number.isFinite(prices[upIdx]) ? prices[upIdx] : undefined,
      noPrice: Number.isFinite(prices[downIdx]) ? prices[downIdx] : undefined,
    };
  }

  return {
    yesPrice: Number.isFinite(prices[0]) ? prices[0] : undefined,
    noPrice: Number.isFinite(prices[1]) ? prices[1] : undefined,
  };
}

/**
 * True when Gamma has explicitly finished resolving the market.
 * Do not treat mid-book prices or final-vs-PTB metadata as resolved.
 */
export function isGammaMarketExplicitlyResolved(
  market: Record<string, unknown>,
  event?: Record<string, unknown> | null,
): boolean {
  const status = normalizeOutcomeLabel(market.umaResolutionStatus);
  if (status === "resolved") return true;

  // Some payloads omit umaResolutionStatus briefly but already flip closed + auto-resolved.
  const marketClosed = market.closed === true;
  const eventClosed = event?.closed === true;
  const auto =
    market.automaticallyResolved === true || event?.automaticallyResolved === true;
  return (marketClosed || eventClosed) && auto;
}

/**
 * Outcome from settled Gamma payout prices only (winning side ~1, losing ~0).
 * Rejects mid-book marks that appear right after the window ends.
 */
export function outcomeFromGammaPrices(
  yesPrice?: number,
  noPrice?: number,
): WindowOutcome | undefined {
  if (
    yesPrice == null ||
    noPrice == null ||
    !Number.isFinite(yesPrice) ||
    !Number.isFinite(noPrice)
  ) {
    return undefined;
  }
  const upWins = yesPrice >= SETTLED_WIN_MIN && noPrice <= SETTLED_LOSS_MAX;
  const downWins = noPrice >= SETTLED_WIN_MIN && yesPrice <= SETTLED_LOSS_MAX;
  if (upWins && !downWins) return "up";
  if (downWins && !upWins) return "down";
  return undefined;
}

/** Official Chainlink close vs PTB (Polymarket Up = close >= open). Kept for tooling; not used to settle. */
export function outcomeFromFinalVsPtb(
  finalPrice?: number,
  priceToBeat?: number,
): WindowOutcome | undefined {
  if (
    finalPrice == null ||
    priceToBeat == null ||
    !Number.isFinite(finalPrice) ||
    !Number.isFinite(priceToBeat)
  ) {
    return undefined;
  }
  return finalPrice >= priceToBeat ? "up" : "down";
}

/**
 * One-shot Gamma lookup by market/event slug.
 * Returns null until the market is explicitly resolved and payout prices are ~1/~0.
 */
export async function fetchGammaWindowResolution(
  slug: string,
  signal?: AbortSignal,
): Promise<GammaWindowResolution | null> {
  const trimmed = slug.trim();
  if (!trimmed) return null;

  const res = await fetchWithTimeout(
    `${GAMMA_EVENTS_URL}?slug=${encodeURIComponent(trimmed)}`,
    { signal },
  );
  if (!res.ok) {
    throw new Error(`Gamma events error (${res.status})`);
  }

  const events = (await res.json()) as Record<string, unknown>[];
  if (!Array.isArray(events) || events.length === 0) return null;

  const event = events[0];
  const markets = event.markets as Record<string, unknown>[] | undefined;
  const market = markets?.[0];
  if (!market) return null;

  if (!isGammaMarketExplicitlyResolved(market, event)) return null;

  const { yesPrice, noPrice } = pricesFromGammaMarket(market);
  const outcome = outcomeFromGammaPrices(yesPrice, noPrice);
  if (!outcome) return null;

  const meta = event.eventMetadata as
    | { finalPrice?: number; priceToBeat?: number }
    | undefined;
  const finalPrice = roundPolymarketAssetPriceMaybe(meta?.finalPrice);
  const priceToBeat = roundPolymarketAssetPriceMaybe(meta?.priceToBeat);

  return { outcome, finalPrice, priceToBeat, yesPrice, noPrice };
}

/**
 * Wait until Gamma marks the market explicitly resolved with settled ~1/~0 prices.
 * Prefer {@link waitForOfficialWindowResolution} for live finalize (Gamma payout).
 */
export async function waitForGammaWindowResolution(
  slug: string,
  options: { maxWaitMs?: number; intervalMs?: number } = {},
): Promise<GammaWindowResolution | null> {
  const maxWaitMs = options.maxWaitMs ?? 90_000;
  const intervalMs = options.intervalMs ?? 1000;
  const startedAt = Date.now();

  while (Date.now() - startedAt <= maxWaitMs) {
    try {
      const resolution = await fetchGammaWindowResolution(slug);
      if (resolution) return resolution;
    } catch {
      // keep polling until maxWaitMs
    }
    await sleepMs(intervalMs);
  }

  return null;
}
