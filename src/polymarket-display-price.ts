/**
 * Polymarket market-page rounding for BTC/ETH/SOL.
 * Their Chainlink print uses 2 decimals when |price| ≥ 1000 (BTC); we use 2 decimals
 * for all assets so PTB / Current / Gap match the $x.xx labels.
 */
export function roundPolymarketAssetPrice(value: number): number {
  if (!Number.isFinite(value)) return value;
  return Math.round(value * 100) / 100;
}

export function roundPolymarketAssetPriceMaybe(
  value: number | undefined,
): number | undefined {
  if (value == null || !Number.isFinite(value)) return undefined;
  return roundPolymarketAssetPrice(value);
}

/** Gap = Current − PTB. Unset until both are published. */
export function assetGapOrUnset(
  current?: number,
  ptb?: number,
): number | undefined {
  if (
    current == null ||
    ptb == null ||
    !Number.isFinite(current) ||
    !Number.isFinite(ptb)
  ) {
    return undefined;
  }
  return current - ptb;
}
