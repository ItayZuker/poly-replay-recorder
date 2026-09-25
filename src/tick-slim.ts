/** Published Replay tick lines: only the fields the trading app still reads. */

const BOOK_LEVELS = 5;

export type TickFileKind = "chainlink" | "clob-raw";

export class TickSlimParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TickSlimParseError";
  }
}

function finiteNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function keepValue(value: unknown): unknown | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") return value;
  return undefined;
}

function isCompactChainlink(doc: Record<string, unknown>): boolean {
  return doc.tMs === undefined && doc.assetPrice === undefined && doc.windowStart === undefined &&
    (doc["1"] != null || doc["2"] != null || doc["3"] != null);
}

/** Named slim chainlink object. Reads full named lines and both compact numeric layouts. */
export function slimChainlinkTick(doc: Record<string, unknown>): Record<string, unknown> {
  const compact = isCompactChainlink(doc);
  const tMs = finiteNumber(compact ? doc["2"] : doc.tMs);
  const assetPrice = finiteNumber(compact ? doc["3"] : doc.assetPrice);
  const prevCloseAsset = finiteNumber(compact ? doc["4"] : doc.prevCloseAsset);
  const source = doc.priceToBeatSource;
  const out: Record<string, unknown> = {};
  if (tMs !== undefined) out.tMs = tMs;
  if (assetPrice !== undefined) out.assetPrice = assetPrice;
  if (prevCloseAsset !== undefined) out.prevCloseAsset = prevCloseAsset;
  if (source === "chainlink" || source === "rest" || source === "gamma") {
    out.priceToBeatSource = source;
  }
  return out;
}

function slimLevels(
  raw: unknown,
  side: "bid" | "ask",
): Array<{ price: unknown; size: unknown }> | undefined {
  if (!Array.isArray(raw)) return undefined;
  const levels: Array<{ price: unknown; size: unknown; n: number }> = [];
  for (const level of raw) {
    if (!level || typeof level !== "object" || Array.isArray(level)) continue;
    const rec = level as Record<string, unknown>;
    const price = rec.price;
    const size = rec.size ?? rec.amount;
    const priceN = finiteNumber(price);
    const sizeN = finiteNumber(size);
    if (priceN == null || sizeN == null || sizeN <= 0) continue;
    levels.push({ price, size, n: priceN });
  }
  levels.sort((a, b) => (side === "bid" ? b.n - a.n : a.n - b.n));
  const top = levels.slice(0, BOOK_LEVELS).map(({ price, size }) => ({ price, size }));
  return top.length > 0 ? top : undefined;
}

function slimBookMessage(msg: Record<string, unknown>): Record<string, unknown> | null {
  const assetId = typeof msg.asset_id === "string" && msg.asset_id ? msg.asset_id : undefined;
  const bids = slimLevels(msg.bids, "bid");
  const asks = slimLevels(msg.asks, "ask");
  const bestBid = keepValue(msg.best_bid);
  const bestAsk = keepValue(msg.best_ask);
  if (!assetId || (!bids && !asks && bestBid === undefined && bestAsk === undefined)) return null;
  const out: Record<string, unknown> = { event_type: "book", asset_id: assetId };
  if (bids) out.bids = bids;
  if (asks) out.asks = asks;
  if (bestBid !== undefined) out.best_bid = bestBid;
  if (bestAsk !== undefined) out.best_ask = bestAsk;
  return out;
}

function slimBestBidAsk(msg: Record<string, unknown>): Record<string, unknown> | null {
  const assetId = typeof msg.asset_id === "string" && msg.asset_id ? msg.asset_id : undefined;
  const bestBid = keepValue(msg.best_bid);
  const bestAsk = keepValue(msg.best_ask);
  if (!assetId || (bestBid === undefined && bestAsk === undefined)) return null;
  const out: Record<string, unknown> = { event_type: "best_bid_ask", asset_id: assetId };
  if (bestBid !== undefined) out.best_bid = bestBid;
  if (bestAsk !== undefined) out.best_ask = bestAsk;
  return out;
}

function slimLastTrade(msg: Record<string, unknown>): Record<string, unknown> | null {
  const assetId = typeof msg.asset_id === "string" && msg.asset_id ? msg.asset_id : undefined;
  const price = keepValue(msg.price);
  if (!assetId || price === undefined) return null;
  return { event_type: "last_trade_price", asset_id: assetId, price };
}

function slimPriceChange(msg: Record<string, unknown>): Record<string, unknown> | null {
  if (!Array.isArray(msg.price_changes)) return null;
  const changes: Array<Record<string, unknown>> = [];
  for (const change of msg.price_changes) {
    if (!change || typeof change !== "object" || Array.isArray(change)) continue;
    const rec = change as Record<string, unknown>;
    const assetId = typeof rec.asset_id === "string" && rec.asset_id ? rec.asset_id : undefined;
    if (!assetId) continue;
    const bestBid = keepValue(rec.best_bid);
    const bestAsk = keepValue(rec.best_ask);
    if (bestBid === undefined && bestAsk === undefined) continue;
    const row: Record<string, unknown> = { asset_id: assetId };
    if (bestBid !== undefined) row.best_bid = bestBid;
    if (bestAsk !== undefined) row.best_ask = bestAsk;
    changes.push(row);
  }
  if (changes.length === 0) return null;
  const out: Record<string, unknown> = { event_type: "price_change", price_changes: changes };
  if (typeof msg.asset_id === "string" && msg.asset_id) out.asset_id = msg.asset_id;
  return out;
}

function slimUnknownMessage(msg: Record<string, unknown>): Record<string, unknown> | null {
  const bids = slimLevels(msg.bids, "bid");
  const asks = slimLevels(msg.asks, "ask");
  const bestBid = keepValue(msg.best_bid);
  const bestAsk = keepValue(msg.best_ask);
  const price = keepValue(msg.price);
  const nested = Array.isArray(msg.price_changes) ? slimPriceChange(msg) : null;
  if (!bids && !asks && bestBid === undefined && bestAsk === undefined && price === undefined && !nested) {
    return null;
  }
  const out: Record<string, unknown> = {};
  if (typeof msg.event_type === "string" && msg.event_type) out.event_type = msg.event_type;
  if (typeof msg.asset_id === "string" && msg.asset_id) out.asset_id = msg.asset_id;
  if (bids) out.bids = bids;
  if (asks) out.asks = asks;
  if (bestBid !== undefined) out.best_bid = bestBid;
  if (bestAsk !== undefined) out.best_ask = bestAsk;
  if (price !== undefined) out.price = price;
  if (nested?.price_changes) out.price_changes = nested.price_changes;
  return out;
}

export function slimClobPayload(payload: unknown): unknown | null {
  if (Array.isArray(payload)) {
    const messages = payload
      .map((message) => slimClobMessage(message))
      .filter((message) => message != null);
    return messages.length > 0 ? messages : null;
  }
  return slimClobMessage(payload);
}

function slimClobMessage(message: unknown): Record<string, unknown> | null {
  if (!message || typeof message !== "object" || Array.isArray(message)) return null;
  const msg = message as Record<string, unknown>;
  const eventType = typeof msg.event_type === "string" ? msg.event_type : "";
  if (eventType === "book") return slimBookMessage(msg);
  if (eventType === "best_bid_ask") return slimBestBidAsk(msg);
  if (eventType === "last_trade_price") return slimLastTrade(msg);
  if (eventType === "price_change") return slimPriceChange(msg);
  return slimUnknownMessage(msg);
}

/** `{ tMs, payload }` or null when the message cannot change the book. */
export function slimClobRawTick(doc: Record<string, unknown>): Record<string, unknown> | null {
  const payload = slimClobPayload(doc.payload);
  if (payload == null) return null;
  const out: Record<string, unknown> = {};
  const tMs = finiteNumber(doc.tMs);
  if (tMs !== undefined) out.tMs = tMs;
  out.payload = payload;
  return out;
}

export function slimJsonlDocument(
  kind: TickFileKind,
  text: string,
): { text: string; linesIn: number; linesOut: number } {
  const lines = text.split("\n");
  const out: string[] = [];
  let linesIn = 0;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? "";
    if (!line.trim()) continue;
    linesIn += 1;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (err) {
      throw new TickSlimParseError(
        `line ${i + 1}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new TickSlimParseError(`line ${i + 1}: expected a JSON object`);
    }
    const slim =
      kind === "chainlink"
        ? slimChainlinkTick(parsed as Record<string, unknown>)
        : slimClobRawTick(parsed as Record<string, unknown>);
    if (slim == null) continue;
    out.push(JSON.stringify(slim));
  }
  return {
    text: out.length > 0 ? `${out.join("\n")}\n` : "",
    linesIn,
    linesOut: out.length,
  };
}

export function tickKindFromPath(filePath: string): TickFileKind | null {
  const base = filePath.replace(/\\/g, "/");
  if (base.endsWith("/chainlink.jsonl") || base.endsWith("/chainlink.jsonl.zst")) return "chainlink";
  if (base.endsWith("/clob-raw.jsonl") || base.endsWith("/clob-raw.jsonl.zst")) return "clob-raw";
  return null;
}
