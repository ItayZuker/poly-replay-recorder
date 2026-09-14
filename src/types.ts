import type { BookLevel } from "./clob-service.js";
import type { PtbHistoryEntry } from "./ptb-history.js";

export type { PtbHistoryEntry, PtbHistorySource } from "./ptb-history.js";

export type WindowOutcome = "up" | "down";
export type TickSource = "clob-book" | "chainlink-tick";

/** Max levels kept when reading historical book ticks (older files may have 10). */
export const BOOK_DEPTH_LEVELS = 10;
/** Levels stored in new recordings and in the live CLOB cache. */
export const RECORDING_BOOK_DEPTH = 5;

export interface MarketDocument {
  _id: string;
  label: string;
  timeframeMinutes: number;
  /** When true, series is shown in the trader app and trading APIs allow it. */
  available: boolean;
  /** When true, a non-executor process captures ticks/windows for this series. */
  recordingEnabled: boolean;
  /** Hot tick/window retention for this series (days). Default 7. */
  retentionDays: number;
  createdAt: string;
  updatedAt: string;
}

export interface RecordedWindowDocument {
  _id: string;
  windowStart: number;
  windowEnd: number;
  savedAt: string;
  updatedAt: string;
  slug?: string;
  question?: string;
  conditionId?: string;
  assetPrice?: number;
  prevCloseAsset?: number;
  /** First raw Chainlink print at/near window open. */
  ptbChainlink?: number;
  /** Official RTDS 30s TWAP at/near window open. */
  ptbTwap30?: number;
  /** Official RTDS 60s TWAP at/near window open. */
  ptbTwap60?: number;
  /** Append-only PTB changes: first Chainlink, then Gamma at windowEnd. */
  ptbHistory?: PtbHistoryEntry[];
  /** Official Gamma eventMetadata.priceToBeat (separate from REST history). */
  gammaPtb?: number;
  assetGap?: number;
  windowOutcome?: WindowOutcome;
  yesPrice?: number;
  noPrice?: number;
  ptbCrossings?: number;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  rangeTop?: number;
  rangeBottom?: number;
  uniqueTraders?: number;
  newWallets?: number;
  knownWallets?: number;
  tickCount: number;
  clobRawCount?: number;
  clobBookCount?: number;
  chainlinkCount?: number;
}

/** Raw CLOB websocket payload for audit replay. */
export interface ClobRawTickDocument {
  _id: string;
  windowStart: number;
  windowEnd: number;
  tMs: number;
  payload: unknown;
}

/** Parsed 5-level book snapshot after each raw WS message. */
export interface ClobBookTickDocument {
  _id: string;
  windowStart: number;
  windowEnd: number;
  tMs: number;
  yesPrice?: number;
  noPrice?: number;
  yesBids: BookLevel[];
  yesAsks: BookLevel[];
  noBids: BookLevel[];
  noAsks: BookLevel[];
}

/** @deprecated Use ClobBookTickDocument */
export type BookTickDocument = ClobBookTickDocument;

/** Chainlink asset price and per-window dynamics. */
export interface ChainlinkTickDocument {
  _id: string;
  windowStart: number;
  windowEnd: number;
  tMs: number;
  assetPrice?: number;
  prevCloseAsset?: number;
  /** chainlink = first RTDS tick; rest = crypto-price open; gamma = official tip. */
  priceToBeatSource?: "chainlink" | "rest" | "gamma";
  assetGap?: number;
  ptbCrossings?: number;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  rangeTop?: number;
  rangeBottom?: number;
}

/** Merged book + chainlink state for replay APIs. */
export interface ReplayTickDocument {
  tMs: number;
  t: number;
  elapsedSec: number;
  source: TickSource;
  yesPrice?: number;
  noPrice?: number;
  yesBid?: number;
  noBid?: number;
  yesAsk?: number;
  noAsk?: number;
  yesBidSize?: number;
  noBidSize?: number;
  yesAskSize?: number;
  noAskSize?: number;
  yesBids?: BookLevel[];
  yesAsks?: BookLevel[];
  noBids?: BookLevel[];
  noAsks?: BookLevel[];
  assetPrice?: number;
  prevCloseAsset?: number;
  priceToBeatSource?: "chainlink" | "rest" | "gamma";
  assetGap?: number;
  ptbCrossings?: number;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  rangeTop?: number;
  rangeBottom?: number;
}

/** @deprecated Use BookTickDocument */
export type TickDocument = BookTickDocument;

export interface WindowHitRecord {
  windowStart: number;
  windowEnd: number;
  slug?: string;
  question?: string;
  conditionId?: string;
  assetPrice?: number;
  prevCloseAsset?: number;
  ptbChainlink?: number;
  ptbTwap30?: number;
  ptbTwap60?: number;
  ptbHistory?: PtbHistoryEntry[];
  gammaPtb?: number;
  assetGap?: number;
  windowOutcome?: WindowOutcome;
  yesPrice?: number;
  noPrice?: number;
  ptbCrossings?: number;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  rangeTop?: number;
  rangeBottom?: number;
  uniqueTraders?: number;
  newWallets?: number;
  knownWallets?: number;
  savedAt?: string;
}

export interface LiveWindowState {
  series: string;
  windowStart: number;
  windowEnd: number;
  slug?: string;
  question?: string;
  prevCloseAsset?: number;
  /** First in-window Chainlink tick — frozen PTB for the Chainlink setting. */
  ptbChainlink?: number;
  /** First in-window 30s TWAP — frozen PTB for Chainlink 30s Avg. */
  ptbTwap30?: number;
  /** First in-window 60s TWAP — frozen PTB for Chainlink 60s Avg. */
  ptbTwap60?: number;
  assetPrice?: number;
  /** Raw Chainlink tick when assetPrice has been overlaid for a user. */
  assetPriceRaw?: number;
  /** Official or computed 30s TWAP (all series). */
  assetPriceTwap30?: number;
  /** Official or computed 60s TWAP (all series). */
  assetPriceTwap60?: number;
  /** Legacy single TWAP slot (prefer twap30 / twap60). */
  assetPriceTwap?: number;
  assetGap?: number;
  /** Where prevCloseAsset (PTB) came from — first Chainlink tick, published open, or Gamma. */
  priceToBeatSource?: "chainlink-rtds" | "polymarket-openPrice" | "gamma";
  /** True once Gamma eventMetadata PTB/close were applied for this window. */
  officialSettled?: boolean;
  yesBid?: number;
  yesAsk?: number;
  noBid?: number;
  noAsk?: number;
  yesBidSize?: number;
  yesAskSize?: number;
  noBidSize?: number;
  noAskSize?: number;
  yesBids?: BookLevel[];
  yesAsks?: BookLevel[];
  noBids?: BookLevel[];
  noAsks?: BookLevel[];
  yesDisplay?: number;
  noDisplay?: number;
  ptbCrossings?: number;
  minAssetPrice?: number;
  maxAssetPrice?: number;
  assetRange?: number;
  uniqueTraders?: number;
  lastTickMs?: number;
  /** Measured CLOB WebSocket round-trip latency (ms). */
  feedLatencyMs?: number;
  priceHistory: Array<{ t: number; price: number }>;
  /** Parallel to priceHistory when TWAP Current is available. */
  priceHistoryTwap?: Array<{ t: number; price: number }>;
  /** Monotonic sequence incremented once per CLOB book update. */
  bookTickSequence?: number;
}

/** Direction / dual-side policy vs PTB. Same meaning for FAK and GTD. */
export type GapVsPtb = "with" | "opposite" | "first" | "both";

/** Optimize off → GTD resting limit; optimize on → immediate FAK. */
export type BuyOrderType = "GTD" | "FAK";

export interface SimPhaseConfig {
  buyEnabled: boolean;
  buyShares: number;
  /** Ask touch / limit price in cents (1–99). */
  buyTrigger: number;
  /** After touching trigger, hunt a better (≤) fill. */
  buyOptimize: boolean;
  /**
   * Buy execution type for this phase (derived from buyOptimize).
   * Optimize off: GTD. Optimize on: FAK.
   */
  buyOrderType: BuyOrderType;
  /** Min |asset−PTB| in $; 0 = ignore. */
  minGap: number;
  /** Max |asset−PTB| in $; 0 = ignore. */
  maxGap: number;
  /** Gap direction relative to the side being bought. */
  gapVsPtb: GapVsPtb;
  /**
   * Abort unfilled buys after this many PTB crossings in the current phase.
   * 0 = off; clamped 0–1000.
   */
  buyAbortOnCrossing: number;
  /**
   * Sell limit = buy + this many cents.
   * 100 = off (hold to settlement, no sell). Clamped 1–100.
   */
  sellProfitCents: number;
}


export interface SimTakerFeeParams {
  feeRate: number;
  feeExponent: number;
}

export interface SimSetup {
  phaseSplit: [number, number];
  phases: [SimPhaseConfig, SimPhaseConfig, SimPhaseConfig];
  /** Simulated order latency before fill re-check (ms). */
  latencyMs: number;
  /**
   * Probability (0–100) that a would-be fill succeeds after latency.
   * 100 = always fill when the book allows; 0 = never fill.
   */
  fillSuccessPct?: number;
  /** Polymarket taker fee params (crypto default; override from CLOB when available). */
  feeParams?: SimTakerFeeParams;
}

/** Phase trading config persisted for replay (no latency or market). */
export interface TradingPhaseSetup {
  phaseSplit: [number, number];
  phases: [SimPhaseConfig, SimPhaseConfig, SimPhaseConfig];
}

export interface TradingSetupRecord {
  /** Owner — required for multi-user isolation. */
  userId: string;
  title: string;
  description?: string;
  color?: string;
  setup: TradingPhaseSetup;
  createdAt: Date;
  /** Lower = higher in the schedule setups list. */
  sortOrder?: number;
  /**
   * True while at least one card using this setup is on the live schedule
   * (`schedual_setups_real`). Sim apps should refuse to delete when set.
   */
  liveScheduleInUse?: boolean;
  /**
   * True while at least one card using this setup is on the sim schedule
   * (`schedual_setups_sim`). Real app disables delete when set.
   */
  simScheduleInUse?: boolean;
}

/** Replay-mode setups live in `trading_setups_replay`; placements in `schedual_setups_replay`. */

export type ScheduleDayId = "mon" | "tue" | "wed" | "thu" | "fri" | "sat" | "sun";

export interface SchedulePlacementRecord {
  /** Owner — required for multi-user isolation. */
  userId: string;
  /** Market series this schedule board belongs to (e.g. btc-5m). */
  series: string;
  setupId: string;
  title: string;
  day: ScheduleDayId;
  startHour: number;
  durationHours: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface SimMarker {
  type: "buy" | "sell";
  side: "up" | "down";
  t: number;
  y: number | null;
  shares: number;
  price: number;
  /** FAK buy execution cap inherited from the armed watch. */
  triggerCents?: number;
  /** Phase index at buy fill (sell profit source). */
  phaseIndex?: number;
  /** Who placed the fill — used for Replay per-trade dots. */
  source?: "phase" | "trigger";
  /** Market Trigger id when source is trigger (chart duration bands). */
  triggerId?: string;
  cost?: number;
  fees?: number;
  proceeds?: number;
  profit?: number;
  /** Total position cost (buy: cost+fees) or total sale (sell: proceeds). */
  total?: number;
  /** Trigger held-to-window settlement (Replay dots: blue/red by outcome, not P/L). */
  heldSettlement?: boolean;
  windowKey: string;
}

export interface SimLastWindow {
  windowKey: string;
  windowStart: number;
  windowEnd?: number;
  outcome?: "up" | "down";
  prevCloseAsset?: number;
  assetPrice?: number;
  assetGap?: number;
  side?: "up" | "down";
  shares?: number;
  buyPrice?: number;
  buyCost?: number;
  buyFees?: number;
  positionCost?: number;
  sold: boolean;
  sellPrice?: number;
  sellProceeds?: number;
  positionWon?: boolean | null;
  pl: number;
  plLabel: "Trade" | "Settlement" | "No trade";
}

/** Fill latch prices per quote box — at most one buy and one sell per side. */
export interface SimQuoteLocks {
  upBuy: number[];
  upSell: number[];
  downBuy: number[];
  downSell: number[];
}

export interface SimPublicState {
  setup: SimSetup;
  markers: SimMarker[];
  quoteLocks: SimQuoteLocks;
  lastWindow: SimLastWindow | null;
}

export interface TradingConfig {
  autoTrade: boolean;
  useSchedule: boolean;
  startTrading: boolean;
  /** Manual buy size (share count or USDC, depending on manualOrderUnit). */
  manualShares: number;
  manualOrderUnit: "shares" | "usdc";
  /** Legacy manual quote Buy order type (manual orders disabled). */
  manualBuyOrderType: "FAK" | "FOK";
  /** Legacy manual quote Sell order type (manual orders disabled). */
  manualSellOrderType: "FAK" | "FOK";
}

export interface LiveSidePosition {
  shares: number;
  avgPrice: number;
  cost: number;
  cardId?: string;
}

export type TradingPositionCardStatus = "open" | "sold" | "win" | "loss";

export interface TradingPositionCard {
  id: string;
  windowKey: string;
  series: string;
  side: "up" | "down";
  shares: number;
  buyPrice: number;
  buyCost: number;
  /** Estimated Polymarket taker fee paid on the buy (USDC). */
  buyFees?: number;
  buyAt: number;
  status: TradingPositionCardStatus;
  sellPrice?: number;
  sellProceeds?: number;
  /** Estimated Polymarket taker fee paid on the sell (USDC). */
  sellFees?: number;
  soldAt?: number;
  pl?: number;
  outcome?: "up" | "down";
  /** Polymarket outcome token id */
  asset?: string;
  conditionId?: string;
  slug?: string;
  /** Whether buy/sell/P/L numbers were confirmed from Polymarket Data API */
  confirmed?: boolean;
  /** Schedule placement that auto-triggered this trade (real schedule only). */
  placementId?: string;
  /**
   * How the buy was initiated. Manual / auto leftover fills do not count
   * in header Market (Trade triggers only) or on Schedule hour cells.
   */
  /** manual = quote-box; auto = phase; trigger = Market Trigger Trade (schedule-attributable). */
  source?: "manual" | "auto" | "trigger";
  /** Market Trigger id when source is trigger — Trade card stats follow this card's settlement. */
  triggerId?: string;
  /** Trigger title at fill time (Positions UI). */
  triggerName?: string;
  /** Exit path for trigger sells: tp / sl (held uses window-end via status win/loss). */
  triggerExitReason?: "tp" | "sl";
  /**
   * Buy fill landed outside the trigger Ask band (or oversized vs Start Shares).
   * Still uses normal Sell / hold-to-settlement; Positions UI shows "Trigger Miss".
   */
  triggerMiss?: boolean;
  /**
   * Server-side Trigger Demo card (no CLOB order). Excluded from Trade / Schedule /
   * Market P/L; credits trigger.demoStats instead of Trade live stats.
   */
  demo?: boolean;
}

/** Live real-trade aggregates for a schedule placement card. */
export interface PlacementLiveStats {
  placementId: string;
  hasData: boolean;
  green: number;
  red: number;
  blue: number;
  pnl: number;
  /** True once the placement has started at least one window (locked until removed). */
  locked: boolean;
}

/** Hour-slot aggregates for one UTC weekday×hour (latest calendar day for that slot: Trigger + legacy phase). */
export interface ScheduleHourSlotStats {
  day: string; // mon..sun
  hour: number; // 0-23
  green: number;
  red: number;
  blue: number;
  stopLoss: number;
  pnl: number;
  hasData: boolean;
}

/** Per CLOB order style inside Market → Trade fill success. */
export interface FillSuccessKindPublicStats {
  attempts: number;
  successes: number;
  /** 0–100; null when there are no attempts for this kind. */
  ratePct: number | null;
}

export interface FillSuccessPublicStats {
  attempts: number;
  successes: number;
  /** 0–100; null when there are no countable attempts in the rolling window. */
  ratePct: number | null;
  cutoffUtc: number;
  /** FAK / FOK / GTD breakdown (partial fill = success). */
  byKind: {
    FAK: FillSuccessKindPublicStats;
    FOK: FillSuccessKindPublicStats;
    GTD: FillSuccessKindPublicStats;
  };
}

/** Trigger card BUY/SELL highlight snapshot for SSE (mirrors Mongo triggers.liveUi). */
export type TriggerLiveUiPublic = {
  side: "up" | "down";
  buy: { price: number; shares: number; atMs?: number } | null;
  sell: { price: number; shares: number; atMs?: number } | null;
  updatedAt: string;
} | null;

export interface TradingPublicState {
  config: TradingConfig;
  positions: { up: LiveSidePosition | null; down: LiveSidePosition | null };
  /**
   * Open / pending-confirm cards only (SSE). Settled gallery: GET /api/trading/positions.
   */
  positionCards: TradingPositionCard[];
  /**
   * True after Mongo live-stat / position-card hydration finished (success or fail).
   * Clients should wait for this before painting the Positions list to avoid a jump.
   */
  positionCardsReady: boolean;
  /** Per trigger id — BUY/SELL highlight from Mongo (Demo + Trade). */
  triggerLiveUi?: Record<string, TriggerLiveUiPublic>;
  /**
   * Prefer GET /api/schedule-placement-stats — usually empty on SSE (client REST + cache).
   */
  placementStats: PlacementLiveStats[];
  /** Increments when a settled trade is written — clients refetch REST aggregates. */
  statsRevision?: number;
  /**
   * Optional Trigger Trade hour-slot stats (latest calendar day per weekday×hour).
   * Prefer GET /api/schedule-hour-stats; may be omitted from SSE snapshots.
   */
  hourSlotStats?: ScheduleHourSlotStats[];
  /**
   * Prefer GET /api/trading/session-memory?mode=live — usually empty on SSE.
   */
  sessionTotals: {
    green: number;
    red: number;
    blue: number;
    pnl: number;
    hasData: boolean;
  };
  /**
   * Latest finished auto-engine window (preview or mirrored) — client accumulates
   * into local "Demo update". Null when Auto Trade is off.
   */
  demoLastWindow: SimLastWindow | null;
  quoteLocks: SimQuoteLocks;
  markers: SimMarker[];
  phaseSetup: TradingPhaseSetup | null;
  phasesVisible: boolean;
  phasesEditable: boolean;
  scheduleTitle: string | null;
  scheduleSetupId: string | null;
  quotesEnabled: boolean;
  previewMode: boolean;
  /**
   * Rolling ~7-day CLOB fill success by order kind (buys + sells; any size = success).
   * GTD counts only when the limit was touched while live.
   */
  fillSuccess: FillSuccessPublicStats;
}

export interface EnrichedLiveWindowState extends LiveWindowState {
  sim: SimPublicState;
  trading: TradingPublicState | null;
}
