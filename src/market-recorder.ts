import { clobMarketFeed } from "./clob-market-feed.js";
import { ASSET_STALL_TIMEOUT_MS, chainlinkPriceFeed } from "./chainlink-price-feed.js";
import {
  fetchOfficialWindowResolution,
  hasOfficialWindowOutcome,
  OFFICIAL_RESOLVE_MAX_WAIT_MS,
  waitForOfficialWindowResolution,
  type OfficialWindowResolution,
} from "./official-window-resolution.js";
import {
  assetGapOrUnset,
  roundPolymarketAssetPrice,
  roundPolymarketAssetPriceMaybe,
} from "./polymarket-display-price.js";
import {
  fetchCurrentUpDownMarket,
  fetchCurrentUpDownMarketWithRetry,
  fetchMarketPairFromSlug,
  fetchUpDownMarketAtWindow,
  parseMarketSeries,
  NEXT_WINDOW_PREFETCH_SEC,
} from "./market-pair.js";
import { pickDisplayPrice } from "./quote-price.js";
import { makeStoredTickId, roundTo4 } from "./tick-compact.js";
import {
  createWindowDynamicsTracker,
  finalizeWindowDynamics,
  isFlatPriceWindow,
  UNUSABLE_PRICE_GAP_MS,
  updateWindowDynamics,
  type WindowDynamicsTracker,
} from "./window-dynamics.js";
import { logService } from "./log-service.js";
import type {
  ChainlinkTickDocument,
  ClobRawTickDocument,
  MarketDocument,
  WindowHitRecord,
} from "./types.js";
import {
  ensureWindowTickDir,
  insertChainlinkTicks,
  insertClobRawTicks,
} from "./db/tick-repository.js";
import {
  deleteWindowJsonlTicks,
  publishWindowTicksToZst,
  windowHasLiveJsonlTicks,
  windowHasReplayZst,
} from "./db/tick-zst.js";
import {
  getRecordedWindow,
  listRecordedWindows,
  saveRecordedWindow,
} from "./db/recorded-window-repository.js";
import {
  appendPtbHistory,
  recordingPtbFields,
} from "./ptb-history.js";
import { deleteRecordedWindowSummary } from "./db/recorded-window-mongo-repository.js";
import { pruneColdMarketData } from "./db/tick-archive.js";
import {
  marketWindowsDir,
  windowTicksDir,
} from "./db/data-dir.js";
import fs from "fs/promises";
import path from "path";

const TICK_FLUSH_MS = 1_500;
const POLL_MS = 500;
/** No raw CLOB or Chainlink ticks into the active window for this long → resume both sockets. */
const RECORDING_SILENCE_MS = 60_000;
/** No raw CLOB messages for this long (Chainlink may still be flowing) → resume CLOB socket. */
const CLOB_SILENCE_MS = 20_000;
/** No raw Chainlink ticks for this long (CLOB may still be flowing) → RTDS reconnect. */
const CHAINLINK_SILENCE_MS = ASSET_STALL_TIMEOUT_MS;
/** Ignore silence right after a window opens (pair/book may still be warming up). */
const WINDOW_START_GRACE_MS = 20_000;
const OFFICIAL_RESOLVE_POLL_MS = 30_000;
/** Official RTDS TWAP may update a bit slower than raw prints. */
const OPEN_TWAP_MAX_AGE_MS = 90_000;

type StateChangeListener = (series: string) => void;

/** Records CLOB book ticks and Chainlink asset ticks in separate collections. */
export class MarketRecorder {
  private readonly market: MarketDocument;
  private readonly onStateChange: StateChangeListener | null;
  private interval: ReturnType<typeof setInterval> | null = null;
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private clobRawUnsub: (() => void) | null = null;
  private chainlinkUnsub: (() => void) | null = null;
  private chainlinkTwapUnsub: (() => void) | null = null;
  private sampleInFlight = false;
  /** Serialize window finalize so a late pair-fetch cannot double-roll. */
  private rollingInFlight: Promise<void> | null = null;
  private windowFetchPending = false;
  private fastRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private finalizedWindowStarts = new Set<number>();
  /** Windows abandoned after Chainlink stall — do not re-open or save. */
  private discardedWindowStarts = new Set<number>();
  private activeWindow: WindowHitRecord | null = null;
  private activeYesTokenId: string | null = null;
  private activeNoTokenId: string | null = null;
  private dynamicsTracker: WindowDynamicsTracker = createWindowDynamicsTracker();
  private clobRawBuffer: ClobRawTickDocument[] = [];
  private chainlinkTickBuffer: ChainlinkTickDocument[] = [];
  private clobRawSeq = 0;
  private chainlinkSeq = 0;
  private windowTickCount = 0;
  private clobRawCount = 0;
  private chainlinkCount = 0;
  private assetPrices: { assetPrice?: number; prevCloseAsset?: number } = {};
  /** True once Gamma eventMetadata PTB/close were applied — stop following crypto-price open. */
  private gammaSettled = false;
  private prefetchedNextWindowStart: number | null = null;
  private nextWindowPrefetchInFlight = false;
  /** Wall-clock time of the last raw CLOB or Chainlink tick for the active window. */
  private lastUsefulTickAtMs = 0;
  /** Wall-clock time of the last raw CLOB message for the active window. */
  private lastClobTickAtMs = 0;
  /** Wall-clock time of the last live RTDS tick (not the opening stub). */
  private lastChainlinkTickAtMs = 0;
  private windowBeganAtMs = 0;
  /** True while captureEndPrices/finalizeWindow run — silence is expected (no in-window ticks). */
  private finalizing = false;
  /** False until Mongo hydrate finishes — do not snap start PTBs from live yet. */
  private headerReady = false;
  /** In-flight background Gamma polls keyed by windowStart (non-blocking). */
  private pendingOfficialResolves = new Map<number, Promise<void>>();
  private startedAtMs = 0;
  private lastSavedAtMs = 0;
  private lastPriceSampleAtMs = 0;
  private lastSampledPrice: number | undefined;
  private longestPriceGapMs = 0;
  private longestFlatStretchMs = 0;
  private currentFlatStartMs = 0;

  constructor(market: MarketDocument, onStateChange: StateChangeListener | null = null) {
    this.market = market;
    this.onStateChange = onStateChange;
  }

  getSeries(): string {
    return this.market._id;
  }

  getMarket(): MarketDocument {
    return this.market;
  }

  /**
   * True when an active window has gone too long without raw CLOB or Chainlink ticks.
   * Used by RecordingManager to resume both sockets. Does not invent ticks.
   */
  needsHealthRecovery(nowMs = Date.now()): boolean {
    if (!this.isActiveWindowEligibleForHealth(nowMs)) return false;
    const last = this.lastUsefulTickAtMs || this.windowBeganAtMs;
    if (!last) return false;
    return nowMs - last >= RECORDING_SILENCE_MS;
  }

  /**
   * True when raw CLOB messages have gone silent while the window is still open.
   * Chainlink-only flow must not mask a dead market WebSocket.
   */
  needsClobRecovery(nowMs = Date.now()): boolean {
    if (!this.isActiveWindowEligibleForHealth(nowMs)) return false;
    const last = this.lastClobTickAtMs || this.windowBeganAtMs;
    if (!last) return false;
    return nowMs - last >= CLOB_SILENCE_MS;
  }

  /**
   * True when raw Chainlink has gone silent while the window is still open.
   * REST prices must not mask a dead RTDS.
   */
  needsChainlinkRecovery(nowMs = Date.now()): boolean {
    if (!this.isActiveWindowEligibleForHealth(nowMs)) return false;
    const { asset } = parseMarketSeries(this.market._id);
    if (chainlinkPriceFeed.isRawFresh(asset, CHAINLINK_SILENCE_MS)) return false;
    return true;
  }

  getLastSavedAtMs(): number {
    return this.lastSavedAtMs;
  }

  getStartedAtMs(): number {
    return this.startedAtMs;
  }

  /** Re-subscribe the active window's YES/NO tokens after a CLOB reconnect. */
  resubscribeActiveClobTokens(): void {
    if (!this.activeYesTokenId || !this.activeNoTokenId) return;
    clobMarketFeed.ensureSubscribed([this.activeYesTokenId, this.activeNoTokenId]);
  }

  /** Resume the CLOB market socket and re-subscribe this window's tokens. */
  resumeClobSocket(): void {
    clobMarketFeed.resumeSocket();
    this.resubscribeActiveClobTokens();
  }

  /** Resume the Chainlink RTDS socket. */
  resumeChainlinkSocket(): void {
    chainlinkPriceFeed.resumeSocket();
  }

  private isActiveWindowEligibleForHealth(nowMs: number): boolean {
    if (!this.interval || !this.activeWindow || this.finalizing) return false;
    if (nowMs >= this.activeWindow.windowEnd * 1000) return false;
    if (this.windowBeganAtMs > 0 && nowMs - this.windowBeganAtMs < WINDOW_START_GRACE_MS) {
      return false;
    }
    return true;
  }

  /** Health silence uses wall clock receipt time, not oracle/event stamps. */
  private noteUsefulTick(_eventTMs?: number): void {
    this.lastUsefulTickAtMs = Date.now();
  }

  private noteClobTick(_eventTMs?: number): void {
    const now = Date.now();
    this.lastClobTickAtMs = now;
    this.lastUsefulTickAtMs = now;
  }

  getActiveWindow(): WindowHitRecord | null {
    return this.activeWindow ? { ...this.activeWindow } : null;
  }

  /** True when this window has received both raw CLOB and Chainlink and both sockets are still fresh. */
  isLiveBothSockets(nowMs = Date.now()): boolean {
    if (!this.interval || !this.activeWindow || this.finalizing) return false;
    if (nowMs >= this.activeWindow.windowEnd * 1000) return false;
    if (this.clobRawCount < 1 || this.chainlinkCount < 1) return false;
    if (this.lastClobTickAtMs <= 0 || nowMs - this.lastClobTickAtMs >= CLOB_SILENCE_MS) {
      return false;
    }
    const { asset } = parseMarketSeries(this.market._id);
    return chainlinkPriceFeed.isRawFresh(asset, CHAINLINK_SILENCE_MS);
  }

  isRunning(): boolean {
    return this.interval != null;
  }

  start(): void {
    if (this.interval) return;
    this.startedAtMs = Date.now();

    const { asset } = parseMarketSeries(this.market._id);

    void this.collectSample().catch((err) => {
      logService.error("recorder", `${this.market._id}: ${String(err)}`);
    });

    this.interval = setInterval(() => {
      void this.collectSample().catch((err) => {
        logService.error("recorder", `${this.market._id}: ${String(err)}`);
      });
    }, POLL_MS);

    this.flushTimer = setInterval(() => {
      void this.flushTicks();
    }, TICK_FLUSH_MS);

    this.clobRawUnsub = clobMarketFeed.onRawMessage((event) => {
      this.recordClobRawMessage(event);
    });

    this.chainlinkUnsub = chainlinkPriceFeed.onUpdate((updatedAsset) => {
      if (updatedAsset !== asset) return;
      this.recordChainlinkTick();
    });
    this.chainlinkTwapUnsub = chainlinkPriceFeed.onTwapUpdate((updatedAsset) => {
      if (updatedAsset !== asset) return;
      this.latchOpenPtbs();
    });

    logService.success("recorder", `Recording started for ${this.market._id}`);
    void this.resumePendingTickPublish();
  }

  stop(): void {
    if (this.fastRetryTimer) {
      clearTimeout(this.fastRetryTimer);
      this.fastRetryTimer = null;
    }
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.clobRawUnsub) {
      this.clobRawUnsub();
      this.clobRawUnsub = null;
    }
    if (this.chainlinkUnsub) {
      this.chainlinkUnsub();
      this.chainlinkUnsub = null;
    }
    if (this.chainlinkTwapUnsub) {
      this.chainlinkTwapUnsub();
      this.chainlinkTwapUnsub = null;
    }
    void this.flushTicks();
    this.resetActiveWindow();
    this.finalizedWindowStarts.clear();
    this.discardedWindowStarts.clear();
    logService.info("recorder", `Recording stopped for ${this.market._id}`);
  }

  /**
   * Abandon the in-progress window without saving — used when Chainlink stalls
   * or the health watchdog detects recording silence.
   */
  discardActiveWindow(reason: string): void {
    if (!this.activeWindow || this.finalizing) return;

    const windowStart = this.activeWindow.windowStart;
    const windowEnd = this.activeWindow.windowEnd;
    this.discardedWindowStarts.add(windowStart);
    void deleteRecordedWindowSummary(this.market._id, windowStart).catch(() => undefined);

    this.clobRawBuffer = [];
    this.chainlinkTickBuffer = [];
    this.resetActiveWindow();
    void this.purgeWindowArtifacts(windowStart);

    logService.warn(
      "recorder",
      `Discarded window ${new Date(windowStart * 1000).toLocaleTimeString()}–${new Date(windowEnd * 1000).toLocaleTimeString()} for ${this.market._id} (${reason})`,
    );
    this.onStateChange?.(this.market._id);
  }

  private async purgeWindowArtifacts(windowStart: number): Promise<void> {
    const series = this.market._id;
    const targets = [
      windowTicksDir(series, windowStart),
      path.join(marketWindowsDir(series), `${windowStart}.json`),
    ];
    await Promise.all(
      targets.map(async (target) => {
        try {
          await fs.rm(target, { recursive: true, force: true });
        } catch {
          // best effort
        }
      }),
    );
  }

  private resetActiveWindow(): void {
    this.activeWindow = null;
    this.activeYesTokenId = null;
    this.activeNoTokenId = null;
    this.dynamicsTracker = createWindowDynamicsTracker();
    this.clobRawBuffer = [];
    this.chainlinkTickBuffer = [];
    this.clobRawSeq = 0;
    this.chainlinkSeq = 0;
    this.windowTickCount = 0;
    this.clobRawCount = 0;
    this.chainlinkCount = 0;
    this.assetPrices = {};
    this.gammaSettled = false;
    this.prefetchedNextWindowStart = null;
    this.nextWindowPrefetchInFlight = false;
    this.lastUsefulTickAtMs = 0;
    this.lastClobTickAtMs = 0;
    this.lastChainlinkTickAtMs = 0;
    this.windowBeganAtMs = 0;
    this.finalizing = false;
    this.headerReady = false;
    this.resetPricePathTracker();
  }

  private resetPricePathTracker(): void {
    this.lastPriceSampleAtMs = 0;
    this.lastSampledPrice = undefined;
    this.longestPriceGapMs = 0;
    this.longestFlatStretchMs = 0;
    this.currentFlatStartMs = 0;
  }

  private notePriceSample(price: number, atMs: number): void {
    if (this.lastPriceSampleAtMs > 0) {
      this.longestPriceGapMs = Math.max(
        this.longestPriceGapMs,
        atMs - this.lastPriceSampleAtMs,
      );
    } else if (this.activeWindow) {
      this.longestPriceGapMs = Math.max(
        this.longestPriceGapMs,
        atMs - this.activeWindow.windowStart * 1000,
      );
    }
    if (this.lastSampledPrice != null && price === this.lastSampledPrice) {
      if (this.currentFlatStartMs === 0) {
        this.currentFlatStartMs = this.lastPriceSampleAtMs || atMs;
      }
      this.longestFlatStretchMs = Math.max(
        this.longestFlatStretchMs,
        atMs - this.currentFlatStartMs,
      );
    } else {
      this.currentFlatStartMs = atMs;
    }
    this.lastSampledPrice = price;
    this.lastPriceSampleAtMs = atMs;
  }

  private isActiveWindowUnusablePricePath(): boolean {
    if (!this.activeWindow) return false;
    const durationMs =
      (this.activeWindow.windowEnd - this.activeWindow.windowStart) * 1000;
    if (durationMs <= 0) return true;
    const endMs = this.activeWindow.windowEnd * 1000;
    const tailGap =
      this.lastPriceSampleAtMs > 0 ? endMs - this.lastPriceSampleAtMs : durationMs;
    const longestGap = Math.max(this.longestPriceGapMs, tailGap);
    const flatMs =
      this.lastSampledPrice != null && this.currentFlatStartMs > 0
        ? Math.max(this.longestFlatStretchMs, endMs - this.currentFlatStartMs)
        : this.longestFlatStretchMs;
    if (longestGap >= UNUSABLE_PRICE_GAP_MS) return true;
    if (flatMs >= durationMs / 2) return true;
    return false;
  }

  private isInWindow(tMs: number): boolean {
    if (!this.activeWindow) return false;
    const startMs = this.activeWindow.windowStart * 1000;
    const endMs = this.activeWindow.windowEnd * 1000;
    return tMs >= startMs && tMs < endMs;
  }

  private nextClobRawId(windowStart: number): string {
    this.clobRawSeq += 1;
    return makeStoredTickId(windowStart, this.clobRawSeq).replace(":", ":raw:");
  }

  private nextChainlinkId(windowStart: number): string {
    this.chainlinkSeq += 1;
    return makeStoredTickId(windowStart, this.chainlinkSeq).replace(":", ":cl:");
  }

  private recordClobRawMessage(event: {
    tMs: number;
    payload: unknown;
    tokenIds: string[];
  }): void {
    if (!this.activeWindow || !this.activeYesTokenId || !this.activeNoTokenId) return;
    if (!this.isInWindow(event.tMs)) return;

    const relevant = event.tokenIds.some(
      (id) => id === this.activeYesTokenId || id === this.activeNoTokenId,
    );
    if (!relevant) return;

    this.clobRawBuffer.push({
      _id: this.nextClobRawId(this.activeWindow.windowStart),
      windowStart: this.activeWindow.windowStart,
      windowEnd: this.activeWindow.windowEnd,
      tMs: event.tMs,
      payload: event.payload,
    });
    this.clobRawCount += 1;
    this.noteClobTick(event.tMs);
    this.onStateChange?.(this.market._id);
  }

  private buildChainlinkTick(tMs: number): ChainlinkTickDocument | null {
    if (!this.activeWindow) return null;
    if (!this.isInWindow(tMs)) return null;

    const tick: ChainlinkTickDocument = {
      _id: this.nextChainlinkId(this.activeWindow.windowStart),
      windowStart: this.activeWindow.windowStart,
      windowEnd: this.activeWindow.windowEnd,
      tMs,
      ptbCrossings: this.activeWindow.ptbCrossings,
      minAssetPrice:
        this.activeWindow.minAssetPrice != null
          ? roundTo4(this.activeWindow.minAssetPrice)
          : undefined,
      maxAssetPrice:
        this.activeWindow.maxAssetPrice != null
          ? roundTo4(this.activeWindow.maxAssetPrice)
          : undefined,
    };

    if (this.assetPrices.assetPrice != null) {
      tick.assetPrice = roundTo4(this.assetPrices.assetPrice);
    }
    if (this.assetPrices.prevCloseAsset != null) {
      tick.prevCloseAsset = roundTo4(this.assetPrices.prevCloseAsset);
      tick.priceToBeatSource = this.gammaSettled ? "gamma" : "chainlink";
    }

    return tick;
  }

  private pushChainlinkTick(tMs: number): void {
    const tick = this.buildChainlinkTick(tMs);
    if (!tick) return;
    this.chainlinkTickBuffer.push(tick);
    this.chainlinkCount += 1;
    if (tick.assetPrice != null && Number.isFinite(tick.assetPrice)) {
      this.notePriceSample(tick.assetPrice, tMs);
    }
    this.noteUsefulTick(tMs);
    this.onStateChange?.(this.market._id);
  }

  private recordChainlinkTick(): void {
    if (!this.activeWindow) return;
    const { asset } = parseMarketSeries(this.market._id);
    if (!chainlinkPriceFeed.isRawFresh(asset, CHAINLINK_SILENCE_MS)) return;
    const live = chainlinkPriceFeed.getLivePrice(asset, {
      maxAgeMs: CHAINLINK_SILENCE_MS,
    });
    if (!live) return;

    this.applyAssetPrice(live.value);
    const tMs = live.timestampMs || Date.now();
    this.lastChainlinkTickAtMs = Date.now();
    this.pushChainlinkTick(tMs);
    this.latchOpenPtbs();
  }

  /** Update live asset price. Start PTBs latch only via latchOpenPtbs (not Gamma close). */
  private applyAssetPrice(assetPrice?: number): void {
    if (!this.activeWindow) return;

    const current = roundPolymarketAssetPriceMaybe(assetPrice);
    if (current != null) {
      this.activeWindow.assetPrice = current;
      this.assetPrices.assetPrice = current;
    }
    this.activeWindow.assetGap = assetGapOrUnset(
      this.activeWindow.assetPrice,
      this.activeWindow.prevCloseAsset,
    );

    updateWindowDynamics(
      this.activeWindow,
      this.dynamicsTracker,
      this.activeWindow.assetPrice,
      this.activeWindow.prevCloseAsset,
    );
  }

  /** Latch first Chainlink print once; does not overwrite TWAP or Gamma. */
  private tryApplyChainlinkOpen(openPrice?: number): boolean {
    if (!this.activeWindow) return false;
    if (this.activeWindow.ptbChainlink != null) return false;
    const ptb = roundPolymarketAssetPriceMaybe(openPrice);
    if (ptb == null) return false;
    this.activeWindow.ptbChainlink = ptb;
    this.activeWindow.prevCloseAsset = ptb;
    this.assetPrices.prevCloseAsset = ptb;
    const tSec = this.lastUsefulTickAtMs
      ? this.lastUsefulTickAtMs / 1000
      : Date.now() / 1000;
    this.activeWindow.ptbHistory = appendPtbHistory(this.activeWindow.ptbHistory, {
      t: tSec,
      ptb,
      source: "chainlink",
    });
    logService.info("recorder", `Chainlink PTB for ${this.market._id} @ ${ptb}`);
    return true;
  }

  /** Snap official 30s/60s TWAP and first Chainlink once each; persist if anything new. */
  private latchOpenPtbs(): void {
    if (!this.activeWindow || !this.headerReady) return;
    const { asset } = parseMarketSeries(this.market._id);
    let changed = false;

    if (this.activeWindow.ptbTwap30 == null) {
      const twap30 = chainlinkPriceFeed.getLiveTwap(asset, 30, {
        maxAgeMs: OPEN_TWAP_MAX_AGE_MS,
      });
      const value = roundPolymarketAssetPriceMaybe(twap30?.value);
      if (value != null) {
        this.activeWindow.ptbTwap30 = value;
        changed = true;
        logService.info("recorder", `30s TWAP PTB for ${this.market._id} @ ${value}`);
      }
    }
    if (this.activeWindow.ptbTwap60 == null) {
      const twap60 = chainlinkPriceFeed.getLiveTwap(asset, 60, {
        maxAgeMs: OPEN_TWAP_MAX_AGE_MS,
      });
      const value = roundPolymarketAssetPriceMaybe(twap60?.value);
      if (value != null) {
        this.activeWindow.ptbTwap60 = value;
        changed = true;
        logService.info("recorder", `60s TWAP PTB for ${this.market._id} @ ${value}`);
      }
    }
    if (this.activeWindow.ptbChainlink == null && chainlinkPriceFeed.isRawFresh(asset)) {
      const live = chainlinkPriceFeed.getLivePrice(asset, {
        maxAgeMs: CHAINLINK_SILENCE_MS,
      });
      if (this.tryApplyChainlinkOpen(live?.value)) changed = true;
    }

    if (changed) void this.persistActiveWindowHeader();
  }

  /** Gamma settle writes gammaPtb only — does not overwrite start PTBs. */
  private applyGammaPtb(priceToBeat?: number): void {
    if (!this.activeWindow) return;
    const ptb = roundPolymarketAssetPriceMaybe(priceToBeat);
    if (ptb == null) return;
    this.gammaSettled = true;
    this.activeWindow.gammaPtb = ptb;
    this.activeWindow.ptbHistory = appendPtbHistory(this.activeWindow.ptbHistory, {
      t: this.activeWindow.windowEnd,
      ptb,
      source: "gamma",
    });
  }

  private async persistActiveWindowHeader(): Promise<void> {
    if (!this.activeWindow) return;
    const win = this.activeWindow;
    const savedAt = new Date().toISOString();
    const doc = {
      windowStart: win.windowStart,
      windowEnd: win.windowEnd,
      savedAt,
      slug: win.slug,
      question: win.question,
      conditionId: win.conditionId,
      prevCloseAsset: win.ptbChainlink ?? win.prevCloseAsset,
      ...recordingPtbFields(win),
      assetPrice: win.assetPrice,
      assetGap: win.assetGap,
      windowOutcome: win.windowOutcome,
      yesPrice: win.yesPrice,
      noPrice: win.noPrice,
      ptbCrossings: win.ptbCrossings,
      minAssetPrice: win.minAssetPrice,
      maxAssetPrice: win.maxAssetPrice,
      assetRange: win.assetRange,
      rangeTop: win.rangeTop,
      rangeBottom: win.rangeBottom,
      tickCount: this.clobRawCount + this.chainlinkCount,
      clobRawCount: this.clobRawCount,
      clobBookCount: 0,
      chainlinkCount: this.chainlinkCount,
    };
    try {
      await saveRecordedWindow(this.market, doc);
    } catch (err) {
      logService.warn(
        "recorder",
        `Failed to persist window header for ${this.market._id}: ${String(err)}`,
      );
    }
  }

  /** Create Mongo stub at window open and latch any PTBs already on RTDS. */
  private async persistWindowStub(): Promise<void> {
    this.latchOpenPtbs();
    await this.persistActiveWindowHeader();
  }

  private async flushTicks(): Promise<void> {
    const rawBatch = this.clobRawBuffer.splice(0, this.clobRawBuffer.length);
    const chainlinkBatch = this.chainlinkTickBuffer.splice(0, this.chainlinkTickBuffer.length);
    if (rawBatch.length === 0 && chainlinkBatch.length === 0) return;

    const [rawResult, chainlinkResult] = await Promise.allSettled([
      rawBatch.length > 0 ? insertClobRawTicks(this.market, rawBatch) : Promise.resolve(),
      chainlinkBatch.length > 0
        ? insertChainlinkTicks(this.market, chainlinkBatch)
        : Promise.resolve(),
    ]);
    if (rawResult.status === "rejected") {
      logService.error(
        "recorder",
        `CLOB tick flush failed (${this.market._id}): ${String(rawResult.reason)}`,
      );
      this.clobRawBuffer.unshift(...rawBatch);
    }
    if (chainlinkResult.status === "rejected") {
      logService.error(
        "recorder",
        `Chainlink tick flush failed (${this.market._id}): ${String(chainlinkResult.reason)}`,
      );
      this.chainlinkTickBuffer.unshift(...chainlinkBatch);
    }
  }

  private async beginWindow(
    windowStart: number,
    windowEnd: number,
    meta: {
      slug?: string;
      question?: string;
      conditionId?: string;
      yesTokenId?: string;
      noTokenId?: string;
    },
  ): Promise<void> {
    if (this.activeWindow?.windowStart === windowStart) return;
    if (this.activeWindow) return;
    this.activeWindow = {
      windowStart,
      windowEnd,
      slug: meta.slug,
      question: meta.question,
      conditionId: meta.conditionId,
    };
    this.dynamicsTracker = createWindowDynamicsTracker();
    this.clobRawSeq = 0;
    this.chainlinkSeq = 0;
    this.windowTickCount = 0;
    this.clobRawCount = 0;
    this.chainlinkCount = 0;
    this.clobRawBuffer = [];
    this.chainlinkTickBuffer = [];
    this.assetPrices = {};
    this.gammaSettled = false;
    this.headerReady = false;
    const now = Date.now();
    this.windowBeganAtMs = now;
    this.lastUsefulTickAtMs = now;
    this.lastClobTickAtMs = now;
    this.lastChainlinkTickAtMs = 0;
    this.resetPricePathTracker();
    if (meta.yesTokenId && meta.noTokenId) {
      this.subscribeWindowTokens(meta.yesTokenId, meta.noTokenId);
    }
    await this.hydrateActiveWindowFromMongo(windowStart);
    this.headerReady = true;
    const { asset } = parseMarketSeries(this.market._id);
    if (chainlinkPriceFeed.isRawFresh(asset, CHAINLINK_SILENCE_MS)) {
      const live = chainlinkPriceFeed.getLivePrice(asset, {
        maxAgeMs: CHAINLINK_SILENCE_MS,
      });
      if (live) this.applyAssetPrice(live.value);
    }
    void ensureWindowTickDir(this.market._id, windowStart);
    await this.persistWindowStub();
    logService.info(
      "recorder",
      `Window started ${new Date(windowStart * 1000).toLocaleTimeString()} for ${this.market._id}`,
    );
  }

  /** Resume an in-progress Mongo stub without re-latching start PTBs from live. */
  private async hydrateActiveWindowFromMongo(windowStart: number): Promise<void> {
    if (!this.activeWindow || this.activeWindow.windowStart !== windowStart) return;
    const existing = await getRecordedWindow(this.market, windowStart);
    if (!existing) return;
    const win = this.activeWindow;
    if (existing.slug) win.slug = existing.slug;
    if (existing.question) win.question = existing.question;
    if (existing.conditionId) win.conditionId = existing.conditionId;
    if (existing.ptbChainlink != null) win.ptbChainlink = existing.ptbChainlink;
    if (existing.ptbTwap30 != null) win.ptbTwap30 = existing.ptbTwap30;
    if (existing.ptbTwap60 != null) win.ptbTwap60 = existing.ptbTwap60;
    if (existing.gammaPtb != null) {
      win.gammaPtb = existing.gammaPtb;
      this.gammaSettled = true;
    }
    if (existing.ptbHistory?.length) win.ptbHistory = existing.ptbHistory;
    const prev = existing.ptbChainlink ?? existing.prevCloseAsset;
    if (prev != null) {
      win.prevCloseAsset = prev;
      this.assetPrices.prevCloseAsset = prev;
    }
    if (existing.minAssetPrice != null) win.minAssetPrice = existing.minAssetPrice;
    if (existing.maxAssetPrice != null) win.maxAssetPrice = existing.maxAssetPrice;
    if (existing.assetRange != null) win.assetRange = existing.assetRange;
    if (existing.ptbCrossings != null) win.ptbCrossings = existing.ptbCrossings;
    if (existing.yesPrice != null) win.yesPrice = existing.yesPrice;
    if (existing.noPrice != null) win.noPrice = existing.noPrice;
    this.clobRawCount = existing.clobRawCount ?? 0;
    this.chainlinkCount = existing.chainlinkCount ?? 0;
    this.clobRawSeq = this.clobRawCount;
    this.chainlinkSeq = this.chainlinkCount;
    logService.info(
      "recorder",
      `Resumed window header ${new Date(windowStart * 1000).toLocaleTimeString()} for ${this.market._id}`,
    );
  }

  private async captureEndPrices(): Promise<void> {
    if (!this.activeWindow?.slug) return;

    // Caller (rollClosedWindow) owns `finalizing` for the whole rollover so this
    // wait cannot leave the recorder permanently stuck if finalize fails.
    // Gamma often lands minutes after windowEnd — do not block the next window;
    // a one-shot check here, then background poll up to 20 minutes after end.
    try {
      const pair = await fetchMarketPairFromSlug(this.activeWindow.slug);
      const yesInfo = clobMarketFeed.getCachedMarketInfo(pair.yesTokenId);
      const noInfo = clobMarketFeed.getCachedMarketInfo(pair.noTokenId);
      if (yesInfo) this.activeWindow.yesPrice = pickDisplayPrice(yesInfo).price;
      if (noInfo) this.activeWindow.noPrice = pickDisplayPrice(noInfo).price;

      const official = await fetchOfficialWindowResolution(this.activeWindow.slug);
      if (official) {
        this.applyAssetPrice(official.finalPrice);
        this.applyGammaPtb(official.priceToBeat);
        this.activeWindow.windowOutcome = official.outcome;
        if (official.yesPrice != null) this.activeWindow.yesPrice = official.yesPrice;
        if (official.noPrice != null) this.activeWindow.noPrice = official.noPrice;
        return;
      }

      logService.info(
        "recorder",
        `Gamma not ready for ${this.activeWindow.slug}; saving without windowOutcome (background poll ≤20m)`,
      );
    } catch {
      // best effort
    }
  }

  /** Restart-safe: finish zst publish / JSONL delete for windows still inside the 20m Gamma window. */
  private async resumePendingTickPublish(): Promise<void> {
    const nowSec = Math.floor(Date.now() / 1000);
    const recentAfter =
      nowSec - Math.floor(OFFICIAL_RESOLVE_MAX_WAIT_MS / 1000) - 60;
    let windows;
    try {
      windows = await listRecordedWindows(this.market);
    } catch (err) {
      logService.warn(
        "recorder",
        `Could not resume pending tick publish for ${this.market._id}: ${String(err)}`,
      );
      return;
    }

    for (const win of windows) {
      if (win.windowEnd < recentAfter) continue;
      if (await windowHasReplayZst(this.market._id, win.windowStart)) continue;

      if (hasOfficialWindowOutcome(win.windowOutcome)) {
        if (await windowHasLiveJsonlTicks(this.market._id, win.windowStart)) {
          const published = await publishWindowTicksToZst(
            this.market._id,
            win.windowStart,
          );
          logService.info(
            "recorder",
            `Resumed zst publish for ${this.market._id} ${win.windowStart} (${published})`,
          );
          this.onStateChange?.(this.market._id);
        }
        continue;
      }

      const deadlineSec = win.windowEnd + OFFICIAL_RESOLVE_MAX_WAIT_MS / 1000;
      if (nowSec >= deadlineSec) {
        await deleteWindowJsonlTicks(this.market._id, win.windowStart);
        continue;
      }

      if (typeof win.slug === "string" && win.slug.trim()) {
        this.scheduleBackgroundOfficialResolve({
          windowStart: win.windowStart,
          windowEnd: win.windowEnd,
          slug: win.slug,
        });
      }
    }
  }

  /** Non-blocking: poll Gamma every 30s until windowEnd+20m, then leave unset. */
  private scheduleBackgroundOfficialResolve(input: {
    windowStart: number;
    windowEnd: number;
    slug: string;
  }): void {
    const slug = input.slug.trim();
    if (!slug || this.pendingOfficialResolves.has(input.windowStart)) return;
    const work = this.runBackgroundOfficialResolve({ ...input, slug }).finally(() => {
      this.pendingOfficialResolves.delete(input.windowStart);
    });
    this.pendingOfficialResolves.set(input.windowStart, work);
  }

  private async runBackgroundOfficialResolve(input: {
    windowStart: number;
    windowEnd: number;
    slug: string;
  }): Promise<void> {
    const deadlineMs = input.windowEnd * 1000 + OFFICIAL_RESOLVE_MAX_WAIT_MS;
    const remainingMs = Math.max(0, deadlineMs - Date.now());
    if (remainingMs <= 0) {
      logService.warn(
        "recorder",
        `Official resolution past 20m for ${input.slug}; deleting JSONL`,
      );
      await deleteWindowJsonlTicks(this.market._id, input.windowStart);
      this.onStateChange?.(this.market._id);
      return;
    }

    logService.info(
      "recorder",
      `Background Gamma poll for ${input.slug} every 30s (up to ${Math.ceil(remainingMs / 1000)}s)`,
    );

    const official = await waitForOfficialWindowResolution(input.slug, {
      maxWaitMs: remainingMs,
      intervalMs: OFFICIAL_RESOLVE_POLL_MS,
    });

    if (!official) {
      logService.warn(
        "recorder",
        `Official resolution unavailable after 20m for ${input.slug}; deleting JSONL`,
      );
      await deleteWindowJsonlTicks(this.market._id, input.windowStart);
      this.onStateChange?.(this.market._id);
      return;
    }

    try {
      await this.applyOfficialResolutionToSavedWindow(input.windowStart, official);
      const published = await publishWindowTicksToZst(this.market._id, input.windowStart);
      logService.success(
        "recorder",
        `Background Gamma settled ${input.slug} → ${official.outcome} (${published})`,
      );
      this.onStateChange?.(this.market._id);
    } catch (err) {
      logService.error(
        "recorder",
        `Failed to apply background Gamma for ${input.slug}: ${String(err)}`,
      );
    }
  }

  private async applyOfficialResolutionToSavedWindow(
    windowStart: number,
    official: OfficialWindowResolution,
  ): Promise<void> {
    const existing = await getRecordedWindow(this.market, windowStart);
    if (!existing) {
      logService.warn(
        "recorder",
        `Background Gamma: no saved window ${windowStart} for ${this.market._id}`,
      );
      return;
    }
    const nextAsset =
      official.finalPrice != null
        ? roundPolymarketAssetPrice(official.finalPrice)
        : existing.assetPrice;
    const gammaPtb =
      official.priceToBeat != null && Number.isFinite(official.priceToBeat)
        ? roundPolymarketAssetPrice(official.priceToBeat)
        : existing.gammaPtb;
    const alreadySettled =
      hasOfficialWindowOutcome(existing.windowOutcome) &&
      existing.windowOutcome === official.outcome &&
      existing.gammaPtb === gammaPtb &&
      existing.assetPrice === nextAsset;
    if (alreadySettled) return;

    const ptbHistory =
      gammaPtb != null && Number.isFinite(gammaPtb)
        ? appendPtbHistory(existing.ptbHistory, {
            t: existing.windowEnd,
            ptb: gammaPtb,
            source: "gamma",
          })
        : existing.ptbHistory;

    const nextDoc = {
      windowStart: existing.windowStart,
      windowEnd: existing.windowEnd,
      savedAt: existing.savedAt,
      slug: existing.slug,
      question: existing.question,
      conditionId: existing.conditionId,
      assetPrice: nextAsset,
      prevCloseAsset: existing.prevCloseAsset,
      ...recordingPtbFields({
        ...existing,
        ptbHistory,
        gammaPtb,
      }),
      assetGap:
        nextAsset != null && existing.prevCloseAsset != null
          ? roundTo4(nextAsset - existing.prevCloseAsset)
          : existing.assetGap,
      windowOutcome: official.outcome,
      yesPrice: official.yesPrice ?? existing.yesPrice,
      noPrice: official.noPrice ?? existing.noPrice,
      ptbCrossings: existing.ptbCrossings,
      minAssetPrice: existing.minAssetPrice,
      maxAssetPrice: existing.maxAssetPrice,
      assetRange: existing.assetRange,
      rangeTop: existing.rangeTop,
      rangeBottom: existing.rangeBottom,
      uniqueTraders: existing.uniqueTraders,
      newWallets: existing.newWallets,
      knownWallets: existing.knownWallets,
      tickCount: existing.tickCount,
      clobRawCount: existing.clobRawCount,
      clobBookCount: existing.clobBookCount,
      chainlinkCount: existing.chainlinkCount,
    };
    await saveRecordedWindow(this.market, nextDoc);
    this.onStateChange?.(this.market._id);
  }

  /**
   * Close the active window after windowEnd: resolve official prices, then save.
   * Always clears `finalizing` / active window so a throw cannot stall recording forever.
   */
  private async rollClosedWindow(): Promise<void> {
    if (!this.activeWindow) return;
    const nowSec = Math.floor(Date.now() / 1000);
    if (nowSec < this.activeWindow.windowEnd) return;

    this.finalizing = true;
    try {
      await this.captureEndPrices();
      await this.finalizeWindow();
    } catch (err) {
      logService.error(
        "recorder",
        `Window rollover failed (${this.market._id}): ${String(err)}`,
      );
    } finally {
      // finalizeWindow normally clears this; belt-and-suspenders if it returned early
      // or threw before its own finally.
      if (this.finalizing || this.activeWindow) {
        this.resetActiveWindow();
      }
    }
  }

  /** Warm the next window's CLOB tokens on the socket before rollover (see NEXT_WINDOW_PREFETCH_SEC). */
  private async prefetchNextWindowTokens(): Promise<void> {
    if (!this.activeWindow || this.nextWindowPrefetchInFlight) return;

    const nowSec = Math.floor(Date.now() / 1000);
    const { windowStart, windowEnd } = this.activeWindow;
    if (nowSec < windowEnd - NEXT_WINDOW_PREFETCH_SEC) return;

    const nextStart = windowEnd;
    if (this.prefetchedNextWindowStart === nextStart) return;

    this.nextWindowPrefetchInFlight = true;
    try {
      const pair = await fetchUpDownMarketAtWindow(this.market._id, nextStart);
      clobMarketFeed.ensureSubscribed([pair.yesTokenId, pair.noTokenId]);
      this.prefetchedNextWindowStart = nextStart;
      logService.info(
        "recorder",
        `Prefetched next window ${new Date(nextStart * 1000).toLocaleTimeString()} tokens for ${this.market._id}`,
      );
    } catch (err) {
      logService.warn(
        "recorder",
        `Next-window prefetch failed (${this.market._id}): ${String(err)}`,
      );
    } finally {
      this.nextWindowPrefetchInFlight = false;
    }
  }

  /** Subscribe Yes/No tokens on the CLOB socket. Does not invent an opening book. */
  private subscribeWindowTokens(yesTokenId: string, noTokenId: string): void {
    this.activeYesTokenId = yesTokenId;
    this.activeNoTokenId = noTokenId;
    clobMarketFeed.ensureSubscribed([yesTokenId, noTokenId]);
  }

  private async pruneOldData(): Promise<void> {
    await pruneColdMarketData(this.market);
  }

  private async finalizeWindow(): Promise<void> {
    if (!this.activeWindow) return;

    const nowSec = Math.floor(Date.now() / 1000);
    if (nowSec < this.activeWindow.windowEnd) return;

    const windowStart = this.activeWindow.windowStart;
    if (this.finalizedWindowStarts.has(windowStart)) {
      this.resetActiveWindow();
      return;
    }

    this.finalizing = true;
    try {
      finalizeWindowDynamics(this.activeWindow);

      let record: WindowHitRecord = {
        windowStart: this.activeWindow.windowStart,
        windowEnd: this.activeWindow.windowEnd,
        slug: this.activeWindow.slug,
        question: this.activeWindow.question,
        conditionId: this.activeWindow.conditionId,
        assetPrice: this.activeWindow.assetPrice,
        prevCloseAsset: this.activeWindow.prevCloseAsset,
        ...recordingPtbFields(this.activeWindow),
        assetGap: this.activeWindow.assetGap,
        ptbCrossings: this.activeWindow.ptbCrossings,
        minAssetPrice: this.activeWindow.minAssetPrice,
        maxAssetPrice: this.activeWindow.maxAssetPrice,
        assetRange: this.activeWindow.assetRange,
        rangeTop: this.activeWindow.rangeTop,
        rangeBottom: this.activeWindow.rangeBottom,
        windowOutcome: this.activeWindow.windowOutcome,
        yesPrice: this.activeWindow.yesPrice,
        noPrice: this.activeWindow.noPrice,
        savedAt: new Date().toISOString(),
      };

      await this.flushTicks();

      if (this.clobRawCount === 0 && this.chainlinkCount === 0) {
        logService.warn(
          "recorder",
          `No raw CLOB or Chainlink ticks for ${this.market._id} @ ${new Date(windowStart * 1000).toLocaleTimeString()} — keeping Mongo header`,
        );
      }

      if (isFlatPriceWindow(record) || this.isActiveWindowUnusablePricePath()) {
        logService.warn(
          "recorder",
          `Thin price path for ${this.market._id} @ ${new Date(windowStart * 1000).toLocaleTimeString()} (gap or flat) — keeping recording`,
        );
      }

      const savedAt = record.savedAt ?? new Date().toISOString();
      const recordedDoc = {
        windowStart: record.windowStart,
        windowEnd: record.windowEnd,
        savedAt,
        slug: record.slug,
        question: record.question,
        conditionId: record.conditionId,
        assetPrice: record.assetPrice,
        prevCloseAsset: record.prevCloseAsset,
        ...recordingPtbFields(record),
        assetGap: record.assetGap,
        windowOutcome: record.windowOutcome,
        yesPrice: record.yesPrice,
        noPrice: record.noPrice,
        ptbCrossings: record.ptbCrossings,
        minAssetPrice: record.minAssetPrice,
        maxAssetPrice: record.maxAssetPrice,
        assetRange: record.assetRange,
        rangeTop: record.rangeTop,
        rangeBottom: record.rangeBottom,
        tickCount: this.clobRawCount + this.chainlinkCount,
        clobRawCount: this.clobRawCount,
        clobBookCount: 0,
        chainlinkCount: this.chainlinkCount,
      };
      await saveRecordedWindow(this.market, recordedDoc);

      await this.pruneOldData();

      this.finalizedWindowStarts.add(windowStart);
      this.lastSavedAtMs = Date.now();
      logService.success(
        "recorder",
        `Window saved ${new Date(windowStart * 1000).toLocaleTimeString()} (${this.clobRawCount} raw, ${this.chainlinkCount} chainlink)`,
      );
      this.onStateChange?.(this.market._id);

      if (hasOfficialWindowOutcome(recordedDoc.windowOutcome)) {
        const published = await publishWindowTicksToZst(this.market._id, windowStart);
        logService.info("recorder", `Published zst for ${this.market._id} (${published})`);
      } else if (
        typeof recordedDoc.slug === "string" &&
        recordedDoc.slug.trim()
      ) {
        this.scheduleBackgroundOfficialResolve({
          windowStart: recordedDoc.windowStart,
          windowEnd: recordedDoc.windowEnd,
          slug: recordedDoc.slug,
        });
      } else {
        await deleteWindowJsonlTicks(this.market._id, windowStart);
      }
    } catch (err) {
      logService.error("recorder", `Failed to finalize window (${this.market._id}): ${String(err)}`);
    } finally {
      this.finalizing = false;
      this.resetActiveWindow();
    }
  }

  private scheduleFastRetry(): void {
    if (this.fastRetryTimer || !this.interval) return;
    this.fastRetryTimer = setTimeout(() => {
      this.fastRetryTimer = null;
      void this.collectSample().catch((err) => {
        logService.error("recorder", `${this.market._id}: ${String(err)}`);
      });
    }, 1000);
  }

  private async fetchMarketPair(rolling = false) {
    if (rolling || this.windowFetchPending) {
      return fetchCurrentUpDownMarketWithRetry(this.market._id, {
        maxWaitMs: 30_000,
        intervalMs: 500,
      });
    }
    return fetchCurrentUpDownMarket(this.market._id);
  }

  private async collectSample(): Promise<void> {
    const rolled = await this.rollIfDue();
    if (rolled || !this.activeWindow) {
      await this.openCurrentWindowFromLivePair();
    }
    if (this.sampleInFlight) return;
    this.sampleInFlight = true;
    try {
      await this.runCollectSample();
    } finally {
      this.sampleInFlight = false;
    }
  }

  /** Finalize a closed window even when a pair fetch is still in flight. */
  private async rollIfDue(): Promise<boolean> {
    if (!this.activeWindow || this.finalizing) return false;
    if (Math.floor(Date.now() / 1000) < this.activeWindow.windowEnd) return false;
    if (this.rollingInFlight) {
      await this.rollingInFlight;
      return true;
    }
    const work = this.rollClosedWindow().finally(() => {
      if (this.rollingInFlight === work) this.rollingInFlight = null;
    });
    this.rollingInFlight = work;
    await work;
    return true;
  }

  /** Open the live window after a roll without waiting on a stuck 30s pair retry. */
  private async openCurrentWindowFromLivePair(): Promise<void> {
    if (this.activeWindow || this.finalizing) return;
    try {
      const pair = await fetchCurrentUpDownMarket(this.market._id);
      if (this.activeWindow || this.finalizing) return;
      if (pair.windowStart == null || pair.windowEnd == null) return;
      if (Math.floor(Date.now() / 1000) >= pair.windowEnd) return;
      if (
        this.discardedWindowStarts.has(pair.windowStart) ||
        this.finalizedWindowStarts.has(pair.windowStart)
      ) {
        return;
      }
      await this.beginWindow(pair.windowStart, pair.windowEnd, {
        question: pair.question,
        slug: pair.slug,
        conditionId: pair.conditionId,
        yesTokenId: pair.yesTokenId,
        noTokenId: pair.noTokenId,
      });
    } catch {
      // runCollectSample / fast-retry still opens the window
    }
  }

  private async runCollectSample(): Promise<void> {
    const nowSec = Math.floor(Date.now() / 1000);
    const rolling = await this.rollIfDue();
    if (rolling) this.windowFetchPending = true;
    else if (this.activeWindow) {
      void this.prefetchNextWindowTokens();
    }

    let pair;
    try {
      pair = await this.fetchMarketPair(rolling);
      this.windowFetchPending = false;
      if (this.fastRetryTimer) {
        clearTimeout(this.fastRetryTimer);
        this.fastRetryTimer = null;
      }
    } catch (err) {
      if (rolling || this.windowFetchPending) {
        this.windowFetchPending = true;
        this.scheduleFastRetry();
      }
      throw err;
    }

    if (pair.windowEnd != null && Math.floor(Date.now() / 1000) >= pair.windowEnd) {
      this.windowFetchPending = true;
      try {
        pair = await fetchCurrentUpDownMarketWithRetry(this.market._id, {
          maxWaitMs: 30_000,
          intervalMs: 500,
        });
        this.windowFetchPending = false;
      } catch (err) {
        this.scheduleFastRetry();
        throw err;
      }
      const freshNow = Math.floor(Date.now() / 1000);
      if (pair.windowEnd != null && freshNow >= pair.windowEnd) {
        this.windowFetchPending = true;
        this.scheduleFastRetry();
        return;
      }
    }

    clobMarketFeed.ensureSubscribed([pair.yesTokenId, pair.noTokenId]);

    if (this.activeWindow && pair.windowStart === this.activeWindow.windowStart) {
      const yesInfo = clobMarketFeed.getCachedMarketInfo(pair.yesTokenId);
      const noInfo = clobMarketFeed.getCachedMarketInfo(pair.noTokenId);
      if (yesInfo) this.activeWindow.yesPrice = pickDisplayPrice(yesInfo).price;
      if (noInfo) this.activeWindow.noPrice = pickDisplayPrice(noInfo).price;
    }

    if (pair.windowStart != null && pair.windowEnd != null) {
      const windowStart = pair.windowStart;
      const windowEnd = pair.windowEnd;

      if (this.activeWindow && this.activeWindow.windowStart !== windowStart) {
        if (nowSec >= this.activeWindow.windowEnd) {
          await this.rollIfDue();
        } else if (this.activeWindow.slug) {
          try {
            pair = await fetchMarketPairFromSlug(this.activeWindow.slug);
          } catch {
            // keep current pair
          }
        }
      }

      if (!this.activeWindow) {
        if (
          this.discardedWindowStarts.has(windowStart) ||
          this.finalizedWindowStarts.has(windowStart)
        ) {
          // Stall-damaged or already finished — wait for the next window.
        } else {
          await this.beginWindow(windowStart, windowEnd, {
            question: pair.question,
            slug: pair.slug,
            conditionId: pair.conditionId,
            yesTokenId: pair.yesTokenId,
            noTokenId: pair.noTokenId,
          });
        }
      } else if (this.activeWindow.windowStart === windowStart && pair.conditionId) {
        this.activeWindow.conditionId = pair.conditionId;
      }

      if (this.activeWindow && this.activeWindow.windowStart === windowStart) {
        this.subscribeWindowTokens(pair.yesTokenId, pair.noTokenId);
      }
    }

    this.onStateChange?.(this.market._id);
  }
}
