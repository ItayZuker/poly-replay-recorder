import type { MarketDocument } from "./types.js";
import { listMarkets } from "./db/market-repository.js";
import { MarketRecorder } from "./market-recorder.js";
import { chainlinkPriceFeed } from "./chainlink-price-feed.js";
import { clobMarketFeed } from "./clob-market-feed.js";
import { logService } from "./log-service.js";
import { canProcessRecord } from "./recording-enabled.js";

const HEALTH_CHECK_MS = 5_000;
/** Avoid thrashing reconnects if silence persists across consecutive windows. */
const RECOVERY_COOLDOWN_MS = 90_000;
/** Recording is on but no window has been saved this long → warn (do not stop). */
const SAVE_STALL_MS = 10 * 60 * 1000;
const SAVE_STALL_LOG_EVERY_MS = 60_000;

export class RecordingManager {
  private recorders = new Map<string, MarketRecorder>();
  private onChange: ((series: string) => void) | null = null;
  private stallUnsub: (() => void) | null = null;
  private healthTimer: ReturnType<typeof setInterval> | null = null;
  private lastRecoveryAtMs = 0;
  private recoveryInFlight = false;
  private lastSaveStallLogAtMs = 0;

  setOnChange(listener: (series: string) => void): void {
    this.onChange = listener;
  }

  private ensureStallHandler(): void {
    if (this.stallUnsub) return;
    this.stallUnsub = chainlinkPriceFeed.onAssetStall((asset) => {
      logService.warn(
        "chainlink",
        `${asset.toUpperCase()} price stalled — reconnecting RTDS (keeping window)`,
      );
    });
  }

  private ensureHealthWatchdog(): void {
    if (this.healthTimer) return;
    this.healthTimer = setInterval(() => {
      void this.checkRecordingHealth();
      this.checkSaveStall();
    }, HEALTH_CHECK_MS);
  }

  /**
   * Reconnect dead feeds. Never discard the in-progress window — a blip must
   * not delete capture. Unusable paths are rejected at finalize instead.
   * - Full silence: reconnect both feeds (do not restart the recorder).
   * - Chainlink-only silence: reconnect RTDS.
   * - CLOB-only silence: reconnect market WS and re-subscribe tokens.
   */
  private async checkRecordingHealth(): Promise<void> {
    if (!canProcessRecord() || this.recorders.size === 0) return;
    if (this.recoveryInFlight) return;

    const now = Date.now();
    if (now - this.lastRecoveryAtMs < RECOVERY_COOLDOWN_MS) return;

    const fullSilence: MarketRecorder[] = [];
    const chainlinkSilence: MarketRecorder[] = [];
    const clobSilence: MarketRecorder[] = [];
    for (const recorder of this.recorders.values()) {
      if (recorder.needsHealthRecovery(now)) {
        fullSilence.push(recorder);
        continue;
      }
      if (recorder.needsChainlinkRecovery(now)) {
        chainlinkSilence.push(recorder);
      }
      if (recorder.needsClobRecovery(now)) {
        clobSilence.push(recorder);
      }
    }
    if (
      fullSilence.length === 0 &&
      chainlinkSilence.length === 0 &&
      clobSilence.length === 0
    ) {
      return;
    }

    this.recoveryInFlight = true;
    this.lastRecoveryAtMs = now;
    try {
      if (fullSilence.length > 0) {
        const labels = fullSilence.map((r) => r.getSeries()).join(", ");
        logService.warn(
          "recorder",
          `Recording silence on ${labels} — reconnecting feeds (keeping window)`,
        );
        chainlinkPriceFeed.forceReconnect();
        clobMarketFeed.forceReconnect();
        for (const recorder of fullSilence) {
          recorder.resubscribeActiveClobTokens();
        }
      }

      if (chainlinkSilence.length > 0) {
        const labels = chainlinkSilence.map((r) => r.getSeries()).join(", ");
        logService.warn(
          "recorder",
          `Chainlink silence on ${labels} — reconnecting RTDS (keeping window)`,
        );
        chainlinkPriceFeed.forceReconnect();
      }

      if (clobSilence.length > 0) {
        const labels = clobSilence.map((r) => r.getSeries()).join(", ");
        logService.warn(
          "recorder",
          `CLOB silence on ${labels} — reconnecting market WebSocket (keeping window)`,
        );
        clobMarketFeed.forceReconnect();
        for (const recorder of clobSilence) {
          recorder.resubscribeActiveClobTokens();
        }
      }
    } catch (err) {
      logService.error("recorder", `Health recovery failed: ${String(err)}`);
    } finally {
      this.recoveryInFlight = false;
    }
  }

  private checkSaveStall(): void {
    if (!canProcessRecord() || this.recorders.size === 0) return;
    const now = Date.now();
    if (now - this.lastSaveStallLogAtMs < SAVE_STALL_LOG_EVERY_MS) return;
    const stalled = [...this.recorders.values()].filter((recorder) => {
      const last = recorder.getLastSavedAtMs() || recorder.getStartedAtMs();
      return last > 0 && now - last >= SAVE_STALL_MS;
    });
    if (stalled.length === 0) return;
    this.lastSaveStallLogAtMs = now;
    const labels = stalled.map((r) => r.getSeries()).join(", ");
    logService.warn(
      "recorder",
      `No window saved for ${Math.round(SAVE_STALL_MS / 60_000)}m on ${labels} — recording is still on`,
    );
  }

  /** Start/stop recorders from each market's `recordingEnabled` flag. */
  async sync(): Promise<void> {
    if (!canProcessRecord()) {
      this.stopAll();
      return;
    }

    this.ensureStallHandler();
    this.ensureHealthWatchdog();
    const markets = await listMarkets();
    const enabled = new Set(
      markets.filter((m) => m.recordingEnabled).map((m) => m._id),
    );

    for (const [series, recorder] of this.recorders) {
      if (!enabled.has(series)) {
        recorder.stop();
        this.recorders.delete(series);
        logService.info("recorder", `Recording stopped for ${series}`);
      }
    }

    for (const market of markets) {
      if (!market.recordingEnabled) continue;
      if (this.recorders.has(market._id)) continue;
      const recorder = new MarketRecorder(market, (s) => this.onChange?.(s));
      recorder.start();
      this.recorders.set(market._id, recorder);
      logService.info("recorder", `Recording started for ${market._id}`);
    }
  }

  async refreshMarket(market: MarketDocument): Promise<void> {
    if (!canProcessRecord()) {
      const existing = this.recorders.get(market._id);
      if (existing) {
        existing.stop();
        this.recorders.delete(market._id);
      }
      return;
    }

    this.ensureStallHandler();
    this.ensureHealthWatchdog();
    const existing = this.recorders.get(market._id);
    if (market.recordingEnabled) {
      if (existing) {
        existing.stop();
        this.recorders.delete(market._id);
      }
      const recorder = new MarketRecorder(market, (s) => this.onChange?.(s));
      recorder.start();
      this.recorders.set(market._id, recorder);
      logService.info("recorder", `Recording started for ${market._id}`);
    } else if (existing) {
      existing.stop();
      this.recorders.delete(market._id);
      logService.info("recorder", `Recording stopped for ${market._id}`);
    }
  }

  getRecorder(series: string): MarketRecorder | undefined {
    return this.recorders.get(series);
  }

  getActiveWindow(series: string) {
    return this.recorders.get(series)?.getActiveWindow() ?? null;
  }

  stopAll(): void {
    if (this.stallUnsub) {
      this.stallUnsub();
      this.stallUnsub = null;
    }
    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = null;
    }
    this.recoveryInFlight = false;
    for (const recorder of this.recorders.values()) {
      recorder.stop();
    }
    this.recorders.clear();
  }
}

export const recordingManager = new RecordingManager();
