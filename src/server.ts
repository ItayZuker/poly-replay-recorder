import "dotenv/config";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { closeMongoClient } from "./db/mongo-client.js";
import { getDataDir, initStorage, ensureMarketDirs } from "./db/data-dir.js";
import {
  listMarkets,
  getMarket,
  updateMarket,
} from "./db/market-repository.js";
import { SEED_MARKETS } from "./collections.js";
import { recordingManager } from "./recording-manager.js";
import { startArchiveScheduler, stopArchiveScheduler } from "./archive-service.js";
import { logService } from "./log-service.js";
import { getWeekCoverage } from "./week-coverage.js";

const PORT = Number(process.env.PORT) || 3849;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseSeries(raw: unknown): string {
  const series = String(raw ?? "").trim();
  if (!SEED_MARKETS.some((m) => m.series === series)) {
    throw new Error("Unknown series");
  }
  return series;
}

async function main(): Promise<void> {
  await initStorage();
  await Promise.all(SEED_MARKETS.map((m) => ensureMarketDirs(m.series)));

  const app = express();
  app.use(express.json({ limit: "32kb" }));
  app.use(express.static(path.join(__dirname, "..", "public")));

  app.get("/api/health", (_req, res) => {
    res.json({
      ok: true,
      role: "recorder",
      dataDir: getDataDir(),
    });
  });

  app.get("/api/markets", async (_req, res) => {
    try {
      const markets = await listMarkets();
      res.json({
        markets: markets.map((m) => ({
          _id: m._id,
          label: m.label,
          timeframeMinutes: m.timeframeMinutes,
          recordingEnabled: m.recordingEnabled === true,
        })),
        dataDir: getDataDir(),
      });
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.patch("/api/markets/:series/recording", async (req, res) => {
    try {
      const series = parseSeries(req.params.series);
      const enabled = req.body?.recordingEnabled === true;
      const updated = await updateMarket(series, { recordingEnabled: enabled });
      if (!updated) {
        res.status(404).json({ error: "Market not found" });
        return;
      }
      await recordingManager.sync();
      res.json({
        _id: updated._id,
        recordingEnabled: updated.recordingEnabled === true,
      });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.get("/api/week-coverage", async (req, res) => {
    try {
      const series = parseSeries(req.query.series);
      const market = await getMarket(series);
      if (!market) {
        res.status(404).json({ error: "Market not found" });
        return;
      }
      const coverage = await getWeekCoverage(series);
      res.json({
        ...coverage,
        recordingEnabled: market.recordingEnabled === true,
        label: market.label,
      });
    } catch (err) {
      res.status(400).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  app.listen(PORT, () => {
    logService.info("server", `Poly Recorder listening on http://localhost:${PORT}`);
    logService.info("server", `DATA_DIR=${getDataDir()}`);
    void recordingManager.sync().catch((err) => {
      logService.warn("recorder", `Initial sync failed: ${String(err)}`);
    });
    startArchiveScheduler();
    setInterval(() => {
      void recordingManager.sync().catch((err) => {
        logService.warn("recorder", `Periodic sync failed: ${String(err)}`);
      });
    }, 30_000).unref?.();
  });

  const shutdown = async () => {
    stopArchiveScheduler();
    recordingManager.stopAll();
    await closeMongoClient().catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());
}

main().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
