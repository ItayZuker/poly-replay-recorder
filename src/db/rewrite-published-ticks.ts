import { compress } from "@mongodb-js/zstd";
import fs from "fs/promises";
import path from "path";
import { getDataDir } from "./data-dir.js";
import { readJsonlZstText } from "./tick-zst.js";
import { TickSlimParseError, slimJsonlDocument, tickKindFromPath, type TickFileKind } from "../tick-slim.js";

const ZSTD_LEVEL = 3;
const TICK_FILES = ["clob-raw.jsonl.zst", "chainlink.jsonl.zst"] as const;

export interface SeriesSlimStats {
  series: string;
  filesChanged: number;
  filesSkipped: number;
  linesIn: number;
  linesOut: number;
  bytesBefore: number;
  bytesAfter: number;
}

async function fileSize(filePath: string): Promise<number> {
  try {
    const st = await fs.stat(filePath);
    return st.isFile() ? st.size : 0;
  } catch {
    return 0;
  }
}

async function replaceWithTemp(target: string, tmp: string, sourceBytes: number): Promise<void> {
  const st = await fs.stat(tmp);
  if (sourceBytes > 0 && st.size === 0) {
    await fs.rm(tmp, { force: true });
    throw new Error(`Refusing empty replacement for ${target}`);
  }
  const bak = `${target}.bak`;
  await fs.rename(target, bak);
  try {
    await fs.rename(tmp, target);
  } catch (err) {
    await fs.rename(bak, target).catch(() => undefined);
    throw err;
  }
  await fs.rm(bak, { force: true });
}

export async function rewritePublishedTickFile(
  zstPath: string,
  kind: TickFileKind,
): Promise<{ status: "changed" | "skipped"; linesIn: number; linesOut: number; bytesBefore: number; bytesAfter: number }> {
  const bytesBefore = await fileSize(zstPath);
  const liveJsonl = zstPath.slice(0, -".zst".length);
  if ((await fileSize(liveJsonl)) > 0) {
    return { status: "skipped", linesIn: 0, linesOut: 0, bytesBefore, bytesAfter: bytesBefore };
  }

  let text: string;
  try {
    text = await readJsonlZstText(zstPath);
  } catch (err) {
    console.error(`[slim] ${zstPath}: decompress failed: ${err instanceof Error ? err.message : String(err)}`);
    return { status: "skipped", linesIn: 0, linesOut: 0, bytesBefore, bytesAfter: bytesBefore };
  }

  let slimmed: { text: string; linesIn: number; linesOut: number };
  try {
    slimmed = slimJsonlDocument(kind, text);
  } catch (err) {
    const detail = err instanceof TickSlimParseError || err instanceof Error ? err.message : String(err);
    console.error(`[slim] ${zstPath}: left unchanged (${detail})`);
    return { status: "skipped", linesIn: 0, linesOut: 0, bytesBefore, bytesAfter: bytesBefore };
  }

  if (slimmed.text === text) {
    return {
      status: "skipped",
      linesIn: slimmed.linesIn,
      linesOut: slimmed.linesOut,
      bytesBefore,
      bytesAfter: bytesBefore,
    };
  }

  if (bytesBefore > 0 && slimmed.text.length === 0) {
    console.error(`[slim] ${zstPath}: left unchanged (slim output empty)`);
    return { status: "skipped", linesIn: slimmed.linesIn, linesOut: 0, bytesBefore, bytesAfter: bytesBefore };
  }

  const tmp = `${zstPath}.slim.tmp`;
  try {
    const packed = await compress(Buffer.from(slimmed.text), ZSTD_LEVEL);
    await fs.writeFile(tmp, packed);
    await replaceWithTemp(zstPath, tmp, bytesBefore);
  } catch (err) {
    await fs.rm(tmp, { force: true }).catch(() => undefined);
    console.error(`[slim] ${zstPath}: left unchanged (${err instanceof Error ? err.message : String(err)})`);
    return { status: "skipped", linesIn: slimmed.linesIn, linesOut: slimmed.linesOut, bytesBefore, bytesAfter: bytesBefore };
  }

  const bytesAfter = await fileSize(zstPath);
  return {
    status: "changed",
    linesIn: slimmed.linesIn,
    linesOut: slimmed.linesOut,
    bytesBefore,
    bytesAfter,
  };
}

export async function rewritePublishedTicks(dataDir = getDataDir()): Promise<SeriesSlimStats[]> {
  const entries = await fs.readdir(dataDir, { withFileTypes: true });
  const stats: SeriesSlimStats[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const ticksRoot = path.join(dataDir, entry.name, "ticks");
    let windows: string[] = [];
    try {
      windows = await fs.readdir(ticksRoot);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === "ENOENT") continue;
      throw err;
    }

    const series: SeriesSlimStats = {
      series: entry.name,
      filesChanged: 0,
      filesSkipped: 0,
      linesIn: 0,
      linesOut: 0,
      bytesBefore: 0,
      bytesAfter: 0,
    };

    const files: string[] = [];
    for (const windowName of windows) {
      for (const name of TICK_FILES) {
        files.push(path.join(ticksRoot, windowName, name));
      }
    }

    const queue = files.slice();
    const workers = Array.from({ length: 4 }, async () => {
      for (;;) {
        const file = queue.pop();
        if (!file) return;
        if ((await fileSize(file)) === 0 && !(await fs.stat(file).then(() => true).catch(() => false))) {
          continue;
        }
        const kind = tickKindFromPath(file);
        if (!kind) continue;
        try {
          await fs.stat(file);
        } catch {
          continue;
        }
        const result = await rewritePublishedTickFile(file, kind);
        if (result.status === "changed") series.filesChanged += 1;
        else series.filesSkipped += 1;
        series.linesIn += result.linesIn;
        series.linesOut += result.linesOut;
        series.bytesBefore += result.bytesBefore;
        series.bytesAfter += result.bytesAfter;
      }
    });
    await Promise.all(workers);

    if (series.filesChanged === 0 && series.filesSkipped === 0 && series.bytesBefore === 0) continue;
    console.log(
      `[slim] ${series.series}: files changed ${series.filesChanged}, files skipped ${series.filesSkipped}, lines in ${series.linesIn}, lines out ${series.linesOut}, bytes before ${series.bytesBefore}, bytes after ${series.bytesAfter}`,
    );
    stats.push(series);
  }
  return stats;
}
