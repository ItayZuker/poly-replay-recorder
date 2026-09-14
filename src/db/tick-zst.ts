import { compress, decompress } from "@mongodb-js/zstd";
import fs from "fs/promises";
import {
  chainlinkTicksPath,
  chainlinkTicksZstPath,
  clobRawTicksPath,
  clobRawTicksZstPath,
} from "./data-dir.js";

const ZSTD_LEVEL = 3;

async function fileNonEmpty(filePath: string): Promise<boolean> {
  try {
    const st = await fs.stat(filePath);
    return st.isFile() && st.size > 0;
  } catch {
    return false;
  }
}

async function removeFile(filePath: string): Promise<void> {
  try {
    await fs.rm(filePath, { force: true });
  } catch {
    // best effort
  }
}

async function jsonlToZst(jsonlPath: string, zstPath: string): Promise<void> {
  const raw = await fs.readFile(jsonlPath);
  if (raw.length === 0) {
    throw new Error(`Empty JSONL: ${jsonlPath}`);
  }
  const packed = await compress(raw, ZSTD_LEVEL);
  await fs.writeFile(zstPath, packed);
}

/** Replay: decompress a `.jsonl.zst` to UTF-8 JSONL text. */
export async function readJsonlZstText(zstPath: string): Promise<string> {
  const packed = await fs.readFile(zstPath);
  const raw = await decompress(packed);
  return raw.toString("utf8");
}

export async function readJsonlZstLines<T>(zstPath: string): Promise<T[]> {
  try {
    const text = await readJsonlZstText(zstPath);
    if (!text.trim()) return [];
    return text
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .map((line) => JSON.parse(line) as T);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return [];
    throw err;
  }
}

/** After Gamma: write separate zst files and delete live JSONL. */
export async function publishWindowTicksToZst(
  series: string,
  windowStart: number,
): Promise<"published" | "skipped"> {
  const rawJsonl = clobRawTicksPath(series, windowStart);
  const chainJsonl = chainlinkTicksPath(series, windowStart);
  const [hasRaw, hasChain] = await Promise.all([
    fileNonEmpty(rawJsonl),
    fileNonEmpty(chainJsonl),
  ]);
  if (!hasRaw || !hasChain) {
    await deleteWindowJsonlTicks(series, windowStart);
    return "skipped";
  }
  await jsonlToZst(rawJsonl, clobRawTicksZstPath(series, windowStart));
  await jsonlToZst(chainJsonl, chainlinkTicksZstPath(series, windowStart));
  await deleteWindowJsonlTicks(series, windowStart);
  return "published";
}

export async function windowHasLiveJsonlTicks(
  series: string,
  windowStart: number,
): Promise<boolean> {
  const [hasRaw, hasChain] = await Promise.all([
    fileNonEmpty(clobRawTicksPath(series, windowStart)),
    fileNonEmpty(chainlinkTicksPath(series, windowStart)),
  ]);
  return hasRaw || hasChain;
}

export async function windowHasReplayZst(
  series: string,
  windowStart: number,
): Promise<boolean> {
  const [hasRaw, hasChain] = await Promise.all([
    fileNonEmpty(clobRawTicksZstPath(series, windowStart)),
    fileNonEmpty(chainlinkTicksZstPath(series, windowStart)),
  ]);
  return hasRaw && hasChain;
}

/** After 20m with no Gamma: drop live JSONL so Dest cannot replay it. */
export async function deleteWindowJsonlTicks(
  series: string,
  windowStart: number,
): Promise<void> {
  await Promise.all([
    removeFile(clobRawTicksPath(series, windowStart)),
    removeFile(chainlinkTicksPath(series, windowStart)),
  ]);
}
