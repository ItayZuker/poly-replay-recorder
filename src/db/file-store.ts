import fs from "fs/promises";
import path from "path";
import { parseWindowStartFromFilename } from "./data-dir.js";

export async function readJsonFile<T>(filePath: string): Promise<T | null> {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw) as T;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return null;
    throw err;
  }
}

export async function writeJsonFile(filePath: string, value: unknown): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

export async function appendJsonlLines(filePath: string, docs: unknown[]): Promise<void> {
  if (docs.length === 0) return;
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const payload = docs.map((doc) => JSON.stringify(doc)).join("\n") + "\n";
  await fs.appendFile(filePath, payload, "utf8");
}

/** Rewrite a JSONL file in place (full replace). */
export async function writeJsonlFile(filePath: string, docs: unknown[]): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  if (docs.length === 0) {
    await fs.writeFile(filePath, "", "utf8");
    return;
  }
  const payload = docs.map((doc) => JSON.stringify(doc)).join("\n") + "\n";
  await fs.writeFile(filePath, payload, "utf8");
}

/** Dropbox / AV can stall reads indefinitely; don't block Replay on one file. */
const JSONL_READ_TIMEOUT_MS = 45_000;

async function readTextFileWithTimeout(filePath: string, timeoutMs: number): Promise<string> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      fs.readFile(filePath, "utf8"),
      new Promise<string>((_, reject) => {
        timer = setTimeout(() => {
          const err = new Error(`Timed out reading ${filePath} after ${timeoutMs}ms`) as NodeJS.ErrnoException;
          err.code = "ETIMEDOUT";
          reject(err);
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function readJsonlFile<T>(filePath: string, limit = Number.MAX_SAFE_INTEGER): Promise<T[]> {
  try {
    const raw = await readTextFileWithTimeout(filePath, JSONL_READ_TIMEOUT_MS);
    if (!raw.trim()) return [];
    const lines = raw.split("\n").filter((line) => line.trim().length > 0);
    return lines.slice(0, limit).map((line) => JSON.parse(line) as T);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return [];
    if (code === "ETIMEDOUT") {
      console.warn(`[data] ${String((err as Error).message)}`);
      return [];
    }
    throw err;
  }
}

export async function listWindowFiles(dir: string): Promise<string[]> {
  try {
    const entries = await fs.readdir(dir);
    return entries.filter((name) => /^\d+\.(json|jsonl)$/.test(name));
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return [];
    throw err;
  }
}

export async function deleteWindowFilesBefore(dir: string, cutoff: number): Promise<number> {
  const files = await listWindowFiles(dir);
  let deleted = 0;
  for (const filename of files) {
    const windowStart = parseWindowStartFromFilename(filename);
    if (windowStart == null || windowStart >= cutoff) continue;
    await fs.unlink(path.join(dir, filename));
    deleted += 1;
  }
  return deleted;
}
