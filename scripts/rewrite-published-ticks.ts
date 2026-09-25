import "dotenv/config";
import { rewritePublishedTicks } from "../src/db/rewrite-published-ticks.js";

const stats = await rewritePublishedTicks();
const totals = stats.reduce(
  (sum, row) => {
    sum.filesChanged += row.filesChanged;
    sum.filesSkipped += row.filesSkipped;
    sum.linesIn += row.linesIn;
    sum.linesOut += row.linesOut;
    sum.bytesBefore += row.bytesBefore;
    sum.bytesAfter += row.bytesAfter;
    return sum;
  },
  { filesChanged: 0, filesSkipped: 0, linesIn: 0, linesOut: 0, bytesBefore: 0, bytesAfter: 0 },
);
console.log(
  `[slim] total: files changed ${totals.filesChanged}, files skipped ${totals.filesSkipped}, lines in ${totals.linesIn}, lines out ${totals.linesOut}, bytes before ${totals.bytesBefore}, bytes after ${totals.bytesAfter}`,
);
