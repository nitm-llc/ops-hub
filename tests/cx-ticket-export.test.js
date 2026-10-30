// Tests for the Zendesk ticket export's date handling, label normalizing and summary rollup.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  zdDateRange,
  zdSearchQuery,
  zdNormalizeAnalysis,
  zdSummarize,
  zdToCsv,
} from "../src/cx-ticket-export.js";

test("date range is inclusive and rolls over month and year ends", () => {
  const r = zdDateRange("2025-11-17", "2025-12-31");
  assert.equal(r.from, "2025-11-17T00:00:00Z");
  assert.equal(r.to, "2026-01-01T00:00:00Z");
  assert.equal(zdSearchQuery(r), "type:ticket created>=2025-11-17 created<=2025-12-31");
});

test("bad or reversed dates are refused", () => {
  assert.throws(() => zdDateRange("11/17/2025", "2025-12-31"));
  assert.throws(() => zdDateRange("2025-12-31", "2025-11-17"));
  assert.throws(() => zdDateRange("2025-11-17' OR 1=1", "2025-12-31"));
});

test("unknown labels from the model fall back instead of being stored", () => {
  const a = zdNormalizeAnalysis({ category: "made_up", resolution: "teleported", product: "null", reason: "  " });
  assert.deepEqual(a, { category: "other", is_return: 0, product: null, reason: null, resolution: "unknown", summary: null });
});

test("return-related is decided by category, not by the model", () => {
  assert.equal(zdNormalizeAnalysis({ category: "damaged" }).is_return, 1);
  assert.equal(zdNormalizeAnalysis({ category: "lost_or_late", is_return: 1 }).is_return, 0);
});

test("summary groups products despite spelling differences and buckets weeks from Monday", () => {
  const rows = [
    { created_at: "2025-11-17T10:00:00Z", category: "damaged", is_return: 1, product: "Clinical Badge Cards", reason: "Bent corners.", resolution: "replacement_sent", channel: "email" },
    { created_at: "2025-11-23T23:00:00Z", category: "defective", is_return: 1, product: "clinical badge cards", reason: "bent corners", resolution: "refunded", channel: "email" },
    { created_at: "2025-11-24T09:00:00Z", category: "lost_or_late", is_return: 0, product: "Clinical Badge Cards", channel: "web" },
    { created_at: "2025-11-24T09:00:00Z", category: null },
  ];
  const s = zdSummarize(rows);
  assert.equal(s.total, 4);
  assert.equal(s.analyzed, 3);
  assert.equal(s.returns, 2);
  assert.deepEqual(s.by_week, [
    { week: "2025-11-17", total: 2, returns: 2 },
    { week: "2025-11-24", total: 1, returns: 0 },
  ]);
  assert.equal(s.products.length, 1);
  assert.equal(s.products[0].total, 3);
  assert.equal(s.products[0].returns, 2);
  assert.deepEqual(s.top_reasons, [{ key: "bent corners", count: 2 }]);
});

test("CSV quotes commas, quotes and line breaks", () => {
  const csv = zdToCsv([{ a: 'say "hi", ok', b: "line1\nline2", c: null }], ["a", "b", "c"]);
  assert.equal(csv, 'a,b,c\r\n"say ""hi"", ok","line1\nline2",');
});
