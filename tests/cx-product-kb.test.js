// Tests for the product knowledge base's pricing and prompts. The two prompts must match
// NITM's existing Fin-profile tool word for word, so these pin the exact text.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  cxModelPricing,
  cxReadPrompt,
  cxFinProfilePrompt,
  cxProfileLength,
  LENGTH_RULES,
} from "../src/cx-product-kb.js";

const perM = (p) => ({ in: +(p.in * 1e6).toFixed(2), out: +(p.out * 1e6).toFixed(2) });

test("pricing matches each specific model ID", () => {
  assert.deepEqual(perM(cxModelPricing("claude-opus-5-5")), { in: 4, out: 20 });
  assert.deepEqual(perM(cxModelPricing("claude-opus-4-8")), { in: 5, out: 25 });
  assert.deepEqual(perM(cxModelPricing("claude-sonnet-5-5")), { in: 2, out: 10 });
  assert.deepEqual(perM(cxModelPricing("claude-haiku-4-5-20251001")), { in: 1, out: 5 });
});

test("Haiku 5.5 switches rate card above a 100k-token prompt", () => {
  assert.deepEqual(perM(cxModelPricing("claude-haiku-5-5", 100000)), { in: 0.1, out: 0.5 });
  assert.deepEqual(perM(cxModelPricing("claude-haiku-5-5", 100001)), { in: 0.5, out: 2.5 });
});

test("unknown models are priced Opus-tier, never cheaper", () => {
  assert.deepEqual(perM(cxModelPricing("claude-something-new")), { in: 5, out: 25 });
});

test("reading prompt fills pages/images and numbers", () => {
  assert.equal(
    cxReadPrompt("pages", 1, 8, 42),
    "These are pages 1–8 of 42 from one Nurse In The Making product (study guide pages, flashcards, or badge reference cards). For each card or page, write its title, then list every section heading, topic, body system, drug class, scale, procedure, and named concept it covers. Topic names only: do not copy numeric values, ranges, doses, or formulas. Use short plain-text bullets grouped under each title. No intro or closing text."
  );
  assert.match(cxReadPrompt("images", 11, 14, 14), /^These are images 11–14 of 14 from/);
});

test("Fin profile prompt: notes, extra notes and length rule", () => {
  const p = cxFinProfilePrompt({ name: "Badge Card Pack", description: "21 cards.", length: "detailed", pageNotes: "NOTES" });
  assert.ok(p.startsWith("You are writing a product knowledge-base entry for Fin, the AI customer-support agent for Nurse In The Making (NITM)."));
  assert.ok(p.includes("Here are notes taken from every page/card of the product, in order:\n\nNOTES\n\nProduct name: Badge Card Pack\nExtra notes from the NITM team (treat as accurate):\n21 cards.\n\nRules:\n"));
  assert.ok(p.includes("- " + LENGTH_RULES.detailed + "\n\nOutput Markdown in exactly this structure and nothing else:"));
  assert.ok(p.endsWith("## Questions this answers\n- <A question a shopper might type, in their words>\n(5–8 bullets)"));
});

test("Fin profile prompt drops the extra-notes block when the description is empty", () => {
  const p = cxFinProfilePrompt({ name: "X", description: "  ", length: "short", pageNotes: "N" });
  assert.ok(!p.includes("Extra notes"));
  assert.ok(p.includes("Product name: X\n\nRules:"));
  assert.ok(p.includes(LENGTH_RULES.short));
});

test("backfill variant uses the coverage-map line instead of the notes line", () => {
  const p = cxFinProfilePrompt({ name: "X", length: "standard", coverageOutline: "MAP" });
  assert.ok(p.includes("Here is the product's coverage map, listing every card/section and the topics it covers:\n\nMAP\n\nProduct name: X"));
  assert.ok(!p.includes("Here are notes taken"));
});

test("unknown length falls back to detailed", () => {
  assert.equal(cxProfileLength("huge"), "detailed");
  assert.equal(cxProfileLength(undefined), "detailed");
  assert.equal(cxProfileLength("short"), "short");
});
