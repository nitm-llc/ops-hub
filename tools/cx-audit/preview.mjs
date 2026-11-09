#!/usr/bin/env node
// CX scorecard audit, Phase 1 preview: fetch a day of Intercom conversations,
// apply the exclusions, print the summary and 3 scrubbed sample transcripts.
// Read-only against Intercom; writes nothing to D1.
//
//   node tools/cx-audit/preview.mjs                 # yesterday, New York time
//   node tools/cx-audit/preview.mjs 2026-11-05      # one day
//   node tools/cx-audit/preview.mjs 2026-11-01 2026-11-05
//
// Token: INTERCOM_ACCESS_TOKEN from the environment, else from .dev.vars
// (gitignored). The full scrubbed output is also saved under .scratch/cx-audit/
// (gitignored) for a closer look.

import fs from "node:fs";
import path from "node:path";
import { fetchAndFilter, pickSamples } from "../../src/cx-audit/pipeline.js";
import { windowFor, yesterdayWindow, DEFAULT_TZ } from "../../src/cx-audit/config.js";
import { loadEnv, requireVars, root } from "./env.mjs";

const env = loadEnv();
requireVars(env, ["INTERCOM_ACCESS_TOKEN"]);

const [from, to] = process.argv.slice(2);
const tz = env.CX_AUDIT_TZ || DEFAULT_TZ;
const win = from ? windowFor(from, to || from, tz) : yesterdayWindow(tz);

const res = await fetchAndFilter(env, win, { onProgress: (m) => console.error(m) });
const samples = pickSamples(res.scoreable);

const outDir = path.join(root, ".scratch/cx-audit");
fs.mkdirSync(outDir, { recursive: true });
const outFile = path.join(outDir, `preview-${win.from}${win.to !== win.from ? "_" + win.to : ""}.json`);
fs.writeFileSync(outFile, JSON.stringify({ summary: res.summary, scoreable: res.scoreable, excluded: res.excluded.map((e) => ({ id: e.record.id, reason: e.reason, rule: e.rule || null })) }, null, 2));

console.log(JSON.stringify(res.summary, null, 2));
for (const s of samples) {
  console.log("\n" + "=".repeat(78));
  console.log(`#${s.id}  ${s.issue_type || "(no issue type)"}  ·  ${s.channel}${s.via_shopify ? " (Shopify form)" : ""}  ·  Fin: ${s.fin_state || "-"}  ·  sentiment: ${s.sentiment || "-"}`);
  console.log(`subject: ${s.subject || "-"}`);
  console.log(`replies in window: Ivy ${s.counts.ivy}, team ${s.counts.team}  ·  ${s.link}`);
  console.log("-".repeat(78));
  for (const m of s.messages) {
    const when = new Date(m.at * 1000).toLocaleString("en-US", { timeZone: tz, month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
    console.log(`${m.in_window ? "▶" : " "} [${when}] ${m.role.toUpperCase()}${m.role === "team" ? ` (${m.name})` : ""}: ${m.text}`);
  }
}
console.error(`\nFull scrubbed output: ${outFile}`);
