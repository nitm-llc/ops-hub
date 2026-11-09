#!/usr/bin/env node
// CX scorecard audit, Phase 2: DRY RUN scorer. Fetches, scrubs and scores
// conversations with Claude, prints the JSON. Writes nothing to D1.
//
//   node tools/cx-audit/score.mjs                      # yesterday, 3 samples
//   node tools/cx-audit/score.mjs 2026-11-08           # that day, 3 samples
//   node tools/cx-audit/score.mjs 2026-11-08 --all     # every scoreable conversation
//   node tools/cx-audit/score.mjs 2026-11-08 --ids 215476748533764,215476712129192
//
// Needs INTERCOM_ACCESS_TOKEN and ANTHROPIC_API_KEY (.dev.vars or environment).
// Scored output is saved under .scratch/cx-audit/ (gitignored).

import fs from "node:fs";
import path from "node:path";
import { loadEnv, requireVars, loadGuidelines, root } from "./env.mjs";
import { fetchAndFilter, pickSamples } from "../../src/cx-audit/pipeline.js";
import { getAppId, getConversations } from "../../src/cx-audit/intercom.js";
import { extract } from "../../src/cx-audit/extract.js";
import { windowFor, yesterdayWindow, DEFAULT_TZ } from "../../src/cx-audit/config.js";
import { DEFAULT_TOGGLES } from "../../src/cx-audit/checks.js";
import { makeClient, scoreMany, scorerSettings, scorerKey } from "../../src/cx-audit/scorer.js";

const env = loadEnv();
requireVars(env, ["INTERCOM_ACCESS_TOKEN", "ANTHROPIC_API_KEY"]);

const args = process.argv.slice(2);
const date = args.find((a) => /^\d{4}-\d{2}-\d{2}$/.test(a));
const idsArg = args.includes("--ids") ? args[args.indexOf("--ids") + 1].split(",").map((s) => s.trim()).filter(Boolean) : null;
const all = args.includes("--all");
const tz = env.CX_AUDIT_TZ || DEFAULT_TZ;
const win = date ? windowFor(date, date, tz) : yesterdayWindow(tz);

let records;
if (idsArg) {
  const appId = await getAppId(env);
  const fetched = await getConversations(env, idsArg);
  records = fetched.map((f) => { if (f.error) throw new Error(`${f.id}: ${f.error}`); return extract(f.conversation, win, { appId }); });
} else {
  const res = await fetchAndFilter(env, win, { onProgress: (m) => console.error(m) });
  records = all ? res.scoreable : pickSamples(res.scoreable);
}

const guidelines = loadGuidelines(env);
const settings = scorerSettings(env, guidelines, DEFAULT_TOGGLES);
const key = await scorerKey(settings);
console.error(`scoring ${records.length} conversation(s) · ${settings.model} · effort ${settings.effort} · guidelines ${guidelines.version} · scorer_key ${key} · DRY RUN`);

const results = await scoreMany(records, { client: makeClient(env), guidelines, settings, tz }, { onProgress: (m) => console.error(m) });

const outDir = path.join(root, ".scratch/cx-audit");
fs.mkdirSync(outDir, { recursive: true });
const outFile = path.join(outDir, `scores-${win.from}-${Date.now()}.json`);
fs.writeFileSync(outFile, JSON.stringify({ settings: { ...settings, scorer_key: key }, results }, null, 2));

console.log(JSON.stringify(results.map((r) => (r.ok ? r.score : r)), null, 2));

const usage = results.filter((r) => r.ok).reduce((a, r) => {
  for (const k of Object.keys(r.score.usage)) a[k] = (a[k] || 0) + r.score.usage[k];
  return a;
}, {});
console.error(`\ntokens: ${JSON.stringify(usage)}`);
console.error(`saved: ${outFile}`);
