#!/usr/bin/env node
// Run the REAL audit job (src/cx-audit/job.js) end to end on this machine,
// against an in-memory SQLite stand-in for D1: real Intercom, real Claude,
// nothing written to the production database. For checking the job before a
// deploy. Costs what the scoring costs (~2-3 cents per conversation).
//
//   node tools/cx-audit/job-local.mjs 2026-11-08
import { loadEnv, requireVars, loadGuidelines } from "./env.mjs";
import { sqliteD1 } from "../../tests/support/sqlite-d1.mjs";
import { ensureTables, saveGuidelines, setSetting } from "../../src/cx-audit/store.js";
import { createRun, tick } from "../../src/cx-audit/job.js";

const env = { ...loadEnv(), DB: sqliteD1() };
requireVars(env, ["INTERCOM_ACCESS_TOKEN", "ANTHROPIC_API_KEY"]);
const day = process.argv[2];
if (!day) { console.error("usage: job-local.mjs YYYY-MM-DD"); process.exit(1); }

await ensureTables(env);
await saveGuidelines(env, loadGuidelines(env));
await setSetting(env, "schedule", "off");
await createRun(env, { kind: "manual", from: day, to: day, by: "local" });

for (let i = 1; i <= 20; i++) {
  const r = await tick(env);
  console.error(`tick ${i}:`, JSON.stringify(r));
  if (r.idle || r.done || r.failed || r.error) break;
}
const all = async (sql) => (await env.DB.prepare(sql).all()).results;
const [run] = await all("SELECT id, status, searched, scored, skipped, errors, error, excluded_json, usage_json FROM cxa_runs");
console.log(JSON.stringify({ run: { ...run, excluded: JSON.parse(run.excluded_json || "{}"), usage: JSON.parse(run.usage_json || "{}"), excluded_json: undefined, usage_json: undefined } }, null, 2));
console.log(JSON.stringify(await all(`SELECT check_id, author_type,
  SUM(result = 'pass') AS pass, SUM(result = 'fail') AS fail, SUM(result = 'na') AS na
  FROM cxa_check_results c JOIN cxa_scores s ON s.id = c.score_id GROUP BY check_id, author_type ORDER BY author_type, check_id`), null, 1));
console.log("queue rows still holding a transcript:", (await all("SELECT COUNT(*) AS n FROM cxa_queue WHERE record_json IS NOT NULL"))[0].n);
const errs = await all("SELECT conversation_id, error FROM cxa_queue WHERE stage = 'error'");
if (errs.length) console.log("errors:", errs);
