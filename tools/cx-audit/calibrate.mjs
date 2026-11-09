#!/usr/bin/env node
// CX scorecard calibration, run LOCALLY: re-score the golden set (Mark's hand
// scores, read from the live D1) with the code, prompt and guidelines on THIS
// machine, and print agreement per check. Use it to check a prompt, guidelines
// or model change before it ships. Writes nothing to D1.
//
//   node tools/cx-audit/calibrate.mjs                       # newest guidelines/vN.md, live toggles
//   node tools/cx-audit/calibrate.mjs --model claude-opus-5-5
//   node tools/cx-audit/calibrate.mjs --guidelines v2       # guidelines/v2.md
//
// After a change ships, the Calibration tab's "Run calibration" does the same
// against production settings and keeps the history.
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { loadEnv, requireVars, loadGuidelines, root } from "./env.mjs";
import { agreementReport, scoreGoldenItem } from "../../src/cx-audit/calibration.js";
import { getAppId } from "../../src/cx-audit/intercom.js";
import { makeClient, scorerSettings } from "../../src/cx-audit/scorer.js";
import { normalizeToggles, DEFAULT_TOGGLES, CHECKS } from "../../src/cx-audit/checks.js";
import { DEFAULT_TZ } from "../../src/cx-audit/config.js";

const args = process.argv.slice(2);
const opt = (k) => (args.includes(k) ? args[args.indexOf(k) + 1] : null);
const env = loadEnv();
if (opt("--model")) env.CX_AUDIT_MODEL = opt("--model");
if (opt("--guidelines")) env.CX_AUDIT_GUIDELINES = opt("--guidelines");
requireVars(env, ["INTERCOM_ACCESS_TOKEN", "ANTHROPIC_API_KEY"]);

function d1(sql) {
  const out = execFileSync("npx", ["wrangler", "d1", "execute", "content-calendar", "--remote", "--json", "--command", sql], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
  return JSON.parse(out)[0].results;
}

const labels = d1("SELECT conversation_id, audit_day, author_type, check_id, expected FROM cxa_golden_labels ORDER BY audit_day, conversation_id");
if (!labels.length) { console.error("The golden set is empty. Score some conversations on the Calibration tab first."); process.exit(1); }
const togglesRow = d1("SELECT value FROM cxa_settings WHERE key = 'toggles'")[0];
const toggles = normalizeToggles(togglesRow ? JSON.parse(togglesRow.value) : DEFAULT_TOGGLES);

const items = new Map();
for (const l of labels) {
  const k = `${l.conversation_id}|${l.audit_day}`;
  if (!items.has(k)) items.set(k, { conversation_id: l.conversation_id, audit_day: l.audit_day, labels: [] });
  items.get(k).labels.push(l);
}

const guidelines = loadGuidelines(env);
const settings = scorerSettings(env, guidelines, toggles);
const tz = env.CX_AUDIT_TZ || DEFAULT_TZ;
const ctx = { client: makeClient(env), guidelines, settings, tz, appId: await getAppId(env) };
console.error(`calibrating ${items.size} conversation(s), ${labels.length} hand labels · ${settings.model} · effort ${settings.effort} · guidelines ${guidelines.version} (local file) · prompt ${settings.prompt_version}`);

const rows = [];
const problems = [];
let done = 0;
const list = [...items.values()];
// First alone so the rest read the cached prompt; then 3 at a time.
const run = async (it) => {
  try {
    const r = await scoreGoldenItem(env, it, ctx);
    rows.push(...r.rows.map((x) => ({ ...x, conversation_id: it.conversation_id, audit_day: it.audit_day, link: r.link })));
    if (r.missing_groups.length) problems.push(`${it.conversation_id} ${it.audit_day}: no ${r.missing_groups.join("/")} reply that day`);
  } catch (e) { problems.push(`${it.conversation_id} ${it.audit_day}: ${e.message}`); }
  console.error(`  ${++done}/${list.length}`);
};
if (list.length) await run(list[0]);
for (let i = 1; i < list.length; i += 3) await Promise.all(list.slice(i, i + 3).map(run));

const rep = agreementReport(rows);
const p = (v) => (v == null ? "  —  " : `${String(v).padStart(5)}%`);
console.log(`\nOVERALL agreement ${p(rep.overall.agreement)}  (${rep.overall.agree}/${rep.overall.compared})   Ivy ${p(rep.by_author.ivy.agreement)}   Team ${p(rep.by_author.team.agreement)}`);
console.log(`\n${"check".padEnd(30)} ${"n".padStart(4)}  agree   missed fails  false fails  na mismatch`);
for (const c of rep.by_check) {
  console.log(`${`${c.n}. ${c.name}`.padEnd(30)} ${String(c.compared).padStart(4)}  ${p(c.agreement)}   ${String(c.missed_fails).padStart(12)}  ${String(c.false_fails).padStart(11)}  ${String(c.na_mismatch).padStart(11)}${c.off ? `   (${c.off} off)` : ""}`);
}
const name = Object.fromEntries(CHECKS.map((c) => [c.id, c.name]));
const dis = rows.filter((r) => r.got !== r.expected && r.got !== "off" && r.got !== "no_reply");
if (dis.length) {
  console.log(`\nDISAGREEMENTS (${dis.length})`);
  for (const r of dis) console.log(`- ${name[r.check_id]} · ${r.author_type} · ${r.audit_day} · you: ${r.expected}, model: ${r.got}\n  ${r.evidence ? `“${r.evidence}”` : ""}\n  ${r.link}`);
}
if (problems.length) console.log(`\nSKIPPED\n- ${problems.join("\n- ")}`);
const outDir = path.join(root, ".scratch/cx-audit");
fs.mkdirSync(outDir, { recursive: true });
const out = path.join(outDir, `calibration-${Date.now()}.json`);
fs.writeFileSync(out, JSON.stringify({ settings: { model: settings.model, effort: settings.effort, guidelines: guidelines.version, prompt: settings.prompt_version }, report: rep, rows, problems }, null, 2));
console.error(`\nsaved: ${out}`);
