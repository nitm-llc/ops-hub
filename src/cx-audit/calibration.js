// ============================================================================
// CX SCORECARD AUDIT — calibration: can the scores be trusted?
//
// GOLDEN SET   conversations (id + audit day) Mark scores by hand, check by
//              check, for Ivy and/or the team: pass | fail | na. A check he
//              leaves blank isn't compared. Edited on the Calibration tab or
//              as a CSV (conversation_id,audit_day,author_type,check_id,expected).
// RUN          re-scores every golden conversation with the CURRENT settings
//              (model, effort, active guidelines, toggles, prompt) and compares
//              each labeled check. Worked a few conversations per cron tick,
//              like the audit. Nothing here touches the daily scores.
// REPORT       per check (and per author): agreement, missed fails (Mark
//              said fail, model didn't) and false fails (model said fail, Mark
//              didn't), plus every disagreement with the model's evidence.
//
// Use it whenever the prompt, guidelines or model change. Locally, before a
// change ships: node tools/cx-audit/calibrate.mjs (same comparison).
// ============================================================================

import { db, ensureTables, getActiveGuidelines, getToggles } from "./store.js";
import { CHECKS } from "./checks.js";
import { getAppId, getConversation } from "./intercom.js";
import { extract } from "./extract.js";
import { windowFor, DEFAULT_TZ } from "./config.js";
import { makeClient, scoreConversation, scorerSettings, scorerKey, scoredIds } from "./scorer.js";

export const EXPECTED = ["pass", "fail", "na"];
const GROUPS = ["ivy", "team"];
const CHECK_IDS = new Set(CHECKS.map((c) => c.id));
const DAY = /^\d{4}-\d{2}-\d{2}$/;
const LEASE_MS = 5 * 60 * 1000;
const TICK_BUDGET_MS = 90 * 1000;
const BATCH = 3;
const MAX_ATTEMPTS = 3;
const now = () => new Date().toISOString();

// "215476759858717", "conversation_215476759858717" or an Intercom inbox URL.
export function parseConversationId(s) {
  const m = String(s || "").trim().match(/(?:conversation[_/])?(\d{6,})\/?$/);
  return m ? m[1] : null;
}

function validLabel(l) {
  return l && GROUPS.includes(l.author_type) && CHECK_IDS.has(l.check_id) && EXPECTED.includes(l.expected);
}

// ---------------------------------------------------------------------------
// Golden set
// ---------------------------------------------------------------------------
export async function listGolden(env) {
  const { results } = await db(env).prepare(`
    SELECT i.conversation_id, i.audit_day, i.note, i.added_by, i.added_at,
      COUNT(l.check_id) AS labels, SUM(l.expected = 'fail') AS fails
    FROM cxa_golden_items i LEFT JOIN cxa_golden_labels l ON l.conversation_id = i.conversation_id AND l.audit_day = i.audit_day
    GROUP BY i.conversation_id, i.audit_day ORDER BY i.added_at DESC`).all();
  return results;
}

export async function getGolden(env, conversationId, day) {
  const d = db(env);
  const item = await d.prepare("SELECT * FROM cxa_golden_items WHERE conversation_id = ? AND audit_day = ?").bind(conversationId, day).first();
  if (!item) return null;
  const { results } = await d.prepare("SELECT author_type, check_id, expected FROM cxa_golden_labels WHERE conversation_id = ? AND audit_day = ?").bind(conversationId, day).all();
  return { ...item, labels: results };
}

// Replaces the item's labels with `labels` (blank checks = not compared).
export async function saveGolden(env, { conversation_id, audit_day, note = null, labels = [] }, by = null) {
  const id = parseConversationId(conversation_id);
  if (!id) throw new Error("Give an Intercom conversation id or link.");
  if (!DAY.test(audit_day || "")) throw new Error("audit_day must be YYYY-MM-DD.");
  const good = labels.filter(validLabel);
  if (good.length !== labels.length) throw new Error("Each label needs author_type ivy|team, a known check and expected pass|fail|na.");
  const d = db(env);
  const at = now();
  await d.batch([
    d.prepare(`INSERT INTO cxa_golden_items (conversation_id, audit_day, note, added_by, added_at) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(conversation_id, audit_day) DO UPDATE SET note = excluded.note`).bind(id, audit_day, note, by, at),
    d.prepare("DELETE FROM cxa_golden_labels WHERE conversation_id = ? AND audit_day = ?").bind(id, audit_day),
    ...good.map((l) => d.prepare("INSERT INTO cxa_golden_labels (conversation_id, audit_day, author_type, check_id, expected, labeled_by, labeled_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
      .bind(id, audit_day, l.author_type, l.check_id, l.expected, by, at)),
  ]);
  return { conversation_id: id, audit_day, labels: good.length };
}

export async function deleteGolden(env, conversationId, day) {
  const d = db(env);
  await d.batch([
    d.prepare("DELETE FROM cxa_golden_labels WHERE conversation_id = ? AND audit_day = ?").bind(conversationId, day),
    d.prepare("DELETE FROM cxa_golden_items WHERE conversation_id = ? AND audit_day = ?").bind(conversationId, day),
  ]);
}

// ---------------------------------------------------------------------------
// CSV: conversation_id,audit_day,author_type,check_id,expected[,note]
// ---------------------------------------------------------------------------
export const CSV_HEADER = ["conversation_id", "audit_day", "author_type", "check_id", "expected", "note"];

function csvCells(line) {
  const out = []; let cur = "", q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (q) { if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; } else if (c === '"') q = false; else cur += c; }
    else if (c === '"') q = true; else if (c === ",") { out.push(cur); cur = ""; } else cur += c;
  }
  out.push(cur);
  return out.map((s) => s.trim());
}
const csvCell = (v) => { const s = String(v ?? ""); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };

export async function exportCsv(env) {
  const { results } = await db(env).prepare(`
    SELECT l.conversation_id, l.audit_day, l.author_type, l.check_id, l.expected, i.note
    FROM cxa_golden_labels l JOIN cxa_golden_items i ON i.conversation_id = l.conversation_id AND i.audit_day = l.audit_day
    ORDER BY l.audit_day, l.conversation_id, l.author_type, l.check_id`).all();
  return [CSV_HEADER.join(","), ...results.map((r) => CSV_HEADER.map((k) => csvCell(r[k])).join(","))].join("\n") + "\n";
}

// Rows for the same conversation/day replace that item's labels as a whole.
export async function importCsv(env, text, by = null) {
  const lines = String(text || "").split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) throw new Error("The CSV is empty.");
  const head = csvCells(lines[0]).map((h) => h.toLowerCase());
  const idx = Object.fromEntries(CSV_HEADER.map((k) => [k, head.indexOf(k)]));
  for (const k of CSV_HEADER.slice(0, 5)) if (idx[k] < 0) throw new Error(`Missing column "${k}". Expected: ${CSV_HEADER.join(",")}`);
  const items = new Map();
  const errors = [];
  lines.slice(1).forEach((line, i) => {
    const c = csvCells(line);
    const row = { conversation_id: parseConversationId(c[idx.conversation_id]), audit_day: c[idx.audit_day], author_type: (c[idx.author_type] || "").toLowerCase(),
      check_id: c[idx.check_id], expected: (c[idx.expected] || "").toLowerCase().replace("n/a", "na"), note: idx.note >= 0 ? c[idx.note] : "" };
    if (!row.expected) return; // blank = not labeled
    if (!row.conversation_id || !DAY.test(row.audit_day) || !validLabel(row)) { errors.push(`line ${i + 2}`); return; }
    const k = `${row.conversation_id}|${row.audit_day}`;
    if (!items.has(k)) items.set(k, { conversation_id: row.conversation_id, audit_day: row.audit_day, note: row.note || null, labels: [] });
    items.get(k).labels.push(row);
  });
  if (errors.length) throw new Error(`Couldn't read ${errors.slice(0, 5).join(", ")}${errors.length > 5 ? ` and ${errors.length - 5} more` : ""}. Check ids, YYYY-MM-DD dates, ivy|team, check ids and pass|fail|na.`);
  for (const item of items.values()) await saveGolden(env, item, by);
  return { conversations: items.size, labels: [...items.values()].reduce((n, i) => n + i.labels.length, 0) };
}

// ---------------------------------------------------------------------------
// Comparison (pure)
// ---------------------------------------------------------------------------
// labels: [{author_type, check_id, expected}], score: scoreConversation() output.
export function compare(labels, score) {
  return labels.map((l) => {
    const g = score.groups[l.author_type];
    // No reply from that group on that day: nothing to compare (flagged on the item).
    if (!g?.scored) return { author_type: l.author_type, check_id: l.check_id, expected: l.expected, got: "no_reply", evidence: "" };
    const c = g.checks?.[l.check_id];
    return { author_type: l.author_type, check_id: l.check_id, expected: l.expected, got: c ? c.result : "na", evidence: c?.evidence || "" };
  });
}

// rows: [{author_type, check_id, expected, got, ...}] -> report.
export function agreementReport(rows) {
  const blank = () => ({ compared: 0, agree: 0, missed_fails: 0, false_fails: 0, na_mismatch: 0, off: 0, no_reply: 0 });
  const add = (a, r) => {
    if (r.got === "off") { a.off++; return; }           // check switched off
    if (r.got === "no_reply") { a.no_reply++; return; } // that group didn't reply that day
    a.compared++;
    if (r.got === r.expected) a.agree++;
    else if (r.expected === "fail") a.missed_fails++;
    else if (r.got === "fail") a.false_fails++;
    else a.na_mismatch++;
  };
  const overall = blank();
  const byAuthor = { ivy: blank(), team: blank() };
  const byCheck = {};
  for (const r of rows) {
    add(overall, r);
    add(byAuthor[r.author_type] ||= blank(), r);
    const k = (byCheck[r.check_id] ||= { all: blank(), ivy: blank(), team: blank() });
    add(k.all, r); add(k[r.author_type], r);
  }
  const rate = (a) => (a.compared ? Math.round((1000 * a.agree) / a.compared) / 10 : null);
  const withRate = (a) => ({ ...a, agreement: rate(a) });
  return {
    overall: withRate(overall),
    by_author: Object.fromEntries(Object.entries(byAuthor).map(([k, v]) => [k, withRate(v)])),
    by_check: CHECKS.filter((c) => byCheck[c.id]).map((c) => ({
      check_id: c.id, name: c.name, n: c.n, ...withRate(byCheck[c.id].all),
      ivy: withRate(byCheck[c.id].ivy), team: withRate(byCheck[c.id].team),
    })),
  };
}

// ---------------------------------------------------------------------------
// Runs (cron-driven)
// ---------------------------------------------------------------------------
export async function createCalibration(env, by = null) {
  await ensureTables(env);
  const d = db(env);
  const busy = await d.prepare("SELECT id FROM cxa_calibration_runs WHERE status IN ('queued', 'running') LIMIT 1").first();
  if (busy) throw new Error(`Calibration run ${busy.id} is still going.`);
  const { results: items } = await d.prepare(`SELECT DISTINCT i.conversation_id, i.audit_day FROM cxa_golden_items i
    JOIN cxa_golden_labels l ON l.conversation_id = i.conversation_id AND l.audit_day = i.audit_day`).all();
  if (!items.length) throw new Error("The golden set is empty. Score a few conversations by hand first.");
  const guidelines = await getActiveGuidelines(env);
  if (!guidelines) throw new Error("No active guidelines version.");
  const settings = scorerSettings(env, guidelines, await getToggles(env));
  const snap = { ...settings, scorer_key: await scorerKey(settings), tz: env.CX_AUDIT_TZ || DEFAULT_TZ };
  const r = await d.prepare("INSERT INTO cxa_calibration_runs (status, settings_json, total, done, errors, created_at, created_by) VALUES ('running', ?, ?, 0, 0, ?, ?)")
    .bind(JSON.stringify(snap), items.length, now(), by).run();
  const id = r.meta.last_row_id;
  for (let i = 0; i < items.length; i += 50) {
    await d.batch(items.slice(i, i + 50).map((it) =>
      d.prepare("INSERT INTO cxa_calibration_queue (run_id, conversation_id, audit_day, stage) VALUES (?, ?, ?, 'pending')").bind(id, it.conversation_id, it.audit_day)));
  }
  return id;
}

// Fetch, scrub, score and compare one golden item. Shared with the local CLI.
export async function scoreGoldenItem(env, { conversation_id, audit_day, labels }, { client, guidelines, settings, tz, appId }) {
  const conv = await getConversation(env, conversation_id);
  const record = extract(conv, windowFor(audit_day, audit_day, tz), { appId });
  const ids = scoredIds(record);
  const missing = [...new Set(labels.map((l) => l.author_type))].filter((g) => !ids[g].length);
  if (!ids.ivy.length && !ids.team.length) throw new Error(`No Ivy or team reply on ${audit_day}; check the day.`);
  const score = await scoreConversation(record, { client, guidelines, settings, tz });
  return { rows: compare(labels, score), missing_groups: missing, link: record.link, usage: score.usage };
}

export async function calibrationTick(env, { client, budgetMs = TICK_BUDGET_MS, nowMs = Date.now() } = {}) {
  await ensureTables(env);
  const d = db(env);
  const run = await d.prepare("SELECT * FROM cxa_calibration_runs WHERE status = 'running' ORDER BY id LIMIT 1").first();
  if (!run) return { idle: true };
  const claimed = await d.prepare("UPDATE cxa_calibration_runs SET lease_until = ? WHERE id = ? AND (lease_until IS NULL OR lease_until < ?)").bind(nowMs + LEASE_MS, run.id, nowMs).run();
  if (claimed.meta?.changes !== 1) return { busy: run.id };
  const deadline = Date.now() + budgetMs;
  try {
    const snap = JSON.parse(run.settings_json);
    const guidelines = await d.prepare("SELECT version, text FROM cxa_guideline_versions WHERE version = ?").bind(snap.guidelines_version).first();
    if (!guidelines) throw new Error(`Guidelines ${snap.guidelines_version} no longer exist.`);
    const c = client || makeClient(env);
    const appId = await getAppId(env);
    const settings = { model: snap.model, effort: snap.effort, guidelines_version: snap.guidelines_version, prompt_version: snap.prompt_version, toggles: snap.toggles };
    while (Date.now() < deadline) {
      const { results: batch } = await d.prepare("SELECT conversation_id, audit_day, attempts FROM cxa_calibration_queue WHERE run_id = ? AND stage = 'pending' LIMIT ?").bind(run.id, BATCH).all();
      if (!batch.length) break;
      await Promise.all(batch.map(async (it) => {
        try {
          const { results: labels } = await d.prepare("SELECT author_type, check_id, expected FROM cxa_golden_labels WHERE conversation_id = ? AND audit_day = ?").bind(it.conversation_id, it.audit_day).all();
          const res = await scoreGoldenItem(env, { ...it, labels }, { client: c, guidelines, settings, tz: snap.tz, appId });
          await d.batch([
            ...res.rows.map((r) => d.prepare("INSERT OR REPLACE INTO cxa_calibration_results (run_id, conversation_id, audit_day, author_type, check_id, expected, got, evidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
              .bind(run.id, it.conversation_id, it.audit_day, r.author_type, r.check_id, r.expected, r.got, r.evidence || null)),
            d.prepare("UPDATE cxa_calibration_queue SET stage = 'done', error = ? WHERE run_id = ? AND conversation_id = ? AND audit_day = ?")
              .bind(res.missing_groups.length ? `no ${res.missing_groups.join("/")} reply that day` : null, run.id, it.conversation_id, it.audit_day),
          ]);
        } catch (e) {
          const last = it.attempts + 1 >= MAX_ATTEMPTS || /No Ivy or team reply/.test(e.message);
          await d.prepare("UPDATE cxa_calibration_queue SET attempts = attempts + 1, error = ?, stage = ? WHERE run_id = ? AND conversation_id = ? AND audit_day = ?")
            .bind(String(e.message || e).slice(0, 300), last ? "error" : "pending", run.id, it.conversation_id, it.audit_day).run();
        }
      }));
    }
    return await finishCalibration(env, run.id);
  } catch (e) {
    await d.prepare("UPDATE cxa_calibration_runs SET status = 'failed', error = ?, finished_at = ? WHERE id = ?").bind(String(e.message || e).slice(0, 500), now(), run.id).run();
    return { run: run.id, failed: true, error: e.message };
  } finally {
    await d.prepare("UPDATE cxa_calibration_runs SET lease_until = NULL WHERE id = ?").bind(run.id).run();
  }
}

async function finishCalibration(env, runId) {
  const d = db(env);
  const { results: stages } = await d.prepare("SELECT stage, COUNT(*) AS n FROM cxa_calibration_queue WHERE run_id = ? GROUP BY stage").bind(runId).all();
  const n = (s) => stages.find((x) => x.stage === s)?.n || 0;
  await d.prepare("UPDATE cxa_calibration_runs SET done = ?, errors = ? WHERE id = ?").bind(n("done"), n("error"), runId).run();
  if (n("pending")) return { run: runId, done: false };
  const { results: rows } = await d.prepare("SELECT author_type, check_id, expected, got FROM cxa_calibration_results WHERE run_id = ?").bind(runId).all();
  await d.prepare("UPDATE cxa_calibration_runs SET status = 'done', report_json = ?, finished_at = ? WHERE id = ?").bind(JSON.stringify(agreementReport(rows)), now(), runId).run();
  return { run: runId, done: true };
}

export async function listCalibrations(env) {
  const { results } = await db(env).prepare("SELECT id, status, settings_json, total, done, errors, report_json, error, created_at, created_by, finished_at FROM cxa_calibration_runs ORDER BY id DESC LIMIT 20").all();
  return results.map((r) => {
    const s = r.settings_json ? JSON.parse(r.settings_json) : {};
    const rep = r.report_json ? JSON.parse(r.report_json) : null;
    return { id: r.id, status: r.status, total: r.total, done: r.done, errors: r.errors, error: r.error, created_at: r.created_at, created_by: r.created_by, finished_at: r.finished_at,
      model: s.model, effort: s.effort, guidelines_version: s.guidelines_version, prompt_version: s.prompt_version, agreement: rep?.overall?.agreement ?? null };
  });
}

export async function getCalibration(env, id) {
  const d = db(env);
  const run = await d.prepare("SELECT * FROM cxa_calibration_runs WHERE id = ?").bind(id).first();
  if (!run) return null;
  const { results: disagreements } = await d.prepare(`
    SELECT r.conversation_id, r.audit_day, r.author_type, r.check_id, r.expected, r.got, r.evidence
    FROM cxa_calibration_results r WHERE r.run_id = ? AND r.got != r.expected AND r.got NOT IN ('off', 'no_reply')
    ORDER BY r.check_id, r.audit_day`).bind(id).all();
  const { results: errors } = await d.prepare("SELECT conversation_id, audit_day, error FROM cxa_calibration_queue WHERE run_id = ? AND error IS NOT NULL").bind(id).all();
  const s = run.settings_json ? JSON.parse(run.settings_json) : {};
  return {
    id: run.id, status: run.status, total: run.total, done: run.done, errors: run.errors, error: run.error, created_at: run.created_at, finished_at: run.finished_at,
    model: s.model, effort: s.effort, guidelines_version: s.guidelines_version, prompt_version: s.prompt_version,
    report: run.report_json ? JSON.parse(run.report_json) : null, disagreements, item_errors: errors,
  };
}
