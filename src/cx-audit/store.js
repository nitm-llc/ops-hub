// ============================================================================
// CX SCORECARD AUDIT — D1 storage (env.DB, content-calendar).
//
// Every table starts with cxa_ and nothing here joins another module's tables,
// so moving the audit to its own database later is: create the DB, copy the
// cxa_* tables, add a CX_AUDIT_DB binding (db() below prefers it).
//
// PRIVACY: no raw transcripts, ever. cxa_queue.record_json holds a SCRUBBED
// transcript only while a conversation waits to be scored, and is cleared the
// moment it is. Long-term storage is scores + scrubbed evidence quotes +
// rewrites + the Intercom id and link.
//
// SCHEMA / SCHEMA_CALIBRATION are the single source; migrations 0013 and 0014
// must match them (tests/cx-audit-job.test.js checks).
// ============================================================================

import { DEFAULT_EXCLUSIONS } from "./config.js";
import { DEFAULT_TOGGLES, normalizeToggles } from "./checks.js";

export const db = (env) => env.CX_AUDIT_DB || env.DB;

export const SCHEMA = `
-- Guideline versions (v1, v2 ...). The text lives here, not in the public repo.
CREATE TABLE IF NOT EXISTS cxa_guideline_versions (
  version     TEXT PRIMARY KEY,
  text        TEXT NOT NULL,
  note        TEXT,
  is_active   INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- Key/value settings: toggles (checks + voice sub-rules), schedule.
CREATE TABLE IF NOT EXISTS cxa_settings (
  key         TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  updated_by  TEXT
);

-- Editable exclusion list (seeded from DEFAULT_EXCLUSIONS on first use).
CREATE TABLE IF NOT EXISTS cxa_exclusions (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  kind        TEXT NOT NULL,
  value       TEXT NOT NULL,
  note        TEXT,
  enabled     INTEGER NOT NULL DEFAULT 1,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- Fin change log, filled in by hand; marked on the trend chart.
CREATE TABLE IF NOT EXISTS cxa_fin_changes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  day         TEXT NOT NULL,
  what        TEXT NOT NULL,
  who         TEXT,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- One row per audit run (scheduled or manual).
CREATE TABLE IF NOT EXISTS cxa_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  kind          TEXT NOT NULL,
  day_from      TEXT NOT NULL,
  day_to        TEXT NOT NULL,
  force         INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL,
  settings_json TEXT,
  searched      INTEGER,
  scored        INTEGER,
  skipped       INTEGER,
  excluded_json TEXT,
  errors        INTEGER,
  error         TEXT,
  usage_json    TEXT,
  lease_until   INTEGER,
  created_at    TEXT NOT NULL,
  created_by    TEXT,
  started_at    TEXT,
  finished_at   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS cxa_runs_scheduled ON cxa_runs (kind, day_from) WHERE kind IN ('daily', 'weekly');

-- Work list for a run, worked through a batch per cron tick.
-- stage: fetch (day = '*') -> score | skipped | excluded -> done | error
CREATE TABLE IF NOT EXISTS cxa_queue (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  day             TEXT NOT NULL,
  stage           TEXT NOT NULL,
  reason          TEXT,
  record_json     TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  error           TEXT,
  PRIMARY KEY (run_id, conversation_id, day)
);
CREATE INDEX IF NOT EXISTS cxa_queue_stage ON cxa_queue (run_id, stage);

-- One row per conversation x audit day x author group x guidelines version.
CREATE TABLE IF NOT EXISTS cxa_scores (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id    TEXT NOT NULL,
  audit_day          TEXT NOT NULL,
  author_type        TEXT NOT NULL,
  guidelines_version TEXT NOT NULL,
  last_reply_id      TEXT,
  reply_ids          TEXT,
  link               TEXT NOT NULL,
  channel            TEXT,
  issue_type         TEXT,
  issue_type_source  TEXT,
  passed             INTEGER NOT NULL,
  failed             INTEGER NOT NULL,
  applicable         INTEGER NOT NULL,
  note               TEXT,
  overall_note       TEXT,
  scorer_key         TEXT,
  model              TEXT,
  run_id             INTEGER,
  scored_at          TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS cxa_scores_key ON cxa_scores (conversation_id, audit_day, author_type, guidelines_version);
CREATE INDEX IF NOT EXISTS cxa_scores_day ON cxa_scores (audit_day);

-- Per-check results. evidence and rewrite are scrubbed text.
CREATE TABLE IF NOT EXISTS cxa_check_results (
  score_id     INTEGER NOT NULL,
  check_id     TEXT NOT NULL,
  result       TEXT NOT NULL,
  evidence     TEXT,
  rewrite      TEXT,
  reply_id     TEXT,
  failed_rules TEXT,
  PRIMARY KEY (score_id, check_id)
);
CREATE INDEX IF NOT EXISTS cxa_check_results_check ON cxa_check_results (check_id, result);
`;

// Phase 6: calibration. A golden set of conversations Mark scores by hand,
// and runs that re-score it with the current model/prompt/guidelines and
// report agreement per check. Labels are pass|fail|na; an unlabeled check is
// simply not compared.
export const SCHEMA_CALIBRATION = `
-- One row per golden conversation/day (what to fetch and re-score).
CREATE TABLE IF NOT EXISTS cxa_golden_items (
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  note            TEXT,
  added_by        TEXT,
  added_at        TEXT NOT NULL,
  PRIMARY KEY (conversation_id, audit_day)
);

-- Mark's hand scores.
CREATE TABLE IF NOT EXISTS cxa_golden_labels (
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  author_type     TEXT NOT NULL,
  check_id        TEXT NOT NULL,
  expected        TEXT NOT NULL,
  labeled_by      TEXT,
  labeled_at      TEXT NOT NULL,
  PRIMARY KEY (conversation_id, audit_day, author_type, check_id)
);

CREATE TABLE IF NOT EXISTS cxa_calibration_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  status        TEXT NOT NULL,
  settings_json TEXT,
  total         INTEGER,
  done          INTEGER,
  errors        INTEGER,
  report_json   TEXT,
  error         TEXT,
  lease_until   INTEGER,
  created_at    TEXT NOT NULL,
  created_by    TEXT,
  finished_at   TEXT
);

CREATE TABLE IF NOT EXISTS cxa_calibration_queue (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  stage           TEXT NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  error           TEXT,
  PRIMARY KEY (run_id, conversation_id, audit_day)
);

-- What the model said for each labeled check, next to the hand label.
CREATE TABLE IF NOT EXISTS cxa_calibration_results (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  author_type     TEXT NOT NULL,
  check_id        TEXT NOT NULL,
  expected        TEXT NOT NULL,
  got             TEXT NOT NULL,
  evidence        TEXT,
  PRIMARY KEY (run_id, conversation_id, audit_day, author_type, check_id)
);
`;

// 2026-11-10: where each failed check comes from (a Fin procedure, guidance,
// setting or team member) and a one-line suggested change. Feeds the fix list.
export const SCHEMA_FIX_SOURCES = `
ALTER TABLE cxa_check_results ADD COLUMN source TEXT;
ALTER TABLE cxa_check_results ADD COLUMN fix TEXT;
`;
const ADDED_COLUMNS = [["cxa_check_results", "source", "TEXT"], ["cxa_check_results", "fix", "TEXT"]];

// Each migration file and the schema it must match.
export const MIGRATIONS = { "0013_cx_audit.sql": SCHEMA, "0014_cx_audit_calibration.sql": SCHEMA_CALIBRATION, "0015_cx_audit_fix_sources.sql": SCHEMA_FIX_SOURCES };

// Strip comments first: a ";" inside a comment would otherwise split a statement.
export const splitSql = (sql) => sql.replace(/--[^\n]*/g, "").split(";").map((x) => x.trim()).filter(Boolean);
const statements = () => [...splitSql(SCHEMA), ...splitSql(SCHEMA_CALIBRATION)];
export const schemaStatements = statements;

const ready = new WeakSet();
export async function ensureTables(env) {
  const d = db(env);
  if (!d) throw new Error("DB not configured");
  if (ready.has(d)) return;
  await d.batch(statements().map((s) => d.prepare(s)));
  // ALTERs can't be IF NOT EXISTS: add each column only when it's missing.
  for (const [table, col, type] of ADDED_COLUMNS) {
    const { results } = await d.prepare(`PRAGMA table_info(${table})`).all();
    if (!results.some((c) => c.name === col)) await d.prepare(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`).run();
  }
  const n = await d.prepare("SELECT COUNT(*) AS n FROM cxa_exclusions").first("n");
  if (!n) {
    const at = new Date().toISOString();
    await d.batch(DEFAULT_EXCLUSIONS.map((r) =>
      d.prepare("INSERT INTO cxa_exclusions (kind, value, note, enabled, created_at, created_by) VALUES (?, ?, ?, 1, ?, 'seed')").bind(r.kind, r.value, r.note || null, at)));
  }
  ready.add(d);
}

const now = () => new Date().toISOString();

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------
export async function getSetting(env, key, fallback = null) {
  const v = await db(env).prepare("SELECT value FROM cxa_settings WHERE key = ?").bind(key).first("value");
  if (v == null) return fallback;
  try { return JSON.parse(v); } catch { return fallback; }
}
export async function setSetting(env, key, value, by = null) {
  await db(env).prepare(
    "INSERT INTO cxa_settings (key, value, updated_at, updated_by) VALUES (?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at, updated_by = excluded.updated_by"
  ).bind(key, JSON.stringify(value), now(), by).run();
}
export async function getToggles(env) {
  return normalizeToggles(await getSetting(env, "toggles", DEFAULT_TOGGLES));
}
// "daily" during the transition; "weekly" afterwards (Mondays, previous Mon-Sun).
export async function getSchedule(env) {
  const s = await getSetting(env, "schedule", "daily");
  return ["daily", "weekly", "off"].includes(s) ? s : "daily";
}

// ---------------------------------------------------------------------------
// Guidelines
// ---------------------------------------------------------------------------
export async function getActiveGuidelines(env) {
  return db(env).prepare("SELECT version, text FROM cxa_guideline_versions WHERE is_active = 1 ORDER BY created_at DESC LIMIT 1").first();
}
export async function listGuidelines(env) {
  const { results } = await db(env).prepare("SELECT version, note, is_active, created_at, created_by, length(text) AS chars FROM cxa_guideline_versions ORDER BY created_at DESC").all();
  return results;
}
export async function saveGuidelines(env, { version, text, note = null, activate = true }, by = null) {
  if (!/^v\d+$/.test(version || "")) throw new Error("version must look like v1, v2 ...");
  if (!String(text || "").trim()) throw new Error("guidelines text is empty");
  const d = db(env);
  const stmts = [];
  if (activate) stmts.push(d.prepare("UPDATE cxa_guideline_versions SET is_active = 0"));
  stmts.push(d.prepare(
    "INSERT INTO cxa_guideline_versions (version, text, note, is_active, created_at, created_by) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(version) DO UPDATE SET text = excluded.text, note = excluded.note, is_active = excluded.is_active"
  ).bind(version, text, note, activate ? 1 : 0, now(), by));
  await d.batch(stmts);
}

// ---------------------------------------------------------------------------
// Exclusions + Fin change log
// ---------------------------------------------------------------------------
export async function listExclusions(env, { enabledOnly = false } = {}) {
  const { results } = await db(env).prepare(`SELECT id, kind, value, note, enabled, created_at, created_by FROM cxa_exclusions ${enabledOnly ? "WHERE enabled = 1" : ""} ORDER BY kind, value`).all();
  return results;
}
const KINDS = ["domain", "email", "subject", "issue_type"];
export async function addExclusion(env, { kind, value, note = null }, by = null) {
  if (!KINDS.includes(kind)) throw new Error(`kind must be one of ${KINDS.join(", ")}`);
  const v = String(value || "").trim();
  if (!v) throw new Error("value is empty");
  if (kind === "subject") { try { new RegExp(v, "i"); } catch { throw new Error("subject must be a valid regular expression"); } }
  const r = await db(env).prepare("INSERT INTO cxa_exclusions (kind, value, note, enabled, created_at, created_by) VALUES (?, ?, ?, 1, ?, ?)").bind(kind, kind === "domain" || kind === "email" ? v.toLowerCase() : v, note, now(), by).run();
  return r.meta?.last_row_id;
}
export async function updateExclusion(env, id, { enabled, note }) {
  if (enabled !== undefined) await db(env).prepare("UPDATE cxa_exclusions SET enabled = ? WHERE id = ?").bind(enabled ? 1 : 0, id).run();
  if (note !== undefined) await db(env).prepare("UPDATE cxa_exclusions SET note = ? WHERE id = ?").bind(note, id).run();
}
export async function deleteExclusion(env, id) {
  await db(env).prepare("DELETE FROM cxa_exclusions WHERE id = ?").bind(id).run();
}
export async function listFinChanges(env) {
  const { results } = await db(env).prepare("SELECT id, day, what, who, created_at, created_by FROM cxa_fin_changes ORDER BY day DESC, id DESC").all();
  return results;
}
export async function addFinChange(env, { day, what, who = null }, by = null) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day || "")) throw new Error("day must be YYYY-MM-DD");
  if (!String(what || "").trim()) throw new Error("what changed is empty");
  const r = await db(env).prepare("INSERT INTO cxa_fin_changes (day, what, who, created_at, created_by) VALUES (?, ?, ?, ?, ?)").bind(day, String(what).trim(), who, now(), by).run();
  return r.meta?.last_row_id;
}
export async function deleteFinChange(env, id) {
  await db(env).prepare("DELETE FROM cxa_fin_changes WHERE id = ?").bind(id).run();
}

// ---------------------------------------------------------------------------
// Scores
// ---------------------------------------------------------------------------
// The newest reply id per group already scored for this conversation/day/version.
export async function existingScores(env, conversationId, day, guidelinesVersion) {
  const { results } = await db(env).prepare(
    "SELECT author_type, last_reply_id FROM cxa_scores WHERE conversation_id = ? AND audit_day = ? AND guidelines_version = ?"
  ).bind(conversationId, day, guidelinesVersion).all();
  return Object.fromEntries(results.map((r) => [r.author_type, r.last_reply_id]));
}

// Replace (not append) the rows for each scored group: a re-score after new
// replies supersedes the earlier one for that day.
export async function saveScore(env, score, { runId, scorerKey }) {
  const d = db(env);
  for (const g of ["ivy", "team"]) {
    const G = score.groups[g];
    if (!G.scored) continue;
    const old = await d.prepare("SELECT id FROM cxa_scores WHERE conversation_id = ? AND audit_day = ? AND author_type = ? AND guidelines_version = ?")
      .bind(score.conversation_id, score.audit_day, g, score.guidelines_version).first("id");
    const stmts = [];
    if (old) {
      stmts.push(d.prepare("DELETE FROM cxa_check_results WHERE score_id = ?").bind(old));
      stmts.push(d.prepare("DELETE FROM cxa_scores WHERE id = ?").bind(old));
    }
    if (stmts.length) await d.batch(stmts);
    const ins = await d.prepare(
      `INSERT INTO cxa_scores (conversation_id, audit_day, author_type, guidelines_version, last_reply_id, reply_ids, link, channel, issue_type, issue_type_source,
         passed, failed, applicable, note, overall_note, scorer_key, model, run_id, scored_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      score.conversation_id, score.audit_day, g, score.guidelines_version, G.replies.at(-1) || null, JSON.stringify(G.replies),
      score.link, score.channel || null, score.issue_type || null, score.issue_type_source || null,
      G.passed, G.failed, G.applicable, G.note || null, score.overall_note || null, scorerKey || null, score.model || null, runId ?? null, now()
    ).run();
    const scoreId = ins.meta.last_row_id;
    await d.batch(Object.entries(G.checks).map(([checkId, c]) =>
      d.prepare("INSERT INTO cxa_check_results (score_id, check_id, result, evidence, rewrite, reply_id, failed_rules, source, fix) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(scoreId, checkId, c.result, c.evidence || null, c.rewrite || null, c.reply_id || null, c.failed_rules?.length ? JSON.stringify(c.failed_rules) : null, c.source || null, c.fix || null)));
  }
}
