// ============================================================================
// CX SCORECARD AUDIT — the run state machine, driven by the Worker's existing
// 2-minute cron (src/index.js scheduled()).
//
//   schedule  "daily": 7-10am New York, audit yesterday. "weekly": Mondays
//             7-10am, audit the previous Mon-Sun. "off": manual runs only.
//             One scheduled run per day/week (unique index on kind+day_from),
//             so overlapping ticks and redeploys can't double-run.
//   run       queued -> running -> done | failed
//             start: snapshot settings (guidelines version, toggles,
//             exclusions, model), search Intercom, queue every id (stage fetch)
//             each tick: fetch + extract + filter a batch, then score a batch,
//             within TICK_BUDGET_MS; a lease stops overlapping ticks
//   idempotent  a conversation is scored once per audit day per guidelines
//             version. Re-scored only when a group has a newer reply than the
//             one already scored, or the run was started with force.
//
// Volume: ~150 conversations searched, ~10-60 scored per day. A day finishes in
// a few ticks; a 31-day manual backfill takes longer but never blocks a tick.
// ============================================================================

import { searchConversationIds, getConversations, getAppId } from "./intercom.js";
import { extract, classify } from "./extract.js";
import { windowFor, localMidnight, todayYmd, addDaysYmd, DEFAULT_TZ } from "./config.js";
import { makeClient, scoreMany, scorerSettings, scorerKey, scoredIds } from "./scorer.js";
import * as store from "./store.js";
import { digestAfterRun } from "./digest.js";
import { calibrationTick } from "./calibration.js";

export const RUN_HOUR = 7;
const WINDOW_HOURS = 3;
const LEASE_MS = 5 * 60 * 1000;
const TICK_BUDGET_MS = 100 * 1000;
const FETCH_BATCH = 20;
const FETCH_CAP = 160;
const SCORE_BATCH = 3;
const SCORE_CAP = 24;
const MAX_ATTEMPTS = 3;
export const MAX_RANGE_DAYS = 31;

const tzOf = (env) => env.CX_AUDIT_TZ || DEFAULT_TZ;

function clock(tz, now) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short", hour: "numeric", hourCycle: "h23" })
    .formatToParts(new Date(now)).map((x) => [x.type, x.value]));
  return { weekday: p.weekday, hour: Number(p.hour) };
}

export function scheduledRunDue(schedule, tz, now = Date.now()) {
  const { weekday, hour } = clock(tz, now);
  if (hour < RUN_HOUR || hour >= RUN_HOUR + WINDOW_HOURS) return null;
  const today = todayYmd(tz, now);
  if (schedule === "daily") { const y = addDaysYmd(today, -1); return { kind: "daily", from: y, to: y }; }
  if (schedule === "weekly" && weekday === "Mon") return { kind: "weekly", from: addDaysYmd(today, -7), to: addDaysYmd(today, -1) };
  return null;
}

export function daysIn(from, to) {
  const out = [];
  for (let d = from; d <= to; d = addDaysYmd(d, 1)) out.push(d);
  return out;
}

export function validateRange(from, to, tz, now = Date.now()) {
  const re = /^\d{4}-\d{2}-\d{2}$/;
  if (!re.test(from || "") || !re.test(to || "")) return "Dates must be YYYY-MM-DD.";
  if (from > to) return "The start date is after the end date.";
  if (to >= todayYmd(tz, now)) return "Only finished days can be audited (yesterday or earlier).";
  if (daysIn(from, to).length > MAX_RANGE_DAYS) return `At most ${MAX_RANGE_DAYS} days per run.`;
  return null;
}

// Returns the new run id, or null when that scheduled run already exists.
export async function createRun(env, { kind, from, to, force = false, by = null }, now = Date.now()) {
  const err = validateRange(from, to, tzOf(env), now);
  if (err) throw new Error(err);
  const r = await store.db(env).prepare(
    "INSERT OR IGNORE INTO cxa_runs (kind, day_from, day_to, force, status, created_at, created_by) VALUES (?, ?, ?, ?, 'queued', ?, ?)"
  ).bind(kind, from, to, force ? 1 : 0, new Date(now).toISOString(), by).run();
  return r.meta?.changes ? r.meta.last_row_id : null;
}

async function claimLease(env, runId, now) {
  const r = await store.db(env).prepare("UPDATE cxa_runs SET lease_until = ? WHERE id = ? AND (lease_until IS NULL OR lease_until < ?)")
    .bind(now + LEASE_MS, runId, now).run();
  return r.meta?.changes === 1;
}
const releaseLease = (env, runId) => store.db(env).prepare("UPDATE cxa_runs SET lease_until = NULL WHERE id = ?").bind(runId).run();

async function failRun(env, runId, message) {
  await store.db(env).prepare("UPDATE cxa_runs SET status = 'failed', error = ?, finished_at = ? WHERE id = ?").bind(message, new Date().toISOString(), runId).run();
}

// ---------------------------------------------------------------------------
// Start: snapshot settings, search, queue.
// ---------------------------------------------------------------------------
async function startRun(env, run) {
  const d = store.db(env);
  const guidelines = await store.getActiveGuidelines(env);
  if (!guidelines) { await failRun(env, run.id, "No active guidelines version. Upload one first."); return null; }
  const toggles = await store.getToggles(env);
  const exclusions = (await store.listExclusions(env, { enabledOnly: true })).map(({ kind, value, note }) => ({ kind, value, note }));
  const settings = scorerSettings(env, guidelines, toggles);
  const snapshot = { ...settings, scorer_key: await scorerKey(settings), exclusions, tz: tzOf(env), app_id: await getAppId(env) };

  const ids = await searchConversationIds(env, { start: localMidnight(run.day_from, snapshot.tz) });
  for (let i = 0; i < ids.length; i += 50) {
    await d.batch(ids.slice(i, i + 50).map((id) =>
      d.prepare("INSERT OR IGNORE INTO cxa_queue (run_id, conversation_id, day, stage) VALUES (?, ?, '*', 'fetch')").bind(run.id, id)));
  }
  await d.prepare("UPDATE cxa_runs SET status = 'running', settings_json = ?, searched = ?, started_at = ? WHERE id = ?")
    .bind(JSON.stringify(snapshot), ids.length, new Date().toISOString(), run.id).run();
  return snapshot;
}

// ---------------------------------------------------------------------------
// Fetch stage: one Intercom fetch per conversation, then one queue row per
// audit day (excluded | skipped | score).
// ---------------------------------------------------------------------------
async function fetchBatch(env, run, snap, days) {
  const d = store.db(env);
  const { results: rows } = await d.prepare("SELECT conversation_id, attempts FROM cxa_queue WHERE run_id = ? AND stage = 'fetch' LIMIT ?")
    .bind(run.id, FETCH_BATCH).all();
  if (!rows.length) return 0;
  const fetched = await getConversations(env, rows.map((r) => r.conversation_id));
  for (let i = 0; i < rows.length; i++) {
    const { conversation_id: id, attempts } = rows[i];
    const f = fetched[i];
    if (f.error) {
      const stage = attempts + 1 >= MAX_ATTEMPTS ? "error" : "fetch";
      await d.prepare("UPDATE cxa_queue SET attempts = attempts + 1, error = ?, stage = ? WHERE run_id = ? AND conversation_id = ? AND day = '*'")
        .bind(f.error.slice(0, 500), stage, run.id, id).run();
      continue;
    }
    const stmts = [];
    for (const day of days) {
      const rec = extract(f.conversation, windowFor(day, day, snap.tz), { appId: snap.app_id });
      const verdict = classify(f.conversation, rec, snap.exclusions);
      if (verdict.status !== "scoreable") {
        // Only keep a row for days the conversation was actually active, or a
        // month backfill writes ~150 x 31 "no reply" rows.
        if (verdict.reason !== "no_reply" || days.length === 1) {
          stmts.push(d.prepare("INSERT OR REPLACE INTO cxa_queue (run_id, conversation_id, day, stage, reason) VALUES (?, ?, ?, 'excluded', ?)")
            .bind(run.id, id, day, verdict.rule ? `${verdict.reason}: ${verdict.rule.kind} ${verdict.rule.value}` : verdict.reason));
        }
        if (verdict.reason !== "no_reply") break; // excluded senders are excluded on every day
        continue;
      }
      const ids = scoredIds(rec);
      const have = await store.existingScores(env, id, day, snap.guidelines_version);
      const changed = ["ivy", "team"].some((g) => ids[g].length && have[g] !== ids[g].at(-1));
      const stage = run.force || changed ? "score" : "skipped";
      stmts.push(d.prepare("INSERT OR REPLACE INTO cxa_queue (run_id, conversation_id, day, stage, record_json) VALUES (?, ?, ?, ?, ?)")
        .bind(run.id, id, day, stage, stage === "score" ? JSON.stringify(rec) : null));
    }
    stmts.push(d.prepare("DELETE FROM cxa_queue WHERE run_id = ? AND conversation_id = ? AND day = '*'").bind(run.id, id));
    await d.batch(stmts);
  }
  return rows.length;
}

// ---------------------------------------------------------------------------
// Score stage
// ---------------------------------------------------------------------------
async function scoreBatch(env, run, snap, guidelines, client, usage) {
  const d = store.db(env);
  const { results: rows } = await d.prepare("SELECT conversation_id, day, record_json, attempts FROM cxa_queue WHERE run_id = ? AND stage = 'score' LIMIT ?")
    .bind(run.id, SCORE_BATCH).all();
  if (!rows.length) return 0;
  const records = rows.map((r) => JSON.parse(r.record_json));
  const settings = { model: snap.model, effort: snap.effort, guidelines_version: snap.guidelines_version, prompt_version: snap.prompt_version, toggles: snap.toggles };
  const results = await scoreMany(records, { client, guidelines, settings, tz: snap.tz }, { concurrency: SCORE_BATCH });
  for (let i = 0; i < rows.length; i++) {
    const { conversation_id: id, day, attempts } = rows[i];
    const r = results[i];
    if (r.ok) {
      await store.saveScore(env, r.score, { runId: run.id, scorerKey: snap.scorer_key });
      for (const k of Object.keys(r.score.usage)) usage[k] = (usage[k] || 0) + r.score.usage[k];
      await d.prepare("UPDATE cxa_queue SET stage = 'done', record_json = NULL, error = NULL WHERE run_id = ? AND conversation_id = ? AND day = ?").bind(run.id, id, day).run();
    } else {
      const last = attempts + 1 >= MAX_ATTEMPTS;
      await d.prepare(`UPDATE cxa_queue SET attempts = attempts + 1, error = ?, stage = ?${last ? ", record_json = NULL" : ""} WHERE run_id = ? AND conversation_id = ? AND day = ?`)
        .bind(String(r.error).slice(0, 500), last ? "error" : "score", run.id, id, day).run();
    }
  }
  return rows.length;
}

async function finishIfDone(env, run, usage) {
  const d = store.db(env);
  const { results } = await d.prepare("SELECT stage, reason, COUNT(*) AS n FROM cxa_queue WHERE run_id = ? GROUP BY stage, reason").bind(run.id).all();
  const by = (s) => results.filter((r) => r.stage === s).reduce((a, r) => a + r.n, 0);
  const prevUsage = run.usage_json ? JSON.parse(run.usage_json) : {};
  for (const k of Object.keys(usage)) prevUsage[k] = (prevUsage[k] || 0) + usage[k];
  const excluded = {};
  for (const r of results.filter((r) => r.stage === "excluded")) excluded[r.reason] = r.n;
  const pending = by("fetch") + by("score");
  await d.prepare("UPDATE cxa_runs SET scored = ?, skipped = ?, errors = ?, excluded_json = ?, usage_json = ? WHERE id = ?")
    .bind(by("done"), by("skipped"), by("error"), JSON.stringify(excluded), JSON.stringify(prevUsage), run.id).run();
  if (pending) return false;
  await d.prepare("UPDATE cxa_runs SET status = 'done', finished_at = ? WHERE id = ?").bind(new Date().toISOString(), run.id).run();
  // Belt and braces: no scrubbed transcript outlives its run.
  await d.prepare("UPDATE cxa_queue SET record_json = NULL WHERE run_id = ?").bind(run.id).run();
  return true;
}

// ---------------------------------------------------------------------------
// tick(): called every 2 minutes. Returns what it did (for logs and tests).
// ---------------------------------------------------------------------------
export async function tick(env, { now = Date.now(), client, budgetMs = TICK_BUDGET_MS, send } = {}) {
  await store.ensureTables(env);
  const d = store.db(env);
  const tz = tzOf(env);

  const due = scheduledRunDue(await store.getSchedule(env), tz, now);
  if (due) await createRun(env, { ...due, by: "schedule" }, now);

  const run = await d.prepare("SELECT * FROM cxa_runs WHERE status IN ('queued', 'running') ORDER BY id LIMIT 1").first();
  if (!run) return { idle: true };
  if (!(await claimLease(env, run.id, now))) return { busy: run.id };

  const deadline = Date.now() + budgetMs;
  const usage = {};
  let fetched = 0, scored = 0;
  try {
    let snap = run.settings_json ? JSON.parse(run.settings_json) : null;
    if (run.status === "queued") {
      snap = await startRun(env, run);
      if (!snap) { await digestAfterRun(env, run, { send }); return { run: run.id, failed: true }; }
    }
    const days = daysIn(run.day_from, run.day_to);
    while (Date.now() < deadline && fetched < FETCH_CAP) {
      const n = await fetchBatch(env, run, snap, days);
      if (!n) break;
      fetched += n;
    }
    const left = await d.prepare("SELECT COUNT(*) AS n FROM cxa_queue WHERE run_id = ? AND stage = 'fetch'").bind(run.id).first("n");
    if (!left) {
      const g = await d.prepare("SELECT version, text FROM cxa_guideline_versions WHERE version = ?").bind(snap.guidelines_version).first();
      if (!g) { await failRun(env, run.id, `Guidelines ${snap.guidelines_version} were deleted mid-run.`); await digestAfterRun(env, run, { send }); return { run: run.id, failed: true }; }
      const c = client || makeClient(env);
      while (Date.now() < deadline && scored < SCORE_CAP) {
        const n = await scoreBatch(env, run, snap, g, c, usage);
        if (!n) break;
        scored += n;
      }
    }
    const done = await finishIfDone(env, run, usage);
    // Scheduled runs only: the morning digest to the digest_to people via Flo.
    const digest = done ? await digestAfterRun(env, run, { send }) : null;
    return { run: run.id, fetched, scored, done, digest };
  } catch (e) {
    // Intercom/Anthropic outages: leave the run as is; the next tick retries.
    // A run that keeps failing for a day is marked failed so it stops blocking.
    console.error("cx-audit tick:", e);
    const ageMs = now - Date.parse(run.created_at);
    if (ageMs > 24 * 3600 * 1000) { await failRun(env, run.id, String(e.message || e).slice(0, 500)); await digestAfterRun(env, run, { send }); }
    return { run: run.id, error: String(e.message || e) };
  } finally {
    await releaseLease(env, run.id);
  }
}

export async function cxAuditCron(env) {
  if (!store.db(env) || !env.INTERCOM_ACCESS_TOKEN || !env.ANTHROPIC_API_KEY) return { skipped: "not configured" };
  const audit = await tick(env);
  // Calibration runs (Phase 6) share the tick; idle is one cheap read.
  const calibration = await calibrationTick(env).catch((e) => ({ error: String(e.message || e) }));
  return { audit, calibration };
}
