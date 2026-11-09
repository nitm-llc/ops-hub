// ============================================================================
// CX SCORECARD AUDIT — ops-hub feature module
// ----------------------------------------------------------------------------
// Scores every customer-facing Intercom reply (Ivy and the CX team) from the
// previous day against the communication guidelines, with Claude.
//
// WIRING (src/index.js):
//   import { handleCxAuditRoutes, cxAuditCron } from "./cx-audit/index.js";
//   fetch:     const cxaResp = await handleCxAuditRoutes(request, env, ctx, path); if (cxaResp) return cxaResp;
//   scheduled: ctx.waitUntil(cxAuditCron(env))
//
// API: /cx-audit/api/* behind the normal Ops Hub Cloudflare Access login,
//   refused on any other hostname (workers.dev skips Access).
//   GET  summary?from&to&author&check&issue&version   dashboard aggregates
//   GET  failures?...&limit&offset                     failed checks, newest first
//   GET  runs                 recent runs
//   GET  runs/:id             one run, with queue counts and errors
//   POST runs                 {from, to, force?}  manual run (processed by cron)
//   POST dry-run              {date, ids?, limit?}  score now, save NOTHING
//   GET  settings / PUT settings      {toggles?, schedule?}
//   GET  exclusions / POST exclusions {kind, value, note?}
//   PATCH exclusions/:id {enabled?, note?} / DELETE exclusions/:id
//   GET  fin-changes / POST fin-changes {day, what, who?} / DELETE fin-changes/:id
//   GET  guidelines / GET guidelines/:version / POST guidelines {version, text, note?, activate?}
//
// SECRETS: INTERCOM_ACCESS_TOKEN (new), ANTHROPIC_API_KEY (shared).
// CONFIG:  CX_AUDIT_MODEL, CX_AUDIT_EFFORT, CX_AUDIT_TZ (wrangler vars).
// DB:      env.DB, cxa_* tables (migrations/0013_cx_audit.sql).
// ============================================================================

import { isTrustedHost, verifyAccessJwt } from "../access.js";
import * as store from "./store.js";
import { createRun, tick, cxAuditCron, validateRange } from "./job.js";
import { getAppId, getConversations } from "./intercom.js";
import { fetchAndFilter, pickSamples } from "./pipeline.js";
import { extract } from "./extract.js";
import { windowFor, DEFAULT_TZ } from "./config.js";
import { CHECKS, VOICE_RULES, DEFAULT_TOGGLES } from "./checks.js";
import { makeClient, scoreMany, scorerSettings } from "./scorer.js";
import { summary, failures, parseFilters } from "./report.js";

export { cxAuditCron };

const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
const DRY_RUN_MAX = 5;

export async function handleCxAuditRoutes(request, env, ctx, path) {
  if (!path.startsWith("/cx-audit/api/")) return null;
  const url = new URL(request.url);
  if (!isTrustedHost(env, url)) return json({ error: "Only served on the Ops Hub login-protected hostname." }, 403);
  if (env.ACCESS_TEAM_DOMAIN && env.ACCESS_AUD) {
    const v = await verifyAccessJwt(env, request.headers.get("Cf-Access-Jwt-Assertion"), env.ACCESS_AUD);
    if (!v.ok) return json({ error: `Cloudflare Access check failed: ${v.reason}` }, 401);
  }
  if (!store.db(env)) return json({ error: "DB not configured" }, 503);
  const method = request.method;
  if (method !== "GET" && method !== "DELETE" && !(request.headers.get("Content-Type") || "").includes("application/json")) {
    return json({ error: "Send JSON." }, 415);
  }
  const by = request.headers.get("Cf-Access-Authenticated-User-Email") || null;
  const sub = path.slice("/cx-audit/api/".length).replace(/\/$/, "");
  const [head, idPart] = sub.split("/");
  const body = method === "POST" || method === "PUT" || method === "PATCH" ? await request.json().catch(() => ({})) : {};

  try {
    await store.ensureTables(env);

    if (head === "summary" && method === "GET") return json(await summary(env, parseFilters(url.searchParams)));
    if (head === "failures" && method === "GET") {
      return json(await failures(env, parseFilters(url.searchParams), { limit: url.searchParams.get("limit"), offset: url.searchParams.get("offset") }));
    }

    if (head === "runs" && method === "GET" && !idPart) {
      const { results } = await store.db(env).prepare(
        "SELECT id, kind, day_from, day_to, force, status, searched, scored, skipped, errors, error, excluded_json, usage_json, created_at, created_by, started_at, finished_at FROM cxa_runs ORDER BY id DESC LIMIT 30"
      ).all();
      return json({ runs: results.map(parseRun) });
    }
    if (head === "runs" && method === "GET" && idPart) {
      const d = store.db(env);
      const run = await d.prepare("SELECT * FROM cxa_runs WHERE id = ?").bind(Number(idPart)).first();
      if (!run) return json({ error: "No such run." }, 404);
      const { results: stages } = await d.prepare("SELECT stage, COUNT(*) AS n FROM cxa_queue WHERE run_id = ? GROUP BY stage").bind(run.id).all();
      const { results: errors } = await d.prepare("SELECT conversation_id, day, error FROM cxa_queue WHERE run_id = ? AND stage = 'error' LIMIT 50").bind(run.id).all();
      const settings = run.settings_json ? JSON.parse(run.settings_json) : null;
      if (settings) delete settings.exclusions;
      return json({ run: { ...parseRun(run), settings }, stages: Object.fromEntries(stages.map((s) => [s.stage, s.n])), errors });
    }
    if (head === "runs" && method === "POST") {
      const tz = env.CX_AUDIT_TZ || DEFAULT_TZ;
      const from = body.from, to = body.to || body.from;
      const err = validateRange(from, to, tz);
      if (err) return json({ error: err }, 400);
      const id = await createRun(env, { kind: "manual", from, to, force: !!body.force, by });
      // Start now rather than waiting up to 2 minutes for the cron.
      if (env.INTERCOM_ACCESS_TOKEN && env.ANTHROPIC_API_KEY) ctx.waitUntil(tick(env).catch((e) => console.error("cx-audit kick:", e)));
      return json({ id, status: "queued" }, 201);
    }

    if (head === "dry-run" && method === "POST") return json(await dryRun(env, body));

    if (head === "settings" && method === "GET") {
      return json({ toggles: await store.getToggles(env), schedule: await store.getSchedule(env), checks: CHECKS, voice_rules: VOICE_RULES, defaults: DEFAULT_TOGGLES,
        model: env.CX_AUDIT_MODEL || null, guidelines: await store.getActiveGuidelines(env).then((g) => g?.version || null) });
    }
    if (head === "settings" && method === "PUT") {
      if (body.toggles) await store.setSetting(env, "toggles", body.toggles, by);
      if (body.schedule) {
        if (!["daily", "weekly", "off"].includes(body.schedule)) return json({ error: "schedule must be daily, weekly or off" }, 400);
        await store.setSetting(env, "schedule", body.schedule, by);
      }
      return json({ toggles: await store.getToggles(env), schedule: await store.getSchedule(env) });
    }

    if (head === "exclusions") {
      if (method === "GET") return json({ exclusions: await store.listExclusions(env) });
      if (method === "POST") return json({ id: await store.addExclusion(env, body, by) }, 201);
      if (method === "PATCH" && idPart) { await store.updateExclusion(env, Number(idPart), body); return json({ ok: true }); }
      if (method === "DELETE" && idPart) { await store.deleteExclusion(env, Number(idPart)); return json({ ok: true }); }
    }
    if (head === "fin-changes") {
      if (method === "GET") return json({ changes: await store.listFinChanges(env) });
      if (method === "POST") return json({ id: await store.addFinChange(env, body, by) }, 201);
      if (method === "DELETE" && idPart) { await store.deleteFinChange(env, Number(idPart)); return json({ ok: true }); }
    }
    if (head === "guidelines") {
      if (method === "GET" && !idPart) return json({ versions: await store.listGuidelines(env) });
      if (method === "GET" && idPart) {
        const g = await store.db(env).prepare("SELECT version, text, note, is_active, created_at FROM cxa_guideline_versions WHERE version = ?").bind(idPart).first();
        return g ? json(g) : json({ error: "No such version." }, 404);
      }
      if (method === "POST") { await store.saveGuidelines(env, body, by); return json({ ok: true }, 201); }
    }
    return json({ error: "Not found." }, 404);
  } catch (e) {
    return json({ error: String(e.message || e) }, 400);
  }
}

function parseRun(r) {
  const out = { ...r };
  delete out.settings_json; delete out.lease_until;
  out.excluded = r.excluded_json ? JSON.parse(r.excluded_json) : null;
  out.usage = r.usage_json ? JSON.parse(r.usage_json) : null;
  delete out.excluded_json; delete out.usage_json;
  return out;
}

// Score now with the CURRENT settings; write nothing. For trying a prompt,
// toggle or guidelines change before it goes into the daily numbers.
export async function dryRun(env, { date, ids, limit = 3, client } = {}) {
  const tz = env.CX_AUDIT_TZ || DEFAULT_TZ;
  const err = validateRange(date, date, tz);
  if (err) throw new Error(err);
  const n = Math.max(1, Math.min(Number(limit) || 3, DRY_RUN_MAX));
  const guidelines = await store.getActiveGuidelines(env);
  if (!guidelines) throw new Error("No active guidelines version. Upload one first.");
  const win = windowFor(date, date, tz);
  let records;
  if (Array.isArray(ids) && ids.length) {
    const appId = await getAppId(env);
    const fetched = await getConversations(env, ids.slice(0, DRY_RUN_MAX).map(String));
    records = fetched.filter((f) => !f.error).map((f) => extract(f.conversation, win, { appId }));
  } else {
    const rules = (await store.listExclusions(env, { enabledOnly: true }));
    const res = await fetchAndFilter(env, win, { rules });
    records = pickSamples(res.scoreable, n);
  }
  const settings = scorerSettings(env, guidelines, await store.getToggles(env));
  const results = await scoreMany(records.slice(0, n), { client: client || makeClient(env), guidelines, settings, tz });
  return { dry_run: true, saved: false, date, guidelines: guidelines.version, model: settings.model, results };
}
