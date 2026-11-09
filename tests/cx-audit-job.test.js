// CX scorecard audit, Phase 3: schema, the cron state machine, idempotency,
// and the API, against a real SQLite (node:sqlite) D1 stand-in, a mocked
// Intercom API and a fake Claude client. Invented conversations.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { sqliteD1 } from "./support/sqlite-d1.mjs";
import { schemaStatements, splitSql, saveGuidelines, ensureTables, setSetting } from "../src/cx-audit/store.js";
import { tick, scheduledRunDue, createRun, validateRange } from "../src/cx-audit/job.js";
import { handleCxAuditRoutes, dryRun } from "../src/cx-audit/index.js";
import { windowFor } from "../src/cx-audit/config.js";
import { CHECKS } from "../src/cx-audit/checks.js";

const NOW = Date.parse("2026-11-09T12:30:00Z"); // 7:30am New York, Monday
const WIN = windowFor("2026-11-08");
const T = (h) => WIN.start + h * 3600;
const ivy = { type: "bot", id: "1", name: "Ivy", from_ai_agent: true };
const aimee = { type: "admin", id: "77", name: "Aimee Jackson" };
const cust = { type: "user", id: "c1", name: "Pat Lee", email: "pat@example.com" };

function conv(id, { parts = [], ca = {}, email = "pat@example.com" } = {}) {
  return {
    id, created_at: T(1), ai_agent_participated: true,
    source: { type: "email", id: `s${id}`, subject: "Help", body: "Where is my order?", author: { ...cust, email } },
    custom_attributes: { "Issue Type": "Shipping & Delivery", Spam: "No", ...ca },
    conversation_parts: { conversation_parts: parts.map((p, i) => ({ id: `${id}-${i + 1}`, body: "", ...p })) },
  };
}

let convs, intercomCalls, claudeCalls, claudeFail;
function fakeClaudeRaw() {
  const results = [];
  for (const group of ["ivy", "team"]) for (const c of CHECKS) {
    results.push({ group, check: c.id, result: c.id === "no_silent_handoff" ? "fail" : "pass", evidence: "quote", rewrite: c.id === "no_silent_handoff" ? "Someone will reply by [time]." : "", reply_id: "", failed_rules: [] });
  }
  return { results, ivy_note: "ok", team_note: "ok", overall_note: "Fine.", issue_type_suggested: "Other" };
}
const client = { beta: { messages: { create: async () => {
  claudeCalls++;
  if (claudeFail) throw new Error("overloaded");
  return { model: "claude-sonnet-5-5", stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify(fakeClaudeRaw()) }], usage: { input_tokens: 100, output_tokens: 50, cache_read_input_tokens: 10, cache_creation_input_tokens: 0 } };
} } } };

beforeEach(() => {
  claudeCalls = 0; claudeFail = false; intercomCalls = [];
  convs = {
    "1": conv("1", { parts: [{ part_type: "comment", author: ivy, body: "Hi Pat, it's on the way.", created_at: T(9) }] }),
    "2": conv("2", { parts: [{ part_type: "comment", author: aimee, body: "Hi Pat! Refund sent. Aimee", created_at: T(10) }] }),
    "3": conv("3", { ca: { Spam: "Yes" }, parts: [{ part_type: "comment", author: ivy, body: "Hello", created_at: T(9) }] }),
    "4": conv("4", { email: "dse@na3.docusign.net", parts: [{ part_type: "comment", author: ivy, body: "Hello", created_at: T(9) }] }),
    "5": conv("5", { parts: [{ part_type: "comment", author: ivy, body: "Old reply", created_at: T(-30) }] }),
  };
  globalThis.fetch = async (url, opts = {}) => {
    const u = new URL(url);
    intercomCalls.push(u.pathname);
    if (u.pathname === "/me") return Response.json({ app: { id_code: "ws1" } });
    if (u.pathname === "/conversations/search") return Response.json({ conversations: Object.keys(convs).map((id) => ({ id })), pages: {} });
    const id = u.pathname.split("/").pop();
    return convs[id] ? Response.json(convs[id]) : new Response("gone", { status: 404 });
  };
});

async function setup() {
  const env = { DB: sqliteD1(), INTERCOM_ACCESS_TOKEN: "t", ANTHROPIC_API_KEY: "k", APP_HOSTNAME: "ops.anurseinthemaking.com" };
  await ensureTables(env);
  await saveGuidelines(env, { version: "v1", text: "# Guidelines\nBe kind." });
  return env;
}
const q = async (env, sql, ...args) => (await env.DB.prepare(sql).bind(...args).all()).results;
async function runToEnd(env, opts = {}) {
  let last;
  for (let i = 0; i < 10; i++) { last = await tick(env, { now: NOW, client, ...opts }); if (last.idle || last.done || last.failed) break; }
  return last;
}

test("migration file matches the schema in store.js", () => {
  const sql = fs.readFileSync(new URL("../migrations/0013_cx_audit.sql", import.meta.url), "utf8");
  const fromFile = splitSql(sql);
  assert.deepEqual(fromFile, schemaStatements());
});

test("schedule windows: daily 7-10am NY for yesterday; weekly on Mondays; nothing outside", () => {
  assert.deepEqual(scheduledRunDue("daily", "America/New_York", NOW), { kind: "daily", from: "2026-11-08", to: "2026-11-08" });
  assert.equal(scheduledRunDue("daily", "America/New_York", Date.parse("2026-11-09T11:30:00Z")), null); // 6:30am
  assert.equal(scheduledRunDue("daily", "America/New_York", Date.parse("2026-11-09T15:30:00Z")), null); // 10:30am
  assert.deepEqual(scheduledRunDue("weekly", "America/New_York", NOW), { kind: "weekly", from: "2026-11-02", to: "2026-11-08" });
  assert.equal(scheduledRunDue("weekly", "America/New_York", NOW + 86400000), null); // Tuesday
  assert.equal(scheduledRunDue("off", "America/New_York", NOW), null);
});

test("validateRange", () => {
  const tz = "America/New_York";
  assert.equal(validateRange("2026-11-08", "2026-11-08", tz, NOW), null);
  assert.match(validateRange("2026-11-09", "2026-11-09", tz, NOW), /finished days/);
  assert.match(validateRange("2026-11-08", "2026-11-01", tz, NOW), /after/);
  assert.match(validateRange("2026-09-01", "2026-11-08", tz, NOW), /At most 31/);
  assert.match(validateRange("11/08", "11/08", tz, NOW), /YYYY-MM-DD/);
});

test("daily run end to end: excludes, scores, stores, keeps no transcripts", async () => {
  const env = await setup();
  const r = await runToEnd(env);
  assert.equal(r.done, true);
  const [run] = await q(env, "SELECT * FROM cxa_runs");
  assert.equal(run.kind, "daily");
  assert.equal(run.day_from, "2026-11-08");
  assert.equal(run.status, "done");
  assert.equal(run.searched, 5);
  assert.equal(run.scored, 2);
  const excluded = JSON.parse(run.excluded_json);
  assert.equal(excluded.spam, 1);
  assert.equal(excluded["exclusion_rule: domain docusign.net"], 1);
  assert.equal(excluded.no_reply, 1);
  assert.equal(JSON.parse(run.usage_json).input, 200);

  const scores = await q(env, "SELECT * FROM cxa_scores ORDER BY conversation_id");
  assert.deepEqual(scores.map((s) => [s.conversation_id, s.author_type, s.audit_day]), [["1", "ivy", "2026-11-08"], ["2", "team", "2026-11-08"]]);
  assert.equal(scores[0].link, "https://app.intercom.com/a/inbox/ws1/inbox/conversation/1");
  assert.equal(scores[0].guidelines_version, "v1");
  assert.equal(scores[0].last_reply_id, "1-1");
  const checks = await q(env, "SELECT * FROM cxa_check_results WHERE score_id = ?", scores[0].id);
  assert.equal(checks.length, CHECKS.length);
  assert.equal(checks.find((c) => c.check_id === "no_silent_handoff").rewrite, "Someone will reply by [time].");
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_queue WHERE record_json IS NOT NULL"))[0].n, 0);
  assert.ok(!JSON.stringify(await q(env, "SELECT * FROM cxa_queue")).includes("pat@example.com"));

  // The scheduled run is created once per day, however many ticks follow.
  assert.equal((await tick(env, { now: NOW + 120000, client })).idle, true);
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_runs"))[0].n, 1);
});

test("idempotent: same replies are skipped; a new reply re-scores and replaces; force re-scores", async () => {
  const env = await setup();
  await setSetting(env, "schedule", "off");
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  await runToEnd(env);
  assert.equal(claudeCalls, 2);

  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  await runToEnd(env);
  assert.equal(claudeCalls, 2, "nothing re-scored");
  const [, second] = await q(env, "SELECT * FROM cxa_runs ORDER BY id");
  assert.equal(second.skipped, 2);
  assert.equal(second.scored, 0);

  convs["1"].conversation_parts.conversation_parts.push({ id: "1-9", part_type: "comment", author: ivy, body: "Update: delivered.", created_at: T(15) });
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  await runToEnd(env);
  assert.equal(claudeCalls, 3, "only conversation 1 re-scored");
  const ivyRows = await q(env, "SELECT * FROM cxa_scores WHERE conversation_id = '1'");
  assert.equal(ivyRows.length, 1, "replaced, not duplicated");
  assert.equal(ivyRows[0].last_reply_id, "1-9");
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_check_results"))[0].n, 2 * CHECKS.length, "old check rows removed");

  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08", force: true }, NOW);
  await runToEnd(env);
  assert.equal(claudeCalls, 5);

  // A new guidelines version scores again, alongside v1.
  await saveGuidelines(env, { version: "v2", text: "# Guidelines v2" });
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  await runToEnd(env);
  assert.equal(claudeCalls, 7);
  assert.deepEqual((await q(env, "SELECT DISTINCT guidelines_version AS v FROM cxa_scores ORDER BY v")).map((r) => r.v), ["v1", "v2"]);
});

test("multi-day manual run: one fetch per conversation, scores per day", async () => {
  const env = await setup();
  await setSetting(env, "schedule", "off");
  convs["1"].conversation_parts.conversation_parts.push({ id: "1-2", part_type: "comment", author: ivy, body: "Earlier", created_at: T(-20) });
  await createRun(env, { kind: "manual", from: "2026-11-07", to: "2026-11-08" }, NOW);
  await runToEnd(env);
  assert.equal(intercomCalls.filter((p) => p === "/conversations/1").length, 1);
  const days = await q(env, "SELECT audit_day FROM cxa_scores WHERE conversation_id = '1' ORDER BY audit_day");
  assert.deepEqual(days.map((d) => d.audit_day), ["2026-11-07", "2026-11-08"]);
});

test("Claude failures retry, then park as errors without blocking the run", async () => {
  const env = await setup();
  await setSetting(env, "schedule", "off");
  claudeFail = true;
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  const r = await runToEnd(env);
  assert.equal(r.done, true);
  const [run] = await q(env, "SELECT * FROM cxa_runs");
  assert.equal(run.errors, 2);
  assert.equal(run.scored, 0);
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_queue WHERE record_json IS NOT NULL"))[0].n, 0);
});

test("no active guidelines: the run fails with a clear message", async () => {
  const env = { DB: sqliteD1(), INTERCOM_ACCESS_TOKEN: "t", ANTHROPIC_API_KEY: "k" };
  await ensureTables(env);
  await tick(env, { now: NOW, client });
  const [run] = await q(env, "SELECT * FROM cxa_runs");
  assert.equal(run.status, "failed");
  assert.match(run.error, /No active guidelines/);
});

test("a held lease makes an overlapping tick back off", async () => {
  const env = await setup();
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08" }, NOW);
  await env.DB.prepare("UPDATE cxa_runs SET lease_until = ?").bind(Date.now() + 60000).run();
  const r = await tick(env, { now: Date.now(), client });
  assert.ok(r.busy);
});

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------
const ctx = { waitUntil: () => {} };
const call = (env, path, init = {}) => handleCxAuditRoutes(
  new Request(`https://ops.anurseinthemaking.com${path}`, { ...init, headers: { "Content-Type": "application/json", ...(init.headers || {}) } }), env, ctx, path);

test("API: host gate, manual run validation, exclusions and fin changes", async () => {
  const env = await setup();
  const off = await handleCxAuditRoutes(new Request("https://x.workers.dev/cx-audit/api/runs"), env, ctx, "/cx-audit/api/runs");
  assert.equal(off.status, 403);
  assert.equal(await handleCxAuditRoutes(new Request("https://ops.anurseinthemaking.com/other"), env, ctx, "/other"), null);

  const bad = await call(env, "/cx-audit/api/runs", { method: "POST", body: JSON.stringify({ from: "2999-01-01" }) });
  assert.equal(bad.status, 400);
  const ok = await call(env, "/cx-audit/api/runs", { method: "POST", body: JSON.stringify({ from: "2026-11-01", to: "2026-11-02" }) });
  assert.equal(ok.status, 201);

  const add = await call(env, "/cx-audit/api/exclusions", { method: "POST", body: JSON.stringify({ kind: "domain", value: "Vendor.COM" }) });
  assert.equal(add.status, 201);
  const list = await (await call(env, "/cx-audit/api/exclusions")).json();
  assert.ok(list.exclusions.some((e) => e.value === "vendor.com"));
  assert.ok(list.exclusions.some((e) => e.value === "docusign.net"), "seeded defaults");
  const badRe = await call(env, "/cx-audit/api/exclusions", { method: "POST", body: JSON.stringify({ kind: "subject", value: "([" }) });
  assert.equal(badRe.status, 400);

  await call(env, "/cx-audit/api/fin-changes", { method: "POST", body: JSON.stringify({ day: "2026-11-05", what: "New handoff guidance", who: "Mark" }) });
  const fc = await (await call(env, "/cx-audit/api/fin-changes")).json();
  assert.equal(fc.changes[0].what, "New handoff guidance");

  const put = await call(env, "/cx-audit/api/settings", { method: "PUT", body: JSON.stringify({ schedule: "weekly", toggles: { voice: { emoji: true } } }) });
  const s = await put.json();
  assert.equal(s.schedule, "weekly");
  assert.equal(s.toggles.voice.emoji, true);

  const form = await handleCxAuditRoutes(new Request("https://ops.anurseinthemaking.com/cx-audit/api/runs", { method: "POST", body: "from=x" }), env, ctx, "/cx-audit/api/runs");
  assert.equal(form.status, 415);
});

test("dry run scores with current settings and saves nothing", async () => {
  const env = await setup();
  const before = (await q(env, "SELECT COUNT(*) AS n FROM cxa_runs"))[0].n;
  const res = await dryRun(env, { date: "2026-11-08", limit: 5, client });
  assert.equal(res.saved, false);
  assert.equal(res.results.length, 2);
  assert.ok(res.results.every((r) => r.ok));
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_scores"))[0].n, 0);
  assert.equal((await q(env, "SELECT COUNT(*) AS n FROM cxa_runs"))[0].n, before);
});
