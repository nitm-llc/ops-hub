// CX scorecard audit, Phase 5: the morning digest. Real SQLite, fake sender
// (no Google Chat calls). Invented scores.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { sqliteD1 } from "./support/sqlite-d1.mjs";
import { ensureTables, saveScore, saveGuidelines, setSetting } from "../src/cx-audit/store.js";
import { formatDigest, buildDigest, digestAfterRun, MAX_CHARS, recipients } from "../src/cx-audit/digest.js";
import { fixList } from "../src/cx-audit/report.js";
import { createRun, tick } from "../src/cx-audit/job.js";
import { handleCxAuditRoutes } from "../src/cx-audit/index.js";
import { windowFor } from "../src/cx-audit/config.js";
import { CHECKS } from "../src/cx-audit/checks.js";

const SOURCES = { no_silent_handoff: "Procedure: Where is my order › Handle Order Status", right_voice: "Fin setting: introduction & identity", answered: "Guidance: Tone" };
function score({ id, day = "2026-11-08", group = "ivy", fails = [], issue = "Order Problem", evidence = "quote", rewrite = "Try this.", source = SOURCES }) {
  const checks = {};
  for (const c of CHECKS) {
    const result = fails.includes(c.id) ? "fail" : "pass";
    const fail = result === "fail";
    checks[c.id] = { result, evidence, rewrite: fail ? rewrite : "", reply_id: "r1", source: fail ? (source[c.id] || "Not sure") : "", fix: fail ? `Fix ${c.id}` : "", ...(c.id === "right_voice" ? { failed_rules: fail ? ["banned_phrases"] : [] } : {}) };
  }
  const vals = Object.values(checks).map((c) => c.result);
  const G = { scored: true, replies: ["r1"], checks, note: "n", passed: vals.filter((v) => v === "pass").length, failed: vals.filter((v) => v === "fail").length, applicable: vals.length };
  const none = { scored: false, replies: [], checks: {}, note: "" };
  return { conversation_id: id, link: `https://app.intercom.com/a/inbox/ws/inbox/conversation/${id}`, audit_day: day, issue_type: issue, issue_type_source: "intercom",
    channel: "email", groups: { ivy: group === "ivy" ? G : none, team: group === "team" ? G : none }, overall_note: "o", guidelines_version: "v1", model: "m", usage: {} };
}

let sent;
const send = async (env, { person, text, kind }) => { sent.push({ person, text, kind }); return { sent: true, message: `Sent to ${person}.` }; };
beforeEach(() => { sent = []; });

async function seeded() {
  const env = { DB: sqliteD1(), APP_HOSTNAME: "ops.anurseinthemaking.com" };
  await ensureTables(env);
  await saveScore(env, score({ id: "1", fails: ["no_silent_handoff", "right_voice", "answered"], evidence: "A human teammate can take it from here <b>*bold*</b>" }), {});
  await saveScore(env, score({ id: "2", fails: ["right_voice"] }), {});
  await saveScore(env, score({ id: "3", fails: [] }), {});
  await saveScore(env, score({ id: "4", group: "team", fails: [] }), {});
  return env;
}

test("fix list groups fails by who, what and where they come from, most frequent first", async () => {
  const env = await seeded();
  const r = await fixList(env, { from: "2026-11-08", to: "2026-11-08", author: "", check: "", issue: "", version: "" });
  assert.equal(r.total_fails, 4);
  const top = r.fixes[0];
  assert.deepEqual([top.who, top.check_id, top.rule, top.source, top.count], ["Ivy", "right_voice", "banned_phrases", "Fin setting: introduction & identity", 2]);
  assert.equal(top.problem, "used a banned phrase");
  assert.equal(top.fix, "Fix right_voice");
  assert.equal(top.examples.length, 2);
});

test("digest leads with what to fix: problem, where it comes from, change, example", async () => {
  const env = await seeded();
  const text = await buildDigest(env, "2026-11-08", "2026-11-08");
  assert.match(text, /^\*CX scorecard · Sun, Nov 8\*\nIvy \d+% · Team 100% of checks passed \(4 conversations\)/);
  assert.match(text, /\*Fix first\*\n1\. \*Ivy used a banned phrase\* \(2×\)\n   From: Fin setting: introduction & identity\n   Change: Fix right_voice\n   <https:\/\/app\.intercom\.com\/a\/inbox\/ws\/inbox\/conversation\/\d\|See an example>/);
  assert.match(text, /2\. \*Ivy (handed off|didn.t fully answer)/);
  assert.match(text, /<https:\/\/ops\.anurseinthemaking\.com\/cx-audit\/#from=2026-11-08&to=2026-11-08\|Fixes and details>/);
  assert.doesNotMatch(text, /Weakest|Worst/);
});

test("customer-derived text can't make Chat links or bold", () => {
  const s = { totals: { ivy: { pass: 1, fail: 1, convs: 1 } } };
  const fixes = [{ who: "Ivy", check_id: "accurate", source: "Guidance: <b>*x*</b>", count: 1, fix: "Say <a> _y_", examples: [{ link: "https://app.intercom.com/c/1" }] }];
  const text = formatDigest({ from: "2026-11-08", to: "2026-11-08", s, fixes, url: "u" });
  assert.match(text, /From: Guidance: ‹b›∗x∗‹\/b›/);
  assert.match(text, /Change: Say ‹a› _y_/);
  assert.doesNotMatch(text.split("\n").slice(3).join("\n"), /<b>|\*x\*/);
});

test("digest always fits Flo's 1500-character limit", () => {
  const long = "x".repeat(900);
  const fixes = [1, 2, 3, 4].map((i) => ({ who: "Ivy", check_id: "answered", source: "Procedure: " + long, count: i, fix: long, examples: [{ link: `https://app.intercom.com/c/${i}` }] }));
  const s = { totals: { ivy: { pass: 1, fail: 5, convs: 3 } } };
  const text = formatDigest({ from: "2026-11-08", to: "2026-11-08", s, fixes, url: "https://ops.anurseinthemaking.com/cx-audit/" });
  assert.ok(text.length <= MAX_CHARS, `got ${text.length}`);
  assert.match(text, /Fix first/);
});

test("digest with nothing scored is one short line", () => {
  const text = formatDigest({ from: "2026-11-08", to: "2026-11-08", s: { totals: {} }, fixes: [], url: "u" });
  assert.match(text, /No Ivy or team replies to score that day/);
});

test("digestAfterRun: scheduled runs only, once per run; a failed run sends a failure notice", async () => {
  const env = await seeded();
  await setSetting(env, "digest_to", ["Mark", "Tia"]);
  assert.deepEqual(await recipients(env), ["Mark", "Tia"]);
  const ins = (kind, status, error = null) => env.DB.prepare("INSERT INTO cxa_runs (kind, day_from, day_to, status, error, created_at) VALUES (?, '2026-11-08', '2026-11-08', ?, ?, 'x')").bind(kind, status, error).run();
  const manual = (await ins("manual", "done")).meta.last_row_id;
  assert.equal(await digestAfterRun(env, { id: manual, kind: "manual", day_from: "2026-11-08", day_to: "2026-11-08" }, { send }), null);
  const daily = (await ins("daily", "done")).meta.last_row_id;
  const r = await digestAfterRun(env, { id: daily, kind: "daily", day_from: "2026-11-08", day_to: "2026-11-08" }, { send });
  assert.deepEqual(r.map((x) => x.person), ["Mark", "Tia"]);
  assert.equal(sent.length, 2);
  assert.equal(sent[0].kind, "digest");
  assert.equal(await digestAfterRun(env, { id: daily, kind: "daily", day_from: "2026-11-08", day_to: "2026-11-08" }, { send }), null, "not twice");
  assert.equal(sent.length, 2);
  const failed = (await ins("weekly", "failed", "Intercom 500")).meta.last_row_id;
  await digestAfterRun(env, { id: failed, kind: "weekly", day_from: "2026-11-08", day_to: "2026-11-08" }, { send });
  assert.match(sent.at(-1).text, /failed: Intercom 500/);
});

test("a send failure is recorded, not thrown, and not retried", async () => {
  const env = await seeded();
  const id = (await env.DB.prepare("INSERT INTO cxa_runs (kind, day_from, day_to, status, created_at) VALUES ('daily', '2026-11-08', '2026-11-08', 'done', 'x')").run()).meta.last_row_id;
  const boom = async () => { throw new Error("Chat down"); };
  const r = await digestAfterRun(env, { id, kind: "daily", day_from: "2026-11-08", day_to: "2026-11-08" }, { send: boom });
  assert.equal(r[0].sent, false);
  assert.match(r[0].message, /Chat down/);
  const row = await env.DB.prepare("SELECT value FROM cxa_settings WHERE key = ?").bind(`digest:${id}`).first("value");
  assert.equal(JSON.parse(row).status, "sent");
});

test("the scheduled job sends the digest when its run finishes", async () => {
  const W = windowFor("2026-11-08");
  const conv = { id: "9", created_at: W.start + 3600, source: { type: "email", id: "s", subject: "Hi", body: "Where is it?", author: { type: "user", name: "Pat", email: "pat@example.com" } },
    custom_attributes: { "Issue Type": "Shipping & Delivery" }, conversation_parts: { conversation_parts: [{ id: "9-1", part_type: "comment", author: { type: "bot", name: "Ivy", from_ai_agent: true }, body: "On its way.", created_at: W.start + 7200 }] } };
  globalThis.fetch = async (url) => {
    const p = new URL(url).pathname;
    if (p === "/me") return Response.json({ app: { id_code: "ws" } });
    if (p === "/conversations/search") return Response.json({ conversations: [{ id: "9" }], pages: {} });
    return Response.json(conv);
  };
  const raw = { results: CHECKS.map((c) => ({ group: "ivy", check: c.id, result: c.id === "no_silent_handoff" ? "fail" : "pass", evidence: "On its way.", rewrite: "Arrives Friday.", reply_id: "9-1", failed_rules: [] })), ivy_note: "", team_note: "", overall_note: "", issue_type_suggested: "Other" };
  const client = { beta: { messages: { create: async () => ({ model: "m", stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify(raw) }], usage: {} }) } } };
  const env = { DB: sqliteD1(), INTERCOM_ACCESS_TOKEN: "t", ANTHROPIC_API_KEY: "k" };
  await ensureTables(env);
  await saveGuidelines(env, { version: "v1", text: "g" });
  const NOW = Date.parse("2026-11-09T12:30:00Z");
  let r;
  for (let i = 0; i < 5 && !(r?.done); i++) r = await tick(env, { now: NOW, client, send });
  assert.equal(r.done, true);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].person, "Mark", "default recipient");
  assert.match(sent[0].text, /Ivy handed off without saying what happens next and when/);
  // A manual run the same day sends nothing.
  await createRun(env, { kind: "manual", from: "2026-11-08", to: "2026-11-08", force: true }, NOW);
  for (let i = 0; i < 5; i++) { r = await tick(env, { now: NOW, client, send }); if (r.done || r.idle) break; }
  assert.equal(sent.length, 1);
});

test("API: digest preview and recipients setting", async () => {
  const env = await seeded();
  const call = (p, init = {}) => handleCxAuditRoutes(new Request(`https://ops.anurseinthemaking.com${p}`, { ...init, headers: { "Content-Type": "application/json" } }), env, { waitUntil() {} }, p);
  const prev = await (await call("/cx-audit/api/digest", { method: "POST", body: JSON.stringify({ from: "2026-11-08" }) })).json();
  assert.match(prev.text, /CX scorecard/);
  assert.equal(prev.sent, null, "preview sends nothing");
  const s = await (await call("/cx-audit/api/settings", { method: "PUT", body: JSON.stringify({ digest_to: "Mark, Tia ,," }) })).json();
  assert.deepEqual(s.digest_to, ["Mark", "Tia"]);
  assert.equal((await call("/cx-audit/api/digest", { method: "POST", body: JSON.stringify({ from: "nope" }) })).status, 400);
});
