// CX scorecard audit, Phase 5: the morning digest. Real SQLite, fake sender
// (no Google Chat calls). Invented scores.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { sqliteD1 } from "./support/sqlite-d1.mjs";
import { ensureTables, saveScore, saveGuidelines, setSetting } from "../src/cx-audit/store.js";
import { formatDigest, buildDigest, digestAfterRun, MAX_CHARS, recipients } from "../src/cx-audit/digest.js";
import { createRun, tick } from "../src/cx-audit/job.js";
import { handleCxAuditRoutes } from "../src/cx-audit/index.js";
import { windowFor } from "../src/cx-audit/config.js";
import { CHECKS } from "../src/cx-audit/checks.js";

function score({ id, day = "2026-11-08", group = "ivy", fails = [], issue = "Order Problem", evidence = "quote", rewrite = "Try this." }) {
  const checks = {};
  for (const c of CHECKS) {
    const result = fails.includes(c.id) ? "fail" : "pass";
    checks[c.id] = { result, evidence, rewrite: result === "fail" ? rewrite : "", reply_id: "r1", ...(c.id === "right_voice" ? { failed_rules: [] } : {}) };
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

test("digest: rates, weakest check, worst replies first, links, inert customer text", async () => {
  const env = await seeded();
  const text = await buildDigest(env, "2026-11-08", "2026-11-08");
  assert.match(text, /^\*CX scorecard · Sun, Nov 8\*/);
  assert.match(text, /Ivy \*\d+%\* \(3 conv\.\) · Team \*100%\* \(1 conv\.\)/);
  assert.match(text, /Weakest check: \*Right voice\* 50% \(2 fails\)/);
  assert.match(text, /\*Worst 2 replies\*\n1\. Ivy · Order Problem · failed Answered the question, No silent handoff, Right voice/);
  assert.match(text, /<https:\/\/app\.intercom\.com\/a\/inbox\/ws\/inbox\/conversation\/1\|Open in Intercom>/);
  assert.match(text, /<https:\/\/ops\.anurseinthemaking\.com\/cx-audit\/#from=2026-11-08&to=2026-11-08\|Full scorecard>/);
  assert.ok(!text.includes("<b>"), "customer text can't make links or bold");
  assert.match(text, /‹b›bold‹\/b›/);
});

test("digest always fits Flo's 1500-character limit", () => {
  const long = "x".repeat(900);
  const worst = [1, 2, 3].map((i) => ({ author_type: "ivy", issue_type: "Other", link: `https://app.intercom.com/c/${i}`,
    fails: [{ check_id: "answered", evidence: long, rewrite: long }, { check_id: "right_voice", evidence: long, rewrite: long }] }));
  const s = { totals: { ivy: { pass: 1, fail: 5, convs: 3 } }, per_check: [{ check_id: "answered", author_type: "ivy", pass: 0, fail: 3 }] };
  const text = formatDigest({ from: "2026-11-08", to: "2026-11-08", s, worst, url: "https://ops.anurseinthemaking.com/cx-audit/" });
  assert.ok(text.length <= MAX_CHARS, `got ${text.length}`);
  assert.match(text, /Worst/);
});

test("digest with nothing scored is one short line", () => {
  const text = formatDigest({ from: "2026-11-08", to: "2026-11-08", s: { totals: {}, per_check: [] }, worst: [], url: "u" });
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
  assert.match(sent[0].text, /failed No silent handoff/);
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
