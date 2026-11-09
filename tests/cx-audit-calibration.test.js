// CX scorecard audit, Phase 6: golden set, CSV, agreement report, and a
// calibration run end to end (real SQLite, mocked Intercom, fake Claude).
import { test } from "node:test";
import assert from "node:assert/strict";
import { sqliteD1 } from "./support/sqlite-d1.mjs";
import { ensureTables, saveGuidelines } from "../src/cx-audit/store.js";
import * as cal from "../src/cx-audit/calibration.js";
import { handleCxAuditRoutes } from "../src/cx-audit/index.js";
import { windowFor } from "../src/cx-audit/config.js";
import { CHECKS } from "../src/cx-audit/checks.js";

async function setup() {
  const env = { DB: sqliteD1(), INTERCOM_ACCESS_TOKEN: "t", ANTHROPIC_API_KEY: "k", APP_HOSTNAME: "ops.anurseinthemaking.com" };
  await ensureTables(env);
  await saveGuidelines(env, { version: "v1", text: "g" });
  return env;
}

test("parseConversationId takes ids, prefixed ids and inbox links", () => {
  assert.equal(cal.parseConversationId("215476759858717"), "215476759858717");
  assert.equal(cal.parseConversationId("conversation_215476759858717"), "215476759858717");
  assert.equal(cal.parseConversationId("https://app.intercom.com/a/inbox/zk/inbox/conversation/215476759858717"), "215476759858717");
  assert.equal(cal.parseConversationId("nope"), null);
});

test("golden set: save replaces labels, validates, lists, deletes", async () => {
  const env = await setup();
  await cal.saveGolden(env, { conversation_id: "https://app.intercom.com/a/inbox/x/inbox/conversation/111111", audit_day: "2026-11-08", note: "damaged box",
    labels: [{ author_type: "ivy", check_id: "answered", expected: "fail" }, { author_type: "ivy", check_id: "accurate", expected: "pass" }] });
  await cal.saveGolden(env, { conversation_id: "111111", audit_day: "2026-11-08", note: "damaged box", labels: [{ author_type: "ivy", check_id: "answered", expected: "pass" }] });
  const g = await cal.getGolden(env, "111111", "2026-11-08");
  assert.deepEqual(g.labels, [{ author_type: "ivy", check_id: "answered", expected: "pass" }]);
  assert.equal((await cal.listGolden(env))[0].labels, 1);
  await assert.rejects(cal.saveGolden(env, { conversation_id: "111111", audit_day: "2026-11-08", labels: [{ author_type: "bot", check_id: "answered", expected: "pass" }] }));
  await assert.rejects(cal.saveGolden(env, { conversation_id: "111111", audit_day: "11/08", labels: [] }));
  await cal.deleteGolden(env, "111111", "2026-11-08");
  assert.equal((await cal.listGolden(env)).length, 0);
});

test("CSV round trip; blank expected = unlabeled; bad rows are reported", async () => {
  const env = await setup();
  const csv = [
    "conversation_id,audit_day,author_type,check_id,expected,note",
    '222222,2026-11-07,ivy,right_voice,fail,"opener, banned"',
    "222222,2026-11-07,ivy,answered,PASS,",
    "222222,2026-11-07,team,one_message,,",
    "333333,2026-11-08,team,accurate,n/a,",
  ].join("\n");
  assert.deepEqual(await cal.importCsv(env, csv), { conversations: 2, labels: 3 });
  const out = await cal.exportCsv(env);
  assert.match(out, /^conversation_id,audit_day,author_type,check_id,expected,note\n/);
  assert.match(out, /222222,2026-11-07,ivy,right_voice,fail,"opener, banned"/);
  assert.match(out, /333333,2026-11-08,team,accurate,na,/);
  await assert.rejects(cal.importCsv(env, "conversation_id,audit_day,author_type,check_id,expected\n1234567,2026-11-08,ivy,not_a_check,pass"), /line 2/);
  await assert.rejects(cal.importCsv(env, "id,day\n1,2"), /Missing column/);
});

test("agreementReport: agreement, missed fails, false fails, off excluded", () => {
  const rows = [
    { author_type: "ivy", check_id: "answered", expected: "fail", got: "fail" },
    { author_type: "ivy", check_id: "answered", expected: "fail", got: "pass" }, // missed
    { author_type: "ivy", check_id: "answered", expected: "pass", got: "fail" }, // false fail
    { author_type: "team", check_id: "answered", expected: "na", got: "pass" },  // na mismatch
    { author_type: "team", check_id: "accurate", expected: "pass", got: "pass" },
    { author_type: "ivy", check_id: "one_message", expected: "pass", got: "off" },
  ];
  const r = cal.agreementReport(rows);
  assert.equal(r.overall.compared, 5);
  assert.equal(r.overall.agree, 2);
  assert.equal(r.overall.agreement, 40);
  const ans = r.by_check.find((c) => c.check_id === "answered");
  assert.deepEqual([ans.missed_fails, ans.false_fails, ans.na_mismatch, ans.agreement], [1, 1, 1, 25]);
  assert.equal(ans.ivy.agreement, 33.3);
  assert.equal(r.by_check.find((c) => c.check_id === "one_message").off, 1);
  assert.equal(r.by_author.team.agreement, 50);
});

test("calibration run end to end via the API, worked by ticks", async () => {
  const env = await setup();
  const W = windowFor("2026-11-08");
  const conv = { id: "444444", created_at: W.start + 3600, source: { type: "email", id: "s", subject: "Box", body: "My box is damaged", author: { type: "user", name: "Pat", email: "p@example.com" } },
    custom_attributes: {}, conversation_parts: { conversation_parts: [{ id: "4-1", part_type: "comment", author: { type: "bot", name: "Ivy", from_ai_agent: true }, body: "A teammate will help.", created_at: W.start + 7200 }] } };
  globalThis.fetch = async (url) => {
    const p = new URL(url).pathname;
    if (p === "/me") return Response.json({ app: { id_code: "ws" } });
    return p.endsWith("/444444") ? Response.json(conv) : new Response("nope", { status: 404 });
  };
  const raw = { results: CHECKS.map((c) => ({ group: "ivy", check: c.id, result: c.id === "no_silent_handoff" ? "fail" : "pass", evidence: "A teammate will help.", rewrite: "x", reply_id: "4-1", failed_rules: [] })),
    ivy_note: "", team_note: "", overall_note: "", issue_type_suggested: "Order Problem" };
  const client = { beta: { messages: { create: async () => ({ model: "m", stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify(raw) }], usage: {} }) } } };

  const call = (p, init = {}) => handleCxAuditRoutes(new Request(`https://ops.anurseinthemaking.com${p}`, { ...init, headers: { "Content-Type": "application/json" } }), { ...env, INTERCOM_ACCESS_TOKEN: "" }, { waitUntil() {} }, p.split("?")[0]);
  assert.equal((await call("/cx-audit/api/calibrations", { method: "POST", body: "{}" })).status, 400, "empty golden set is refused");

  const save = await call("/cx-audit/api/golden", { method: "POST", body: JSON.stringify({ conversation_id: "444444", audit_day: "2026-11-08", labels: [
    { author_type: "ivy", check_id: "no_silent_handoff", expected: "fail" },
    { author_type: "ivy", check_id: "answered", expected: "fail" },
    { author_type: "ivy", check_id: "accurate", expected: "pass" },
    { author_type: "team", check_id: "accurate", expected: "pass" }] }) });
  assert.equal(save.status, 201);
  await cal.saveGolden(env, { conversation_id: "555555", audit_day: "2026-11-08", labels: [{ author_type: "ivy", check_id: "answered", expected: "pass" }] }); // 404 in Intercom

  const tr = await (await handleCxAuditRoutes(new Request("https://ops.anurseinthemaking.com/cx-audit/api/golden/transcript?id=444444&day=2026-11-08"), env, { waitUntil() {} }, "/cx-audit/api/golden/transcript")).json();
  assert.equal(tr.counts.ivy, 1);
  assert.ok(tr.messages.some((m) => m.role === "ivy" && m.in_window));

  const created = await (await call("/cx-audit/api/calibrations", { method: "POST", body: "{}" })).json();
  assert.ok(created.id);
  assert.equal((await call("/cx-audit/api/calibrations", { method: "POST", body: "{}" })).status, 400, "one run at a time");
  let r;
  for (let i = 0; i < 6; i++) { r = await cal.calibrationTick(env, { client }); if (r.done || r.idle) break; }
  assert.equal(r.done, true);
  const detail = await cal.getCalibration(env, created.id);
  assert.equal(detail.status, "done");
  assert.equal(detail.errors, 1, "the missing conversation is reported, not fatal");
  assert.equal(detail.report.overall.compared, 3, "the team label is left out: no team reply that day");
  assert.equal(detail.report.overall.no_reply, 1);
  assert.equal(detail.report.overall.agree, 2, "handoff + accurate agree");
  const dis = detail.disagreements.map((d) => [d.author_type, d.check_id, d.expected, d.got]);
  assert.deepEqual(dis, [["ivy", "answered", "fail", "pass"]]);
  assert.ok(detail.item_errors.some((e) => /no team reply/.test(e.error)), "labels for a group with no reply that day are flagged");
  const list = await cal.listCalibrations(env);
  assert.equal(list[0].agreement, 66.7);
  assert.equal(list[0].guidelines_version, "v1");

  const csv = await handleCxAuditRoutes(new Request("https://ops.anurseinthemaking.com/cx-audit/api/golden/csv"), env, { waitUntil() {} }, "/cx-audit/api/golden/csv");
  assert.equal(csv.headers.get("Content-Type"), "text/csv; charset=utf-8");
});
