// CX scorecard audit, Phase 4: dashboard queries on real SQLite. Invented scores.
import { test } from "node:test";
import assert from "node:assert/strict";
import { sqliteD1 } from "./support/sqlite-d1.mjs";
import { ensureTables, saveScore, addFinChange } from "../src/cx-audit/store.js";
import { summary, failures, parseFilters } from "../src/cx-audit/report.js";
import { handleCxAuditRoutes } from "../src/cx-audit/index.js";
import { CHECKS } from "../src/cx-audit/checks.js";

// A score where `fails` lists the failing check ids; everything else passes,
// except encouragement which is na.
function score({ id, day, group = "ivy", fails = [], issue = "Shipping & Delivery", version = "v1" }) {
  const checks = {};
  for (const c of CHECKS) {
    const result = fails.includes(c.id) ? "fail" : c.id === "encouragement_placed_right" ? "na" : "pass";
    checks[c.id] = { result, evidence: `${c.id} quote`, rewrite: result === "fail" ? `${c.id} rewrite` : "", reply_id: "r1", ...(c.id === "right_voice" ? { failed_rules: result === "fail" ? ["banned_phrases"] : [] } : {}) };
  }
  const vals = Object.values(checks).map((c) => c.result);
  const G = { scored: true, replies: ["r1"], checks, note: "n", passed: vals.filter((v) => v === "pass").length, failed: vals.filter((v) => v === "fail").length, applicable: vals.filter((v) => v !== "na").length };
  const empty = { scored: false, replies: [], checks: {}, note: "" };
  return {
    conversation_id: id, link: `https://app.intercom.com/a/inbox/ws/inbox/conversation/${id}`, audit_day: day, issue_type: issue,
    issue_type_source: "intercom", channel: "email", groups: { ivy: group === "ivy" ? G : empty, team: group === "team" ? G : empty },
    overall_note: "o", guidelines_version: version, model: "m", usage: {},
  };
}

async function seeded() {
  const env = { DB: sqliteD1() };
  await ensureTables(env);
  await saveScore(env, score({ id: "1", day: "2026-11-07", fails: ["right_voice", "no_silent_handoff"] }), {});
  await saveScore(env, score({ id: "2", day: "2026-11-08", fails: ["right_voice"], issue: "Order Problem" }), {});
  await saveScore(env, score({ id: "3", day: "2026-11-08", group: "team" }), {});
  // Conversation 2 re-scored under v2 later: only this newer row may count.
  await new Promise((r) => setTimeout(r, 5));
  await saveScore(env, score({ id: "2", day: "2026-11-08", fails: [], issue: "Order Problem", version: "v2" }), {});
  await addFinChange(env, { day: "2026-11-08", what: "Handoff guidance updated", who: "Mark" });
  return env;
}
const F = (o = {}) => parseFilters({ from: "2026-11-01", to: "2026-11-30", ...o });

test("parseFilters validates dates and drops junk", () => {
  assert.throws(() => parseFilters({ from: "x", to: "2026-11-01" }));
  assert.throws(() => parseFilters({ from: "2026-11-09", to: "2026-11-01" }));
  const f = parseFilters({ from: "2026-11-01", to: "2026-11-02", author: "robot", check: "DROP TABLE" });
  assert.equal(f.author, "");
  assert.equal(f.check, "");
});

test("summary: per-check rates, newest version only, totals, trend, Fin changes", async () => {
  const env = await seeded();
  const s = await summary(env, F());
  const rv = s.per_check.find((r) => r.check_id === "right_voice" && r.author_type === "ivy");
  assert.equal(rv.pass + rv.fail, 2, "conversation 2 counted once");
  assert.equal(rv.fail, 1, "the v2 re-score (a pass) replaced the v1 fail");
  assert.equal(s.totals.ivy.convs, 2);
  assert.equal(s.totals.team.convs, 1);
  const enc = s.per_check.find((r) => r.check_id === "encouragement_placed_right" && r.author_type === "ivy");
  assert.equal(enc.pass + enc.fail, 0);
  assert.equal(enc.na, 2);
  assert.deepEqual(s.trend.filter((t) => t.author_type === "ivy").map((t) => t.day), ["2026-11-07", "2026-11-08"]);
  assert.deepEqual(s.issue_types, ["Order Problem", "Shipping & Delivery"]);
  assert.deepEqual(s.versions, ["v1", "v2"]);
  assert.equal(s.fin_changes[0].what, "Handoff guidance updated");
});

test("summary filters: author, issue, check, pinned version", async () => {
  const env = await seeded();
  assert.deepEqual(Object.keys((await summary(env, F({ author: "team" }))).totals), ["team"]);
  assert.equal((await summary(env, F({ issue: "Order Problem" }))).totals.ivy.convs, 1);
  const byCheck = await summary(env, F({ check: "no_silent_handoff" }));
  assert.equal(byCheck.totals.ivy.pass + byCheck.totals.ivy.fail, 2, "totals scoped to the one check");
  const v1 = await summary(env, F({ version: "v1" }));
  assert.equal(v1.per_check.find((r) => r.check_id === "right_voice" && r.author_type === "ivy").fail, 2);
});

test("failures: newest first, with evidence, rewrite, link and paging", async () => {
  const env = await seeded();
  const r = await failures(env, F());
  assert.deepEqual(r.failures.map((x) => [x.day, x.check_id]), [["2026-11-07", "no_silent_handoff"], ["2026-11-07", "right_voice"]]);
  assert.equal(r.failures[1].rewrite, "right_voice rewrite");
  assert.deepEqual(r.failures[1].failed_rules, ["banned_phrases"]);
  assert.match(r.failures[0].link, /conversation\/1$/);
  const page = await failures(env, F(), { limit: 1 });
  assert.equal(page.failures.length, 1);
  assert.equal(page.more, true);
  assert.equal(page.next_offset, 1);
  assert.equal((await failures(env, F({ check: "right_voice" }))).failures.length, 1);
});

test("API: summary and failures endpoints", async () => {
  const env = { ...(await seeded()), APP_HOSTNAME: "ops.anurseinthemaking.com" };
  const get = (p) => handleCxAuditRoutes(new Request(`https://ops.anurseinthemaking.com${p}`), env, { waitUntil() {} }, p.split("?")[0]);
  const s = await (await get("/cx-audit/api/summary?from=2026-11-01&to=2026-11-30&author=ivy")).json();
  assert.equal(s.totals.ivy.convs, 2);
  const f = await (await get("/cx-audit/api/failures?from=2026-11-01&to=2026-11-30&limit=1")).json();
  assert.equal(f.failures.length, 1);
  assert.equal((await get("/cx-audit/api/summary?from=bad&to=2026-11-30")).status, 400);
});
