// CX scorecard audit, Phase 2: prompt, schema, normalization, the API call
// shape, against a fake client (no network, no spend). Invented records.
import { test } from "node:test";
import assert from "node:assert/strict";
import { DEFAULT_TOGGLES, normalizeToggles, CHECKS } from "../src/cx-audit/checks.js";
import { buildSystem, buildUserMessage, outputSchema, normalizeScore, scoreConversation, scorerSettings, scorerKey, scoreMany, rubric } from "../src/cx-audit/scorer.js";
import { windowFor } from "../src/cx-audit/config.js";

const WIN = windowFor("2026-11-08");
const at = (h) => WIN.start + h * 3600;
const record = (messages) => ({
  id: "500", link: "https://app.intercom.com/a/inbox/ws/inbox/conversation/500", channel: "email", via_shopify: true,
  issue_type: null, sentiment: "Negative", fin_state: "Escalated", window: { from: WIN.from, to: WIN.to, start: WIN.start, end: WIN.end },
  last_reply_part_id: "3", messages,
});
const R = record([
  { id: "source-1", at: at(-20), role: "customer", name: "Customer", in_window: false, text: "My box arrived damaged. Order [order number]." },
  { id: "2", at: at(9), role: "ivy", name: "Ivy", in_window: true, text: "Hi there, I'm well trained to answer your query." },
  { id: "3", at: at(9.1), role: "event", name: "Ivy", in_window: true, text: "[Ivy handed the conversation to the team queue (unassigned); this step sent the customer no message]" },
  { id: "4", at: at(30), role: "team", name: "Aimee", in_window: false, text: "Next-day reply" },
]);
const G = { version: "v1", text: "# Guidelines\nBe kind." };
const T = normalizeToggles(DEFAULT_TOGGLES);

test("open-decision voice rules are off by default; all-off voice disables right_voice", () => {
  assert.equal(T.voice.speaker_identity, false);
  assert.equal(T.voice.banned_phrases, true);
  const none = normalizeToggles({ voice: Object.fromEntries(Object.keys(T.voice).map((k) => [k, false])) });
  assert.equal(none.checks.right_voice, false);
});

test("system: guidelines first, rubric last with the only cache breakpoint", () => {
  const sys = buildSystem(G, T);
  assert.match(sys[0].text, /<guidelines version="v1">/);
  assert.equal(sys[0].cache_control, undefined);
  assert.deepEqual(sys[1].cache_control, { type: "ephemeral" });
  assert.match(sys[1].text, /banned_phrases/);
  assert.doesNotMatch(sys[1].text, /speaker_identity/, "disabled rules are not in the prompt");
});

test("disabled checks leave the prompt and the schema", () => {
  const t = normalizeToggles({ checks: { one_message: false } });
  assert.doesNotMatch(rubric(t), /one_message/);
  const ids = outputSchema(t).properties.results.items.properties.check.enum;
  assert.ok(!ids.includes("one_message"));
  assert.ok(ids.includes("answered"));
});

test("user message: marks ▶SCORE, hides messages after the window", () => {
  const u = buildUserMessage(R);
  assert.match(u, /\[2\] .* · IVY ▶SCORE/);
  assert.match(u, /Ivy replies to score: 2\. Team replies to score: none\./);
  assert.doesNotMatch(u, /Next-day reply/);
  assert.match(u, /Shopify contact form/);
});

test("schema is strict: every object closed, every property required", () => {
  const walk = (s) => {
    if (s.type === "object") {
      assert.equal(s.additionalProperties, false);
      assert.deepEqual([...s.required].sort(), Object.keys(s.properties).sort());
      Object.values(s.properties).forEach(walk);
    }
    if (s.type === "array") walk(s.items);
  };
  walk(outputSchema(T));
});

function fakeRaw({ drop = [] } = {}) {
  const results = [];
  for (const group of ["ivy", "team"]) {
    for (const c of CHECKS.filter((c) => T.checks[c.id])) {
      if (drop.includes(`${group}:${c.id}`)) continue;
      let r = { group, check: c.id, result: "pass", evidence: "ok", rewrite: "should be dropped", reply_id: "2", failed_rules: [] };
      if (c.id === "right_voice") r = { ...r, result: "fail", evidence: "well trained to answer your query", rewrite: "Hi! I'm Ivy.", failed_rules: ["banned_phrases", "speaker_identity"] };
      if (c.id === "no_silent_handoff") r = { ...r, result: "fail", evidence: "no message", rewrite: "Someone from our team will reply by [time].", reply_id: "999" };
      results.push(r);
    }
  }
  results.push({ group: "ivy", check: "answered", result: "fail", evidence: "dup", rewrite: "", reply_id: "2", failed_rules: [] }); // duplicate: ignored
  return { results, ivy_note: "Opener breaks voice.", team_note: "x", overall_note: "Ivy handed off silently.", issue_type_suggested: "Order Problem" };
}

test("normalizeScore: off/na/rewrite rules and reply ids", () => {
  const t = normalizeToggles({ checks: { one_message: false } });
  const n = normalizeScore(fakeRaw(), R, t);
  assert.equal(n.groups.ivy.scored, true);
  assert.equal(n.groups.ivy.checks.one_message.result, "off");
  assert.equal(n.groups.ivy.checks.answered.rewrite, "", "rewrite only on fails");
  assert.deepEqual(n.groups.ivy.checks.right_voice.failed_rules, ["banned_phrases"], "disabled sub-rule dropped");
  assert.equal(n.groups.ivy.checks.no_silent_handoff.reply_id, "2", "unknown reply id falls back to a scored reply");
  assert.equal(n.groups.team.scored, false);
  assert.ok(Object.values(n.groups.team.checks).every((c) => c.result === "na" || c.result === "off"));
  assert.equal(n.groups.ivy.failed, 2);
  assert.equal(n.issue_type_suggested, "Order Problem");
  assert.equal(n.groups.ivy.checks.answered.result, "pass", "first answer wins on duplicates");
  assert.equal(n.groups.ivy.note, "Opener breaks voice.");
  assert.equal(n.missing_checks, 0);
});

test("normalizeScore: a check the model skipped is na and counted", () => {
  const n = normalizeScore(fakeRaw({ drop: ["ivy:accurate"] }), R, T);
  assert.equal(n.groups.ivy.checks.accurate.result, "na");
  assert.match(n.groups.ivy.checks.accurate.evidence, /not returned/);
  assert.equal(n.missing_checks, 1);
});

test("scoreConversation: request shape and result", async () => {
  let req;
  const client = { beta: { messages: { create: async (r) => { req = r; return {
    model: "claude-opus-5-5", stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify(fakeRaw()) }],
    usage: { input_tokens: 900, output_tokens: 700, cache_read_input_tokens: 5000, cache_creation_input_tokens: 0 },
  }; } } } };
  const settings = scorerSettings({}, G, DEFAULT_TOGGLES);
  const s = await scoreConversation(R, { client, guidelines: G, settings });
  assert.equal(req.model, "claude-sonnet-5-5");
  assert.equal(req.output_config.effort, "high");
  assert.equal(req.output_config.format.type, "json_schema");
  assert.equal(req.fallbacks, "default");
  assert.deepEqual(req.betas, ["server-side-fallback-2026-07-01"]);
  assert.equal(s.issue_type, "Order Problem");
  assert.equal(s.issue_type_source, "model");
  assert.equal(s.guidelines_version, "v1");
  assert.equal(s.usage.cache_read, 5000);
  assert.equal(s.link, R.link);
});

test("model settings come from env; scorer_key changes with toggles and guidelines", async () => {
  const a = scorerSettings({ CX_AUDIT_MODEL: "claude-sonnet-5-5", CX_AUDIT_EFFORT: "medium" }, G, DEFAULT_TOGGLES);
  assert.equal(a.model, "claude-sonnet-5-5");
  assert.equal(a.effort, "medium");
  const k1 = await scorerKey(scorerSettings({}, G, DEFAULT_TOGGLES));
  const k2 = await scorerKey(scorerSettings({}, G, { voice: { emoji: true } }));
  const k3 = await scorerKey(scorerSettings({}, { ...G, version: "v2" }, DEFAULT_TOGGLES));
  assert.notEqual(k1, k2);
  assert.notEqual(k1, k3);
  assert.equal(k1, await scorerKey(scorerSettings({}, G, DEFAULT_TOGGLES)), "stable");
});

test("refusal, truncation and bad JSON are reported per conversation, not thrown", async () => {
  const mk = (resp) => ({ beta: { messages: { create: async () => resp } } });
  const settings = scorerSettings({}, G, DEFAULT_TOGGLES);
  const out = await scoreMany([R, R, R], { guidelines: G, settings, client: mk({ stop_reason: "refusal", stop_details: { category: "cyber" }, content: [] }) });
  assert.ok(out.every((o) => !o.ok && /declined \(cyber\)/.test(o.error)));
  const bad = await scoreMany([R], { guidelines: G, settings, client: mk({ stop_reason: "end_turn", content: [{ type: "text", text: "{nope" }] }) });
  assert.match(bad[0].error, /invalid JSON/);
  const cut = await scoreMany([R], { guidelines: G, settings, client: mk({ stop_reason: "max_tokens", content: [] }) });
  assert.match(cut[0].error, /max_tokens/);
});

test("makeClient sends the workspace header only when configured", async () => {
  const { makeClient } = await import("../src/cx-audit/scorer.js");
  const seen = [];
  const orig = globalThis.fetch;
  globalThis.fetch = async (url, init) => { seen.push(new Headers(init.headers).get("anthropic-workspace-id")); return new Response(JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "stop" } }), { status: 400, headers: { "content-type": "application/json" } }); };
  try {
    for (const env of [{ ANTHROPIC_API_KEY: "k", ANTHROPIC_WORKSPACE_ID: "wrkspc_1" }, { ANTHROPIC_API_KEY: "k" }]) {
      await makeClient(env).messages.create({ model: "m", max_tokens: 1, messages: [{ role: "user", content: "x" }] }).catch(() => {});
    }
  } finally { globalThis.fetch = orig; }
  assert.deepEqual(seen, ["wrkspc_1", null]);
});

test("scoreMany warms the cache: first call finishes before the rest start", async () => {
  const log = [];
  let n = 0;
  const client = { beta: { messages: { create: async () => {
    const i = n++; log.push(`start${i}`);
    await new Promise((r) => setTimeout(r, i === 0 ? 20 : 1));
    log.push(`end${i}`);
    return { model: "m", stop_reason: "end_turn", content: [{ type: "text", text: JSON.stringify(fakeRaw()) }], usage: {} };
  } } } };
  const settings = scorerSettings({}, G, DEFAULT_TOGGLES);
  const out = await scoreMany([R, R, R, R], { client, guidelines: G, settings }, { concurrency: 3 });
  assert.equal(out.filter((o) => o.ok).length, 4);
  assert.deepEqual(log.slice(0, 2), ["start0", "end0"]);
});
