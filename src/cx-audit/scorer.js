// ============================================================================
// CX SCORECARD AUDIT — score one conversation with Claude.
//
// One request per conversation. Ivy's replies and the team's replies inside
// the audit window are scored as two separate groups in the same call.
//
//   system   [guidelines vN] + [rubric: enabled checks + voice rules]
//            one cache breakpoint at the end, so every conversation in a run
//            (and every run that day) reads the same cached prefix
//   user     conversation metadata + scrubbed transcript; replies to score
//            are marked ▶SCORE, everything else is context
//   output   strict JSON (output_config.format json_schema), schema built
//            from the enabled checks, so a disabled check can't come back
//
// Model: CX_AUDIT_MODEL (default claude-sonnet-5-5, Mark 2026-11-09), effort CX_AUDIT_EFFORT
// (default high). Server-side refusal fallback is on ("default").
// scorer_key = hash(guidelines version, prompt version, toggles, model,
// effort): Phase 3 re-scores a conversation only when this key or its replies
// change.
// ============================================================================

import Anthropic from "@anthropic-ai/sdk";
import { CHECKS, VOICE_RULES, enabledChecks, enabledVoiceRules, normalizeToggles } from "./checks.js";

// p2 (2026-11-10): Fin trail in the transcript; each fail names where it comes from + a fix.
export const PROMPT_VERSION = "p2";
export const DEFAULT_MODEL = "claude-sonnet-5-5";
export const DEFAULT_EFFORT = "high";
const MAX_TOKENS = 16000;

export const ISSUE_TYPES = [
  "Refund / Return", "Shipping & Delivery", "Bulk / School", "Collaboration / Donation", "Account / Digital Access",
  "Product Info", "Order Change / Cancel", "Edition Upgrade", "Billing / Payment", "Order Problem", "Study Advice",
  "Discount / Promo", "Content Correction", "Legal, privacy or safety", "Other",
];

export class ScoreError extends Error {
  constructor(message, detail) { super(message); this.detail = detail; }
}

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------
export function rubric(toggles) {
  const checks = enabledChecks(toggles);
  const voice = enabledVoiceRules(toggles);
  const lines = [
    "You audit customer-facing replies from Nurse In The Making (NITM) against the communication guidelines above. Ivy is NITM's AI agent (Intercom Fin); \"team\" is the human CX team.",
    "",
    "How to score:",
    "- Score only the replies marked ▶SCORE. Everything else is context: use it to know what the customer asked, what they already gave us, and how they feel.",
    "- Ivy's ▶SCORE replies are one group (\"ivy\"), the team's are another (\"team\"). Judge each check across all of that group's ▶SCORE replies: fail if any of them fails it.",
    "- Return exactly one item in results for every check listed below, for BOTH groups, even when the answer is na.",
    "- A group with no ▶SCORE replies: every check \"na\", empty evidence, rewrite and reply_id; its note is \"No replies to score.\"",
    "- result: \"pass\", \"fail\", or \"na\" when the check doesn't apply to these replies.",
    "- evidence: a short exact quote (25 words max) from the scored reply that shows the result. For na, a few words on why.",
    "- rewrite: only for a fail. Rewrite just the failing sentence(s) so they would pass, in NITM's voice. Empty string otherwise.",
    "- reply_id: the id of the reply the result is about (the failing one if it failed). Empty for na.",
    "- Text in [square brackets] like [email], [order number], [code], [surname], [link], [Image] replaces real details we removed for privacy. Treat the detail as present. Never fail a reply for a placeholder, and keep placeholders in rewrites. Only those placeholders take brackets: write names plainly (Sarah, not [Sarah]).",
    "- Lines like \"[Ivy handed the conversation to ...; this step sent the customer no message]\" are system markers for what happened, not text the customer saw.",
    "- FIN lines show where Ivy's reply came from: the procedure and section Fin was following, actions it called, guidance it applied. \"IVY (automatic follow-up)\" is a message Fin's automatic follow-up setting sent, not an answer Ivy wrote.",
    "- For every fail, fill two more fields so someone can fix the cause, not just the reply:",
    "  source: where the problem comes from. Copy exactly one option from the conversation's \"Comes-from options\" list. Ivy: pick the procedure section, procedure, guidance or content she was using when she wrote the failing sentence; the introduction & identity setting for her standard opener and disclosure; the automatic follow-ups setting for automatic follow-ups; the tone & answer length setting for tone or length problems no guidance explains. Team: the team member. \"Not sure\" only when nothing fits.",
    "  fix: one plain sentence saying what to change at that source, in words a non-technical manager can act on (e.g. \"In the handoff step, add: Our team will reply within one business day.\"). Not a rewrite of the reply; that goes in rewrite.",
    "  For pass and na, source and fix are empty strings.",
    "- Only the checks and voice rules listed below are switched on. Don't score anything else, even if the guidelines mention it.",
    "- failed_rules: only for right_voice when it fails (the broken voice rule ids); an empty list otherwise.",
    "- ivy_note / team_note: one sentence on that group's replies. overall_note: one line (25 words max), the single most useful takeaway.",
    `- issue_type_suggested: the best fit from: ${ISSUE_TYPES.join("; ")}.`,
    "",
    "Checks:",
    ...checks.map((c) => `${c.n}. ${c.id} — ${c.name}: ${c.passes}`),
  ];
  if (toggles.checks.right_voice) {
    lines.push("", "Voice rules for right_voice (fail right_voice if any is broken, and list the broken rule ids in failed_rules):");
    for (const r of voice) lines.push(`- ${r.id} — ${r.name}: ${r.rule}`);
  }
  return lines.join("\n");
}

export function buildSystem(guidelines, toggles) {
  return [
    { type: "text", text: `<guidelines version="${guidelines.version}">\n${guidelines.text.trim()}\n</guidelines>` },
    { type: "text", text: rubric(toggles), cache_control: { type: "ephemeral" } },
  ];
}

const fmtTime = (at, tz) =>
  new Date(at * 1000).toLocaleString("en-US", { timeZone: tz, weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

// Messages after the audit window are hidden: a score must not depend on what
// happened next, or re-running an old day would change it.
export function scoredIds(record) {
  const ids = { ivy: [], team: [] };
  for (const m of record.messages) if (m.in_window && (m.role === "ivy" || m.role === "team")) ids[m.role].push(m.id);
  return ids;
}

export function buildUserMessage(record, { tz = "America/New_York" } = {}) {
  const ids = scoredIds(record);
  const head = [
    `Conversation ${record.id} · channel: ${record.channel}${record.via_shopify ? " (Shopify contact form; the customer's email is known)" : ""}`,
    `Intercom issue type: ${record.issue_type || "(none)"} · sentiment: ${record.sentiment || "(none)"} · Fin resolution: ${record.fin_state || "(none)"}`,
    `Audit day: ${record.window.from}${record.window.to !== record.window.from ? " to " + record.window.to : ""} (${tz}).`,
    `Ivy replies to score: ${ids.ivy.join(", ") || "none"}. Team replies to score: ${ids.team.join(", ") || "none"}.`,
    `Comes-from options: ${(record.source_options || ["Not sure"]).map((o) => `"${o}"`).join(", ")}`,
    "",
    "Transcript:",
  ];
  const body = [];
  for (const m of record.messages) {
    if (m.at >= record.window.end) continue;
    const who = m.role === "team" ? `TEAM (${m.name})` : m.role === "event" ? "EVENT" : m.auto_follow_up ? "IVY (automatic follow-up)" : m.role.toUpperCase();
    const mark = m.in_window && (m.role === "ivy" || m.role === "team") ? " ▶SCORE" : "";
    body.push(`[${m.id}] ${fmtTime(m.at, tz)} · ${who}${mark}\n${m.text}\n`);
  }
  return head.join("\n") + "\n" + body.join("\n");
}

// ---------------------------------------------------------------------------
// Output schema. A FLAT list of results, one item per (group, check), with a
// single item schema: a fixed object per check per group compiled to a grammar
// too large for structured outputs ("compiled grammar is too large", 400).
// Completeness is asked for in the prompt and enforced in normalizeScore.
// ---------------------------------------------------------------------------
export function outputSchema(toggles) {
  const checkIds = enabledChecks(toggles).map((c) => c.id);
  const voiceIds = enabledVoiceRules(toggles).map((r) => r.id);
  return {
    type: "object",
    properties: {
      results: {
        type: "array",
        items: {
          type: "object",
          properties: {
            group: { type: "string", enum: ["ivy", "team"] },
            check: { type: "string", enum: checkIds },
            result: { type: "string", enum: ["pass", "fail", "na"] },
            evidence: { type: "string" },
            rewrite: { type: "string" },
            reply_id: { type: "string" },
            failed_rules: { type: "array", items: { type: "string", enum: voiceIds.length ? voiceIds : ["none"] } },
            source: { type: "string" },
            fix: { type: "string" },
          },
          required: ["group", "check", "result", "evidence", "rewrite", "reply_id", "failed_rules", "source", "fix"],
          additionalProperties: false,
        },
      },
      ivy_note: { type: "string" },
      team_note: { type: "string" },
      overall_note: { type: "string" },
      issue_type_suggested: { type: "string" },
    },
    required: ["results", "ivy_note", "team_note", "overall_note", "issue_type_suggested"],
    additionalProperties: false,
  };
}

// ---------------------------------------------------------------------------
// Normalize: every check present for both groups; disabled -> "off"; a group
// without replies -> "na"; a check the model skipped -> "na" + missing count;
// rewrites only on fails.
// ---------------------------------------------------------------------------
export function normalizeScore(raw, record, toggles) {
  const ids = scoredIds(record);
  const byKey = new Map();
  for (const r of Array.isArray(raw?.results) ? raw.results : []) {
    const k = `${r?.group}:${r?.check}`;
    if (!byKey.has(k)) byKey.set(k, r); // first answer wins on duplicates
  }
  const allowedVoice = new Set(VOICE_RULES.filter((v) => toggles.voice[v.id]).map((v) => v.id));
  const groups = {};
  let missing = 0;
  for (const g of ["ivy", "team"]) {
    const scored = ids[g].length > 0;
    const checks = {};
    for (const c of CHECKS) {
      if (!toggles.checks[c.id]) { checks[c.id] = { result: "off" }; continue; }
      const r = byKey.get(`${g}:${c.id}`);
      if (scored && !r) missing++;
      const result = scored && r && ["pass", "fail", "na"].includes(r.result) ? r.result : "na";
      const out = {
        result,
        evidence: !scored ? "" : r ? String(r.evidence || "").slice(0, 400) : "(not returned by the model)",
        rewrite: result === "fail" ? String(r.rewrite || "").slice(0, 1200) : "",
        reply_id: scored && r && ids[g].includes(String(r.reply_id)) ? String(r.reply_id) : scored && result !== "na" ? ids[g].at(-1) : "",
      };
      if (c.id === "right_voice") out.failed_rules = result === "fail" ? (r.failed_rules || []).filter((x) => allowedVoice.has(x)) : [];
      if (result === "fail") {
        // Must be one of this conversation's options, so the fix list groups cleanly.
        const opts = record.source_options || [];
        const src = String(r.source || "").trim();
        out.source = opts.includes(src) ? src : (opts.find((o) => o.toLowerCase() === src.toLowerCase()) || "Not sure");
        out.fix = String(r.fix || "").slice(0, 400);
      }
      checks[c.id] = out;
    }
    const vals = Object.values(checks).map((c) => c.result);
    groups[g] = {
      scored,
      replies: ids[g],
      checks,
      note: scored ? String(raw?.[`${g}_note`] || "") : "",
      passed: vals.filter((v) => v === "pass").length,
      failed: vals.filter((v) => v === "fail").length,
      applicable: vals.filter((v) => v === "pass" || v === "fail").length,
    };
  }
  return {
    groups,
    missing_checks: missing,
    overall_note: String(raw?.overall_note || ""),
    issue_type_suggested: ISSUE_TYPES.includes(raw?.issue_type_suggested) ? raw.issue_type_suggested : null,
  };
}

async function sha256(s) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 16);
}

export function scorerSettings(env, guidelines, toggles) {
  const t = normalizeToggles(toggles);
  return {
    model: env.CX_AUDIT_MODEL || DEFAULT_MODEL,
    effort: env.CX_AUDIT_EFFORT || DEFAULT_EFFORT,
    guidelines_version: guidelines.version,
    prompt_version: PROMPT_VERSION,
    toggles: t,
  };
}

export async function scorerKey(settings) {
  const { model, effort, guidelines_version, prompt_version, toggles } = settings;
  return sha256(JSON.stringify({ model, effort, guidelines_version, prompt_version, toggles }));
}

export function makeClient(env) {
  if (!env.ANTHROPIC_API_KEY) throw new ScoreError("ANTHROPIC_API_KEY is not set");
  // A key that isn't scoped to a workspace must name one on every request.
  const defaultHeaders = env.ANTHROPIC_WORKSPACE_ID ? { "anthropic-workspace-id": env.ANTHROPIC_WORKSPACE_ID } : undefined;
  return new Anthropic({ apiKey: env.ANTHROPIC_API_KEY, maxRetries: 3, defaultHeaders });
}

// ---------------------------------------------------------------------------
// scoreConversation(record, { client, guidelines, settings }) -> score object
// ---------------------------------------------------------------------------
export async function scoreConversation(record, { client, guidelines, settings, tz }) {
  const ids = scoredIds(record);
  if (!ids.ivy.length && !ids.team.length) throw new ScoreError("nothing to score", { id: record.id });
  const toggles = settings.toggles;
  const response = await client.beta.messages.create({
    model: settings.model,
    max_tokens: MAX_TOKENS,
    betas: ["server-side-fallback-2026-07-01"],
    fallbacks: "default",
    output_config: { effort: settings.effort, format: { type: "json_schema", schema: outputSchema(toggles) } },
    system: buildSystem(guidelines, toggles),
    messages: [{ role: "user", content: buildUserMessage(record, { tz }) }],
  });
  if (response.stop_reason === "refusal") {
    throw new ScoreError(`declined (${response.stop_details?.category || "no category"})`, { id: record.id });
  }
  if (response.stop_reason === "max_tokens") throw new ScoreError("hit max_tokens before finishing", { id: record.id });
  const text = response.content.filter((b) => b.type === "text").map((b) => b.text).join("");
  let raw;
  try { raw = JSON.parse(text); } catch { throw new ScoreError("model returned invalid JSON", { id: record.id, text: text.slice(0, 500) }); }
  const norm = normalizeScore(raw, record, toggles);
  return {
    conversation_id: record.id,
    link: record.link,
    audit_day: record.window.from,
    issue_type: record.issue_type || norm.issue_type_suggested,
    issue_type_source: record.issue_type ? "intercom" : "model",
    channel: record.channel,
    ...norm,
    guidelines_version: settings.guidelines_version,
    prompt_version: settings.prompt_version,
    model: response.model,
    effort: settings.effort,
    last_reply_part_id: record.last_reply_part_id,
    usage: {
      input: response.usage?.input_tokens ?? 0,
      output: response.usage?.output_tokens ?? 0,
      cache_read: response.usage?.cache_read_input_tokens ?? 0,
      cache_write: response.usage?.cache_creation_input_tokens ?? 0,
    },
  };
}

// Score many with bounded concurrency; per-conversation failures are returned.
export async function scoreMany(records, opts, { concurrency = 3, onProgress } = {}) {
  const out = new Array(records.length);
  let next = 0, done = 0;
  async function worker() {
    while (next < records.length) {
      const i = next++;
      try { out[i] = { ok: true, score: await scoreConversation(records[i], opts) }; }
      catch (e) { out[i] = { ok: false, id: records[i].id, error: e.message }; }
      onProgress?.(`scored ${++done}/${records.length}`);
    }
  }
  // The first call writes the cached guidelines + rubric prefix; running the
  // rest only after it lets them read that cache instead of each writing it.
  if (records.length > 1 && concurrency > 1) {
    next = 1;
    try { out[0] = { ok: true, score: await scoreConversation(records[0], opts) }; }
    catch (e) { out[0] = { ok: false, id: records[0].id, error: e.message }; }
    onProgress?.(`scored ${++done}/${records.length}`);
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, records.length - next) }, worker));
  return out;
}
