// ============================================================================
// CX SCORECARD AUDIT — Phase 1 pipeline: fetch -> extract -> filter -> summary.
// Shared by the Worker (later phases) and tools/cx-audit/preview.mjs.
// ============================================================================

import { getAppId, searchConversationIds, getConversations } from "./intercom.js";
import { extract, classify } from "./extract.js";
import { DEFAULT_EXCLUSIONS } from "./config.js";

const FREEMAIL = /^(gmail|googlemail|yahoo|ymail|aol|icloud|me|mac|hotmail|outlook|live|msn|comcast|att|sbcglobal|verizon|proton|protonmail)\.(com|net|me)$/;

export async function fetchAndFilter(env, win, { rules = DEFAULT_EXCLUSIONS, onProgress } = {}) {
  const appId = await getAppId(env);
  const ids = await searchConversationIds(env, win);
  onProgress?.(`search: ${ids.length} conversations updated since ${win.from}`);
  const fetched = await getConversations(env, ids);
  onProgress?.(`fetched ${fetched.length}`);

  const scoreable = [];
  const excluded = [];
  const errors = [];
  for (const f of fetched) {
    if (f.error) { errors.push({ id: f.id, error: f.error }); continue; }
    const record = extract(f.conversation, win, { appId });
    const verdict = classify(f.conversation, record, rules);
    if (verdict.status === "scoreable") scoreable.push(record);
    else excluded.push({ record, ...verdict });
  }
  return { appId, searched: ids.length, scoreable, excluded, errors, summary: summarize(win, ids.length, scoreable, excluded, errors) };
}

const bump = (o, k, n = 1) => { o[k] = (o[k] || 0) + n; };

export function summarize(win, searched, scoreable, excluded, errors) {
  const excludedBy = { preview: 0, spam: 0, exclusion_rule: 0, no_reply: 0 };
  const byRule = {};
  const excludedList = [];
  for (const e of excluded) {
    bump(excludedBy, e.reason);
    if (e.rule) bump(byRule, `${e.rule.kind}: ${e.rule.value}`);
    if (e.reason === "preview") continue; // dozens of Fin preview tests: count only
    excludedList.push({
      id: e.record.id, reason: e.reason, rule: e.rule ? `${e.rule.kind}: ${e.rule.value}` : null,
      sender_domain: e.record.sender_domain, issue_type: e.record.issue_type,
      subject: (e.record.subject || "").slice(0, 80),
    });
  }
  const replies = { ivy: 0, team: 0, automation: 0 };
  const shape = { ivy_only: 0, team_only: 0, both: 0 };
  const issueTypes = {};
  const domains = {};
  for (const r of scoreable) {
    for (const k of Object.keys(replies)) replies[k] += r.counts[k];
    if (r.counts.ivy && r.counts.team) shape.both++;
    else if (r.counts.ivy) shape.ivy_only++;
    else shape.team_only++;
    bump(issueTypes, r.issue_type || "(none)");
    const d = r.via_shopify ? "shopify contact form" : r.sender_domain || `(${r.channel})`;
    bump(domains, FREEMAIL.test(d) ? "(personal email)" : d);
  }
  return {
    window: { from: win.from, to: win.to, tz: win.tz },
    searched,
    fetch_errors: errors.length,
    excluded: excludedBy,
    excluded_by_rule: byRule,
    scoreable: scoreable.length,
    scoreable_shape: shape,
    replies_in_window: replies,
    issue_types: issueTypes,
    // For spotting senders that should be on the exclusion list.
    scoreable_senders: Object.fromEntries(Object.entries(domains).sort((a, b) => b[1] - a[1])),
    excluded_list: excludedList,
  };
}

// Three samples with variety: Ivy only, team only, both (falling back to any).
export function pickSamples(scoreable, n = 3) {
  const pick = [];
  const tryAdd = (r) => { if (r && !pick.includes(r) && pick.length < n) pick.push(r); };
  tryAdd(scoreable.find((r) => r.counts.ivy && !r.counts.team));
  tryAdd(scoreable.find((r) => r.counts.team && !r.counts.ivy));
  tryAdd(scoreable.find((r) => r.counts.ivy && r.counts.team));
  for (const r of scoreable) tryAdd(r);
  return pick;
}
