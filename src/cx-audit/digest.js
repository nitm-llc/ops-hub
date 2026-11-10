// ============================================================================
// CX SCORECARD AUDIT — the morning digest, sent as a Google Chat DM from Flo.
//
// After each SCHEDULED run finishes (daily, or weekly once switched), the
// people in the `digest_to` setting (default: Mark) get:
//   - pass rate for Ivy and the team, in one line
//   - "Fix first": the top 2-3 problems, where each comes from in Intercom
//     (procedure, guidance, Fin setting, team member), a suggested change, and
//     an example link
//   - a link to the full scorecard for that day
//
// Delivery reuses Flo (src/nitm-pm/outreach.js messagePerson, kind "digest"):
// the DM Flo already has with the person, logged on the Flo page, and a reply
// in that DM has the digest as context. messagePerson caps a message at 1500
// characters, so the digest is built to fit (fewer replies, shorter quotes).
//
// Sent once per run: claimed in cxa_settings ("digest:<run id>") first.
// A failed scheduled run sends a one-line failure notice instead.
// ============================================================================

import { db, getSetting } from "./store.js";
import { summary, fixList } from "./report.js";
import { CHECKS, PROBLEM } from "./checks.js";
import { messagePerson } from "../nitm-pm/outreach.js";

export const MAX_CHARS = 1500;
const NAME = Object.fromEntries(CHECKS.map((c) => [c.id, c.name]));
const LABEL = { ivy: "Ivy", team: "Team" };

const pct = (p, f) => (p + f ? Math.round((100 * p) / (p + f)) : null);
// Chat treats <...> as links and *...* as bold: keep customer-derived text inert.
const inert = (s) => String(s || "").replace(/[<>]/g, (c) => (c === "<" ? "‹" : "›")).replace(/\*/g, "∗").replace(/~/g, "∼").replace(/`/g, "'").replace(/\s+/g, " ").trim();
const clip = (s, n) => (s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : s);
const dayLabel = (d) => new Date(d + "T12:00:00Z").toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", timeZone: "UTC" });

export function scorecardUrl(env, from, to) {
  return `https://${env.APP_HOSTNAME || "ops.anurseinthemaking.com"}/cx-audit/#from=${from}&to=${to}`;
}

// Pure: data in, text out. Leads with what to fix, not with scores: the
// top problems from the fix list, each with where it comes from in Intercom,
// a suggested change and one example. Shrinks (3 -> 2 -> 1 items, shorter
// changes) until it fits Flo's 1500 characters.
export function formatDigest({ from, to, s, fixes, url }) {
  const period = from === to ? dayLabel(from) : `${dayLabel(from)} – ${dayLabel(to)}`;
  const parts = [];
  let convs = 0;
  for (const g of ["ivy", "team"]) {
    const t = s.totals[g];
    if (t) { parts.push(`${LABEL[g]} ${pct(t.pass, t.fail) ?? "—"}%`); convs += t.convs; }
  }
  const title = `*CX scorecard · ${period}*`;
  if (!parts.length) return `${title}\nNo Ivy or team replies to score${from === to ? " that day" : ""}.\n<${url}|Open the scorecard>`;
  const head = [title, `${parts.join(" · ")} of checks passed (${convs} conversation${convs === 1 ? "" : "s"})`];
  if (!fixes.length) return [...head, "", "Nothing failed. 🎉", `<${url}|Open the scorecard>`].join("\n");

  const render = (count, fixLen) => {
    const lines = [...head, "", "*Fix first*"];
    fixes.slice(0, count).forEach((g, i) => {
      lines.push(`${i + 1}. *${g.who} ${g.problem || PROBLEM[g.check_id] || NAME[g.check_id] || g.check_id}* (${g.count}×)`);
      if (g.source && !g.source.startsWith("Not recorded") && g.source !== "Not sure") lines.push(`   From: ${inert(g.source)}`);
      if (g.fix) lines.push(`   Change: ${clip(inert(g.fix), fixLen)}`);
      if (g.examples[0]) lines.push(`   <${g.examples[0].link}|See an example>`);
    });
    const more = fixes.length - count;
    lines.push("", `<${url}|${more > 0 ? `All ${fixes.length} fixes and details` : "Fixes and details"}>`);
    return lines.join("\n");
  };
  for (const [count, len] of [[3, 260], [3, 160], [2, 200], [1, 200]]) {
    const text = render(count, len);
    if (text.length <= MAX_CHARS) return text;
  }
  return render(1, 80);
}

export async function buildDigest(env, from, to) {
  const f = { from, to, author: "", check: "", issue: "", version: "" };
  const [s, fx] = await Promise.all([summary(env, f), fixList(env, f, { limit: 10 })]);
  return formatDigest({ from, to, s, fixes: fx.fixes, url: scorecardUrl(env, from, to) });
}

export async function recipients(env) {
  const v = await getSetting(env, "digest_to", ["Mark"]);
  return (Array.isArray(v) ? v : String(v).split(/[,;]+/)).map((x) => String(x).trim()).filter(Boolean);
}

export async function sendToRecipients(env, text, { send = messagePerson } = {}) {
  const out = [];
  for (const person of await recipients(env)) {
    try {
      const r = await send(env, { person, text, kind: "digest" });
      out.push({ person, sent: !!r.sent, message: r.message });
    } catch (e) {
      out.push({ person, sent: false, message: String(e.message || e) });
    }
  }
  return out;
}

// Called by the job when a scheduled run reaches done or failed. Never throws.
export async function digestAfterRun(env, run, { send } = {}) {
  if (!["daily", "weekly"].includes(run.kind)) return null;
  const d = db(env);
  const key = `digest:${run.id}`;
  const claim = await d.prepare("INSERT OR IGNORE INTO cxa_settings (key, value, updated_at, updated_by) VALUES (?, ?, ?, 'digest')")
    .bind(key, JSON.stringify({ status: "sending" }), new Date().toISOString()).run();
  if (!claim.meta?.changes) return null; // already sent (or being sent)
  try {
    const fresh = await d.prepare("SELECT status, error FROM cxa_runs WHERE id = ?").bind(run.id).first();
    const text = fresh?.status === "failed"
      ? `*CX scorecard* · the audit for ${run.day_from === run.day_to ? dayLabel(run.day_from) : `${dayLabel(run.day_from)} – ${dayLabel(run.day_to)}`} failed: ${inert(fresh.error || "unknown error")}\n<${scorecardUrl(env, run.day_from, run.day_to)}|Open the scorecard>`
      : await buildDigest(env, run.day_from, run.day_to);
    const result = await sendToRecipients(env, text, { send });
    await d.prepare("UPDATE cxa_settings SET value = ?, updated_at = ? WHERE key = ?").bind(JSON.stringify({ status: "sent", result }), new Date().toISOString(), key).run();
    return result;
  } catch (e) {
    console.error("cx-audit digest:", e);
    await d.prepare("UPDATE cxa_settings SET value = ?, updated_at = ? WHERE key = ?").bind(JSON.stringify({ status: "error", error: String(e.message || e) }), new Date().toISOString(), key).run();
    return null;
  }
}
