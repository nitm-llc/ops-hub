// ============================================================================
// CX SCORECARD AUDIT — the morning digest, sent as a Google Chat DM from Flo.
//
// After each SCHEDULED run finishes (daily, or weekly once switched), the
// people in the `digest_to` setting (default: Mark) get:
//   - pass rate for Ivy and the team, with conversation counts
//   - the weakest check
//   - the 2-3 worst replies: what failed, the quote, a suggested rewrite, link
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
import { summary, worstReplies } from "./report.js";
import { CHECKS } from "./checks.js";
import { messagePerson } from "../nitm-pm/outreach.js";

export const MAX_CHARS = 1500;
const NAME = Object.fromEntries(CHECKS.map((c) => [c.id, c.name]));
const LABEL = { ivy: "Ivy", team: "Team" };

const pct = (p, f) => (p + f ? Math.round((100 * p) / (p + f)) : null);
// Chat treats <...> as links and *...* as bold: keep customer-derived text inert.
const inert = (s) => String(s || "").replace(/[<>]/g, (c) => (c === "<" ? "‹" : "›")).replace(/[*_~`]/g, "").replace(/\s+/g, " ").trim();
const unquote = (s) => String(s).trim().replace(/^["“”']+|["“”']+$/g, "");
const clip = (s, n) => (s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : s);
const dayLabel = (d) => new Date(d + "T12:00:00Z").toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", timeZone: "UTC" });

export function scorecardUrl(env, from, to) {
  return `https://${env.APP_HOSTNAME || "ops.anurseinthemaking.com"}/cx-audit/#from=${from}&to=${to}`;
}

// Pure: data in, text out. Tries 3 worst replies, then 2, then shorter quotes,
// until the message fits.
export function formatDigest({ from, to, s, worst, url }) {
  const period = from === to ? dayLabel(from) : `${dayLabel(from)} – ${dayLabel(to)}`;
  const head = [`*CX scorecard · ${period}*`];
  const parts = [];
  for (const g of ["ivy", "team"]) {
    const t = s.totals[g];
    if (t) parts.push(`${LABEL[g]} *${pct(t.pass, t.fail) ?? "—"}%* (${t.convs} conv.)`);
  }
  if (!parts.length) return `${head[0]}\nNo Ivy or team replies to score${from === to ? " that day" : ""}.\n<${url}|Open the scorecard>`;
  head.push(parts.join(" · "));

  const agg = {};
  for (const r of s.per_check) { const a = (agg[r.check_id] ||= { pass: 0, fail: 0 }); a.pass += r.pass; a.fail += r.fail; }
  // Same rule as the dashboard tile: checks with at least 5 results first, so
  // one failure out of one reply doesn't headline the day.
  const failing = Object.entries(agg).filter(([, a]) => a.fail > 0).map(([id, a]) => ({ id, rate: pct(a.pass, a.fail), n: a.pass + a.fail, ...a }));
  const byRate = (x, y) => x.rate - y.rate || y.fail - x.fail;
  const weakest = (failing.filter((c) => c.n >= 5).sort(byRate)[0]) || failing.sort(byRate)[0];
  head.push(weakest ? `Weakest check: *${NAME[weakest.id] || weakest.id}* ${weakest.rate}% (${weakest.fail} fail${weakest.fail === 1 ? "" : "s"})` : "No failed checks. 🎉");

  const render = (count, quoteLen, rewriteLen) => {
    const lines = [...head];
    const list = worst.slice(0, count);
    if (list.length) lines.push("", `*Worst ${list.length === 1 ? "reply" : `${list.length} replies`}*`);
    list.forEach((w, i) => {
      const names = w.fails.map((f) => NAME[f.check_id] || f.check_id).join(", ");
      lines.push(`${i + 1}. ${LABEL[w.author_type]}${w.issue_type ? ` · ${inert(w.issue_type)}` : ""} · failed ${names}`);
      const top = w.fails.find((f) => f.rewrite) || w.fails[0];
      if (top?.evidence) lines.push(`   “${clip(inert(unquote(top.evidence)), quoteLen)}”`);
      if (top?.rewrite) lines.push(`   → Try: ${clip(inert(top.rewrite), rewriteLen)}`);
      lines.push(`   <${w.link}|Open in Intercom>`);
    });
    lines.push("", `<${url}|Full scorecard>`);
    return lines.join("\n");
  };
  for (const [count, q, r] of [[3, 140, 220], [3, 100, 160], [2, 120, 200], [2, 80, 140], [1, 80, 140]]) {
    const text = render(count, q, r);
    if (text.length <= MAX_CHARS) return text;
  }
  return render(0, 0, 0);
}

export async function buildDigest(env, from, to) {
  const f = { from, to, author: "", check: "", issue: "", version: "" };
  const [s, worst] = await Promise.all([summary(env, f), worstReplies(env, f, 3)]);
  return formatDigest({ from, to, s, worst, url: scorecardUrl(env, from, to) });
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
