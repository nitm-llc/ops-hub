// ============================================================================
// CX SCORECARD AUDIT — turn an Intercom conversation into a scrubbed transcript
// and decide whether it gets scored. Pure functions: no fetch, no D1.
//
// Who said what (from real NITM data, Nov 2026):
//   customer    author.type user | lead | contact, non-empty body. Arrives as
//               part_type "comment" OR "open" (a customer reply reopens a
//               closed conversation, and that part carries the message).
//   ivy         author.type bot with from_ai_agent / is_ai_answer, non-empty body.
//               Ivy also writes many EMPTY parts (ai_agent_event,
//               fin_guidance_applied, custom_action_*): ignored.
//   automation  author.type bot WITHOUT the AI flag but with a body (workflow
//               messages). Shown as context, not scored; counted in the summary.
//   team        author.type admin, non-empty body, any part_type except notes
//               (admin replies sometimes arrive as "assignment" parts).
//   event       bodiless assignment / close by Ivy or the team. Kept as a
//               one-line marker so the scorer can see a handoff that sent the
//               customer nothing (check 5, silent handoff).
//
// Only replies created inside the window are scored; earlier messages are
// context. Nothing raw leaves this file: every text is scrubForAudit()'d, and
// sender emails are used only for the exclusion decision, never returned.
// ============================================================================

import { scrubForAudit } from "../pii.js";
import { PREVIEW_EMAIL, SHOPIFY_MAILER } from "./config.js";

const CUSTOMER_TYPES = new Set(["user", "lead", "contact"]);
const EVENT_PARTS = new Set(["assignment", "default_assignment", "away_mode_assignment", "assign_and_unsnooze", "close", "snoozed"]);
const MAX_MESSAGE_CHARS = 3000;

// ---------------------------------------------------------------------------
// Text cleanup: drop quoted email history and mail-client footers, so the
// scorer reads what this message added, not the whole thread again.
// ---------------------------------------------------------------------------
const ENTITIES = { "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&#39;": "'", "&rsquo;": "’", "&lsquo;": "‘", "&ldquo;": "“", "&rdquo;": "”" };

export function cleanText(body) {
  let t = String(body || "");
  if (/<[a-z][\s\S]*>/i.test(t)) {
    t = t.replace(/<(br|\/p|\/div|\/li|\/tr|\/h\d)\s*\/?>/gi, "\n").replace(/<[^>]+>/g, " ");
  }
  t = t.replace(/&[a-z#0-9]+;/gi, (e) => ENTITIES[e.toLowerCase()] ?? " ");
  t = t.replace(/​/g, "");
  const cuts = [
    /^\s*On\s.{3,200}\bwrote:\s*$/im,
    /^\s*-{2,}\s*Original Message\s*-{2,}/im,
    /^\s*From:\s.*\n\s*(?:Sent|Date):\s/im,
    /^\s*_{8,}\s*$/m,
  ];
  for (const re of cuts) {
    const m = re.exec(t);
    if (m) t = t.slice(0, m.index);
  }
  t = t
    .split("\n")
    .filter((l) => !/^\s*>/.test(l))
    .map((l) => l.replace(/[ \t]+/g, " ").trim())
    .join("\n")
    .replace(/\b(Get\s+Outlook\s+for\s+(iOS|Android)|Sent\s+from\s+my\s+(iPhone|iPad|Android|Galaxy)[^\n]*|Sent\s+from\s+Yahoo\s+Mail[^\n]*)/gi, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n\s*\n\s*\n+/g, "\n\n")
    .trim();
  return t.length > MAX_MESSAGE_CHARS ? t.slice(0, MAX_MESSAGE_CHARS) + " […]" : t;
}

// Attachments are noted by kind and count only (file names can identify
// people). Without this the scorer can't know a customer sent photos, and
// fails a reply that correctly says "I can see from your photos".
export function withAttachments(text, attachments) {
  const list = Array.isArray(attachments) ? attachments : [];
  if (!list.length) return text;
  const kinds = {};
  for (const a of list) {
    const ct = String(a?.content_type || "");
    const k = ct.startsWith("image/") ? "photo" : ct === "application/pdf" ? "PDF" : ct.startsWith("video/") ? "video" : "file";
    kinds[k] = (kinds[k] || 0) + 1;
  }
  const note = Object.entries(kinds).map(([k, n]) => `${n} ${k}${n > 1 ? "s" : ""}`).join(", ");
  return `${text ? text + "\n" : ""}[attached: ${note}]`;
}

const firstName = (name) => String(name || "").trim().split(/\s+/)[0] || "";
const domainOf = (email) => (String(email || "").split("@")[1] || "").toLowerCase();

function replyToEmail(rt) {
  if (!rt) return null;
  if (typeof rt === "string") return rt;
  if (Array.isArray(rt)) return rt[0]?.email || null;
  return rt.email || null;
}

// The real sender. Shopify contact forms arrive from mailer@shopify.com with
// the customer in reply_to; those are real customers, so use reply_to.
export function senderOf(conv) {
  const a = conv?.source?.author || {};
  if (a.type === "admin" || a.type === "bot") return { email: null, initiatedBy: a.type, viaShopify: false };
  const email = a.email || null;
  if (email && SHOPIFY_MAILER.test(email)) {
    return { email: replyToEmail(conv.source.reply_to) || email, initiatedBy: "customer", viaShopify: true, rawEmail: email };
  }
  return { email, initiatedBy: "customer", viaShopify: false, rawEmail: email };
}

export function matchExclusion(conv, sender, rules) {
  const email = String(sender.email || "").toLowerCase();
  const domain = domainOf(email);
  const subject = cleanText(conv?.source?.subject || "");
  const issue = conv?.custom_attributes?.["Issue Type"] || "";
  for (const r of rules || []) {
    if (r.enabled === false || r.enabled === 0) continue;
    const v = String(r.value || "").toLowerCase().trim();
    if (!v) continue;
    if (r.kind === "domain" && domain && (domain === v || domain.endsWith("." + v))) return r;
    if (r.kind === "email" && email && email === v) return r;
    if (r.kind === "issue_type" && issue.toLowerCase() === v) return r;
    if (r.kind === "subject") {
      try { if (new RegExp(r.value, "i").test(subject)) return r; } catch { /* bad regex: skip */ }
    }
  }
  return null;
}

function roleOf(author, partType, body) {
  const t = author?.type;
  if (/note/.test(partType || "")) return null; // internal notes never reach the customer
  if (body) {
    if (CUSTOMER_TYPES.has(t)) return "customer";
    if (t === "bot") return author.from_ai_agent || author.is_ai_answer ? "ivy" : "automation";
    if (t === "admin") return "team";
    return null;
  }
  if ((t === "bot" || t === "admin") && EVENT_PARTS.has(partType)) return "event";
  return null;
}

function eventText(part, actor) {
  const pt = part.part_type;
  if (/assign/.test(pt)) {
    const to = part.assigned_to;
    if (to?.type === "admin" && String(to.id) === String(part.author?.id)) return `[${actor} took the conversation]`;
    const who = to?.type === "team" ? "a team inbox" : to?.type === "admin" ? "a teammate" : "the team queue (unassigned)";
    return `[${actor} handed the conversation to ${who}; this step sent the customer no message]`;
  }
  if (pt === "close") return `[${actor} closed the conversation]`;
  if (pt === "snoozed") return `[${actor} snoozed the conversation]`;
  return `[${actor}: ${pt}]`;
}

// Teammates go by first name. Shared inboxes ("Customer Support", "NITM Team")
// keep their full name, or "Customer Support" would read as the customer.
const SHARED_ACCOUNT = /support|team|help|nurse ?in ?the ?making|nitm|info|admin/i;
const teamName = (name) => (SHARED_ACCOUNT.test(name || "") ? String(name).trim() : firstName(name)) || "Team";
const nameFor = (role, author) => (role === "ivy" ? "Ivy" : role === "team" || role === "event" ? teamName(author?.name) : role === "automation" ? "Automation" : "Customer");

// ---------------------------------------------------------------------------
// extract(conv, window, { appId }) -> scrubbed record, safe to log and store.
// ---------------------------------------------------------------------------
export function extract(conv, win, { appId = "_" } = {}) {
  const ca = conv.custom_attributes || {};
  const sender = senderOf(conv);
  const parts = conv.conversation_parts?.conversation_parts || [];

  // Customer full names, to strip surnames from every message.
  const names = new Set();
  const add = (n) => { if (n && !/^(shopify|customer)$/i.test(n)) names.add(n); };
  if (CUSTOMER_TYPES.has(conv.source?.author?.type)) add(conv.source.author.name);
  for (const p of parts) if (CUSTOMER_TYPES.has(p.author?.type)) add(p.author.name);
  const rt = conv.source?.reply_to;
  if (Array.isArray(rt)) for (const r of rt) add(r?.name);
  const scrub = (s) => scrubForAudit(s, { names: [...names] });

  const inWin = (at) => at >= win.start && at < win.end;
  const messages = [];
  const src = conv.source || {};
  const srcBody = withAttachments(cleanText(src.body), src.attachments);
  const srcRole = srcBody ? roleOf(src.author, "source", srcBody) : null;
  if (srcRole) {
    messages.push({ id: `source-${src.id || conv.id}`, at: conv.created_at, role: srcRole, name: nameFor(srcRole, src.author), part_type: "source", in_window: inWin(conv.created_at), text: scrub(srcBody) });
  }
  for (const p of parts) {
    const body = withAttachments(cleanText(p.body), p.attachments);
    const role = roleOf(p.author, p.part_type, body);
    if (!role) continue;
    const name = nameFor(role, p.author);
    const actor = p.author?.type === "bot" ? "Ivy" : name;
    messages.push({
      id: String(p.id), at: p.created_at, role, name, part_type: p.part_type, in_window: inWin(p.created_at),
      text: role === "event" ? eventText(p, actor) : scrub(body),
    });
  }
  messages.sort((a, b) => a.at - b.at);

  const counts = { customer: 0, ivy: 0, team: 0, automation: 0 };
  let lastReplyId = null;
  for (const m of messages) {
    if (!m.in_window || !(m.role in counts)) continue;
    counts[m.role]++;
    if (m.role === "ivy" || m.role === "team") lastReplyId = m.id;
  }

  return {
    id: String(conv.id),
    link: `https://app.intercom.com/a/inbox/${appId}/inbox/conversation/${conv.id}`,
    channel: src.type || null,
    subject: scrub(cleanText(src.subject)) || null,
    issue_type: ca["Issue Type"] || null,
    sentiment: ca["Sentiment"] || null,
    fin_state: ca["Fin AI Agent resolution state"] || null,
    ai_participated: !!conv.ai_agent_participated,
    sender_domain: domainOf(sender.email) || null,
    via_shopify: sender.viaShopify,
    initiated_by: sender.initiatedBy,
    messages,
    window: { from: win.from, to: win.to, start: win.start, end: win.end },
    counts, // in-window only
    last_reply_part_id: lastReplyId,
  };
}

// ---------------------------------------------------------------------------
// classify(conv, record, rules) -> { status: "scoreable" | "excluded", reason, rule }
// Reasons, in the order checked: preview, spam, exclusion_rule, no_reply.
// ---------------------------------------------------------------------------
export function classify(conv, record, rules) {
  const ca = conv.custom_attributes || {};
  const sender = senderOf(conv);
  if (PREVIEW_EMAIL.test(sender.rawEmail || sender.email || "") || ca["Fin AI Agent: Preview"] === true) {
    return { status: "excluded", reason: "preview" };
  }
  if (ca.Spam === "Yes" || ca.Spam === true) return { status: "excluded", reason: "spam" };
  const rule = matchExclusion(conv, sender, rules);
  if (rule) return { status: "excluded", reason: "exclusion_rule", rule: { kind: rule.kind, value: rule.value, note: rule.note || null } };
  if (!record.counts.ivy && !record.counts.team) return { status: "excluded", reason: "no_reply" };
  return { status: "scoreable" };
}
