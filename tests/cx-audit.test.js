// CX scorecard audit, Phase 1: windows, extraction, exclusions, the pipeline
// against a mocked Intercom API. Invented conversations, shaped like the real
// ones (part types, author flags) observed in NITM's workspace in Nov 2026.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { windowFor, localMidnight, yesterdayWindow, DEFAULT_EXCLUSIONS } from "../src/cx-audit/config.js";
import { extract, classify, cleanText, senderOf } from "../src/cx-audit/extract.js";
import { fetchAndFilter, pickSamples } from "../src/cx-audit/pipeline.js";

const WIN = windowFor("2026-11-05"); // EST (UTC-5)
const T = (h, m = 0) => WIN.start + h * 3600 + m * 60; // h hours after local midnight

const ivy = { type: "bot", id: "1", name: "Ivy", from_ai_agent: true, is_ai_answer: false };
const ivyQuiet = { type: "bot", id: "1", name: "Ivy", from_ai_agent: false };
const aimee = { type: "admin", id: "77", name: "Aimee Jackson" };
const cust = { type: "user", id: "c1", name: "Jane Okafor", email: "jane@example.com" };

function conv({ id = "100", source = {}, parts = [], ca = {}, created = T(9) } = {}) {
  return {
    id, created_at: created, ai_agent_participated: true,
    source: { type: "email", id: "s1", delivered_as: "customer_initiated", subject: "Where is my order?", body: "Hi, where is my order? It's #10423.", author: cust, ...source },
    custom_attributes: { "Issue Type": "Shipping & Delivery", Spam: "No", Sentiment: "Negative", "Fin AI Agent: Preview": false, ...ca },
    conversation_parts: { type: "conversation_part.list", conversation_parts: parts.map((p, i) => ({ id: String(i + 1), body: "", ...p })) },
  };
}

test("windowFor: New York calendar day, DST-safe", () => {
  assert.equal(WIN.start, Date.parse("2026-11-05T05:00:00Z") / 1000);
  assert.equal(WIN.end, Date.parse("2026-11-06T05:00:00Z") / 1000);
  // DST ends 2026-11-01: that day is 25 hours long.
  const w = windowFor("2026-11-01");
  assert.equal(w.end - w.start, 25 * 3600);
  assert.equal(localMidnight("2026-07-04"), Date.parse("2026-07-04T04:00:00Z") / 1000);
});

test("yesterdayWindow at 7am ET", () => {
  const w = yesterdayWindow("America/New_York", Date.parse("2026-11-06T12:00:00Z"));
  assert.equal(w.from, "2026-11-05");
});

test("cleanText strips quoted history, footers and HTML", () => {
  assert.equal(cleanText("Thanks!\n\nGet Outlook for iOS\n\nOn Tue, Nov 4, 2026 at 9:00 AM Ivy <x@y.com> wrote:\n> old"), "Thanks!");
  assert.equal(cleanText("<p>Hi&nbsp;there</p><p>Second</p>"), "Hi there\nSecond");
  assert.equal(cleanText("New reply\n> quoted line\nmore"), "New reply\nmore");
});

test("extract: roles, window flags, scrubbing, events", () => {
  const c = conv({
    parts: [
      { part_type: "ai_agent_event", author: ivyQuiet, created_at: T(9, 1) },
      { part_type: "comment", author: ivy, body: "Hi Jane, I'm Ivy. Your order is on its way.", created_at: T(9, 2) },
      { part_type: "open", author: cust, body: "Thanks Ivy. Jane Okafor, 555-123-4567", created_at: T(10) },
      { part_type: "assignment", author: ivyQuiet, assigned_to: { type: "nobody_admin", id: null }, created_at: T(10, 1) },
      { part_type: "assignment", author: aimee, assigned_to: { type: "admin", id: "77" }, created_at: T(11) },
      { part_type: "note", author: aimee, body: "internal: customer is a VIP", created_at: T(11, 1) },
      { part_type: "assignment", author: aimee, body: "Hi Jane, I've sent a replacement. Aimee", assigned_to: { type: "admin", id: "77" }, created_at: T(11, 2) },
      { part_type: "comment", author: aimee, body: "Anything else?", created_at: T(30) }, // next day: context only
    ],
  });
  const r = extract(c, WIN, { appId: "abc123" });
  assert.equal(r.link, "https://app.intercom.com/a/inbox/abc123/inbox/conversation/100");
  assert.deepEqual(r.counts, { customer: 2, ivy: 1, team: 1, automation: 0 });
  assert.deepEqual(r.messages.map((m) => m.role), ["customer", "ivy", "customer", "event", "event", "team", "team"]);
  assert.equal(r.messages.at(-1).in_window, false);
  assert.match(r.messages[0].text, /\[order number\]/);
  assert.equal(r.messages[2].text, "Thanks Ivy. Jane [surname], [phone]");
  assert.match(r.messages[3].text, /Ivy handed the conversation to the team queue/);
  assert.equal(r.messages[4].text, "[Aimee took the conversation]");
  assert.equal(r.messages[5].name, "Aimee");
  assert.ok(!JSON.stringify(r).includes("VIP"), "notes never extracted");
  assert.ok(!JSON.stringify(r).includes("jane@example.com"), "sender email never returned");
  assert.equal(r.last_reply_part_id, "7");
});

test("Shopify contact form: the real customer is reply_to, and is kept", () => {
  const c = conv({ source: { author: { type: "user", name: "Shopify", email: "mailer@shopify.com" }, reply_to: [{ email: "someone@docusign.net", name: null }] } });
  assert.equal(senderOf(c).email, "someone@docusign.net");
  assert.equal(senderOf(c).viaShopify, true);
  const ok = conv({ source: { author: { type: "user", name: "Shopify", email: "mailer@shopify.com" }, reply_to: [{ email: "pat@gmail.com" }] }, parts: [{ part_type: "comment", author: ivy, body: "Hi!", created_at: T(9, 5) }] });
  const r = extract(ok, WIN);
  assert.equal(classify(ok, r, DEFAULT_EXCLUSIONS).status, "scoreable");
  assert.equal(r.via_shopify, true);
});

test("classify: preview, spam, rule, no reply, in that order", () => {
  const reply = [{ part_type: "comment", author: ivy, body: "Hello", created_at: T(9, 5) }];
  const check = (c) => classify(c, extract(c, WIN), DEFAULT_EXCLUSIONS);
  assert.equal(check(conv({ parts: reply, source: { author: { type: "lead", email: "preview-user+abc@intercom.io" } } })).reason, "preview");
  assert.equal(check(conv({ parts: reply, ca: { "Fin AI Agent: Preview": true } })).reason, "preview");
  assert.equal(check(conv({ parts: reply, ca: { Spam: "Yes" } })).reason, "spam");
  const doc = check(conv({ parts: reply, source: { author: { type: "user", email: "dse@na3.docusign.net" } } }));
  assert.equal(doc.reason, "exclusion_rule");
  assert.equal(doc.rule.value, "docusign.net");
  assert.equal(check(conv({ parts: reply, source: { subject: "Our November newsletter" } })).reason, "exclusion_rule");
  assert.equal(check(conv({ parts: [{ part_type: "comment", author: ivy, body: "old", created_at: T(-30) }] })).reason, "no_reply");
  assert.equal(check(conv({ parts: reply })).status, "scoreable");
  // Amazon marketplace buyers are customers.
  assert.equal(check(conv({ parts: reply, source: { author: { type: "user", email: "x@marketplace.amazon.com" } } })).status, "scoreable");
});

test("classify: disabled rules and issue_type rules", () => {
  const c = conv({ parts: [{ part_type: "comment", author: aimee, body: "Hi", created_at: T(12) }], ca: { "Issue Type": "Collaboration / Donation" } });
  const r = extract(c, WIN);
  assert.equal(classify(c, r, [{ kind: "issue_type", value: "Collaboration / Donation" }]).reason, "exclusion_rule");
  assert.equal(classify(c, r, [{ kind: "issue_type", value: "Collaboration / Donation", enabled: 0 }]).status, "scoreable");
  assert.equal(classify(c, r, [{ kind: "subject", value: "([" }]).status, "scoreable", "bad regex is skipped, not thrown");
});

// ---------------------------------------------------------------------------
// Pipeline against a mocked Intercom API
// ---------------------------------------------------------------------------
let calls;
beforeEach(() => { calls = []; });

function mockIntercom(conversations) {
  globalThis.fetch = async (url, opts = {}) => {
    const u = new URL(url);
    calls.push({ path: u.pathname, method: opts.method || "GET", body: opts.body ? JSON.parse(opts.body) : null, headers: opts.headers });
    if (u.pathname === "/me") return Response.json({ app: { id_code: "ws1" } });
    if (u.pathname === "/conversations/search") {
      const body = JSON.parse(opts.body);
      const ids = Object.keys(conversations);
      const page = body.pagination.starting_after ? ids.slice(2) : ids.slice(0, 2);
      return Response.json({ conversations: page.map((id) => ({ id })), pages: body.pagination.starting_after || ids.length <= 2 ? {} : { next: { starting_after: "cur2" } } });
    }
    const id = u.pathname.split("/").pop();
    if (!conversations[id]) return new Response("nope", { status: 404 });
    return Response.json(conversations[id]);
  };
}

test("fetchAndFilter: pages the search, fetches plaintext, summarizes", async () => {
  const reply = (author, h) => [{ part_type: "comment", author, body: "Reply", created_at: T(h) }];
  mockIntercom({
    1: conv({ id: "1", parts: reply(ivy, 9) }),
    2: conv({ id: "2", parts: reply(aimee, 10) }),
    3: conv({ id: "3", parts: reply(ivy, 9), ca: { Spam: "Yes" } }),
    4: conv({ id: "4", parts: reply(ivy, 9), source: { author: { type: "lead", email: "preview-user+1@intercom.io" } } }),
    5: null, // 404 -> fetch error, not a crash
  });
  const env = { INTERCOM_ACCESS_TOKEN: "t" };
  const res = await fetchAndFilter(env, WIN, { rules: DEFAULT_EXCLUSIONS });
  const search = calls.filter((c) => c.path === "/conversations/search");
  assert.equal(search.length, 2, "follows the cursor");
  assert.deepEqual(search[0].body.query.value, [{ field: "updated_at", operator: ">=", value: WIN.start }]);
  assert.equal(search[0].body.pagination.per_page, 150);
  assert.equal(search[0].headers.Authorization, "Bearer t");
  assert.ok(calls.some((c) => c.path === "/conversations/1"));
  assert.equal(res.summary.searched, 5);
  assert.equal(res.summary.scoreable, 2);
  assert.equal(res.summary.fetch_errors, 1);
  assert.equal(res.summary.excluded.spam, 1);
  assert.equal(res.summary.excluded.preview, 1);
  assert.deepEqual(res.summary.scoreable_shape, { ivy_only: 1, team_only: 1, both: 0 });
  assert.equal(res.scoreable[0].link, "https://app.intercom.com/a/inbox/ws1/inbox/conversation/1");
  assert.equal(pickSamples(res.scoreable).length, 2);
});

test("missing token fails clearly", async () => {
  mockIntercom({});
  await assert.rejects(fetchAndFilter({}, WIN), /INTERCOM_ACCESS_TOKEN is not set/);
});

test("shared admin accounts keep their full name", () => {
  const shared = { type: "admin", id: "9", name: "Customer Support" };
  const c = conv({ parts: [{ part_type: "comment", author: shared, body: "Hi! Kristine", created_at: T(12) }] });
  const r = extract(c, WIN);
  assert.equal(r.messages.at(-1).name, "Customer Support");
  assert.equal(r.messages.at(-1).role, "team");
});

test("attachments: noted by kind and count, never file names; attachment-only messages kept", () => {
  const c = conv({
    source: { attachments: [{ content_type: "image/jpeg", name: "jane-okafor-box.jpg" }, { content_type: "image/png", name: "x.png" }] },
    parts: [
      { part_type: "comment", author: cust, body: "", attachments: [{ content_type: "application/pdf", name: "receipt.pdf" }], created_at: T(9, 30) },
      { part_type: "comment", author: ivy, body: "I can see the damage in your photos.", created_at: T(10) },
    ],
  });
  const r = extract(c, WIN);
  assert.match(r.messages[0].text, /\[attached: 2 photos\]$/);
  assert.equal(r.messages[1].role, "customer");
  assert.equal(r.messages[1].text, "[attached: 1 PDF]");
  assert.ok(!JSON.stringify(r).includes("okafor-box"));
});
