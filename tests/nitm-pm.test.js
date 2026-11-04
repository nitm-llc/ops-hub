// NITM PM Google Chat app against a mocked Google (JWKS, OAuth, Chat API),
// ClickUp and Claude, with a real SQLite standing in for D1. Real field/user
// ids (they're config); invented cards.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { handleNitmPmRoutes, CHAT_PATH, endpointUrl } from "../src/nitm-pm/index.js";
import { _clearJwksCache, _clearTokenCache } from "../src/nitm-pm/google.js";
import { _resetReady, createPending } from "../src/nitm-pm/store.js";
import { validateAction, applyAction, RefusedError, _clearFieldCache } from "../src/nitm-pm/clickup.js";
import { parseApproval } from "../src/nitm-pm/approvals.js";
import { LIST_ID, FIELDS, PEOPLE, todayYmd, addDays, ymdToClickUpMs } from "../src/nitm-pm/config.js";

const PROJECT = "123456789";
const HOST = "https://ops.anurseinthemaking.com";
const AVERY = { email: "avery@anurseinthemaking.com", displayName: "Avery Pless" };
const DIANA = { email: "diana@anurseinthemaking.com", displayName: "Diana Boyle" };
const SPACE = "spaces/AAA";
const THREAD = "spaces/AAA/threads/T1";
const TODAY = todayYmd();

// ---------------------------------------------------------------------------
// D1 on node:sqlite
// ---------------------------------------------------------------------------
function makeD1() {
  const db = new DatabaseSync(":memory:");
  const stmt = (sql, args = []) => ({
    bind: (...a) => stmt(sql, a),
    run: async () => {
      const s = db.prepare(sql);
      if (/\bRETURNING\b/i.test(sql)) return { results: s.all(...args), meta: { changes: 1 } };
      const r = s.run(...args);
      return { meta: { changes: Number(r.changes) } };
    },
    first: async () => db.prepare(sql).get(...args) ?? null,
    all: async () => ({ results: db.prepare(sql).all(...args) }),
  });
  return { raw: db, prepare: (sql) => stmt(sql) };
}

// ---------------------------------------------------------------------------
// Keys: one "Google" signing key, one service-account key
// ---------------------------------------------------------------------------
const RSA = { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" };
const googleKey = await crypto.subtle.generateKey(RSA, true, ["sign", "verify"]);
const otherKey = await crypto.subtle.generateKey(RSA, true, ["sign", "verify"]);
const googleJwk = { ...(await crypto.subtle.exportKey("jwk", googleKey.publicKey)), kid: "k1", alg: "RS256", use: "sig" };
const saKey = await crypto.subtle.generateKey(RSA, true, ["sign", "verify"]);
const saPem =
  "-----BEGIN PRIVATE KEY-----\n" +
  Buffer.from(await crypto.subtle.exportKey("pkcs8", saKey.privateKey)).toString("base64").match(/.{1,64}/g).join("\n") +
  "\n-----END PRIVATE KEY-----\n";
const SA_JSON = JSON.stringify({ type: "service_account", client_email: "nitm-pm@nitm-pm.iam.gserviceaccount.com", private_key: saPem, private_key_id: "sa1" });

const b64u = (s) => Buffer.from(s).toString("base64url");
async function signJwt(payload, { key = googleKey.privateKey, kid = "k1" } = {}) {
  const input = `${b64u(JSON.stringify({ alg: "RS256", kid, typ: "JWT" }))}.${b64u(JSON.stringify(payload))}`;
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(input));
  return `${input}.${Buffer.from(sig).toString("base64url")}`;
}
const now = () => Math.floor(Date.now() / 1000);
const addonToken = (over = {}) =>
  signJwt({
    iss: "https://accounts.google.com",
    aud: endpointUrl({}),
    email: `service-${PROJECT}@gcp-sa-gsuiteaddons.iam.gserviceaccount.com`,
    email_verified: true,
    iat: now(),
    exp: now() + 3600,
    ...over,
  });

// ---------------------------------------------------------------------------
// Cards
// ---------------------------------------------------------------------------
const STAGE_OPTS = [
  { id: "st-edit", name: "Ready To Edit / Design", orderindex: 3 },
  { id: "st-review", name: "T: Drive Review", orderindex: 9 },
  { id: "st-posted", name: "Posted", orderindex: 18 },
  { id: "st-hold", name: "Schedule On Hold", orderindex: 22 },
  { id: "st-backlog", name: "Backlog", orderindex: 0 },
];
const TYPE_OPTS = [
  { id: "ty-quiz", name: "Quiz", orderindex: 12 },
  { id: "ty-funny", name: "Funny", orderindex: 0 },
];
function card(id, { name = id, stage, type = "ty-quiz", due, post, assignees = [], list = LIST_ID } = {}) {
  return {
    id,
    name,
    url: `https://app.clickup.com/t/${id}`,
    list: { id: list },
    status: { status: "to do" },
    due_date: due ? String(ymdToClickUpMs(due)) : null,
    priority: null,
    assignees: assignees.map((a) => ({ id: a, username: String(a) })),
    custom_fields: [
      { id: FIELDS.stage, type: "drop_down", type_config: { options: STAGE_OPTS }, value: stage },
      { id: FIELDS.type, type: "drop_down", type_config: { options: TYPE_OPTS }, value: type },
      { id: FIELDS.postDate, type: "date", value: post ? String(ymdToClickUpMs(post)) : null },
    ],
  };
}

// ---------------------------------------------------------------------------
// Mocked network
// ---------------------------------------------------------------------------
let env, calls, tasks, comments, chatPosts, claudeScript, claudeRequests;

const WHO_FILMS = "98221b2f-2280-4c72-8695-30cc4b0910c2";
const NOTES = "44e266b0-0ae0-4c9b-8770-80186e1b596a";
const FORMULA = "a090c6a9-6705-4fd6-8948-bbf1aeb5c902";
const LIST_FIELDS = [
  { id: FIELDS.stage, name: "Stage", type: "drop_down", type_config: { options: [] } },
  { id: FIELDS.type, name: "Type", type: "drop_down", type_config: { options: TYPE_OPTS } },
  { id: FIELDS.postDate, name: "Post Date", type: "date", type_config: {} },
  { id: WHO_FILMS, name: "Who's Filming", type: "drop_down", type_config: { options: [{ id: "wf-k", name: "KRISTINE (Team 1)", orderindex: 0 }, { id: "wf-d", name: "DIANA (Team 2)", orderindex: 1 }] } },
  { id: NOTES, name: "Notes", type: "text", type_config: {} },
  { id: FORMULA, name: "Days Until Post", type: "formula", type_config: {} },
];
LIST_FIELDS[0].type_config.options = [
  ...STAGE_OPTS,
  { id: "st-sched", name: "Scheduled 📅", orderindex: 17 },
  { id: "st-ready", name: "Ready to Schedule", orderindex: 15 },
];

beforeEach(() => {
  _clearFieldCache();
  _clearJwksCache();
  _clearTokenCache();
  _resetReady();
  calls = [];
  chatPosts = [];
  claudeRequests = [];
  claudeScript = [];
  comments = {};
  tasks = {
    "peds-quiz": card("peds-quiz", { name: "Peds Infectious Diseases Quiz", stage: "st-edit", due: addDays(TODAY, -2), post: addDays(TODAY, 6), assignees: [PEOPLE.gaby] }),
    "posted-old": card("posted-old", { name: "Posted already", stage: "st-posted", due: addDays(TODAY, -3), post: addDays(TODAY, -1) }),
    "on-hold": card("on-hold", { name: "Paused one", stage: "st-hold", due: addDays(TODAY, -4), post: addDays(TODAY, 3) }),
    "not-due": card("not-due", { name: "Fine for now", stage: "st-review", due: addDays(TODAY, 2), post: addDays(TODAY, 9), assignees: [PEOPLE.diana] }),
    "backlog-nodate": card("backlog-nodate", { name: "Idea", stage: "st-backlog", due: addDays(TODAY, -10) }),
    "funny-one": card("funny-one", { name: "Funny charting", stage: "st-review", type: "ty-funny", due: addDays(TODAY, -1), post: addDays(TODAY, 4), assignees: [PEOPLE.tia] }),
  };
  env = {
    DB: makeD1(),
    CLICKUP_TOKEN: "pk_test",
    ANTHROPIC_API_KEY: "sk-test",
    GOOGLE_CHAT_SA_KEY: SA_JSON,
    PM_CHAT_PROJECT_NUMBER: PROJECT,
    PM_APPROVERS: "mark@anurseinthemaking.com, avery@anurseinthemaking.com",
  };

  globalThis.fetch = async (input, opts = {}) => {
    const url = String(input);
    const method = opts.method || "GET";
    const body = opts.body && typeof opts.body === "string" && opts.body.startsWith("{") ? JSON.parse(opts.body) : opts.body;
    calls.push({ url, method, body });
    const ok = (b) => new Response(JSON.stringify(b), { status: 200 });

    if (url.includes("googleapis.com/oauth2/v3/certs") || url.includes("service_accounts/v1/jwk/chat@system")) {
      return ok({ keys: [googleJwk] });
    }
    if (url === "https://oauth2.googleapis.com/token") return ok({ access_token: "ya29.test", expires_in: 3600 });
    if (url.startsWith("https://chat.googleapis.com/")) {
      chatPosts.push({ url, body });
      return ok({ name: `${SPACE}/messages/m1` });
    }
    if (url.startsWith("https://api.anthropic.com/")) {
      claudeRequests.push(body);
      const step = claudeScript.shift();
      if (!step) throw new Error("Claude called more times than scripted");
      if (step.delay) {
        await new Promise((resolve, reject) => {
          const t = setTimeout(resolve, step.delay);
          opts.signal?.addEventListener("abort", () => {
            clearTimeout(t);
            reject(opts.signal.reason);
          });
        });
      }
      if (step.status) return new Response(JSON.stringify({ error: { message: "boom" } }), { status: step.status });
      return ok(typeof step.reply === "function" ? step.reply(body) : step.reply);
    }
    if (url.startsWith("https://api.clickup.com/api/v2/")) {
      const path = url.slice("https://api.clickup.com/api/v2".length).split("?")[0];
      let m;
      if (method === "GET" && path === `/list/${LIST_ID}/field`) return ok({ fields: LIST_FIELDS });
      if ((m = path.match(/^\/task\/([^/]+)\/field\/([^/]+)$/))) return ok({});
      if (method === "GET" && path === `/list/${LIST_ID}/task`) {
        // Honor the due-date window like ClickUp does (so a too-tight bound is
        // caught); other filters are ignored and must be re-applied in code.
        const q = new URL(url).searchParams;
        const lt = q.get("due_date_lt") ? Number(q.get("due_date_lt")) : Infinity;
        const gt = q.get("due_date_gt") ? Number(q.get("due_date_gt")) : -Infinity;
        const inWindow = (t) => (lt === Infinity && gt === -Infinity) || (t.due_date && Number(t.due_date) < lt && Number(t.due_date) > gt);
        return ok({ tasks: Object.values(tasks).filter(inWindow), last_page: true });
      }
      if ((m = path.match(/^\/task\/([^/]+)\/comment$/))) {
        if (method === "GET") return ok({ comments: comments[m[1]] || [] });
        return ok({ id: "c1" });
      }
      if ((m = path.match(/^\/task\/([^/]+)$/))) {
        if (method === "GET") return tasks[m[1]] ? ok(tasks[m[1]]) : new Response("{}", { status: 404 });
        return ok({});
      }
    }
    throw new Error(`Unmocked fetch: ${method} ${url}`);
  };
});

const writes = () => calls.filter((c) => c.url.includes("api.clickup.com") && c.method !== "GET");
const puts = () => writes().filter((c) => c.method === "PUT");
const fieldWrites = () => writes().filter((c) => /\/field\//.test(c.url));
const cuComments = () => writes().filter((c) => c.method === "POST" && c.url.endsWith("/comment"));
const pendingRows = () => env.DB.raw.prepare("SELECT * FROM pm_pending_actions ORDER BY number").all();
const logRows = () => env.DB.raw.prepare("SELECT * FROM pm_action_log ORDER BY id").all();

function addonMessage(text, { sender = AVERY, dm = false } = {}) {
  const space = dm ? { name: SPACE, spaceType: "DIRECT_MESSAGE" } : { name: SPACE, spaceType: "SPACE" };
  return {
    commonEventObject: { hostApp: "CHAT" },
    chat: {
      user: { name: "users/1", ...sender, type: "HUMAN" },
      space,
      messagePayload: {
        space,
        message: { name: `${SPACE}/messages/x`, text, argumentText: text, sender: { name: "users/1", ...sender }, thread: { name: THREAD }, space },
      },
    },
  };
}

async function send(payload, { token, syncMs = 2000 } = {}) {
  const t = token === undefined ? await addonToken() : token;
  const waits = [];
  const res = await handleNitmPmRoutes(
    new Request(`${HOST}${CHAT_PATH}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(t ? { Authorization: `Bearer ${t}` } : {}) },
      body: JSON.stringify(payload),
    }),
    env,
    { waitUntil: (p) => waits.push(p) },
    CHAT_PATH,
    { syncMs }
  );
  const json = await res.json();
  return { res, json, text: json?.hostAppDataAction?.chatDataAction?.createMessageAction?.message?.text ?? json?.text, waits };
}

// Claude reply helpers
const toolUse = (name, input, id = `tu_${name}_${Math.random().toString(36).slice(2, 7)}`) => ({
  stop_reason: "tool_use",
  content: [{ type: "tool_use", id, name, input }],
});
const toolUses = (...uses) => ({
  stop_reason: "tool_use",
  content: uses.map(([name, input], i) => ({ type: "tool_use", id: `tu_${i}`, name, input })),
});
const say = (text) => ({ stop_reason: "end_turn", content: [{ type: "text", text }] });
const lastToolResults = (req) => req.messages.at(-1).content.filter((b) => b.type === "tool_result");

// ===========================================================================
// Verification
// ===========================================================================
test("missing Authorization header is rejected", async () => {
  const { res } = await send(addonMessage("hi"), { token: null });
  assert.equal(res.status, 401);
});

test("token signed by the wrong key is rejected", async () => {
  const forged = await signJwt(
    { iss: "https://accounts.google.com", aud: endpointUrl({}), email: `service-${PROJECT}@gcp-sa-gsuiteaddons.iam.gserviceaccount.com`, email_verified: true, iat: now(), exp: now() + 3600 },
    { key: otherKey.privateKey }
  );
  const { res } = await send(addonMessage("hi"), { token: forged });
  assert.equal(res.status, 401);
});

test("token for another URL, another project, or expired is rejected", async () => {
  for (const over of [
    { aud: "https://evil.example.com/hook" },
    { email: "service-999@gcp-sa-gsuiteaddons.iam.gserviceaccount.com" },
    { email: "chat@system.gserviceaccount.com" }, // legacy endpoint-URL audience: any project can mint these
    { exp: now() - 600 },
  ]) {
    const { res } = await send(addonMessage("hi"), { token: await addonToken(over) });
    assert.equal(res.status, 401, JSON.stringify(over));
  }
});

test("garbage bearer token is rejected, and nothing verifies without a project number", async () => {
  assert.equal((await send(addonMessage("hi"), { token: "not.a.jwt" })).res.status, 401);
  delete env.PM_CHAT_PROJECT_NUMBER;
  assert.equal((await send(addonMessage("hi"))).res.status, 401);
});

test("legacy Project Number token is accepted and gets a legacy-shaped reply", async () => {
  const token = await signJwt({ iss: "chat@system.gserviceaccount.com", aud: PROJECT, iat: now(), exp: now() + 3600 });
  const legacy = { type: "ADDED_TO_SPACE", user: AVERY, space: { name: SPACE, type: "ROOM" } };
  const { res, json } = await send(legacy, { token });
  assert.equal(res.status, 200);
  assert.match(json.text, /NITM PM/);
  assert.equal(json.hostAppDataAction, undefined);
});

test("ADDED_TO_SPACE (add-on) says hello and explains itself", async () => {
  const payload = { chat: { user: AVERY, space: { name: SPACE }, addedToSpacePayload: { space: { name: SPACE } } } };
  const { text } = await send(payload);
  assert.match(text, /What's stuck today/);
  assert.match(text, /approve 1 and 3/);
});

test("REMOVED_FROM_SPACE returns an empty response and is logged", async () => {
  const payload = { chat: { user: AVERY, space: { name: SPACE }, removedFromSpacePayload: { space: { name: SPACE } } } };
  const { res, json } = await send(payload);
  assert.equal(res.status, 200);
  assert.deepEqual(json, {});
  assert.equal(logRows().at(-1).event, "removed_from_space");
});

test("someone outside the domain is turned away without calling Claude", async () => {
  const { text } = await send(addonMessage("What's stuck today?", { sender: { email: "x@gmail.com", displayName: "X" } }));
  assert.match(text, /only work with the NITM team/);
  assert.equal(claudeRequests.length, 0);
});

// ===========================================================================
// Q&A
// ===========================================================================
test("\"What's stuck today?\" returns only overdue cards that aren't done or paused", async () => {
  comments["peds-quiz"] = [{ date: String(Date.now()), user: { username: "Gaby" }, comment_text: "Waiting on footage" }];
  claudeScript = [
    { reply: toolUse("find_cards", { overdue: true, with_comments: true }) },
    { reply: say("*2 stuck:* <https://app.clickup.com/t/peds-quiz|Peds Infectious Diseases Quiz> ...") },
  ];
  const { text } = await send(addonMessage("What's stuck today?"));
  assert.match(text, /stuck/);

  const result = JSON.parse(lastToolResults(claudeRequests[1])[0].content);
  const ids = result.cards.map((c) => c.id).sort();
  assert.deepEqual(ids, ["funny-one", "peds-quiz"]); // not posted-old, on-hold, not-due, backlog-nodate
  const peds = result.cards.find((c) => c.id === "peds-quiz");
  assert.equal(peds.stage, "Ready To Edit / Design");
  assert.equal(peds.expected_owner, "Gaby");
  assert.equal(peds.owner_matches, true);
  assert.equal(peds.recent_comments[0].text, "Waiting on footage");
  const funny = result.cards.find((c) => c.id === "funny-one");
  assert.equal(funny.expected_owner, "Diana"); // review stage -> Diana, currently Tia
  assert.equal(funny.owner_matches, false);
  assert.equal(writes().length, 0);
});

test("a due date stored at 04:00 UTC today counts as yesterday in New York (overdue)", async () => {
  // Real case, Nov 4 2026: "Match each medication to its antidote" had
  // due_date 2026-11-04T04:00Z = Nov 3, 11pm in New York, and was missed.
  const t = card("antidote", { name: "Match each medication to its antidote", stage: "st-edit", post: addDays(TODAY, 3), assignees: [PEOPLE.tia] });
  t.due_date = String(Date.parse(TODAY + "T04:00:00Z"));
  tasks.antidote = t;
  claudeScript = [{ reply: toolUse("find_cards", { overdue: true }) }, { reply: say("ok") }];
  await send(addonMessage("What's stuck today?"));
  const out = JSON.parse(lastToolResults(claudeRequests[1])[0].content);
  const c = out.cards.find((x) => x.id === "antidote");
  assert.ok(c, "the 04:00Z card is reported as overdue");
  assert.equal(c.due_date, addDays(TODAY, -1));
  assert.equal(c.expected_owner, "Gaby"); // a Quiz in an edit stage
  assert.equal(c.owner_matches, false);
});

test("the system prompt carries the instructions + guardrails, cached, with context on the message", async () => {
  claudeScript = [{ reply: say("Nothing posting next week yet.") }];
  await send(addonMessage("What's posting next week?"));
  const req = claudeRequests[0];
  assert.equal(req.model, "claude-sonnet-5-5");
  assert.match(req.system[0].text, /TikTok AI PM: Operating Instructions/);
  assert.match(req.system[0].text, /When a person explicitly asks for one, propose exactly what they asked/);
  assert.deepEqual(req.system[0].cache_control, { type: "ephemeral" });
  assert.deepEqual(req.tools.map((t) => t.name), ["find_cards", "get_card", "calendar", "propose_change", "apply_approved"]);
  assert.match(req.messages.at(-1).content, /<context>[\s\S]*approver: yes[\s\S]*<\/context>\s+What's posting next week\?/);
});

test("PM_MODEL overrides the model", async () => {
  env.PM_MODEL = "claude-opus-5-5";
  claudeScript = [{ reply: say("ok") }];
  await send(addonMessage("hi there"));
  assert.equal(claudeRequests[0].model, "claude-opus-5-5");
});

test("calendar groups cards by Post Date with Type and Stage", async () => {
  claudeScript = [
    { reply: toolUse("calendar", { from: TODAY, to: addDays(TODAY, 10) }) },
    { reply: say("Here's the week.") },
  ];
  await send(addonMessage("What's posting next week?"));
  const out = JSON.parse(lastToolResults(claudeRequests[1])[0].content);
  const day = out.days.find((d) => d.date === addDays(TODAY, 6));
  assert.deepEqual(day.cards.map((c) => [c.name, c.type, c.stage]), [["Peds Infectious Diseases Quiz", "Quiz", "Ready To Edit / Design"]]);
});

test("follow-up messages include the thread's earlier messages", async () => {
  claudeScript = [{ reply: say("First answer") }, { reply: say("Second answer") }];
  await send(addonMessage("What's stuck today?"));
  await send(addonMessage("and for Kristine?"));
  const msgs = claudeRequests[1].messages;
  assert.equal(msgs[0].role, "user");
  assert.match(msgs[0].content, /What's stuck today\?/);
  assert.equal(msgs[1].content, "First answer");
  assert.match(msgs[2].content, /and for Kristine\?/);
});

test("a Claude API failure is reported plainly, not dressed up", async () => {
  claudeScript = [{ status: 529 }];
  const { text } = await send(addonMessage("What's stuck today?"));
  assert.match(text, /Claude\) returned an error/);
});

test("a ClickUp failure reaches Claude as a tool error", async () => {
  claudeScript = [{ reply: toolUse("get_card", { task_id: "missing-card" }) }, { reply: say("ClickUp couldn't find that card.") }];
  await send(addonMessage("Tell me about card missing-card"));
  const r = lastToolResults(claudeRequests[1])[0];
  assert.equal(r.is_error, true);
  assert.match(r.content, /ClickUp GET \/task\/missing-card -> 404/);
});

// ===========================================================================
// Proposals and approvals
// ===========================================================================
test("a change request creates a pending action and changes nothing", async () => {
  const friday = addDays(TODAY, 2);
  claudeScript = [
    { reply: toolUse("propose_change", { type: "due_date", task_id: "peds-quiz", date: friday, reason: "Diana asked" }) },
    { reply: say("I can do that. *#1*: move the due date. Reply \"yes\" to apply.") },
  ];
  const { text } = await send(addonMessage("Move the peds quiz due date to Friday", { sender: DIANA }));
  assert.match(text, /#1/);
  const rows = pendingRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].number, 1);
  assert.equal(rows[0].status, "pending");
  assert.equal(rows[0].proposed_by, DIANA.email);
  assert.deepEqual(JSON.parse(rows[0].action), { type: "due_date", task_id: "peds-quiz", date: friday });
  assert.match(rows[0].summary, /Peds Infectious Diseases Quiz/);
  assert.equal(writes().length, 0, "nothing written to ClickUp");
  const tr = JSON.parse(lastToolResults(claudeRequests[1])[0].content);
  assert.equal(tr.number, 1);
  assert.match(tr.status, /nothing has changed/);
});

test("\"yes\" from an approver applies the single pending proposal, comments, and logs it", async () => {
  const friday = addDays(TODAY, 2);
  claudeScript = [{ reply: toolUse("propose_change", { type: "due_date", task_id: "peds-quiz", date: friday }) }, { reply: say("#1 ready") }];
  await send(addonMessage("Move the peds quiz due date to Friday"));
  const { text } = await send(addonMessage("yes"));
  assert.equal(claudeRequests.length, 2, "approval handled without Claude");
  assert.match(text, /✅ #1/);
  assert.equal(puts().length, 1);
  assert.deepEqual(puts()[0].body, { due_date: ymdToClickUpMs(friday), due_date_time: false });
  assert.equal(cuComments().length, 1);
  assert.match(cuComments()[0].body.comment_text, /^Due date moved to .+, approved by Avery in Chat\.$/);
  assert.equal(pendingRows()[0].status, "approved");
  assert.equal(pendingRows()[0].decided_by, AVERY.email);
  const applied = logRows().find((r) => r.event === "applied");
  assert.equal(applied.actor, AVERY.email);
  assert.equal(applied.number, 1);
});

async function seedThree() {
  const conv = { space: SPACE, thread: THREAD };
  await send(addonMessage("warm up")).catch(() => {}); // creates tables
  const mk = (action, summary) => createPending(env, conv, { proposedBy: AVERY.email, action, summary });
  await mk({ type: "priority", task_id: "peds-quiz", priority: "high" }, "Set priority on Peds to high");
  await mk({ type: "assignee", task_id: "funny-one", add: [PEOPLE.diana], remove: [PEOPLE.tia] }, "Assign Diana on Funny");
  await mk({ type: "comment", task_id: "not-due", text: "Checking in on this one. Anything blocking it?" }, "Comment on Fine for now");
}

test("\"approve 1 and 3\" applies exactly those two", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const { text } = await send(addonMessage("approve 1 and 3"));
  assert.match(text, /✅ #1/);
  assert.match(text, /✅ #3/);
  assert.doesNotMatch(text, /#2/);

  assert.equal(puts().length, 1);
  assert.match(puts()[0].url, /\/task\/peds-quiz$/);
  assert.deepEqual(puts()[0].body, { priority: 2 });
  const commented = cuComments().map((c) => c.url.match(/task\/([^/]+)\/comment/)[1]).sort();
  assert.deepEqual(commented, ["not-due", "peds-quiz"]); // paper trail on #1, the comment itself for #3
  assert.match(cuComments().find((c) => c.url.includes("not-due")).body.comment_text, /Anything blocking it\?[\s\S]*approved by Avery in Chat/);
  assert.equal(writes().some((c) => c.url.includes("funny-one")), false);
  assert.deepEqual(pendingRows().map((r) => r.status), ["approved", "pending", "approved"]);
});

test("an approval from someone not on the list is refused politely and changes nothing", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const { text } = await send(addonMessage("approve 1 and 3", { sender: DIANA }));
  assert.match(text, /Thanks, Diana!/);
  assert.match(text, /approver list/);
  assert.equal(writes().length, 0);
  assert.deepEqual(pendingRows().map((r) => r.status), ["pending", "pending", "pending"]);
  assert.equal(logRows().at(-1).event, "approval_refused");
});

test("apply_approved from Claude only applies numbers the approver typed this message", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  claudeScript = [
    { reply: toolUse("apply_approved", { numbers: [1, 2, 3] }) },
    { reply: say("Applied #2; #1 and #3 weren't approved.") },
  ];
  await send(addonMessage("Looks good, approve 2 but make the comment friendlier"));
  const r = JSON.parse(lastToolResults(claudeRequests.at(-1))[0].content);
  assert.match(r.results.join("\n"), /✅ #2/);
  assert.match(r.results.join("\n"), /Not applied: #1, #3/);
  assert.deepEqual(pendingRows().map((x) => x.status), ["pending", "approved", "pending"]);
});

test("\"yes\" with several pending asks which ones instead of guessing", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const { text } = await send(addonMessage("yes"));
  assert.match(text, /3 pending proposals/);
  assert.equal(writes().length, 0);
});

test("\"reject 2\" drops a proposal without changing anything", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const { text } = await send(addonMessage("reject 2"));
  assert.match(text, /#2: dropped/);
  assert.equal(writes().length, 0);
  assert.equal(pendingRows()[1].status, "rejected");
});

test("an expired pending action can't be approved", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const old = new Date(Date.now() - 49 * 3600 * 1000).toISOString();
  env.DB.raw.prepare("UPDATE pm_pending_actions SET created_at = ? WHERE number = 1").run(old);
  const { text } = await send(addonMessage("approve 1"));
  assert.match(text, /#1: already expired|#1: expired/);
  assert.equal(writes().length, 0);
  assert.equal(pendingRows()[0].status, "expired");
});

test("an already-approved action isn't applied twice", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  await send(addonMessage("approve 1"));
  const before = writes().length;
  const { text } = await send(addonMessage("approve 1"));
  assert.match(text, /already approved/);
  assert.equal(writes().length, before);
});

test("a ClickUp failure while applying is reported, and the action is marked failed", async () => {
  claudeScript = [{ reply: say("hello") }];
  await seedThree();
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (u, o = {}) =>
    o.method === "PUT" ? new Response("rate limited", { status: 429 }) : realFetch(u, o);
  const { text } = await send(addonMessage("approve 1"));
  assert.match(text, /❌ #1.*Not changed.*429/s);
  assert.equal(pendingRows()[0].status, "failed");
  assert.equal(logRows().at(-1).event, "apply_failed");
});

// ===========================================================================
// Post Date, Stage and other custom fields: allowed when asked, still gated
// ===========================================================================
async function proposeAndApprove(input, ask) {
  claudeScript = [{ reply: toolUse("propose_change", input) }, { reply: say("Proposed #1.") }];
  await send(addonMessage(ask, { sender: DIANA }));
  assert.equal(writes().length, 0, "nothing written before approval");
  assert.equal(pendingRows().at(-1).status, "pending");
  return send(addonMessage("yes"));
}

test("a requested Post Date change is proposed, then made once approved", async () => {
  const date = addDays(TODAY, 7);
  const { text } = await proposeAndApprove({ type: "post_date", task_id: "peds-quiz", date }, "Move the peds quiz post date out a week");
  assert.match(text, /✅ #1/);
  assert.equal(fieldWrites().length, 1);
  assert.match(fieldWrites()[0].url, new RegExp(`/task/peds-quiz/field/${FIELDS.postDate}$`));
  assert.equal(fieldWrites()[0].method, "POST");
  assert.deepEqual(fieldWrites()[0].body, { value: ymdToClickUpMs(date), value_options: { time: false } });
  assert.match(cuComments().at(-1).body.comment_text, /^Post Date set to .+, approved by Avery in Chat\.$/);
  assert.equal(logRows().find((r) => r.event === "applied").number, 1);
});

test("a requested Stage change resolves the option by name, then is made once approved", async () => {
  const { text } = await proposeAndApprove({ type: "stage", task_id: "peds-quiz", stage: "ready to schedule" }, "Set the peds quiz to Ready to Schedule");
  assert.match(text, /✅ #1/);
  assert.match(pendingRows()[0].summary, /Set Stage on \*Peds Infectious Diseases Quiz\* to Ready to Schedule/);
  assert.match(fieldWrites()[0].url, new RegExp(`/field/${FIELDS.stage}$`));
  assert.deepEqual(fieldWrites()[0].body, { value: "st-ready" });
  assert.match(cuComments().at(-1).body.comment_text, /^Stage set to Ready to Schedule, approved by Avery in Chat\.$/);
});

test("emoji in option names don't get in the way (\"Scheduled\" -> \"Scheduled 📅\")", async () => {
  await proposeAndApprove({ type: "stage", task_id: "peds-quiz", stage: "Scheduled" }, "Mark it scheduled");
  assert.deepEqual(fieldWrites()[0].body, { value: "st-sched" });
});

test("another custom field (dropdown or text) can be changed by name", async () => {
  await proposeAndApprove({ type: "custom_field", task_id: "peds-quiz", field: "who's filming", value: "Diana (Team 2)" }, "Diana's filming the peds quiz");
  assert.match(fieldWrites()[0].url, new RegExp(`/field/${WHO_FILMS}$`));
  assert.deepEqual(fieldWrites()[0].body, { value: "wf-d" });
  assert.match(cuComments().at(-1).body.comment_text, /^Who's Filming set to DIANA \(Team 2\)/);
});

test("an empty value clears the field", async () => {
  await proposeAndApprove({ type: "custom_field", task_id: "peds-quiz", field: "Notes", value: "" }, "Clear the notes on the peds quiz");
  assert.equal(fieldWrites()[0].method, "DELETE");
  assert.match(fieldWrites()[0].url, new RegExp(`/field/${NOTES}$`));
});

test("bad field names and option values are refused with the valid choices, and record nothing", async () => {
  claudeScript = [
    {
      reply: toolUses(
        ["propose_change", { type: "stage", task_id: "peds-quiz", stage: "Almost done" }],
        ["propose_change", { type: "custom_field", task_id: "peds-quiz", field: "Mood", value: "happy" }],
        ["propose_change", { type: "custom_field", task_id: "peds-quiz", field: "Days Until Post", value: 3 }],
        ["propose_change", { type: "post_date", task_id: "peds-quiz", date: "next friday" }]
      ),
    },
    { reply: say("Some of those didn't work.") },
  ];
  await send(addonMessage("do a few things"));
  const results = lastToolResults(claudeRequests[1]).map((r) => JSON.parse(r.content));
  assert.ok(results.every((r) => r.refused === true));
  assert.match(results[0].message, /isn't an option for Stage\. Options: .*Ready to Schedule/);
  assert.match(results[1].message, /no field called "Mood"/);
  assert.match(results[2].message, /can't set Days Until Post \(a formula field\)/);
  assert.match(results[3].message, /YYYY-MM-DD/);
  assert.equal(pendingRows().length, 0);
  assert.equal(writes().length, 0);
});

test("validateAction still refuses things that aren't offered (status, name, unknown keys)", () => {
  const cases = [
    { type: "status", task_id: "peds-quiz" },
    { type: "rename", task_id: "peds-quiz", text: "x" },
    { type: "due_date", task_id: "peds-quiz", date: TODAY, custom_fields: [{ id: FIELDS.postDate }] },
    { type: "due_date", task_id: "../etc", date: TODAY },
  ];
  for (const c of cases) assert.throws(() => validateAction(c), RefusedError, JSON.stringify(c));
});

test("a Post Date change still waits for an approver: Diana's yes changes nothing", async () => {
  claudeScript = [{ reply: toolUse("propose_change", { type: "post_date", task_id: "peds-quiz", date: addDays(TODAY, 9) }) }, { reply: say("#1") }];
  await send(addonMessage("move the post date", { sender: DIANA }));
  const { text } = await send(addonMessage("yes", { sender: DIANA }));
  assert.match(text, /approver list/);
  assert.equal(writes().length, 0);
});

test("a tampered pending row with smuggled keys is refused at apply time", async () => {
  claudeScript = [{ reply: say("hello") }];
  await send(addonMessage("warm up"));
  await createPending(env, { space: SPACE, thread: THREAD }, {
    proposedBy: "x",
    action: { type: "due_date", task_id: "peds-quiz", date: TODAY, custom_fields: [{ id: FIELDS.postDate, value: 1 }] },
    summary: "sneaky",
  });
  const { text } = await send(addonMessage("approve 1"));
  assert.match(text, /❌ #1.*Not changed/s);
  assert.equal(writes().length, 0);
});

test("applyAction refuses a card that isn't on the Production list", async () => {
  tasks["elsewhere"] = card("elsewhere", { stage: "st-edit", list: "999" });
  await assert.rejects(() => applyAction(env, { type: "priority", task_id: "elsewhere", priority: "high" }, "Avery"), /isn't on the TikTok Production list/);
  assert.equal(writes().length, 0);
});

// ===========================================================================
// Slow answers
// ===========================================================================
test("a slow answer replies \"On it…\" and then posts in the same thread", async () => {
  claudeScript = [{ reply: say("Here's the *daily check*: all clear."), delay: 150 }];
  const { text, waits } = await send(addonMessage("Run the daily check"), { syncMs: 20 });
  assert.equal(text, "On it…");
  assert.equal(waits.length, 1);
  await Promise.all(waits);
  assert.equal(chatPosts.length, 1);
  assert.equal(chatPosts[0].url, `https://chat.googleapis.com/v1/${SPACE}/messages?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD`);
  assert.deepEqual(chatPosts[0].body, { text: "Here's the *daily check*: all clear.", thread: { name: THREAD } });
  // The service-account token came from a signed JWT assertion.
  const tokenCall = calls.find((c) => c.url === "https://oauth2.googleapis.com/token");
  assert.match(tokenCall.body, /grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=ey/);
  const assertion = tokenCall.body.split("assertion=")[1];
  const claims = JSON.parse(Buffer.from(assertion.split(".")[1], "base64url").toString());
  assert.equal(claims.scope, "https://www.googleapis.com/auth/chat.bot");
  assert.equal(claims.iss, "nitm-pm@nitm-pm.iam.gserviceaccount.com");
  const ok = await crypto.subtle.verify(
    "RSASSA-PKCS1-v1_5",
    saKey.publicKey,
    Buffer.from(assertion.split(".")[2], "base64url"),
    new TextEncoder().encode(assertion.split(".").slice(0, 2).join("."))
  );
  assert.equal(ok, true, "assertion is RS256-signed with the service account key");
});

test("a fast answer is returned directly with no async post", async () => {
  claudeScript = [{ reply: say("Quick one.") }];
  const { text, waits } = await send(addonMessage("What's stuck today?"));
  assert.equal(text, "Quick one.");
  assert.equal(waits.length, 0);
  assert.equal(chatPosts.length, 0);
});

test("in a DM the async answer posts to the space with no thread", async () => {
  claudeScript = [{ reply: say("DM answer"), delay: 100 }];
  const { waits } = await send(addonMessage("What's stuck today?", { dm: true }), { syncMs: 10 });
  await Promise.all(waits);
  assert.deepEqual(chatPosts[0].body, { text: "DM answer" });
  assert.equal(chatPosts[0].url, `https://chat.googleapis.com/v1/${SPACE}/messages`);
});

test("past the deadline the bot says it stopped, and lists proposals it made", async () => {
  const { runTurn } = await import("../src/nitm-pm/brain.js");
  claudeScript = [{ reply: say("hello") }];
  await send(addonMessage("warm up")); // creates tables
  claudeScript = [
    { reply: toolUse("propose_change", { type: "priority", task_id: "peds-quiz", priority: "high" }) },
    { reply: say("never reached"), delay: 5000 },
  ];
  const reply = await runTurn(env, {
    conv: { space: SPACE, thread: THREAD },
    sender: AVERY,
    text: "Run the daily check",
    tz: "America/New_York",
    history: [],
    approvedNumbers: [],
    deadline: Date.now() + 4000,
  });
  assert.match(reply, /longer than I'm allowed to run/);
  assert.match(reply, /proposal #1 before stopping\. Nothing has changed/);
  assert.equal(writes().length, 0);
});

// ===========================================================================
// Parsing and formatting
// ===========================================================================
test("parseApproval understands the common phrasings", () => {
  assert.deepEqual(parseApproval("approve 1 and 3").numbers, [1, 3]);
  assert.equal(parseApproval("approve 1 and 3").pure, true);
  assert.deepEqual(parseApproval("Approve #2, #4 & 5.").numbers, [2, 4, 5]);
  assert.equal(parseApproval("yes").kind, "yes");
  assert.equal(parseApproval("Go ahead!").kind, "yes");
  assert.equal(parseApproval("approve all").kind, "approve_all");
  assert.deepEqual(parseApproval("reject 2").reject, [2]);
  assert.equal(parseApproval("ok approve 2 but make it Thursday").pure, false);
  assert.deepEqual(parseApproval("ok approve 2 but make it Thursday").numbers, [2]);
  assert.equal(parseApproval("What's stuck today?").kind, "none");
  assert.equal(parseApproval("yes, what about Kristine?").kind, "none");
});

test("Markdown in a reply is converted to Chat formatting", async () => {
  claudeScript = [{ reply: say("**Stuck:** [Peds quiz](https://app.clickup.com/t/peds-quiz)") }];
  const { text } = await send(addonMessage("What's stuck today?"));
  assert.equal(text, "*Stuck:* <https://app.clickup.com/t/peds-quiz|Peds quiz>");
});
