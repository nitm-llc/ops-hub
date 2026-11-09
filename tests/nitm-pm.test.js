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
import { _clearTeamCache, messagePerson } from "../src/nitm-pm/outreach.js";
import { dueRuns, floCron } from "../src/nitm-pm/scheduler.js";
import { recordPerson } from "../src/nitm-pm/store.js";

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
  return {
    raw: db,
    prepare: (sql) => stmt(sql),
    batch: async (stmts) => Promise.all(stmts.map((st) => st.run())),
  };
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
  { id: "st-edit", name: "Ready To Edit / Design", orderindex: 3, color: "#ffc53d" },
  { id: "st-review", name: "T: Drive Review", orderindex: 9, color: "#02BCD4" },
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

// ClickUp workspace members (GET /team), for name -> email.
const TEAM = [
  [89106215, "Avery Pless", AVERY.email],
  [50405997, "Mark Podlas", "mark@anurseinthemaking.com"],
  [89106214, "Diana Boyle", DIANA.email],
  [89133528, "Gabriela Eisenbraun", "gaby@anurseinthemaking.com"],
  [89173497, "Cassandra Van Nostrand", "cassandra@anurseinthemaking.com"],
  [89106213, "Kristine Tuttle", "kristine.tuttle@anurseinthemaking.com"],
];
const dmOf = (email) => `spaces/DM-${email.split("@")[0]}`;
const sayHi = (email, name) => recordPerson(env, { email, displayName: name, chatUser: "users/1", dmSpace: dmOf(email) });

beforeEach(() => {
  _clearTeamCache();
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
      if (method === "GET" && path === "/team") {
        return ok({ teams: [{ id: "9017029478", members: TEAM.map(([id, username, email]) => ({ user: { id, username, email } })) }] });
      }
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
  assert.match(json.text, /I'm \*Flo\*/);
  assert.match(json.text, /marketing project management/);
  assert.equal(json.hostAppDataAction, undefined);
});

test("ADDED_TO_SPACE (add-on) says hello and explains itself", async () => {
  const payload = { chat: { user: AVERY, space: { name: SPACE }, addedToSpacePayload: { space: { name: SPACE } } } };
  const { text } = await send(payload);
  assert.match(text, /What's stuck today/);
  assert.match(text, /approve 1 and 3/);
  assert.ok(text.split("\n").length <= 3, "the greeting stays short");
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
  assert.deepEqual(req.tools.map((t) => t.name), ["find_cards", "get_card", "calendar", "propose_change", "message_person", "apply_approved"]);
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
  assert.match(text, /Thanks, Diana\. Only approvers can approve/);
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
  assert.match(text, /^Which ones\? #1, #2, #3 are pending/);
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
  assert.match(text, /Only approvers can approve/);
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
  await Promise.all(waits);
  const marks = logRows().filter((r) => r.event === "turn").map((r) => r.result);
  assert.ok(marks.some((m) => /^replied "On it" after/.test(m)), "timing mark for the On it reply");
  assert.ok(marks.some((m) => /^posted the answer after/.test(m)), "timing mark for the async answer");
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

test("the On it deadline counts from when the request arrived", async () => {
  // Slow setup (e.g. a fresh server) must not push the reply past Google's window.
  const realPrepare = env.DB.prepare;
  let slowed = false;
  env.DB.prepare = (sql) => {
    const st = realPrepare(sql);
    if (!slowed && /FROM pm_threads/.test(sql)) {
      slowed = true;
      const first = st.bind;
      return { ...st, bind: (...a) => { const b = first(...a); return { ...b, first: async () => { await new Promise((r) => setTimeout(r, 120)); return b.first(); } }; } };
    }
    return st;
  };
  claudeScript = [{ reply: say("late answer"), delay: 200 }];
  const t = Date.now();
  const { text, waits } = await send(addonMessage("What's stuck today?"), { syncMs: 100 });
  assert.equal(text, "On it…");
  assert.ok(Date.now() - t < 200, `replied within the window (took ${Date.now() - t}ms)`);
  await Promise.all(waits);
  env.DB.prepare = realPrepare;
});

test("a fast answer is returned directly with no async post", async () => {
  claudeScript = [{ reply: say("Quick one.") }];
  const { text, waits } = await send(addonMessage("What's stuck today?"));
  assert.equal(text, "Quick one.");
  await Promise.all(waits);
  assert.equal(chatPosts.length, 0);
  assert.match(logRows().find((r) => r.event === "turn").result, /^answered directly after/);
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
  assert.match(reply, /longer than I can spend on one message/);
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

// ===========================================================================
// /flo/ page API
// ===========================================================================
import { handleFloApi } from "../src/nitm-pm/admin.js";
import { DEFAULT_INSTRUCTIONS } from "../src/nitm-pm/brain.js";

async function floApi(path, { method = "GET", body, as = "avery@anurseinthemaking.com", host = HOST } = {}) {
  const res = await handleFloApi(
    new Request(`${host}/flo/api/${path}`, {
      method,
      headers: {
        ...(as ? { "Cf-Access-Authenticated-User-Email": as } : {}),
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    }),
    env,
    `/flo/api/${path.split("?")[0]}` // the Worker passes url.pathname, without the query
  );
  return { status: res.status, json: await res.json() };
}
const NEW_TEXT = "# Flo test instructions\n\n" + "Always mention the follow-up window. ".repeat(10);

test("overview shows the code version, the built-in rules, approvers and setup", async () => {
  const { status, json } = await floApi("overview");
  assert.equal(status, 200);
  assert.equal(json.instructions.is_default, true);
  assert.equal(json.instructions.text, DEFAULT_INSTRUCTIONS);
  assert.match(json.guardrails, /You are "Flo"/);
  assert.deepEqual(json.approvers, ["mark@anurseinthemaking.com", "avery@anurseinthemaking.com"]);
  assert.equal(json.can_edit, true);
  assert.deepEqual(json.configured, { google_project: true, google_key: true, claude: true, clickup: true });
  assert.equal((await floApi("overview", { as: DIANA.email })).json.can_edit, false);
});

test("the page API is refused on any other hostname", async () => {
  const { status } = await floApi("overview", { host: "https://ops-hub.example.workers.dev" });
  assert.equal(status, 403);
});

test("only approvers can edit instructions; a save takes effect on Flo's next message", async () => {
  const denied = await floApi("instructions", { method: "PUT", body: { text: NEW_TEXT }, as: DIANA.email });
  assert.equal(denied.status, 403);
  assert.match(denied.json.error, /Only approvers/);

  const ok = await floApi("instructions", { method: "PUT", body: { text: NEW_TEXT, note: "testing" } });
  assert.equal(ok.status, 200);
  const ov = (await floApi("overview")).json;
  assert.equal(ov.instructions.is_default, false);
  assert.equal(ov.instructions.updated_by, AVERY.email);

  claudeScript = [{ reply: say("ok") }];
  await send(addonMessage("What's stuck today?"));
  const system = claudeRequests[0].system[0].text;
  assert.ok(system.startsWith("# Flo test instructions"), "edited instructions are used");
  assert.match(system, /# How you work in Google Chat/, "built-in rules are still appended");
  assert.doesNotMatch(system, /TikTok AI PM: Operating Instructions/);
});

test("too-short instructions are refused, and non-JSON writes are refused", async () => {
  const short = await floApi("instructions", { method: "PUT", body: { text: "be nice" } });
  assert.equal(short.status, 400);
  const res = await handleFloApi(
    new Request(`${HOST}/flo/api/instructions`, { method: "PUT", headers: { "Cf-Access-Authenticated-User-Email": AVERY.email, "Content-Type": "text/plain" }, body: "x" }),
    env,
    "/flo/api/instructions"
  );
  assert.equal(res.status, 415);
});

test("history records each save with its text, and reset goes back to the code version", async () => {
  await floApi("instructions", { method: "PUT", body: { text: NEW_TEXT, note: "first edit" } });
  const reset = await floApi("instructions/reset", { method: "POST", body: {} });
  assert.equal(reset.status, 200);
  assert.equal((await floApi("overview")).json.instructions.is_default, true);

  const { history } = (await floApi("history")).json;
  assert.equal(history.length, 2);
  assert.equal(history[0].note, "Reset to the version in code");
  assert.equal(history[0].chars, null);
  assert.equal(history[1].note, "first edit");
  const v = (await floApi(`history?id=${history[1].id}`)).json;
  assert.equal(v.text, NEW_TEXT.trim());
});

test("activity log is shaped for the page, with card links", async () => {
  claudeScript = [{ reply: toolUse("propose_change", { type: "priority", task_id: "peds-quiz", priority: "high" }) }, { reply: say("#1") }];
  await send(addonMessage("bump the peds quiz"));
  await send(addonMessage("yes"));
  const { log } = (await floApi("log?filter=changes")).json;
  assert.deepEqual(log.map((r) => r.event), ["applied", "proposed"]);
  assert.equal(log[0].label, "Changed");
  assert.equal(log[0].card_url, "https://app.clickup.com/t/peds-quiz");
  const ov = (await floApi("overview")).json;
  assert.equal(ov.stats.changed_7d, 1);
  assert.equal(ov.pending.length, 0);
});

// ===========================================================================
// Reaching people
// ===========================================================================
const postsTo = (space) => chatPosts.filter((p) => p.url.startsWith(`https://chat.googleapis.com/v1/${space}/messages`));

test("a DM to Flo records how to reach that person", async () => {
  claudeScript = [{ reply: say("hi") }];
  await send(addonMessage("hello", { dm: true }));
  const row = env.DB.raw.prepare("SELECT * FROM pm_people WHERE email = ?").get(AVERY.email);
  assert.equal(row.dm_space, SPACE);
  assert.equal(row.display_name, "Avery Pless");
});

test("message_person DMs someone who has said hi, logs it, and keeps it in their history", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi("gaby@anurseinthemaking.com", "Gabriela Eisenbraun");
  const r = await messagePerson(env, { person: "Gaby", text: "Checking in on the peds quiz, it posts Friday. Anything blocking it?", taskId: "peds-quiz", kind: "check_in" });
  assert.equal(r.sent, true);
  assert.equal(r.to, "Gabriela");
  const posts = postsTo(dmOf("gaby@anurseinthemaking.com"));
  assert.equal(posts.length, 1);
  assert.match(posts[0].body.text, /Anything blocking it\?/);
  const out = env.DB.raw.prepare("SELECT status, to_email, task_id FROM pm_outreach").all();
  assert.deepEqual(out.map((o) => ({ ...o })), [{ status: "sent", to_email: "gaby@anurseinthemaking.com", task_id: "peds-quiz" }]);
  const hist = JSON.parse(env.DB.raw.prepare("SELECT messages FROM pm_threads WHERE space = ?").get(dmOf("gaby@anurseinthemaking.com")).messages);
  assert.equal(hist.at(-1).role, "assistant");
});

test("someone Flo can't reach: the message goes to Avery and Mark to pass on", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  await sayHi("mark@anurseinthemaking.com", "Mark Podlas");
  const r = await messagePerson(env, { person: "Kristine", text: "Can you record the VO for the peds quiz today?", taskId: "peds-quiz", kind: "check_in" });
  assert.equal(r.sent, true);
  assert.equal(r.via_fallback, true);
  assert.match(r.message, /hasn't opened a chat with me yet/);
  for (const email of [AVERY.email, "mark@anurseinthemaking.com"]) {
    const posts = postsTo(dmOf(email));
    assert.equal(posts.length, 1, email);
    assert.match(posts[0].body.text, /^For \*Kristine\* \(can't reach them yet, please pass on\):\nCan you record the VO/);
  }
  assert.equal(env.DB.raw.prepare("SELECT status FROM pm_outreach").get().status, "fallback");
});

test("if nobody can be reached, nothing is sent and Flo says so", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  const r = await messagePerson(env, { person: "Kristine", text: "hello", kind: "check_in" });
  assert.equal(r.sent, false);
  assert.match(r.message, /Nothing was sent/);
  assert.equal(chatPosts.length, 0);
});

test("one message per person per card per day, and unknown names are refused", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi("gaby@anurseinthemaking.com", "Gabriela Eisenbraun");
  await messagePerson(env, { person: "Gaby", text: "first", taskId: "peds-quiz", kind: "check_in" });
  const again = await messagePerson(env, { person: "Gaby", text: "second", taskId: "peds-quiz", kind: "check_in" });
  assert.equal(again.sent, false);
  assert.match(again.message, /Already messaged Gabriela about this card today/);
  const nobody = await messagePerson(env, { person: "Zed", text: "hi" });
  assert.match(nobody.message, /don't know who "Zed" is/);
});

test("Claude can message a person, and short names match (Cass -> Cassandra)", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi("cassandra@anurseinthemaking.com", "Cassandra Van Nostrand");
  claudeScript = [
    { reply: toolUse("message_person", { person: "Cass", text: "Nov 11 to 13 are three Sim Teaching in a row. Swap the 12th with Friday's quiz?", kind: "calendar" }) },
    { reply: say("Sent Cass a swap suggestion.") },
  ];
  await send(addonMessage("Tell Cass about the Sim Teaching pile-up"));
  assert.equal(postsTo(dmOf("cassandra@anurseinthemaking.com")).length, 1);
  assert.equal(JSON.parse(lastToolResults(claudeRequests.at(-1))[0].content).sent, true);
});

test("a non-approver's proposal is filed with the lead approver and sent to them", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  claudeScript = [
    { reply: toolUse("propose_change", { type: "due_date", task_id: "peds-quiz", date: addDays(TODAY, 2) }) },
    { reply: say("Sent to Avery to approve.") },
  ];
  await send(addonMessage("Can the peds quiz be due Friday?", { sender: DIANA }));
  const rows = pendingRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].space, dmOf(AVERY.email), "filed in Avery's DM");
  assert.equal(rows[0].thread, "");
  assert.equal(rows[0].proposed_by, DIANA.email);
  const tr = JSON.parse(lastToolResults(claudeRequests.at(-1))[0].content);
  assert.match(tr.status, /sent to Avery to approve/);
  const toAvery = postsTo(dmOf(AVERY.email));
  assert.equal(toAvery.length, 1);
  assert.match(toAvery[0].body.text, /^Diana asks: \*#1\* .*\nReply "approve 1" or "reject 1"\.$/s);

  // Avery approves in her DM.
  const avDm = { commonEventObject: { hostApp: "CHAT" }, chat: { user: AVERY, space: { name: dmOf(AVERY.email), spaceType: "DIRECT_MESSAGE" },
    messagePayload: { space: { name: dmOf(AVERY.email), spaceType: "DIRECT_MESSAGE" }, message: { text: "approve 1", argumentText: "approve 1", sender: { name: "users/9", ...AVERY }, thread: { name: `${dmOf(AVERY.email)}/threads/x` } } } } };
  const { text } = await send(avDm);
  assert.match(text, /✅ #1/);
  assert.equal(puts().length, 1);
});

// ===========================================================================
// Schedule
// ===========================================================================
const nyTime = (isoLocal) => Date.parse(isoLocal + "-05:00"); // Nov = EST

test("dueRuns: weekday mornings for the daily check, Wednesday for planning, never late", () => {
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-04T08:59:00")), []);
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-04T09:05:00")), ["daily"]);
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-04T10:30:00")), ["daily", "weekly"]);
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-04T15:00:00")), [], "a 3pm deploy doesn't fire a late run");
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-05T10:30:00")), ["daily"]);
  assert.deepEqual(dueRuns("America/New_York", nyTime("2026-11-07T10:00:00")), [], "Saturday");
});

test("the cron runs the daily check once, in the lead's DM, and posts the summary there", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  const before = claudeRequests.length;
  claudeScript = [
    { reply: toolUse("propose_change", { type: "priority", task_id: "peds-quiz", priority: "high" }) },
    { reply: say("*#1*: set the peds quiz to high. Nothing else is stuck.") },
  ];
  const nine = nyTime(`${todayYmd()}T09:10:00`);
  await floCron(env, nine);
  assert.equal(claudeRequests.length, before + 2);
  assert.match(claudeRequests.at(-1).messages[0].content, /Scheduled run: the weekday daily check/);
  const posts = postsTo(dmOf(AVERY.email));
  assert.equal(posts.length, 1);
  assert.equal(posts[0].body.text, undefined, "sent as a card");
  assert.match(posts[0].body.fallbackText, /^Daily check, /);
  const card = posts[0].body.cardsV2[0].card;
  assert.equal(card.header.title, "Daily check");
  assert.match(JSON.stringify(card.sections), /<b>#1<\/b>: set the peds quiz to high/);
  assert.equal(pendingRows()[0].space, dmOf(AVERY.email));
  assert.equal(writes().length, 0, "still nothing changed in ClickUp");

  await floCron(env, nine + 120000); // next tick
  assert.equal(claudeRequests.length, before + 2, "not run twice");
});

test("the schedule can be switched off; a run-now request still runs", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  const off = await floApi("settings", { method: "PUT", body: { proactive: false } });
  assert.equal(off.status, 200);
  const before = claudeRequests.length;
  await floCron(env, nyTime(`${todayYmd()}T09:10:00`));
  assert.equal(claudeRequests.length, before, "off means no scheduled run");

  assert.equal((await floApi("run", { method: "POST", body: { kind: "daily" }, as: DIANA.email })).status, 403);
  assert.equal((await floApi("run", { method: "POST", body: { kind: "daily" } })).status, 200);
  claudeScript = [{ reply: say("Nothing is stuck today.") }];
  await floCron(env, nyTime(`${todayYmd()}T15:00:00`));
  assert.equal(claudeRequests.length, before + 1, "run-now ran despite the time and the switch");
  assert.match(JSON.stringify(postsTo(dmOf(AVERY.email)).at(-1).body.cardsV2), /Nothing is stuck today/);
});

test("with no reachable lead the scheduled run is skipped and logged", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  const before = claudeRequests.length;
  await floCron(env, nyTime(`${todayYmd()}T09:10:00`));
  assert.equal(claudeRequests.length, before);
  assert.ok(logRows().some((r) => r.event === "run_skipped"));
});

test("the page overview shows who Flo can reach", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi("gaby@anurseinthemaking.com", "Gabriela Eisenbraun");
  const o = (await floApi("overview")).json.outreach;
  const gaby = o.people.find((p) => p.name === "Gaby");
  const kristine = o.people.find((p) => p.name === "Kristine");
  assert.equal(gaby.reachable, true);
  assert.equal(kristine.reachable, false);
  assert.ok(o.people.find((p) => p.name === "Mark").fallback);
  assert.equal(o.proactive, true);
});

test("with PM_CLICKUP_TOKEN, changes and comments go out as Flo; reads keep the shared token", async () => {
  env.PM_CLICKUP_TOKEN = "pk_flo";
  const seen = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (u, o = {}) => {
    if (String(u).includes("api.clickup.com")) seen.push({ method: o.method || "GET", auth: (o.headers || {}).Authorization, url: String(u) });
    if (String(u) === "https://api.clickup.com/api/v2/user") return new Response(JSON.stringify({ user: { username: "Flo", email: "flo@anurseinthemaking.com" } }));
    return realFetch(u, o);
  };
  claudeScript = [{ reply: toolUse("propose_change", { type: "priority", task_id: "peds-quiz", priority: "high" }) }, { reply: say("#1") }];
  await send(addonMessage("bump the peds quiz"));
  await send(addonMessage("yes"));
  const w = seen.filter((c) => c.method !== "GET");
  assert.ok(w.length >= 2, "a change and a comment");
  assert.ok(w.every((c) => c.auth === "pk_flo"), "every write uses Flo's token");
  assert.ok(seen.filter((c) => c.method === "GET" && !c.url.endsWith("/user")).every((c) => c.auth === "pk_test"), "reads use the shared token");
  const ov = (await floApi("overview")).json;
  assert.deepEqual(ov.clickup_writer, { as_flo: true, name: "Flo", email: "flo@anurseinthemaking.com", error: null });
});

test("Tia gets a copy of the morning summary and can approve from it; first approval wins", async () => {
  env.PM_DIGEST_CC = "Tia";
  await send(addonMessage("warm up")).catch(() => {});
  TEAM.push([89106216, "Tia Freeman", "tia@anurseinthemaking.com"]);
  _clearTeamCache();
  await sayHi(AVERY.email, "Avery Pless");
  await sayHi("tia@anurseinthemaking.com", "Tia Freeman");
  claudeScript = [
    { reply: toolUse("propose_change", { type: "priority", task_id: "peds-quiz", priority: "high" }) },
    { reply: say("*#1* Peds quiz: priority → high\nReply \"yes\".") },
  ];
  await floCron(env, nyTime(`${todayYmd()}T09:10:00`));
  TEAM.pop();
  const tiaDm = dmOf("tia@anurseinthemaking.com");
  const copy = postsTo(tiaDm);
  assert.equal(copy.length, 1);
  assert.match(copy[0].body.cardsV2[0].card.header.subtitle, /copy, Avery has it too, either of you can approve$/);
  assert.match(JSON.stringify(copy[0].body.cardsV2[0].card.sections), /<b>#1<\/b> Peds quiz/);
  assert.equal(postsTo(dmOf(AVERY.email)).length, 1);

  const fromDm = (who, space, text) => ({ commonEventObject: { hostApp: "CHAT" }, chat: { user: who, space: { name: space, spaceType: "DIRECT_MESSAGE" },
    messagePayload: { space: { name: space, spaceType: "DIRECT_MESSAGE" }, message: { text, argumentText: text, sender: { name: "users/7", ...who }, thread: { name: `${space}/threads/z` } } } } });
  const TIA = { email: "tia@anurseinthemaking.com", displayName: "Tia Freeman" };
  env.PM_APPROVERS += ",tia@anurseinthemaking.com";
  const first = await send(fromDm(TIA, tiaDm, "approve 1"));
  assert.match(first.text, /✅ #1 done/);
  assert.equal(puts().length, 1);
  const second = await send(fromDm(AVERY, dmOf(AVERY.email), "approve 1"));
  assert.match(second.text, /already approved \(by tia@/);
  assert.equal(puts().length, 1, "applied once");
});

test("Reaching people also lists anyone else who has messaged Flo", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi("newperson@anurseinthemaking.com", "Jordan Lee");
  const people = (await floApi("overview")).json.outreach.people;
  const j = people.find((p) => p.email === "newperson@anurseinthemaking.com");
  assert.deepEqual({ name: j.name, reachable: j.reachable }, { name: "Jordan", reachable: true });
  assert.ok(people.some((p) => p.name === "Samantha"), "Samantha is on the team list");
});

// ===========================================================================
// Summary card
// ===========================================================================
import { summaryCard, stageInfo, toCardHtml, stageColor } from "../src/nitm-pm/cards.js";

const NOV9 = `*Proposals* (reply "approve 16 to 29", or pick numbers):
*#26, #29* <https://app.clickup.com/t/86e38njvm|ACLS Meds Whiteboard>: due → today, check-in comment (posts today)
*#20, #16* <https://app.clickup.com/t/peds-quiz|Vital Signs Quiz>: owner Kristine → Diana, due → today

*Messaged:* Gaby (ACLS Meds), Tia (IV Fluids).

Left alone: <https://app.clickup.com/t/not-due|Sterile field mistakes>. It posts tomorrow & I'll recheck.`;

test("summary card keeps Flo's words and lays them out in sections and card rows", () => {
  const stages = new Map([["peds-quiz", { stage: "Ready To Edit / Design", color: "#ffc53d" }]]);
  const msg = summaryCard({ title: "Daily check", subtitle: "Mon, Nov 9", body: NOV9, stages, pageUrl: "https://ops.example/flo/" });
  const { header, sections } = msg.cardsV2[0].card;
  assert.deepEqual(header, { title: "Daily check", subtitle: "Mon, Nov 9" });
  assert.equal(msg.fallbackText, "Daily check, Mon, Nov 9");
  assert.deepEqual(sections.map((s) => s.header), ["Proposals", "Messaged", "Left alone", undefined]);

  const [proposals, messaged, left, footer] = sections;
  const row = proposals.widgets[1].decoratedText;
  assert.equal(row.topLabel, "#20, #16 · Ready To Edit / Design");
  assert.equal(row.text, '<font color="#ffc53d">■</font> <a href="https://app.clickup.com/t/peds-quiz">Vital Signs Quiz</a>: owner Kristine → Diana, due → today');
  assert.equal(row.onClick.openLink.url, "https://app.clickup.com/t/peds-quiz");
  assert.equal(row.button, undefined, "no Open button; the row opens the card");
  assert.equal(proposals.widgets[0].decoratedText.topLabel, "#26, #29", "no Stage known, no tag");
  assert.ok(!proposals.widgets[0].decoratedText.text.includes("■"));
  assert.equal(proposals.widgets[2].textParagraph.text, '<font color="#80868b">Reply "approve 16 to 29", or pick numbers</font>', "the reply hint moves under the list");
  assert.equal(proposals.collapsible, undefined, "short lists don't fold");
  assert.equal(messaged.widgets[0].textParagraph.text, "Gaby (ACLS Meds), Tia (IV Fluids).");
  assert.match(left.widgets[0].decoratedText.text, /It posts tomorrow &amp; I'll recheck\.$/);
  assert.equal(footer.widgets[0].buttonList.buttons[0].onClick.openLink.url, "https://ops.example/flo/");
});

test("a long proposal list folds after 5 rows", () => {
  const body = "*Proposals:*\n" + Array.from({ length: 8 }, (_, i) => `*#${i + 1}* <https://app.clickup.com/t/c${i}|Card ${i}>: due → today`).join("\n");
  const s = summaryCard({ title: "Daily check", subtitle: "x", body }).cardsV2[0].card.sections[0];
  assert.equal(s.widgets.length, 8, "nothing dropped");
  assert.equal(s.collapsible, true);
  assert.equal(s.uncollapsibleWidgetsCount, 5);
});

test("card text is escaped, and Stage colors are made safe", () => {
  assert.equal(toCardHtml("- *Stale:* a <b> tag & _this_"), "• <b>Stale:</b> a &lt;b&gt; tag &amp; <i>this</i>");
  assert.equal(toCardHtml("snake_case_name stays"), "snake_case_name stays");
  assert.equal(stageColor("#fff"), "#9e9e9e");
  assert.equal(stageColor("#02BCD4"), "#02bcd4");
  assert.equal(stageColor("#f80"), "#ff8800");
  assert.equal(stageColor("red"), "#9e9e9e");
});

test("stageInfo reads each linked card's Stage and color from ClickUp", async () => {
  const s = await stageInfo(env, "<https://app.clickup.com/t/peds-quiz|Peds> and <https://app.clickup.com/t/nope|Gone>");
  assert.deepEqual(s.get("peds-quiz"), { stage: "Ready To Edit / Design", color: "#ffc53d" });
  assert.equal(s.has("nope"), false, "a missing card is skipped, not an error");
});

test("if Chat refuses the card, the summary still goes out as text", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (u, o = {}) => {
    if (String(u).startsWith("https://chat.googleapis.com/") && String(o.body).includes("cardsV2")) {
      return new Response('{"error":{"message":"Invalid card"}}', { status: 400 });
    }
    return realFetch(u, o);
  };
  claudeScript = [{ reply: say("Nothing is stuck today.") }];
  await floCron(env, nyTime(`${todayYmd()}T09:10:00`));
  const posts = postsTo(dmOf(AVERY.email));
  assert.equal(posts.length, 1);
  assert.match(posts[0].body.text, /^\*Daily check, .+\*\nNothing is stuck today/);
  assert.ok(logRows().some((r) => r.event === "card_post_failed"));
});

test("Send me a preview re-sends the latest summary as a card to the asker only", async () => {
  await send(addonMessage("warm up")).catch(() => {});
  await sayHi(AVERY.email, "Avery Pless");
  claudeScript = [{ reply: say("Left alone: <https://app.clickup.com/t/peds-quiz|Peds quiz>.") }];
  await floCron(env, nyTime(`${todayYmd()}T09:10:00`));
  const before = claudeRequests.length;
  const MARK = "mark@anurseinthemaking.com";
  assert.equal((await floApi("preview", { method: "POST", body: {}, as: MARK })).status, 400, "Mark hasn't messaged Flo yet");
  await sayHi("mark@anurseinthemaking.com", "Mark Podlas");
  const r = await floApi("preview", { method: "POST", body: {}, as: MARK });
  assert.equal(r.status, 200);
  assert.equal(postsTo(dmOf(AVERY.email)).length, 1, "Avery isn't sent anything again");
  assert.equal(claudeRequests.length, before, "nothing re-ran");
  const card = postsTo(dmOf("mark@anurseinthemaking.com")).at(-1).body.cardsV2[0].card;
  assert.match(card.header.subtitle, / · preview$/);
  assert.equal(card.sections[0].widgets[0].decoratedText.topLabel, "Ready To Edit / Design");
  assert.match(card.sections[0].widgets[0].decoratedText.text, /^<font color="#ffc53d">■<\/font> /);
  assert.equal(writes().length, 0);
});
