// TikTok router against a mocked ClickUp API and a fake D1. Real user/field
// ids (they are config, not secrets); invented tasks.
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { handleTikTokRouterRoutes, WEBHOOK_PATH } from "../src/tiktok-router/index.js";
import { PEOPLE, FIELDS, LIST_ID, DEFAULT_RULES, normalizeRules } from "../src/tiktok-router/routing.js";
const ROADBLOCK = DEFAULT_RULES.roadblock;

const SECRET = "test-secret";
const FORMAT_OPTIONS = [
  { id: "opt-quiz", name: "Quiz", orderindex: 4 },
  { id: "opt-funny", name: "Funny", orderindex: 5 },
  { id: "opt-trend", name: "Trend", orderindex: 6 },
];

let calls, logRows, currentTask, rulesRow, ruleChanges;

beforeEach(() => {
  calls = [];
  logRows = [];
  currentTask = null;
  rulesRow = null; // no saved rules -> DEFAULT_RULES
  ruleChanges = [];
  globalThis.fetch = async (url, opts = {}) => {
    const method = opts.method || "GET";
    calls.push({ url: String(url), method, body: opts.body ? JSON.parse(opts.body) : null });
    if (method === "GET") return new Response(JSON.stringify(currentTask), { status: 200 });
    return new Response("{}", { status: 200 });
  };
});

// Fake D1: just enough SQL-sniffing for the tables this module touches.
function stmt(sql, args = []) {
  return {
    bind: (...a) => stmt(sql, a),
    run: async () => {
      if (sql.startsWith("INSERT INTO tiktok_routing_log")) {
        const [at, task_id, task_name, event, status, action] = args;
        logRows.push({ at, task_id, task_name, event, status, action });
      } else if (sql.startsWith("INSERT INTO tiktok_router_rules")) {
        const [rules, updated_at, updated_by] = args;
        rulesRow = { rules, updated_at, updated_by };
      } else if (sql.startsWith("INSERT INTO tiktok_router_rule_changes")) {
        ruleChanges.push(args);
      } else if (sql.startsWith("DELETE FROM tiktok_router_rules")) {
        rulesRow = null;
      }
    },
    first: async () => (sql.includes("FROM tiktok_router_rules") ? rulesRow : null),
    all: async () => ({ results: sql.includes("FROM tiktok_routing_log") ? logRows.slice().reverse() : [] }),
  };
}
const db = { prepare: (sql) => stmt(sql) };
const saveRules = (r) => { rulesRow = { rules: JSON.stringify(normalizeRules(r)), updated_at: "t", updated_by: "x" }; };
const withRules = (mutate) => { const r = structuredClone(DEFAULT_RULES); mutate(r); saveRules(r); };
const env = { CLICKUP_TOKEN: "pk_test", TIKTOK_ROUTER_WEBHOOK_SECRET: SECRET, DB: db };

function card(status, { writer = null, format, assignees = [], tags = [], list = LIST_ID } = {}) {
  return {
    id: "t1",
    list: { id: list },
    status: { status },
    assignees: assignees.map((id) => ({ id })),
    tags: tags.map((name) => ({ name })),
    custom_fields: [
      { id: FIELDS.writer, type: "users", value: writer ? [{ id: writer }] : undefined },
      { id: FIELDS.format, type: "drop_down", type_config: { options: FORMAT_OPTIONS }, value: format },
    ],
  };
}

async function deliver(task, { event = "taskStatusUpdated", history = [], signature } = {}) {
  currentTask = task;
  const body = JSON.stringify({ event, task_id: "t1", webhook_id: "w1", history_items: history });
  const sig = signature ?? crypto.createHmac("sha256", SECRET).update(body).digest("hex");
  const waits = [];
  const res = await handleTikTokRouterRoutes(
    new Request(`https://ops.anurseinthemaking.com${WEBHOOK_PATH}`, {
      method: "POST",
      body,
      headers: { "X-Signature": sig },
    }),
    env,
    { waitUntil: (p) => waits.push(p) },
    WEBHOOK_PATH
  );
  await Promise.all(waits);
  return res;
}

const puts = () => calls.filter((c) => c.method === "PUT");
const comments = () => calls.filter((c) => c.method === "POST");
const assignedTo = () => puts()[0]?.body?.assignees;

// ---- each stage's owner -------------------------------------------------

const FIXED = [
  ["package", PEOPLE.tia],
  ["voiceover", PEOPLE.kristine],
  ["schedule", PEOPLE.cass],
];
for (const [stage, owner] of FIXED) {
  test(`${stage} -> its fixed owner, previous assignee removed`, async () => {
    const res = await deliver(card(stage, { assignees: [PEOPLE.gaby] }));
    assert.equal(res.status, 200);
    assert.deepEqual(assignedTo(), { add: [owner], rem: [PEOPLE.gaby] });
    assert.match(logRows.at(-1).action, new RegExp(`assigned ${owner}`));
    assert.equal(logRows.at(-1).status, stage);
  });
}

for (const stage of ["script review", "accuracy check", "final review"]) {
  test(`${stage}: Diana wrote it -> Karli`, async () => {
    await deliver(card(stage, { writer: PEOPLE.diana, assignees: [PEOPLE.tia] }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.karli], rem: [PEOPLE.tia] });
  });
  test(`${stage}: someone else wrote it -> Diana`, async () => {
    await deliver(card(stage, { writer: PEOPLE.karli, assignees: [PEOPLE.tia] }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.diana], rem: [PEOPLE.tia] });
  });
  test(`${stage}: no writer -> Diana`, async () => {
    await deliver(card(stage, {}));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.diana], rem: [] });
  });
}

for (const stage of ["edit", "titles"]) {
  test(`${stage}: Funny as orderindex (number) -> Tia`, async () => {
    await deliver(card(stage, { format: 5, assignees: [PEOPLE.kristine] }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.tia], rem: [PEOPLE.kristine] });
  });
  test(`${stage}: Trend as orderindex string -> Tia`, async () => {
    await deliver(card(stage, { format: "6" }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.tia], rem: [] });
  });
  test(`${stage}: Funny as option id -> Tia`, async () => {
    await deliver(card(stage, { format: "opt-funny" }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.tia], rem: [] });
  });
  test(`${stage}: Quiz as option id -> Gaby`, async () => {
    await deliver(card(stage, { format: "opt-quiz" }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.gaby], rem: [] });
  });
  test(`${stage}: Quiz as orderindex 4 -> Gaby`, async () => {
    await deliver(card(stage, { format: 4 }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.gaby], rem: [] });
  });
  test(`${stage}: Format empty -> Gaby`, async () => {
    await deliver(card(stage, { format: undefined }));
    assert.deepEqual(assignedTo(), { add: [PEOPLE.gaby], rem: [] });
  });
}

test("edit: orderindex 0 is a real value, not empty", async () => {
  // orderindex 0 is falsy; it must still resolve to an option (here: none -> Gaby).
  await deliver(card("edit", { format: 0 }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.gaby], rem: [] });
});

// ---- skips -----------------------------------------------------------------

test("already assigned correctly -> no PUT, logged", async () => {
  await deliver(card("schedule", { assignees: [PEOPLE.cass] }));
  assert.equal(puts().length, 0);
  assert.match(logRows.at(-1).action, /already assigned/);
});

test("correct owner plus an extra -> only removes the extra", async () => {
  await deliver(card("schedule", { assignees: [PEOPLE.cass, PEOPLE.tia] }));
  assert.deepEqual(assignedTo(), { add: [], rem: [PEOPLE.tia] });
});

test("task from another list -> ignored, no writes", async () => {
  await deliver(card("package", { list: "999" }));
  assert.equal(puts().length, 0);
  assert.match(logRows.at(-1).action, /ignored: task is in list 999/);
});

test("prep with a null owner (Amber) -> skipped, assignees untouched", async () => {
  assert.equal(PEOPLE.amber, null);
  await deliver(card("prep", { assignees: [PEOPLE.karli] }));
  assert.equal(puts().length, 0);
  assert.match(logRows.at(-1).action, /skipped: no owner/);
});

for (const stage of ["draft", "film", "posted", "cancelled"]) {
  test(`${stage} -> left alone`, async () => {
    await deliver(card(stage, { assignees: [PEOPLE.karli] }));
    assert.equal(puts().length, 0);
    assert.match(logRows.at(-1).action, /left alone/);
  });
}

test("status name is matched case-insensitively", async () => {
  await deliver(card("Script Review", { writer: PEOPLE.diana }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.karli], rem: [] });
});

test("unsubscribed event (assignee change) -> ignored, no ClickUp calls", async () => {
  await deliver(card("package"), { event: "taskAssigneeUpdated" });
  assert.equal(calls.length, 0);
  assert.match(logRows.at(-1).action, /event not handled/);
});

// ---- roadblock -------------------------------------------------------------

test("roadblock newly added -> priority High + comment assigned to Avery", async () => {
  await deliver(card("edit", { tags: ["roadblock"] }), {
    event: "taskTagUpdated",
    history: [{ field: "tag", before: [], after: [{ name: "roadblock" }] }],
  });
  assert.deepEqual(puts().map((c) => c.body), [{ priority: 2 }]);
  assert.equal(comments().length, 1);
  assert.match(comments()[0].url, /\/task\/t1\/comment$/);
  assert.deepEqual(comments()[0].body, {
    comment_text: "Roadblock added. Avery, can you help get this unstuck?",
    assignee: PEOPLE.avery,
    notify_all: false,
  });
  assert.match(logRows.at(-1).action, /roadblock: priority raised/);
});

test("roadblock already present (another tag added) -> nothing", async () => {
  await deliver(card("edit", { tags: ["roadblock", "x"] }), {
    event: "taskTagUpdated",
    history: [{ field: "tag", before: [{ name: "roadblock" }], after: [{ name: "roadblock" }, { name: "x" }] }],
  });
  assert.equal(puts().length + comments().length, 0);
  assert.match(logRows.at(-1).action, /not newly added/);
});

test("roadblock added but gone again by the time we look -> nothing", async () => {
  await deliver(card("edit", { tags: [] }), {
    event: "taskTagUpdated",
    history: [{ field: "tag", before: [], after: [{ name: "roadblock" }] }],
  });
  assert.equal(puts().length + comments().length, 0);
});

// ---- signature + errors ------------------------------------------------------

test("bad signature -> 401, no ClickUp calls, no log", async () => {
  const res = await deliver(card("package"), { signature: "nope" });
  assert.equal(res.status, 401);
  assert.equal(calls.length, 0);
  assert.equal(logRows.length, 0);
});

test("missing signature -> 401", async () => {
  const res = await deliver(card("package"), { signature: "" });
  assert.equal(res.status, 401);
});

test("no secret configured -> fails closed with 401", async () => {
  const saved = env.TIKTOK_ROUTER_WEBHOOK_SECRET;
  env.TIKTOK_ROUTER_WEBHOOK_SECRET = undefined;
  try {
    const res = await deliver(card("package"));
    assert.equal(res.status, 401);
  } finally {
    env.TIKTOK_ROUTER_WEBHOOK_SECRET = saved;
  }
});

test("ClickUp error is logged, response is still 200", async () => {
  globalThis.fetch = async () => new Response("boom", { status: 500 });
  const res = await deliver(card("package"));
  assert.equal(res.status, 200);
  assert.match(logRows.at(-1).action, /^error: ClickUp GET .* 500/);
});

test("other paths fall through (null)", async () => {
  const res = await handleTikTokRouterRoutes(new Request("https://x/clickup-automation/webhook"), env, {}, "/clickup-automation/webhook");
  assert.equal(res, null);
});

// ---- rules saved from the page ----------------------------------------------

test("saved rules override the defaults: Voiceover -> Karli", async () => {
  withRules((r) => { r.stages.voiceover = { mode: "person", owner: PEOPLE.karli }; });
  await deliver(card("voiceover", { assignees: [PEOPLE.kristine] }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.karli], rem: [PEOPLE.kristine] });
});

test("a stage switched to Leave alone is not touched", async () => {
  withRules((r) => { r.stages.package = { mode: "off" }; });
  await deliver(card("package", { assignees: [PEOPLE.gaby] }));
  assert.equal(puts().length, 0);
});

test("a stage moved onto the review rule follows it", async () => {
  withRules((r) => { r.stages.film = { mode: "review" }; });
  await deliver(card("film", { writer: PEOPLE.diana }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.karli], rem: [] });
});

test("edit formats are editable: adding Quiz sends Quiz to Tia", async () => {
  withRules((r) => { r.edit.formats = ["Funny", "Trend", "Quiz"]; });
  await deliver(card("edit", { format: "opt-quiz" }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.tia], rem: [] });
});

test("review rule people are editable", async () => {
  withRules((r) => { r.review = { writer: PEOPLE.karli, then: PEOPLE.diana, otherwise: PEOPLE.karli }; });
  await deliver(card("final review", { writer: PEOPLE.tia }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.karli], rem: [] });
});

test("roadblock switched off -> no priority change, no comment", async () => {
  withRules((r) => { r.roadblock.enabled = false; });
  await deliver(card("edit", { tags: ["roadblock"] }), {
    event: "taskTagUpdated",
    history: [{ field: "tag", before: [], after: [{ name: "roadblock" }] }],
  });
  assert.equal(puts().length + comments().length, 0);
});

test("roadblock notify person + comment are editable", async () => {
  withRules((r) => { r.roadblock.notify = PEOPLE.cass; r.roadblock.notify_name = "Cassandra Van Nostrand"; r.roadblock.comment = "Help, {person}!"; });
  await deliver(card("edit", { tags: ["roadblock"] }), {
    event: "taskTagUpdated",
    history: [{ field: "tag", before: [], after: [{ name: "roadblock" }] }],
  });
  assert.deepEqual(comments()[0].body, { comment_text: "Help, Cassandra!", assignee: PEOPLE.cass, notify_all: false });
});

test("normalizeRules rejects an unknown mode and a non-user owner", () => {
  assert.throws(() => normalizeRules({ stages: { edit: { mode: "random" } } }), /Unknown setting/);
  assert.throws(() => normalizeRules({ stages: { edit: { mode: "person", owner: "tia" } } }), /must be a ClickUp user/);
  assert.equal(normalizeRules(DEFAULT_RULES).stages.prep.owner, null);
});

// ---- page API ------------------------------------------------------------------

async function apiCall(sub, { method = "GET", body, host = "ops.anurseinthemaking.com", type = "application/json" } = {}) {
  const path = `/tiktok-router/api/${sub}`;
  const res = await handleTikTokRouterRoutes(
    new Request(`https://${host}${path}`, {
      method,
      body: body === undefined ? undefined : JSON.stringify(body),
      headers: { "Content-Type": type, "Cf-Access-Authenticated-User-Email": "mark@anurseinthemaking.com" },
    }),
    env,
    { waitUntil() {} },
    path
  );
  return { status: res.status, data: await res.json() };
}

test("API GET rules returns defaults before anyone saves", async () => {
  const { status, data } = await apiCall("rules");
  assert.equal(status, 200);
  assert.equal(data.is_default, true);
  assert.deepEqual(data.rules, DEFAULT_RULES);
});

test("API PUT rules saves, records who, and routing uses it immediately", async () => {
  const r = structuredClone(DEFAULT_RULES);
  r.stages.schedule = { mode: "person", owner: PEOPLE.tia };
  const { status, data } = await apiCall("rules", { method: "PUT", body: { rules: r } });
  assert.equal(status, 200);
  assert.equal(data.updated_by, "mark@anurseinthemaking.com");
  assert.equal(ruleChanges.length, 1);
  await deliver(card("schedule", { assignees: [PEOPLE.cass] }));
  assert.deepEqual(assignedTo(), { add: [PEOPLE.tia], rem: [PEOPLE.cass] });
});

test("API PUT with invalid rules -> 400, nothing saved", async () => {
  const { status } = await apiCall("rules", { method: "PUT", body: { rules: { stages: { edit: { mode: "nope" } } } } });
  assert.equal(status, 400);
  assert.equal(rulesRow, null);
});

test("API reset puts the defaults back", async () => {
  withRules((r) => { r.stages.package = { mode: "off" }; });
  const { data } = await apiCall("rules/reset", { method: "POST", body: {} });
  assert.equal(data.is_default, true);
  assert.equal(rulesRow, null);
});

test("API refuses the workers.dev hostname (no Access gate there)", async () => {
  const { status } = await apiCall("rules", { host: "ops-hub.benjamin-976.workers.dev" });
  assert.equal(status, 403);
});

test("API refuses a non-JSON write (cross-site form)", async () => {
  const { status } = await apiCall("rules", { method: "PUT", body: { rules: DEFAULT_RULES }, type: "text/plain" });
  assert.equal(status, 415);
  assert.equal(rulesRow, null);
});

test("the log records the card's name", async () => {
  await deliver({ ...card("package"), name: "Insulin myths" });
  assert.equal(logRows.at(-1).task_name, "Insulin myths");
});

test("API preview: who gets a Funny card Diana wrote, under unsaved rules", async () => {
  const r = structuredClone(DEFAULT_RULES);
  r.stages.voiceover = { mode: "person", owner: PEOPLE.karli };
  const { status, data } = await apiCall("preview", {
    method: "POST",
    body: { rules: r, card: { writerId: PEOPLE.diana, format: "Funny" } },
  });
  assert.equal(status, 200);
  const by = Object.fromEntries(data.preview.map((p) => [p.stage, p.owner ?? null]));
  assert.equal(by["script review"], PEOPLE.karli);
  assert.equal(by["voiceover"], PEOPLE.karli);
  assert.equal(by["edit"], PEOPLE.tia);
  assert.equal(by["film"], null);
  assert.equal(rulesRow, null, "preview must not save");
});
