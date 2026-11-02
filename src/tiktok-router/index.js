// ============================================================================
// TIKTOK ROUTER  —  ops-hub feature module
// ----------------------------------------------------------------------------
// When a card in Organic Social > TikTok Production changes status, assign that
// stage's owner and remove everyone else. Adding the `roadblock` tag raises
// priority and pings Avery. Replaces the list's ClickUp automations.
//
// RULES live in ./routing.js — that is the file to edit. This file is plumbing.
//
// WIRING (src/index.js):
//   import { handleTikTokRouterRoutes } from "./tiktok-router/index.js";
//   const ttResp = await handleTikTokRouterRoutes(request, env, ctx, path);
//   if (ttResp) return ttResp;          // above the ClickUp Automation mount
//
// ENDPOINT: POST /clickup-automation/webhook/tiktok
//   Deliberately UNDER the existing Cloudflare Access bypass for
//   /clickup-automation/webhook (Access path rules cover subpaths), so no new
//   Access application is needed. ClickUp cannot log in; the HMAC signature is
//   the only guard, and it fails closed.
//
// SECRETS: CLICKUP_TOKEN (shared with the other ClickUp modules),
//          TIKTOK_ROUTER_WEBHOOK_SECRET (printed + saved by
//          tools/tiktok-router/register-webhook.mjs). NOT named
//          CLICKUP_WEBHOOK_SECRET: src/clickup-automation.js already accepts
//          that binding for its own webhook, so reusing the name would let
//          either webhook's signature open the other's endpoint.
// DB:      env.DB (content-calendar), table tiktok_routing_log —
//          mirrors migrations/0007_tiktok_routing_log.sql.
//
// ADDING A HANDLER (AI PM checks, creative-engine sync): write a function
// `(ctx) => Promise<void>` and add it to HANDLERS for the event(s) it needs.
// It gets the freshly fetched task and a `log()` that writes the run log.
// ============================================================================

import { ROUTES, ROADBLOCK, FIELDS, LIST_ID } from "./routing.js";

const CLICKUP_API = "https://api.clickup.com/api/v2";
export const WEBHOOK_PATH = "/clickup-automation/webhook/tiktok";

// ---------------------------------------------------------------------------
// Schema — mirrors migrations/0007_tiktok_routing_log.sql
// ---------------------------------------------------------------------------
let _tableReady = false;
async function ensureTable(env) {
  if (_tableReady || !env.DB) return;
  await env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS tiktok_routing_log (
       id      INTEGER PRIMARY KEY AUTOINCREMENT,
       at      TEXT NOT NULL,
       task_id TEXT NOT NULL,
       event   TEXT NOT NULL,
       status  TEXT,
       action  TEXT NOT NULL
     )`
  ).run();
  await env.DB.prepare(
    "CREATE INDEX IF NOT EXISTS idx_tiktok_routing_log_task ON tiktok_routing_log (task_id, id DESC)"
  ).run();
  _tableReady = true;
}

async function writeLog(env, { taskId, event, status = null, action }) {
  if (!env.DB) return;
  try {
    await env.DB.prepare(
      "INSERT INTO tiktok_routing_log (at, task_id, event, status, action) VALUES (?, ?, ?, ?, ?)"
    )
      .bind(new Date().toISOString(), String(taskId || "?"), String(event || "?"), status, String(action).slice(0, 1000))
      .run();
  } catch (e) {
    // The log must never be the thing that breaks routing.
    console.error("tiktok-router log write failed:", e);
  }
}

// ---------------------------------------------------------------------------
// Signature — HMAC-SHA256 hex of the RAW body. Constant-time compare.
// ---------------------------------------------------------------------------
function timingSafeEqual(a, b) {
  const x = String(a ?? "");
  const y = String(b ?? "");
  if (x.length !== y.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x.charCodeAt(i) ^ y.charCodeAt(i);
  return diff === 0;
}

async function hmacHex(secret, body) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const mac = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
  return [...new Uint8Array(mac)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export async function verifySignature(secret, rawBody, signature) {
  if (!secret || !signature) return false;
  return timingSafeEqual(await hmacHex(secret, rawBody), signature.toLowerCase());
}

// ---------------------------------------------------------------------------
// ClickUp client
// ---------------------------------------------------------------------------
async function clickup(env, method, path, body) {
  if (!env.CLICKUP_TOKEN) throw new Error("CLICKUP_TOKEN is not set");
  const res = await fetch(`${CLICKUP_API}${path}`, {
    method,
    headers: { Authorization: env.CLICKUP_TOKEN, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`ClickUp ${method} ${path} -> ${res.status}: ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : {};
}

// ---------------------------------------------------------------------------
// Reading the card
// ---------------------------------------------------------------------------

// Format arrives as the option's orderindex (number or numeric string) OR its
// id, depending on the dropdown's age. Resolve both to the option name.
export function readCard(task) {
  const fields = task.custom_fields || [];
  const writer = fields.find((f) => f.id === FIELDS.writer);
  const format = fields.find((f) => f.id === FIELDS.format);

  const w = Array.isArray(writer?.value) ? writer.value[0] : writer?.value;
  const writerId = w && w.id != null ? Number(w.id) : null;

  let formatName = null;
  const v = format?.value;
  if (v !== undefined && v !== null && v !== "") {
    const opts = format.type_config?.options || [];
    const opt =
      opts.find((o) => o.id === v) ||
      opts.find((o) => o.orderindex != null && String(o.orderindex) === String(v));
    formatName = opt?.name || null;
  }
  return { writerId, format: formatName };
}

function tagNames(v) {
  if (!Array.isArray(v)) return [];
  return v.map((t) => String(t?.name ?? t).toLowerCase());
}

// ---------------------------------------------------------------------------
// Handlers. Each gets { env, event, task, stage, log }.
// ---------------------------------------------------------------------------

async function routeStage({ env, task, stage, log }) {
  if (!Object.prototype.hasOwnProperty.call(ROUTES, stage)) {
    return log(`left alone: no route for "${stage}"`);
  }
  const target = ROUTES[stage](readCard(task));
  if (target == null) return log(`skipped: no owner set for "${stage}" in routing.js`);

  const current = (task.assignees || []).map((a) => Number(a.id));
  const rem = current.filter((id) => id !== target);
  const add = current.includes(target) ? [] : [target];
  if (!add.length && !rem.length) return log(`already assigned to ${target}`);

  await clickup(env, "PUT", `/task/${encodeURIComponent(task.id)}`, { assignees: { add, rem } });
  return log(`assigned ${target}; removed ${rem.join(",") || "none"}`);
}

async function roadblock({ env, event, task, log }) {
  const added = (event.history_items || []).some(
    (h) =>
      (h.field === "tag" || h.field === "tags") &&
      tagNames(h.after).includes(ROADBLOCK.tag) &&
      !tagNames(h.before).includes(ROADBLOCK.tag)
  );
  if (!added) return log("tag change: roadblock not newly added");
  // The fresh task is the truth: if the tag was already removed again, do nothing.
  if (!tagNames(task.tags).includes(ROADBLOCK.tag)) return log("roadblock added then removed; nothing done");

  await clickup(env, "PUT", `/task/${encodeURIComponent(task.id)}`, { priority: ROADBLOCK.priority });
  if (ROADBLOCK.notify == null) return log("roadblock: priority high; no one set to notify");
  await clickup(env, "POST", `/task/${encodeURIComponent(task.id)}/comment`, {
    comment_text: ROADBLOCK.comment,
    assignee: ROADBLOCK.notify,
    notify_all: false,
  });
  return log(`roadblock: priority high, comment assigned to ${ROADBLOCK.notify}`);
}

// Event -> handlers, run in order. Add AI PM checks here.
const HANDLERS = {
  taskStatusUpdated: [routeStage],
  taskTagUpdated: [roadblock],
};

// ---------------------------------------------------------------------------
// Processing — runs inside ctx.waitUntil, after ClickUp has its 200.
// Every outcome, including errors, lands in the run log.
// ---------------------------------------------------------------------------
export async function processEvent(env, event) {
  const taskId = event.task_id;
  const name = event.event;
  let stage = null;
  const log = (action) => writeLog(env, { taskId, event: name, status: stage, action });

  try {
    await ensureTable(env);
    const handlers = HANDLERS[name];
    if (!handlers) return log("ignored: event not handled");
    if (!taskId) return log("ignored: no task_id");

    // Always act on fresh data, never on the webhook's snapshot.
    const task = await clickup(env, "GET", `/task/${encodeURIComponent(taskId)}?custom_fields=true`);
    if (String(task?.list?.id) !== LIST_ID) return log(`ignored: task is in list ${task?.list?.id}`);
    stage = String(task.status?.status || "").toLowerCase() || null;

    for (const h of handlers) await h({ env, event, task, stage, log });
  } catch (e) {
    await log(`error: ${e?.message || e}`);
  }
}

// ---------------------------------------------------------------------------
// Router. Returns a Response for our path, or null to fall through.
// Never 5xx: repeated 5xx makes ClickUp disable the webhook. 401 is the only
// non-200, for a bad signature.
// ---------------------------------------------------------------------------
const reply = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

export async function handleTikTokRouterRoutes(request, env, ctx, path) {
  if (path !== WEBHOOK_PATH) return null;
  if (request.method !== "POST") return reply({ error: "POST only" }, 405);

  const rawBody = await request.text();
  const ok = await verifySignature(env.TIKTOK_ROUTER_WEBHOOK_SECRET, rawBody, request.headers.get("X-Signature"));
  if (!ok) return reply({ error: "invalid signature" }, 401);

  let event;
  try {
    event = JSON.parse(rawBody);
  } catch {
    return reply({ ok: false, error: "invalid json" });
  }

  ctx.waitUntil(processEvent(env, event));
  return reply({ ok: true, queued: event.task_id || null });
}
