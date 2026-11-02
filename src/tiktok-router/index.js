// ============================================================================
// TIKTOK ROUTER  —  ops-hub feature module
// ----------------------------------------------------------------------------
// When a card in Organic Social > TikTok Production changes status, assign that
// stage's owner and remove everyone else. Adding the `roadblock` tag raises
// priority and pings Avery. Replaces the list's ClickUp automations.
//
// RULES are edited at /tiktok-router/ (stored in D1, tiktok_router_rules).
// ./routing.js holds the defaults and the pure decision logic.
//
// WIRING (src/index.js):
//   import { handleTikTokRouterRoutes } from "./tiktok-router/index.js";
//   const ttResp = await handleTikTokRouterRoutes(request, env, ctx, path);
//   if (ttResp) return ttResp;          // above the ClickUp Automation mount
//
// PAGE + API: /tiktok-router/ and /tiktok-router/api/* — behind the normal
//   Ops Hub Cloudflare Access login, refused on any other hostname.
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
// DB:      env.DB (content-calendar): tiktok_routing_log (0007),
//          tiktok_router_rules + tiktok_router_rule_changes (0008).
//
// ADDING A HANDLER (AI PM checks, creative-engine sync): write a function
// `(ctx) => Promise<void>` and add it to HANDLERS for the event(s) it needs.
// It gets the freshly fetched task, the current rules and a `log()`.
// ============================================================================

import { DEFAULT_RULES, FIELDS, LIST_ID, STAGES, WORKSPACE_ID, ownerFor, normalizeRules } from "./routing.js";
import { isTrustedHost, verifyAccessJwt } from "../access.js";

const CLICKUP_API = "https://api.clickup.com/api/v2";
export const WEBHOOK_PATH = "/clickup-automation/webhook/tiktok";

// ---------------------------------------------------------------------------
// Schema — mirrors migrations/0007 + 0008
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
  await env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS tiktok_router_rules (
       id         INTEGER PRIMARY KEY CHECK (id = 1),
       rules      TEXT NOT NULL,
       updated_at TEXT NOT NULL,
       updated_by TEXT
     )`
  ).run();
  await env.DB.prepare(
    `CREATE TABLE IF NOT EXISTS tiktok_router_rule_changes (
       id    INTEGER PRIMARY KEY AUTOINCREMENT,
       at    TEXT NOT NULL,
       by    TEXT,
       rules TEXT NOT NULL
     )`
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

// The saved rules, or the defaults if nobody has saved from the page yet.
// Read per event (one indexed row) so a save takes effect on the next card move.
export async function loadRules(env) {
  const row = env.DB
    ? await env.DB.prepare("SELECT rules, updated_at, updated_by FROM tiktok_router_rules WHERE id = 1").first()
    : null;
  if (!row) return { rules: DEFAULT_RULES, isDefault: true, updatedAt: null, updatedBy: null };
  return { rules: JSON.parse(row.rules), isDefault: false, updatedAt: row.updated_at, updatedBy: row.updated_by };
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

async function routeStage({ env, task, stage, rules, log }) {
  const decision = ownerFor(rules, stage, readCard(task));
  if (decision.action === "skip") return log(decision.reason);
  const target = decision.owner;

  const current = (task.assignees || []).map((a) => Number(a.id));
  const rem = current.filter((id) => id !== target);
  const add = current.includes(target) ? [] : [target];
  if (!add.length && !rem.length) return log(`already assigned to ${target}`);

  await clickup(env, "PUT", `/task/${encodeURIComponent(task.id)}`, { assignees: { add, rem } });
  return log(`assigned ${target}; removed ${rem.join(",") || "none"}`);
}

async function roadblock({ env, event, task, rules, log }) {
  const rb = rules.roadblock || {};
  if (rb.enabled === false) return log("tag change: roadblock handling is switched off");
  const tag = String(rb.tag || "roadblock").toLowerCase();
  const added = (event.history_items || []).some(
    (h) =>
      (h.field === "tag" || h.field === "tags") &&
      tagNames(h.after).includes(tag) &&
      !tagNames(h.before).includes(tag)
  );
  if (!added) return log("tag change: roadblock not newly added");
  // The fresh task is the truth: if the tag was already removed again, do nothing.
  if (!tagNames(task.tags).includes(tag)) return log("roadblock added then removed; nothing done");

  await clickup(env, "PUT", `/task/${encodeURIComponent(task.id)}`, { priority: rb.priority || 2 });
  if (rb.notify == null) return log("roadblock: priority raised; no one set to notify");
  await clickup(env, "POST", `/task/${encodeURIComponent(task.id)}/comment`, {
    comment_text: rb.comment,
    assignee: rb.notify,
    notify_all: false,
  });
  return log(`roadblock: priority raised, comment assigned to ${rb.notify}`);
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
    const { rules } = await loadRules(env);

    for (const h of handlers) await h({ env, event, task, stage, rules, log });
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
  if (path.startsWith("/tiktok-router/api/")) return handleApi(request, env, path.slice("/tiktok-router/api/".length));
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

// ---------------------------------------------------------------------------
// Admin API for the /tiktok-router/ page. Same-origin JSON only.
// ---------------------------------------------------------------------------
const NO_STORE = { "Content-Type": "application/json", "Cache-Control": "no-store" };
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: NO_STORE });
const actor = (request) => request.headers.get("Cf-Access-Authenticated-User-Email") || null;

let _clickupCache = null; // { at, data } — members/statuses/formats change rarely
const CLICKUP_CACHE_MS = 10 * 60 * 1000;

async function clickupContext(env, refresh) {
  if (!refresh && _clickupCache && Date.now() - _clickupCache.at < CLICKUP_CACHE_MS) return _clickupCache.data;
  const [teams, list, fields] = await Promise.all([
    clickup(env, "GET", "/team"),
    clickup(env, "GET", `/list/${LIST_ID}`),
    clickup(env, "GET", `/list/${LIST_ID}/field`),
  ]);
  const team = (teams.teams || []).find((t) => String(t.id) === WORKSPACE_ID) || { members: [] };
  const members = (team.members || [])
    .map((m) => m.user)
    .filter((u) => u && u.id)
    .map((u) => ({ id: Number(u.id), name: u.username || u.email, email: u.email || null }))
    .sort((a, b) => String(a.name).localeCompare(String(b.name)));
  const statuses = (list.statuses || [])
    .slice()
    .sort((a, b) => a.orderindex - b.orderindex)
    .map((st) => String(st.status).toLowerCase());
  const formatField = (fields.fields || []).find((f) => f.id === FIELDS.format);
  const formats = (formatField?.type_config?.options || []).map((o) => o.name);
  const data = { members, statuses, formats };
  _clickupCache = { at: Date.now(), data };
  return data;
}

async function handleApi(request, env, sub) {
  const url = new URL(request.url);
  if (!isTrustedHost(env, url)) {
    return json({ error: "Only served on the Ops Hub login-protected hostname." }, 403);
  }
  if (env.ACCESS_TEAM_DOMAIN && env.ACCESS_AUD) {
    const v = await verifyAccessJwt(env, request.headers.get("Cf-Access-Jwt-Assertion"), env.ACCESS_AUD);
    if (!v.ok) return json({ error: `Cloudflare Access check failed: ${v.reason}` }, 401);
  }
  if (!env.DB) return json({ error: "DB not configured" }, 503);
  const method = request.method;
  // Writes must be JSON: a cross-site form cannot send that without a preflight we never answer.
  if (method !== "GET" && !(request.headers.get("Content-Type") || "").includes("application/json")) {
    return json({ error: "Send JSON." }, 415);
  }

  try {
    await ensureTable(env);

    if (sub === "rules" && method === "GET") {
      const r = await loadRules(env);
      return json({ rules: r.rules, is_default: r.isDefault, updated_at: r.updatedAt, updated_by: r.updatedBy, defaults: DEFAULT_RULES, stages: STAGES });
    }

    if (sub === "rules" && method === "PUT") {
      const body = await request.json().catch(() => null);
      let rules;
      try {
        rules = normalizeRules(body?.rules);
      } catch (e) {
        return json({ error: e.message }, 400);
      }
      const at = new Date().toISOString();
      const text = JSON.stringify(rules);
      await env.DB.prepare(
        `INSERT INTO tiktok_router_rules (id, rules, updated_at, updated_by) VALUES (1, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET rules = excluded.rules, updated_at = excluded.updated_at, updated_by = excluded.updated_by`
      ).bind(text, at, actor(request)).run();
      await env.DB.prepare("INSERT INTO tiktok_router_rule_changes (at, by, rules) VALUES (?, ?, ?)")
        .bind(at, actor(request), text).run();
      return json({ rules, is_default: false, updated_at: at, updated_by: actor(request) });
    }

    if (sub === "rules/reset" && method === "POST") {
      const at = new Date().toISOString();
      await env.DB.prepare("DELETE FROM tiktok_router_rules WHERE id = 1").run();
      await env.DB.prepare("INSERT INTO tiktok_router_rule_changes (at, by, rules) VALUES (?, ?, ?)")
        .bind(at, actor(request), JSON.stringify({ reset_to_defaults: true })).run();
      return json({ rules: DEFAULT_RULES, is_default: true, updated_at: null, updated_by: null });
    }

    if (sub === "clickup" && method === "GET") {
      return json(await clickupContext(env, url.searchParams.get("refresh") === "1"));
    }

    if (sub === "log" && method === "GET") {
      const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit")) || 50));
      const { results } = await env.DB.prepare(
        "SELECT id, at, task_id, event, status, action FROM tiktok_routing_log ORDER BY id DESC LIMIT ?"
      ).bind(limit).all();
      return json({ log: results || [] });
    }

    if (sub === "history" && method === "GET") {
      const { results } = await env.DB.prepare(
        "SELECT id, at, by FROM tiktok_router_rule_changes ORDER BY id DESC LIMIT 20"
      ).all();
      return json({ history: results || [] });
    }

    return json({ error: `Unknown endpoint: ${sub}` }, 404);
  } catch (e) {
    return json({ error: String(e?.message || e) }, 500);
  }
}
