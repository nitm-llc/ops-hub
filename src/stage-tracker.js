// ============================================================================
// STAGE TRACKER  —  ops-hub feature module
// ----------------------------------------------------------------------------
// Tracks how long tasks spend in each "stage" (a ClickUp dropdown custom field)
// per list, by listening to ClickUp Automation webhooks.
//
// WHY THIS EXISTS:
//   ClickUp does not expose custom-field change history via its public API.
//   So instead of reading history, we CAPTURE it going forward: every time a
//   tracked task's stage field changes, ClickUp fires a webhook to us, and we
//   diff it against the last-known stage to record a transition with a real
//   timestamp. Stage durations are computed from consecutive transitions.
//
// HOW IT WIRES INTO src/index.js:
//   1. import { handleStageRoutes } from "./stage-tracker.js";   (top of file)
//   2. Inside your fetch handler, AFTER `const path = url.pathname;` and your
//      other route blocks, add:
//          const stageResp = await handleStageRoutes(request, env, ctx, path);
//          if (stageResp) return stageResp;
//      (returns null when the path isn't a /stage/ route, so it falls through
//       to your existing routes harmlessly.)
//
// DB: uses your existing `env.DB` binding (content-calendar). Tables self-create.
// TOKEN: uses your existing `env.CLICKUP_TOKEN` secret for field lookups.
// ============================================================================

const CLICKUP_API = "https://api.clickup.com/api/v2";

// ----------------------------------------------------------------------------
// Schema — self-creating, same pattern as tpl_orders in your index.js
// ----------------------------------------------------------------------------
async function ensureStageTables(env) {
  // Config: which lists we track, and which dropdown field is the "stage".
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS stage_tracked_lists (
    list_id           TEXT PRIMARY KEY,
    list_name         TEXT,
    stage_field_id    TEXT NOT NULL,
    stage_field_name  TEXT,
    stage_options     TEXT,            -- JSON: { "<optionId>": {"name":..,"order":..} }
    active            INTEGER DEFAULT 1,
    created_at        TEXT DEFAULT (datetime('now'))
  )`).run();

  // Current known stage per task (the "before" we diff against).
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS stage_current (
    task_id      TEXT PRIMARY KEY,
    list_id      TEXT NOT NULL,
    stage_id     TEXT,
    stage_name   TEXT,
    since_ts     INTEGER,             -- when the task entered this stage (ms)
    task_name    TEXT,
    updated_at   TEXT DEFAULT (datetime('now'))
  )`).run();

  // Closed-out stage durations — one row per completed time-in-stage.
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS stage_transitions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      TEXT NOT NULL,
    list_id      TEXT NOT NULL,
    task_name    TEXT,
    stage_id     TEXT,
    stage_name   TEXT,
    entered_at   INTEGER,             -- ms
    exited_at    INTEGER,             -- ms
    duration_ms  INTEGER,
    next_stage   TEXT,                -- where it went next (name)
    created_at   TEXT DEFAULT (datetime('now'))
  )`).run();

  await env.DB.prepare(
    `CREATE INDEX IF NOT EXISTS idx_trans_list ON stage_transitions (list_id, stage_name)`
  ).run();
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// Pull the stage field's value out of a ClickUp Automation webhook payload.
// Automation payloads put custom fields under payload.fields[] as {field_id, value}.
function extractStageValue(payload, stageFieldId) {
  const fields = payload?.fields || payload?.custom_fields || [];
  for (const f of fields) {
    if (f.field_id === stageFieldId || f.id === stageFieldId) {
      return f.value; // dropdown -> option UUID (or null if cleared)
    }
  }
  return undefined; // field not present in this payload
}

// Best-effort timestamp from the payload, else now.
function payloadTimestamp(payload, body) {
  const t =
    body?.date /* webhook.site wrapper */ ||
    payload?.time_mgmt?.date_updated ||
    payload?.date_updated;
  if (typeof t === "string" && /^\d+$/.test(t)) return parseInt(t, 10);
  if (typeof t === "number") return t;
  if (typeof t === "string") {
    const ms = Date.parse(t);
    if (!isNaN(ms)) return ms;
  }
  return Date.now();
}

// ----------------------------------------------------------------------------
// Webhook handler — the heart of it
// ----------------------------------------------------------------------------
async function handleStageWebhook(request, env, ctx) {
  await ensureStageTables(env);

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ ok: false, error: "invalid json" }, 400);
  }

  // ClickUp Automation webhooks nest the task under `payload`. Direct API
  // webhooks send the task at the top level. Support both.
  const payload = body.payload || body;
  const taskId = payload.id || payload.task_id || body.task_id;
  if (!taskId) return json({ ok: false, error: "no task id" }, 200);

  // Which list is this? (home list)
  const listId =
    payload.subcategory ||
    (Array.isArray(payload.lists)
      ? payload.lists.find((l) => l.type === "home")?.list_id
      : null) ||
    payload.list?.id;

  if (!listId) return json({ ok: false, error: "no list id" }, 200);

  // Is this list tracked?
  const cfg = await env.DB.prepare(
    "SELECT * FROM stage_tracked_lists WHERE list_id = ? AND active = 1"
  ).bind(String(listId)).first();

  if (!cfg) {
    // Not a tracked list — acknowledge so ClickUp doesn't retry.
    return json({ ok: true, ignored: "list not tracked", listId }, 200);
  }

  const newStageId = extractStageValue(payload, cfg.stage_field_id);
  if (newStageId === undefined) {
    // Stage field wasn't in this payload (some other field changed). Ignore.
    return json({ ok: true, ignored: "stage field not in payload" }, 200);
  }

  const options = cfg.stage_options ? JSON.parse(cfg.stage_options) : {};
  const newStageName = newStageId
    ? options[newStageId]?.name || newStageId
    : "(none)";
  const ts = payloadTimestamp(payload, body);
  const taskName = payload.name || null;

  // Look up the last-known stage for this task.
  const prev = await env.DB.prepare(
    "SELECT * FROM stage_current WHERE task_id = ?"
  ).bind(taskId).first();

  // No change? Nothing to do.
  if (prev && prev.stage_id === newStageId) {
    return json({ ok: true, noChange: true, stage: newStageName }, 200);
  }

  // If there was a previous stage, close it out as a completed transition.
  if (prev && prev.since_ts) {
    const duration = ts - prev.since_ts;
    await env.DB.prepare(
      `INSERT INTO stage_transitions
        (task_id, list_id, task_name, stage_id, stage_name, entered_at, exited_at, duration_ms, next_stage)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      taskId, String(listId), prev.task_name || taskName,
      prev.stage_id, prev.stage_name,
      prev.since_ts, ts, duration > 0 ? duration : 0,
      newStageName
    ).run();
  }

  // Record the new current stage.
  await env.DB.prepare(
    `INSERT OR REPLACE INTO stage_current
      (task_id, list_id, stage_id, stage_name, since_ts, task_name, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`
  ).bind(taskId, String(listId), newStageId, newStageName, ts, taskName).run();

  return json({
    ok: true,
    recorded: true,
    task: taskName,
    from: prev?.stage_name || null,
    to: newStageName,
  });
}

// ----------------------------------------------------------------------------
// ClickUp lookups for the admin UI (resolve list -> its dropdown fields by name)
// ----------------------------------------------------------------------------
async function cuFetch(env, path) {
  const r = await fetch(`${CLICKUP_API}${path}`, {
    headers: { Authorization: env.CLICKUP_TOKEN },
  });
  if (!r.ok) throw new Error(`ClickUp ${r.status}`);
  return r.json();
}

// Given a list_id, return its dropdown custom fields (id, name, options).
async function getListDropdownFields(env, listId) {
  const data = await cuFetch(env, `/list/${listId}/field`);
  const fields = (data.fields || [])
    .filter((f) => f.type === "drop_down")
    .map((f) => ({
      id: f.id,
      name: f.name,
      options: (f.type_config?.options || []).reduce((acc, o) => {
        acc[o.id] = { name: o.name, order: o.orderindex };
        return acc;
      }, {}),
    }));
  return fields;
}

// ----------------------------------------------------------------------------
// Reporting — per-stage durations per list
// ----------------------------------------------------------------------------
function fmtDuration(ms) {
  if (ms == null) return null;
  const days = ms / 86400000;
  return Math.round(days * 10) / 10; // days, 1 decimal
}

async function buildReport(env, listId) {
  await ensureStageTables(env);

  // Completed transitions -> avg/median/count per stage.
  const { results: rows } = await env.DB.prepare(
    `SELECT stage_name, duration_ms
       FROM stage_transitions
      WHERE list_id = ? AND duration_ms IS NOT NULL
      ORDER BY stage_name`
  ).bind(String(listId)).all();

  const byStage = {};
  for (const r of rows) {
    (byStage[r.stage_name] ||= []).push(r.duration_ms);
  }

  const stages = Object.entries(byStage).map(([name, arr]) => {
    arr.sort((a, b) => a - b);
    const sum = arr.reduce((s, v) => s + v, 0);
    const median = arr.length % 2
      ? arr[(arr.length - 1) / 2]
      : (arr[arr.length / 2 - 1] + arr[arr.length / 2]) / 2;
    return {
      stage: name,
      completed_count: arr.length,
      avg_days: fmtDuration(sum / arr.length),
      median_days: fmtDuration(median),
    };
  });

  // Currently in-flight tasks (time-so-far, not yet a completed transition).
  const { results: current } = await env.DB.prepare(
    `SELECT stage_name, task_name, since_ts FROM stage_current WHERE list_id = ?`
  ).bind(String(listId)).all();

  const now = Date.now();
  const inFlight = current.map((c) => ({
    task: c.task_name,
    stage: c.stage_name,
    days_in_stage: fmtDuration(now - c.since_ts),
  })).sort((a, b) => (b.days_in_stage || 0) - (a.days_in_stage || 0));

  return { stages, inFlight, transition_rows: rows.length };
}

// ----------------------------------------------------------------------------
// Route dispatcher — call this from src/index.js
// Returns a Response for /stage/* paths, or null otherwise (falls through).
// ----------------------------------------------------------------------------
export async function handleStageRoutes(request, env, ctx, path) {
  // --- Webhook (ClickUp Automation points here) ---
  if (path === "/stage/webhook/clickup" && request.method === "POST") {
    return handleStageWebhook(request, env, ctx);
  }

  // --- List tracked lists ---
  if (path === "/stage/api/lists" && request.method === "GET") {
    await ensureStageTables(env);
    const { results } = await env.DB.prepare(
      "SELECT list_id, list_name, stage_field_id, stage_field_name, active, created_at FROM stage_tracked_lists ORDER BY created_at DESC"
    ).all();
    return json({ ok: true, lists: results });
  }

  // --- Add / update a tracked list ---
  // Body: { list_id, list_name, stage_field_id, stage_field_name, stage_options }
  if (path === "/stage/api/lists" && request.method === "POST") {
    await ensureStageTables(env);
    let b;
    try { b = await request.json(); } catch { return json({ ok: false, error: "bad json" }, 400); }
    if (!b.list_id || !b.stage_field_id) {
      return json({ ok: false, error: "list_id and stage_field_id required" }, 400);
    }
    await env.DB.prepare(
      `INSERT OR REPLACE INTO stage_tracked_lists
        (list_id, list_name, stage_field_id, stage_field_name, stage_options, active)
       VALUES (?, ?, ?, ?, ?, 1)`
    ).bind(
      String(b.list_id), b.list_name || null, b.stage_field_id,
      b.stage_field_name || null,
      b.stage_options ? JSON.stringify(b.stage_options) : null
    ).run();
    return json({ ok: true, saved: b.list_id });
  }

  // --- Remove a tracked list ---
  const delMatch = path.match(/^\/stage\/api\/lists\/([^/]+)$/);
  if (delMatch && request.method === "DELETE") {
    await ensureStageTables(env);
    await env.DB.prepare("DELETE FROM stage_tracked_lists WHERE list_id = ?")
      .bind(delMatch[1]).run();
    return json({ ok: true, removed: delMatch[1] });
  }

  // --- Look up a list's dropdown fields by name (for the add-list UI) ---
  // GET /stage/api/clickup-fields?list_id=901708279415
  if (path === "/stage/api/clickup-fields" && request.method === "GET") {
    const url = new URL(request.url);
    const listId = url.searchParams.get("list_id");
    if (!listId) return json({ ok: false, error: "list_id required" }, 400);
    try {
      const fields = await getListDropdownFields(env, listId);
      // also try to get the list name
      let listName = null;
      try { listName = (await cuFetch(env, `/list/${listId}`)).name; } catch {}
      return json({ ok: true, list_id: listId, list_name: listName, fields });
    } catch (e) {
      return json({ ok: false, error: String(e.message || e) }, 502);
    }
  }

  // --- Report: per-stage durations for a list ---
  // GET /stage/api/report?list_id=901708279415
  if (path === "/stage/api/report" && request.method === "GET") {
    const url = new URL(request.url);
    const listId = url.searchParams.get("list_id");
    if (!listId) return json({ ok: false, error: "list_id required" }, 400);
    const report = await buildReport(env, listId);
    return json({ ok: true, list_id: listId, ...report });
  }

  return null; // not a /stage/ route — let other handlers run
}
