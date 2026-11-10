// ============================================================================
// FLO — the /flo/ page's API (/flo/api/*). Behind the normal Ops Hub
// Cloudflare Access login and refused on any other hostname, like the TikTok
// router's page. Anyone signed in can LOOK; only people on PM_APPROVERS can
// edit or reset the instructions.
//
// The chat rules (GUARDRAILS in brain.js) are shown read-only: they're the
// safety part, so they stay in code where a change goes through review.
// ============================================================================

import { isTrustedHost, verifyAccessJwt } from "../access.js";
import { DEFAULT_INSTRUCTIONS, GUARDRAILS, DEFAULT_MODEL } from "./brain.js";
import { approvers, isApprover } from "./approvals.js";
import { LIST_ID, LIST_URL, DEFAULT_TZ, PERSON_NAME } from "./config.js";
import { resolvePerson, fallbackNames, findLead } from "./outreach.js";
import { DAILY_HOUR, WEEKLY_HOUR, sendPreview } from "./scheduler.js";
import {
  ensureTables,
  expireOld,
  loadInstructions,
  saveInstructions,
  resetInstructions,
  listInstructionChanges,
  getInstructionChange,
  PENDING_TTL_MS,
  getPerson,
  listPeople,
  listOutreach,
  listRuns,
  getSetting,
  setSetting,
} from "./store.js";

export const API_PREFIX = "/flo/api/";
const MIN_CHARS = 200;
const MAX_CHARS = 60000;

const json = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" } });
const viewerOf = (request) => String(request.headers.get("Cf-Access-Authenticated-User-Email") || "").toLowerCase() || null;

function parseJson(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

// Activity rows for the page: friendly label + the card link when there is one.
const EVENT_LABELS = {
  proposed: "Proposed",
  applied: "Changed",
  auto_applied: "Changed by Flo (routine)",
  pending_cleared: "Cleared old suggestions",
  apply_failed: "Change failed",
  rejected: "Turned down",
  approval_refused: "Approval refused (not an approver)",
  approval_expired: "Approval too late (expired)",
  proposal_refused: "Couldn't propose",
  refused_sender: "Turned away (outside NITM)",
  async_post_failed: "Couldn't post answer",
  added_to_space: "Added to a space",
  removed_from_space: "Removed from a space",
  run_daily: "Ran the daily check",
  run_weekly: "Ran weekly planning",
  run_skipped: "Scheduled run skipped",
  outreach_undelivered: "Couldn't reach anyone",
};
function shapeLog(row) {
  const action = parseJson(row.action);
  const taskId = action && typeof action === "object" ? action.task_id : null;
  return {
    id: row.id,
    at: row.at,
    actor: row.actor,
    event: row.event,
    label: EVENT_LABELS[row.event] || row.event,
    number: row.number,
    result: row.result,
    card_url: taskId ? `https://app.clickup.com/t/${encodeURIComponent(taskId)}` : null,
    dm: row.thread === "",
  };
}

// Everyone Flo might need to reach: the TikTok team plus the fallback people,
// and whether each has opened a chat with Flo (so it can DM them).
async function reachability(env) {
  const names = [...new Set([...Object.values(PERSON_NAME), ...fallbackNames(env)])];
  const out = [];
  for (const n of names) {
    let p = null;
    try {
      p = await resolvePerson(env, n);
    } catch {
      /* ClickUp unavailable: show the name without status */
    }
    const row = p ? await getPerson(env, p.email) : null;
    out.push({ name: n, email: p?.email || null, reachable: !!row?.dm_space, last_seen: row?.last_seen || null, fallback: fallbackNames(env).includes(n) });
  }
  // Plus anyone else who has messaged Flo, so new people show up on their own.
  const known = new Set(out.map((x) => x.email).filter(Boolean));
  for (const r of await listPeople(env)) {
    if (known.has(r.email)) continue;
    out.push({ name: String(r.display_name || r.email).split(/\s+/)[0], email: r.email, reachable: !!r.reachable, last_seen: r.last_seen, fallback: false });
  }
  return out;
}

// Who ClickUp shows as the author of Flo's changes (cached; one GET /user).
let _writer = null;
async function writerIdentity(env) {
  if (!env.PM_CLICKUP_TOKEN) return { as_flo: false, name: null, error: null };
  if (_writer && Date.now() - _writer.at < 10 * 60 * 1000) return _writer.v;
  let v;
  try {
    const res = await fetch("https://api.clickup.com/api/v2/user", { headers: { Authorization: env.PM_CLICKUP_TOKEN } });
    const body = await res.json().catch(() => ({}));
    v = res.ok ? { as_flo: true, name: body.user?.username || body.user?.email || "?", email: body.user?.email || null, error: null }
               : { as_flo: true, name: null, error: `ClickUp said ${res.status}: the token may be wrong` };
  } catch (e) {
    v = { as_flo: true, name: null, error: e.message };
  }
  _writer = { at: Date.now(), v };
  return v;
}

async function overview(env, viewer) {
  await expireOld(env);
  const instr = await loadInstructions(env, DEFAULT_INSTRUCTIONS);
  const cutoff = new Date(Date.now() - PENDING_TTL_MS).toISOString();
  const { results: pending } = await env.DB.prepare(
    `SELECT number, summary, proposed_by, created_at, thread = '' AS dm
       FROM pm_pending_actions WHERE status = 'pending' AND created_at >= ? ORDER BY created_at DESC LIMIT 50`
  ).bind(cutoff).all();
  const weekAgo = new Date(Date.now() - 7 * 86400e3).toISOString();
  const stats = await env.DB.prepare(
    `SELECT MAX(at) AS last_at,
            SUM(CASE WHEN at > ? AND event = 'applied' THEN 1 ELSE 0 END) AS changed_7d,
            SUM(CASE WHEN at > ? AND event = 'proposed' THEN 1 ELSE 0 END) AS proposed_7d,
            SUM(CASE WHEN at > ? AND event IN ('apply_failed','async_post_failed') THEN 1 ELSE 0 END) AS errors_7d
       FROM pm_action_log`
  ).bind(weekAgo, weekAgo, weekAgo).first();
  const lastQuestion = await env.DB.prepare("SELECT MAX(updated_at) AS at FROM pm_threads").first();

  return {
    viewer,
    can_edit: isApprover(env, viewer),
    instructions: { text: instr.text, is_default: instr.isDefault, updated_at: instr.updatedAt, updated_by: instr.updatedBy },
    default_text: DEFAULT_INSTRUCTIONS,
    guardrails: GUARDRAILS,
    approvers: approvers(env),
    model: env.PM_MODEL || DEFAULT_MODEL,
    effort: env.PM_EFFORT || "medium",
    timezone: env.PM_TIMEZONE || DEFAULT_TZ,
    list: { id: LIST_ID, url: LIST_URL, name: "TikTok space › Production" },
    configured: {
      google_project: /^\d+$/.test(String(env.PM_CHAT_PROJECT_NUMBER || "").trim()),
      google_key: !!env.GOOGLE_CHAT_SA_KEY,
      claude: !!env.ANTHROPIC_API_KEY,
      clickup: !!env.CLICKUP_TOKEN,
    },
    clickup_writer: await writerIdentity(env),
    pending: pending || [],
    outreach: {
      proactive: (await getSetting(env, "proactive", "on")) !== "off",
      run_requested: !!(await getSetting(env, "run_request")),
      schedule: { daily: `Weekdays from ${DAILY_HOUR}:00`, weekly: `Wednesdays from ${WEEKLY_HOUR}:00`, timezone: env.PM_TIMEZONE || DEFAULT_TZ },
      lead: await findLead(env).then((l) => (l ? { name: l.name, email: l.email } : null)).catch(() => null),
      copies: String(env.PM_DIGEST_CC || "").split(/[\s,;]+/).filter(Boolean),
      fallback: fallbackNames(env),
      people: await reachability(env),
      runs: await listRuns(env, 10),
      recent: await listOutreach(env, 25),
    },
    stats: { ...(stats || {}), last_question_at: lastQuestion?.at || null },
  };
}

export async function handleFloApi(request, env, path) {
  if (!path.startsWith(API_PREFIX)) return null;
  const sub = path.slice(API_PREFIX.length);
  const url = new URL(request.url);
  if (!isTrustedHost(env, url)) return json({ error: "Only served on the Ops Hub login-protected hostname." }, 403);
  if (env.ACCESS_TEAM_DOMAIN && env.ACCESS_AUD) {
    const v = await verifyAccessJwt(env, request.headers.get("Cf-Access-Jwt-Assertion"), env.ACCESS_AUD);
    if (!v.ok) return json({ error: `Cloudflare Access check failed: ${v.reason}` }, 401);
  }
  if (!env.DB) return json({ error: "DB not configured" }, 503);
  const method = request.method;
  // Writes must be JSON: a cross-site form can't send that without a preflight we never answer.
  if (method !== "GET" && !(request.headers.get("Content-Type") || "").includes("application/json")) {
    return json({ error: "Send JSON." }, 415);
  }
  const viewer = viewerOf(request);
  const mustEdit = () =>
    isApprover(env, viewer)
      ? null
      : json({ error: "Only approvers can change Flo's instructions. Ask Mark or Avery." }, 403);

  try {
    await ensureTables(env);

    if (sub === "overview" && method === "GET") return json(await overview(env, viewer));

    if (sub === "instructions" && method === "PUT") {
      const denied = mustEdit();
      if (denied) return denied;
      const body = await request.json().catch(() => null);
      const text = String(body?.text ?? "").replace(/\r\n/g, "\n").trim();
      if (text.length < MIN_CHARS) return json({ error: `That's too short (${text.length} characters). Flo needs the full instructions.` }, 400);
      if (text.length > MAX_CHARS) return json({ error: `That's too long (${text.length} characters; the limit is ${MAX_CHARS}).` }, 400);
      const note = String(body?.note ?? "").trim().slice(0, 300) || null;
      const at = await saveInstructions(env, text, viewer, note);
      return json({ ok: true, updated_at: at, updated_by: viewer });
    }

    if (sub === "instructions/reset" && method === "POST") {
      const denied = mustEdit();
      if (denied) return denied;
      await resetInstructions(env, viewer);
      return json({ ok: true });
    }

    if (sub === "settings" && method === "PUT") {
      const denied = mustEdit();
      if (denied) return denied;
      const body = await request.json().catch(() => null);
      if (typeof body?.proactive !== "boolean") return json({ error: "Send {proactive: true|false}." }, 400);
      await setSetting(env, "proactive", body.proactive ? "on" : "off", viewer);
      return json({ ok: true, proactive: body.proactive });
    }

    if (sub === "run" && method === "POST") {
      const denied = mustEdit();
      if (denied) return denied;
      const body = await request.json().catch(() => null);
      if (!["daily", "weekly"].includes(body?.kind)) return json({ error: "kind must be daily or weekly." }, 400);
      // The cron (every 2 minutes, up to 15 minutes per run) does the work;
      // a page request only has ~30 seconds.
      await setSetting(env, "run_request", JSON.stringify({ kind: body.kind, by: viewer }), viewer);
      return json({ ok: true, message: "Queued. It starts within about 2 minutes." });
    }

    if (sub === "preview" && method === "POST") {
      const r = await sendPreview(env, viewer);
      return json(r, r.ok ? 200 : 400);
    }

    if (sub === "history" && method === "GET") {
      const id = url.searchParams.get("id");
      if (id) {
        const row = await getInstructionChange(env, Number(id));
        return row ? json(row) : json({ error: "No such version." }, 404);
      }
      return json({ history: await listInstructionChanges(env, 30) });
    }

    if (sub === "log" && method === "GET") {
      const limit = Math.min(300, Math.max(1, Number(url.searchParams.get("limit")) || 100));
      const changesOnly = url.searchParams.get("filter") === "changes";
      const { results } = await env.DB.prepare(
        `SELECT id, at, space, thread, number, actor, event, action, result FROM pm_action_log
          ${changesOnly ? "WHERE event IN ('proposed','applied','auto_applied','apply_failed','rejected','approval_refused','approval_expired')" : ""}
          ORDER BY id DESC LIMIT ?`
      ).bind(limit).all();
      return json({ log: (results || []).map(shapeLog) });
    }

    return json({ error: `Unknown endpoint: ${sub}` }, 404);
  } catch (e) {
    return json({ error: String(e?.message || e) }, 500);
  }
}
