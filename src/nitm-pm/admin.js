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
import { LIST_ID, LIST_URL, DEFAULT_TZ } from "./config.js";
import {
  ensureTables,
  expireOld,
  loadInstructions,
  saveInstructions,
  resetInstructions,
  listInstructionChanges,
  getInstructionChange,
  PENDING_TTL_MS,
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
  apply_failed: "Change failed",
  rejected: "Turned down",
  approval_refused: "Approval refused (not an approver)",
  approval_expired: "Approval too late (expired)",
  proposal_refused: "Couldn't propose",
  refused_sender: "Turned away (outside NITM)",
  async_post_failed: "Couldn't post answer",
  added_to_space: "Added to a space",
  removed_from_space: "Removed from a space",
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
    pending: pending || [],
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
          ${changesOnly ? "WHERE event IN ('proposed','applied','apply_failed','rejected','approval_refused','approval_expired')" : ""}
          ORDER BY id DESC LIMIT ?`
      ).bind(limit).all();
      return json({ log: (results || []).map(shapeLog) });
    }

    return json({ error: `Unknown endpoint: ${sub}` }, 404);
  } catch (e) {
    return json({ error: String(e?.message || e) }, 500);
  }
}
