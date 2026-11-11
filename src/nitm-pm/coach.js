// ============================================================================
// FLO — coaching (Mark, Nov 11). While Flo runs in shadow mode, everything it
// would have done is logged (pm_action_log, event 'shadow'). On /flo/coach/
// Tia (or any approver) goes through those one at a time: Right / Not quite,
// and in her own words what really happens. Those words become lessons
// (pm_lessons) that go into Flo's instructions on its next message, so the
// team shapes Flo by typing, not by editing a rules document.
//
// The Right/Not quite counts per kind of action are how we decide when Flo has
// earned an action back (switching shadow mode off on the Flo page).
// ============================================================================

import { clickup, readCard } from "./clickup.js";
import { FIELDS } from "./config.js";
import { stageColor } from "./cards.js";

const REVIEW_DAYS = 10;
const MAX_ITEMS = 60;
const MAX_CARD_LOOKUPS = 25;
const MAX_LESSONS_IN_PROMPT = 40;
export const MAX_NOTE = 600;

const nowIso = () => new Date().toISOString();
const since = (days) => new Date(Date.now() - days * 86400e3).toISOString();

export const KIND_LABELS = {
  due_date: "Move a due date",
  priority: "Change priority",
  comment: "Comment on the card",
  assignee: "Change who has it",
  message: "Message someone",
  custom_field: "Change a field",
};

function parse(row) {
  let action = {};
  try {
    action = JSON.parse(row.action || "{}");
  } catch {
    // keep {}
  }
  const [summary, ...rest] = String(row.result || "").split(" · ");
  const outcome = rest.join(" · ");
  return {
    id: row.id,
    at: row.at,
    kind: action.type || "other",
    kind_label: KIND_LABELS[action.type] || "Something else",
    task_id: action.task_id || null,
    person: action.person || null,
    summary: summary.replace(/^Would have messaged /, "Message "),
    would: action.type === "message" ? "would have sent it" : /would have changed it/.test(outcome) ? "would have done it itself" : "would have asked a person first",
    verdict: row.verdict || null,
    note: row.note || null,
  };
}

async function cardInfo(env, taskIds, tz) {
  const out = new Map();
  await Promise.all(
    [...new Set(taskIds.filter(Boolean))].slice(0, MAX_CARD_LOOKUPS).map(async (id) => {
      try {
        const task = await clickup(env, "GET", `/task/${encodeURIComponent(id)}`);
        const c = readCard(task, tz);
        const f = (task.custom_fields || []).find((x) => x.id === FIELDS.stage);
        const opt = (f?.type_config?.options || []).find((o) => o.id === f?.value || String(o.orderindex) === String(f?.value));
        out.set(id, {
          name: String(c.name).replace(/^[a-z0-9]{6,12}\s+-\s+|^\d{4}\.\d+\s+-\s+/i, ""),
          url: c.url,
          stage: c.stage,
          stage_color: opt ? stageColor(opt.color) : null,
          post_date: c.post_date,
          due_date: c.due_date,
          assignees: c.assignees,
        });
      } catch {
        // The card may be gone or moved; the item still shows without it.
      }
    })
  );
  return out;
}

/** Things Flo would have done that nobody has reviewed yet, newest first, with card details. */
export async function toReview(env, { tz } = {}) {
  const { results } = await env.DB.prepare(
    `SELECT l.id, l.at, l.action, l.result FROM pm_action_log l
       LEFT JOIN pm_coaching c ON c.log_id = l.id
      WHERE l.event = 'shadow' AND l.at > ? AND c.id IS NULL
      ORDER BY l.id DESC LIMIT ?`
  )
    .bind(since(REVIEW_DAYS), MAX_ITEMS)
    .all();
  const items = (results || []).map(parse);
  const cards = await cardInfo(env, items.map((i) => i.task_id), tz);
  return items.map((i) => ({ ...i, card: i.task_id ? cards.get(i.task_id) || null : null }));
}

export async function countToReview(env) {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS n FROM pm_action_log l LEFT JOIN pm_coaching c ON c.log_id = l.id
      WHERE l.event = 'shadow' AND l.at > ? AND c.id IS NULL`
  )
    .bind(since(REVIEW_DAYS))
    .first();
  return Number(row?.n || 0);
}

/** Right/Not quite per kind of action, last 30 days. */
export async function coachStats(env) {
  const { results } = await env.DB.prepare(
    `SELECT l.action, c.verdict FROM pm_coaching c JOIN pm_action_log l ON l.id = c.log_id
      WHERE c.at > ? AND c.verdict IN ('right','wrong')`
  )
    .bind(since(30))
    .all();
  const by = {};
  for (const r of results || []) {
    let kind = "other";
    try {
      kind = JSON.parse(r.action).type || "other";
    } catch {
      // other
    }
    by[kind] = by[kind] || { kind, label: KIND_LABELS[kind] || "Something else", right: 0, wrong: 0 };
    by[kind][r.verdict === "right" ? "right" : "wrong"]++;
  }
  return Object.values(by).sort((a, b) => b.right + b.wrong - (a.right + a.wrong));
}

/**
 * Save a verdict. With a note and teach=true it also becomes a lesson.
 * verdict: right | wrong | skip.
 */
export async function recordVerdict(env, { logId, verdict, note, teach = true, by }) {
  if (!["right", "wrong", "skip"].includes(verdict)) throw new Error("verdict must be right, wrong or skip");
  const row = await env.DB.prepare("SELECT id, action, result FROM pm_action_log WHERE id = ? AND event = 'shadow'").bind(Number(logId)).first();
  if (!row) throw new Error("That item isn't there anymore.");
  const text = String(note || "").trim().slice(0, MAX_NOTE);
  await env.DB.prepare(
    `INSERT INTO pm_coaching (log_id, verdict, note, by, at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(log_id) DO UPDATE SET verdict = excluded.verdict, note = excluded.note, by = excluded.by, at = excluded.at`
  )
    .bind(row.id, verdict, text || null, by || null, nowIso())
    .run();
  let lesson = null;
  if (text && teach && verdict !== "skip") {
    const about = parse(row).summary.slice(0, 200);
    lesson = await addLesson(env, { text, by, about, sourceLogId: row.id });
  }
  return { ok: true, lesson };
}

export async function addLesson(env, { text, by, about = null, sourceLogId = null }) {
  const t = String(text || "").trim().slice(0, MAX_NOTE);
  if (!t) throw new Error("Write what Flo should know.");
  const r = await env.DB.prepare("INSERT INTO pm_lessons (text, by, at, about, source_log_id, active) VALUES (?, ?, ?, ?, ?, 1) RETURNING id")
    .bind(t, by || null, nowIso(), about, sourceLogId)
    .first();
  return { id: r?.id, text: t, about };
}

export async function listLessons(env, { includeOff = false } = {}) {
  const { results } = await env.DB.prepare(
    `SELECT id, text, by, at, about, active FROM pm_lessons ${includeOff ? "" : "WHERE active = 1"} ORDER BY id DESC LIMIT 200`
  ).all();
  return results || [];
}

export async function setLessonActive(env, id, active) {
  await env.DB.prepare("UPDATE pm_lessons SET active = ? WHERE id = ?").bind(active ? 1 : 0, Number(id)).run();
}

const firstOf = (email) => {
  const n = String(email || "").split("@")[0].split(/[._-]/)[0];
  return n ? n.charAt(0).toUpperCase() + n.slice(1) : "The team";
};

/** The team's lessons, as a block for Flo's instructions. Empty string if none. */
export async function lessonsForPrompt(env) {
  const lessons = (await listLessons(env)).slice(0, MAX_LESSONS_IN_PROMPT);
  if (!lessons.length) return "";
  const lines = lessons.map((l) => `- ${l.text} (${firstOf(l.by)}${l.about ? `, about Flo wanting to: ${l.about}` : ""})`);
  return (
    "# What the team has taught you\n" +
    "These come from the people you work with, about how things really work here. They override the routine and the stage tables above when they disagree. Newest first.\n" +
    lines.join("\n")
  );
}
