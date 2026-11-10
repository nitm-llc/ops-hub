// ============================================================================
// FLO — which changes are "routine", so a scheduled run makes them itself.
//
// Mark, Nov 10: on the daily check and weekly planning, Flo does the routine
// steps of Avery's process on its own and only asks about judgment calls.
// Routine means exactly the steps in the instructions, checked here in code:
//
//   due date   today or later, not a weekend, before the card's Post Date
//   priority   High
//   owner      the card ends up with exactly the owner the Stage table names
//   comment    at most one of Flo's own comments per card every 2 days
//
// Everything else (Post Date, Stage, other fields, any other reassignment)
// stays a proposal for a person. Chat requests from people are never routine:
// this only applies when turn.scheduled is true.
// ============================================================================

import { expectedOwner, isWeekend } from "./config.js";

const COMMENT_GAP_MS = 2 * 24 * 3600 * 1000;
const MAX_COMMENT = 600;

async function commentedRecently(env, taskId) {
  const since = new Date(Date.now() - COMMENT_GAP_MS).toISOString();
  const row = await env.DB.prepare(
    "SELECT 1 FROM pm_action_log WHERE event = 'auto_applied' AND at > ? AND action LIKE ? AND action LIKE ? LIMIT 1"
  )
    .bind(since, `%"task_id":${JSON.stringify(String(taskId))}%`, '%"type":"comment"%')
    .first();
  return !!row;
}

/**
 * { routine: true } or { routine: false, why } for a validated action on a card
 * (readCard output). `today` is the New York day.
 */
export async function routineCheck(env, action, card, today) {
  switch (action.type) {
    case "due_date": {
      if (action.date < today) return { routine: false, why: "due date in the past" };
      if (isWeekend(action.date)) return { routine: false, why: "due date on a weekend" };
      if (card.post_date && action.date >= card.post_date) return { routine: false, why: "due date on or after the Post Date" };
      return { routine: true };
    }
    case "priority":
      return action.priority === "high" ? { routine: true } : { routine: false, why: "only raising to High is routine" };
    case "assignee": {
      const owner = expectedOwner(card.stage, card.type);
      if (!owner) return { routine: false, why: "the Stage table names no owner for this stage" };
      const after = new Set((card.assignee_ids || []).filter((id) => !action.remove.includes(id)));
      for (const id of action.add) after.add(id);
      return after.size === 1 && after.has(owner) ? { routine: true } : { routine: false, why: "not the owner the Stage table names" };
    }
    case "comment": {
      if (String(action.text || "").length > MAX_COMMENT) return { routine: false, why: "comment too long" };
      if (await commentedRecently(env, action.task_id)) return { routine: false, why: "Flo already commented on this card in the last 2 days" };
      return { routine: true };
    }
    default:
      return { routine: false, why: "Post Date, Stage and other fields need a person" };
  }
}
