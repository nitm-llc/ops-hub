// ============================================================================
// NITM PM — ClickUp reads, and the ONLY code path that writes to ClickUp.
//
// GUARDRAILS live here, not in the prompt:
//   * Nothing here runs without an approver's yes (see approvals.js).
//   * validateAction() whitelists the change types: due date, priority,
//     assignee, comment, Post Date, Stage and other custom fields (Mark,
//     2026-11-04: when a person asks for a Post Date/Stage change it's on
//     purpose, so it must actually work). Status, name, list moves, tags and
//     deletes are not offered. Unknown keys are refused, not dropped.
//   * Custom-field values are resolved against the list's live field
//     definitions at propose AND apply time; applyAction() builds the request
//     from that resolution only.
//   * Every write is to a task re-fetched and confirmed to be on LIST_ID.
// ============================================================================

import {
  LIST_ID,
  FIELDS,
  PEOPLE,
  PERSON_NAME,
  PRIORITY,
  PRIORITY_NAME,
  expectedOwner,
  isDoneStage,
  isPausedStage,
  isUnusedStage,
  isKristineStage,
  ymd,
  todayYmd,
  isYmd,
  addDays,
  workdaysBetween,
  weekdayName,
  ymdToClickUpMs,
  rangeStartMs,
  rangeEndMs,
} from "./config.js";

const CLICKUP_API = "https://api.clickup.com/api/v2";
const MAX_PAGES = 10;

export async function clickup(env, method, path, body) {
  if (!env.CLICKUP_TOKEN) throw new Error("CLICKUP_TOKEN is not set");
  const res = await fetch(`${CLICKUP_API}${path}`, {
    method,
    headers: { Authorization: env.CLICKUP_TOKEN, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`ClickUp ${method} ${path.split("?")[0]} -> ${res.status}: ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : {};
}

// ---------------------------------------------------------------------------
// Reading a card
// ---------------------------------------------------------------------------

// Dropdowns arrive as the option id or (older dropdowns) its orderindex.
function dropdownName(field) {
  const v = field?.value;
  if (v === undefined || v === null || v === "") return null;
  const opts = field.type_config?.options || [];
  const opt = opts.find((o) => o.id === v) || opts.find((o) => o.orderindex != null && String(o.orderindex) === String(v));
  return opt ? String(opt.name).trim() : null;
}

export function readCard(task, tz, today = todayYmd(tz)) {
  const fields = task.custom_fields || [];
  const f = (id) => fields.find((x) => x.id === id);
  const stage = dropdownName(f(FIELDS.stage));
  const type = dropdownName(f(FIELDS.type));
  const postDate = ymd(f(FIELDS.postDate)?.value, tz);
  const dueDate = ymd(task.due_date, tz);
  const assigneeIds = (task.assignees || []).map((a) => Number(a.id));
  const owner = expectedOwner(stage, type);
  const done = isDoneStage(stage);
  const paused = isPausedStage(stage);
  const overdue = !!dueDate && dueDate < today && !done && !paused;
  const card = {
    id: task.id,
    name: task.name,
    url: task.url || `https://app.clickup.com/t/${task.id}`,
    stage,
    type,
    post_date: postDate,
    due_date: dueDate,
    assignees: assigneeIds.map((id) => PERSON_NAME[id] || (task.assignees.find((a) => Number(a.id) === id)?.username ?? String(id))),
    assignee_ids: assigneeIds,
    priority: task.priority?.priority ? String(task.priority.priority).toLowerCase() : null,
    done,
    paused,
    unused_stage: isUnusedStage(stage),
    overdue,
    days_overdue: overdue ? workdaysBetween(dueDate, today) : 0,
    workdays_to_post: postDate ? workdaysBetween(today, postDate) : null,
    follow_up_window: isKristineStage(stage) || assigneeIds.includes(PEOPLE.kristine) ? 7 : 5,
    expected_owner: owner ? PERSON_NAME[owner] : null,
    owner_matches: owner ? assigneeIds.length === 1 && assigneeIds[0] === owner : null,
    list_id: String(task.list?.id ?? ""),
  };
  const doc = f(FIELDS.googleDoc)?.value;
  if (doc) card.script = doc;
  return card;
}

// ---------------------------------------------------------------------------
// Fetching cards. Server-side filters narrow the pages; every filter is then
// re-applied in code, so a filter the API ignores costs time, not correctness.
// ---------------------------------------------------------------------------
async function fetchTasks(env, params) {
  const out = [];
  for (let page = 0; page < MAX_PAGES; page++) {
    const qs = new URLSearchParams({ ...params, page: String(page), subtasks: "true", include_closed: "true" });
    const body = await clickup(env, "GET", `/list/${LIST_ID}/task?${qs}`);
    out.push(...(body.tasks || []));
    if (body.last_page !== false || !(body.tasks || []).length) break;
  }
  return out;
}

const postDateRange = (from, to) =>
  JSON.stringify([{ field_id: FIELDS.postDate, operator: "RANGE", value: [rangeStartMs(from), rangeEndMs(to)] }]);

const lower = (s) => String(s || "").toLowerCase();
const matchesAny = (value, wanted) => !wanted?.length || wanted.some((w) => lower(w).trim() === lower(value).trim());

function personId(nameOrId) {
  if (nameOrId === undefined || nameOrId === null || nameOrId === "") return null;
  if (/^\d+$/.test(String(nameOrId))) return Number(nameOrId);
  return PEOPLE[lower(nameOrId).trim().split(/\s+/)[0]] ?? null;
}

/**
 * find_cards. See TOOLS in brain.js for the input contract.
 */
export async function findCards(env, input = {}, { tz, today = todayYmd(tz) } = {}) {
  const limit = Math.min(Math.max(Number(input.limit) || 40, 1), 80);
  let from = isYmd(input.post_date_from) ? input.post_date_from : null;
  let to = isYmd(input.post_date_to) ? input.post_date_to : null;
  if (from && !to) to = addDays(from, 60);
  if (to && !from) from = addDays(to, -60);

  let tasks;
  if (input.overdue) {
    // Bounded look-back, deliberately generous: ClickUp stores date-only due
    // dates at different hours (04:00Z, 09:00Z...), and 04:00Z on today's UTC
    // date is YESTERDAY evening in New York. So ask for everything up to the
    // end of today and let readCard() decide "overdue" by New York day.
    tasks = await fetchTasks(env, {
      due_date_lt: String(rangeEndMs(today)),
      due_date_gt: String(rangeStartMs(addDays(today, -120))),
      ...(from ? { custom_fields: postDateRange(from, to) } : {}),
    });
  } else {
    const f = from || addDays(today, -14);
    const t = to || addDays(today, 42);
    tasks = await fetchTasks(env, { custom_fields: postDateRange(f, t) });
    from = f;
    to = t;
  }

  const wantAssignee = personId(input.assignee);
  let cards = tasks
    .filter((t) => String(t.list?.id ?? LIST_ID) === LIST_ID)
    .map((t) => readCard(t, tz, today))
    .filter((c) => (input.overdue ? c.overdue : true))
    .filter((c) => !from || (c.post_date && c.post_date >= from && c.post_date <= to) || (input.overdue && !c.post_date && !from))
    .filter((c) => matchesAny(c.stage, input.stage))
    .filter((c) => matchesAny(c.type, input.type))
    .filter((c) => (wantAssignee ? c.assignee_ids.includes(wantAssignee) : true))
    .filter((c) => (input.name_contains ? lower(c.name).includes(lower(input.name_contains)) : true))
    .filter((c) => (input.include_done ? true : !c.done))
    // The instructions: ignore Backlog/Outline cards with no Post Date.
    .filter((c) => !(input.overdue && !c.post_date && ["backlog", "outline"].includes(lower(c.stage))));

  cards.sort((a, b) => String(a.post_date || "9999").localeCompare(String(b.post_date || "9999")));
  const total = cards.length;
  cards = cards.slice(0, limit);

  if (input.with_comments) {
    await Promise.all(
      cards.slice(0, 25).map(async (c) => {
        try {
          c.recent_comments = await recentComments(env, c.id, 3, tz);
        } catch (e) {
          c.recent_comments_error = String(e.message || e);
        }
      })
    );
  }
  for (const c of cards) delete c.list_id;
  return { today, total, shown: cards.length, cards };
}

async function recentComments(env, taskId, n, tz) {
  const body = await clickup(env, "GET", `/task/${encodeURIComponent(taskId)}/comment`);
  return (body.comments || [])
    .slice(0, n)
    .map((c) => ({
      date: ymd(c.date, tz),
      by: c.user?.username || c.user?.email || "?",
      text: String(c.comment_text || "").slice(0, 300),
    }));
}

export async function getCard(env, input, { tz, today = todayYmd(tz) } = {}) {
  const taskId = String(input?.task_id || "").trim();
  if (!taskId) throw new Error("task_id is required");
  const task = await clickup(env, "GET", `/task/${encodeURIComponent(taskId)}?custom_fields=true`);
  const card = readCard(task, tz, today);
  if (card.list_id !== LIST_ID) throw new Error(`Task ${taskId} is not on the TikTok Production list`);
  delete card.list_id;
  card.description = String(task.text_content || task.description || "").slice(0, 600);
  card.recent_comments = await recentComments(env, taskId, 10, tz);
  return card;
}

export async function calendar(env, input, { tz, today = todayYmd(tz) } = {}) {
  const from = isYmd(input?.from) ? input.from : today;
  const to = isYmd(input?.to) ? input.to : addDays(from, 20);
  if (to < from) throw new Error("`to` is before `from`");
  if (to > addDays(from, 62)) throw new Error("Keep the range to about two months");
  const tasks = await fetchTasks(env, { custom_fields: postDateRange(from, to) });
  const cards = tasks
    .map((t) => readCard(t, tz, today))
    .filter((c) => c.list_id === LIST_ID || !c.list_id)
    .filter((c) => c.post_date && c.post_date >= from && c.post_date <= to)
    .sort((a, b) => a.post_date.localeCompare(b.post_date));
  const days = [];
  for (let d = from; d <= to; d = addDays(d, 1)) {
    const on = cards.filter((c) => c.post_date === d);
    days.push({
      date: d,
      day: weekdayName(d),
      cards: on.map((c) => ({ id: c.id, name: c.name, url: c.url, type: c.type, stage: c.stage, done: c.done, paused: c.paused })),
    });
  }
  return {
    from,
    to,
    days,
    note: "Instagram posts are not connected yet, so the TikTok/Instagram same-day rules can't be checked.",
  };
}

// ---------------------------------------------------------------------------
// Changes
// ---------------------------------------------------------------------------

export class RefusedError extends Error {}

const ALLOWED_KEYS = ["type", "task_id", "date", "priority", "add", "remove", "text", "stage", "field", "value", "reason"];
const TYPES = ["due_date", "priority", "assignee", "comment", "post_date", "stage", "custom_field"];
const CANT = "I can change a due date, priority, assignees, the Post Date, the Stage or another custom field, or add a comment.";

/**
 * Check the SHAPE of a proposed change. Returns a clean action or throws
 * RefusedError. Custom-field values are checked against ClickUp by
 * resolveCustomField(). Clean actions:
 *   {type:"due_date", task_id, date} | {type:"priority", task_id, priority}
 *   {type:"assignee", task_id, add, remove} | {type:"comment", task_id, text}
 *   {type:"custom_field", task_id, field, value}   (post_date and stage
 *   normalise to this, with field = the Post Date / Stage field id)
 */
export function validateAction(raw) {
  if (!raw || typeof raw !== "object") throw new RefusedError("No change given.");
  const type = String(raw.type || "").trim().toLowerCase();
  const extra = Object.keys(raw).filter((k) => !ALLOWED_KEYS.includes(k));
  if (extra.length) throw new RefusedError(`Refused: unexpected ${extra.join(", ")}. ${CANT}`);
  if (!TYPES.includes(type)) throw new RefusedError(`Refused: "${raw.type}" isn't a change I can make. ${CANT}`);
  const taskId = String(raw.task_id || "").trim();
  if (!/^[A-Za-z0-9_-]{4,40}$/.test(taskId)) throw new RefusedError("A valid task_id is required.");

  if (type === "due_date") {
    if (!isYmd(raw.date)) throw new RefusedError("date must be YYYY-MM-DD.");
    return { type, task_id: taskId, date: raw.date };
  }
  if (type === "priority") {
    const p = lower(raw.priority).trim();
    if (!PRIORITY[p]) throw new RefusedError("priority must be urgent, high, normal or low.");
    return { type, task_id: taskId, priority: p };
  }
  if (type === "assignee") {
    const ids = (xs) => (Array.isArray(xs) ? xs : xs == null ? [] : [xs]).map(personId);
    const add = ids(raw.add);
    const remove = ids(raw.remove);
    if (add.includes(null) || remove.includes(null)) throw new RefusedError("Unknown person. Use one of: " + Object.values(PERSON_NAME).join(", "));
    if (!add.length && !remove.length) throw new RefusedError("Say who to add or remove.");
    return { type, task_id: taskId, add, remove };
  }
  if (type === "comment") {
    const text = String(raw.text || "").trim();
    if (!text) throw new RefusedError("Comment text is required.");
    if (text.length > 1500) throw new RefusedError("Keep the comment under 1500 characters.");
    return { type, task_id: taskId, text };
  }
  if (type === "post_date") {
    const date = raw.date ?? raw.value;
    if (date !== null && date !== "" && !isYmd(date)) throw new RefusedError("date must be YYYY-MM-DD (or empty to clear it).");
    return { type: "custom_field", task_id: taskId, field: FIELDS.postDate, value: date || null };
  }
  if (type === "stage") {
    const stage = String(raw.stage ?? raw.value ?? "").trim();
    if (!stage) throw new RefusedError("Say which Stage.");
    return { type: "custom_field", task_id: taskId, field: FIELDS.stage, value: stage };
  }
  // custom_field
  const field = String(raw.field || "").trim();
  if (!field) throw new RefusedError("Say which field.");
  const value = raw.value === undefined ? null : raw.value;
  return { type: "custom_field", task_id: taskId, field, value };
}

// ---- custom fields ---------------------------------------------------------
let _fieldCache = null; // { at, fields }
const FIELD_CACHE_MS = 10 * 60 * 1000;
export const _clearFieldCache = () => {
  _fieldCache = null;
};

async function listFields(env) {
  if (_fieldCache && Date.now() - _fieldCache.at < FIELD_CACHE_MS) return _fieldCache.fields;
  const body = await clickup(env, "GET", `/list/${LIST_ID}/field`);
  _fieldCache = { at: Date.now(), fields: body.fields || [] };
  return _fieldCache.fields;
}

const optName = (o) => String(o.name ?? o.label ?? "").trim();
function findOption(options, wanted) {
  const w = lower(wanted).trim();
  return (
    options.find((o) => o.id === wanted) ||
    options.find((o) => lower(optName(o)) === w) ||
    // "Scheduled" for "Scheduled 📅", "Product" for "Product ": ignore emoji/punctuation.
    options.find((o) => lower(optName(o)).replace(/[^a-z0-9]+/g, " ").trim() === w.replace(/[^a-z0-9]+/g, " ").trim())
  );
}

/**
 * Turn {field, value} into the exact ClickUp request, using the list's live
 * field definitions. Returns { field_id, field_name, clear, body, display }.
 */
export async function resolveCustomField(env, action) {
  const fields = await listFields(env);
  const f =
    fields.find((x) => x.id === action.field) ||
    fields.find((x) => lower(x.name).trim() === lower(action.field).trim());
  if (!f) {
    throw new RefusedError(`There's no field called "${action.field}" on the Production list. Fields: ${fields.map((x) => x.name).join(", ")}`);
  }
  const name = String(f.name).trim();
  const v = action.value;
  const base = { field_id: f.id, field_name: name };
  if (v === null || v === "" || (Array.isArray(v) && !v.length)) return { ...base, clear: true, display: "cleared" };

  const options = f.type_config?.options || [];
  switch (f.type) {
    case "drop_down": {
      const o = findOption(options, String(v));
      if (!o) throw new RefusedError(`"${v}" isn't an option for ${name}. Options: ${options.map(optName).join(", ")}`);
      return { ...base, body: { value: o.id }, display: optName(o) };
    }
    case "labels": {
      const wanted = Array.isArray(v) ? v : String(v).split(",");
      const picked = wanted.map((w) => [w, findOption(options, String(w).trim())]);
      const bad = picked.filter(([, o]) => !o).map(([w]) => w);
      if (bad.length) throw new RefusedError(`Not options for ${name}: ${bad.join(", ")}. Options: ${options.map(optName).join(", ")}`);
      return { ...base, body: { value: picked.map(([, o]) => o.id) }, display: picked.map(([, o]) => optName(o)).join(", ") };
    }
    case "date": {
      if (!isYmd(String(v))) throw new RefusedError(`${name} needs a date as YYYY-MM-DD.`);
      return { ...base, body: { value: ymdToClickUpMs(String(v)), value_options: { time: false } }, display: weekdayName(String(v)) };
    }
    case "text":
    case "short_text":
    case "url":
    case "email":
    case "phone":
      return { ...base, body: { value: String(v) }, display: `"${String(v).slice(0, 80)}"` };
    case "number":
    case "currency": {
      const n = Number(v);
      if (!Number.isFinite(n)) throw new RefusedError(`${name} needs a number.`);
      return { ...base, body: { value: n }, display: String(n) };
    }
    case "checkbox": {
      const b = typeof v === "boolean" ? v : /^(true|yes|y|1|checked|on)$/i.test(String(v)) ? true : /^(false|no|n|0|unchecked|off)$/i.test(String(v)) ? false : null;
      if (b === null) throw new RefusedError(`${name} is a checkbox: say yes or no.`);
      return { ...base, body: { value: b }, display: b ? "checked" : "unchecked" };
    }
    default:
      throw new RefusedError(`I can't set ${name} (a ${f.type} field) yet.`);
  }
}

export function describeAction(action, cardName, resolved) {
  const on = cardName ? `*${cardName}*` : `card ${action.task_id}`;
  if (action.type === "due_date") return `Move the due date on ${on} to ${weekdayName(action.date)}`;
  if (action.type === "priority") return `Set priority on ${on} to ${action.priority}`;
  if (action.type === "assignee") {
    const names = (xs) => xs.map((id) => PERSON_NAME[id] || id).join(", ");
    const parts = [];
    if (action.add.length) parts.push(`assign ${names(action.add)}`);
    if (action.remove.length) parts.push(`remove ${names(action.remove)}`);
    return `On ${on}: ${parts.join(" and ")}`;
  }
  if (action.type === "comment") return `Comment on ${on}: "${action.text.slice(0, 120)}${action.text.length > 120 ? "…" : ""}"`;
  if (action.type === "custom_field" && resolved) {
    return resolved.clear ? `Clear ${resolved.field_name} on ${on}` : `Set ${resolved.field_name} on ${on} to ${resolved.display}`;
  }
  return `Change ${on}`;
}

function paperTrail(action, approver, resolved) {
  const by = `approved by ${approver} in Chat`;
  if (action.type === "due_date") return `Due date moved to ${weekdayName(action.date)}, ${by}.`;
  if (action.type === "priority") return `Priority set to ${action.priority}, ${by}.`;
  if (action.type === "assignee") {
    const names = (xs) => xs.map((id) => PERSON_NAME[id] || id).join(", ");
    const parts = [];
    if (action.add.length) parts.push(`assigned ${names(action.add)}`);
    if (action.remove.length) parts.push(`removed ${names(action.remove)}`);
    const t = parts.join(" and ");
    return `${t[0].toUpperCase()}${t.slice(1)}, ${by}.`;
  }
  if (action.type === "custom_field") {
    return resolved.clear ? `${resolved.field_name} cleared, ${by}.` : `${resolved.field_name} set to ${resolved.display}, ${by}.`;
  }
  return null;
}

// Confirm the card exists and is on our list. Used at propose AND apply time.
export async function loadListTask(env, taskId) {
  const task = await clickup(env, "GET", `/task/${encodeURIComponent(taskId)}?custom_fields=true`);
  if (String(task?.list?.id) !== LIST_ID) throw new RefusedError(`Card ${taskId} isn't on the TikTok Production list, so I won't touch it.`);
  return task;
}

/**
 * Validate and resolve an action for a proposal. Returns { action, resolved }.
 */
export async function prepareAction(env, raw) {
  const action = validateAction(raw);
  const resolved = action.type === "custom_field" ? await resolveCustomField(env, action) : null;
  return { action, resolved };
}

/**
 * Carry out one action and leave the paper-trail comment.
 * Returns { changed: bool, commented: bool, detail }. Throws on the main
 * change failing; a comment failure after a successful change is reported,
 * not thrown, so the caller can say exactly what happened.
 */
export async function applyAction(env, rawAction, approverName) {
  // Re-validate and re-resolve: the stored row is not trusted, and the
  // field's options may have changed since the proposal.
  const { action, resolved } = await prepareAction(env, rawAction);
  await loadListTask(env, action.task_id);
  const path = `/task/${encodeURIComponent(action.task_id)}`;

  if (action.type === "comment") {
    await clickup(env, "POST", `${path}/comment`, {
      comment_text: `${action.text}\n\n(Posted by Flo, approved by ${approverName} in Chat.)`,
      notify_all: false,
    });
    return { changed: true, commented: true, detail: "comment posted" };
  }

  if (action.type === "custom_field") {
    const fieldPath = `${path}/field/${encodeURIComponent(resolved.field_id)}`;
    if (resolved.clear) await clickup(env, "DELETE", fieldPath);
    else await clickup(env, "POST", fieldPath, resolved.body);
  } else {
    let body;
    if (action.type === "due_date") body = { due_date: ymdToClickUpMs(action.date), due_date_time: false };
    else if (action.type === "priority") body = { priority: PRIORITY[action.priority] };
    else if (action.type === "assignee") body = { assignees: { add: action.add, rem: action.remove } };
    await clickup(env, "PUT", path, body);
  }

  try {
    await clickup(env, "POST", `${path}/comment`, { comment_text: paperTrail(action, approverName, resolved), notify_all: false });
    return { changed: true, commented: true, detail: "changed and commented" };
  } catch (e) {
    return { changed: true, commented: false, detail: `changed, but the card comment failed: ${e.message}` };
  }
}

export { PRIORITY_NAME };
