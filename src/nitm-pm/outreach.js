// ============================================================================
// FLO — reaching people. Flo DMs the person it needs; if it can't, it sends
// the message to the fallback people (Avery, then Mark) instead, saying who
// it was for.
//
// GOOGLE'S RULES (checked 2026-11-04): a Chat app can post into an EXISTING
// DM with app auth, but it can't create a DM (spaces.setup needs user auth)
// and can't find one by email (findDirectMessage needs a numeric user id). So
// Flo learns each person's DM the first time they message it (pm_people), and
// someone who has never said hi can't be reached directly.
//
// Names -> emails come from ClickUp's member list at runtime, so no team
// emails live in this (public) repo.
//
// LIMITS, so nobody gets spammed: one message per person per card per day,
// MAX_PER_PERSON per person per day, MAX_PER_DAY overall. Everything lands
// in pm_outreach and shows on the Flo page.
// ============================================================================

import { clickup } from "./clickup.js";
import { getPerson, logOutreach, countOutreach, appendHistory, logAction } from "./store.js";
import { postChatMessage } from "./google.js";
import { teamOut, isOut } from "./teamout.js";
import { todayYmd, DEFAULT_TZ, PEOPLE, weekdayName } from "./config.js";

const MAX_PER_PERSON = 6;
const MAX_PER_DAY = 40;
const TEAM_CACHE_MS = 10 * 60 * 1000;
let _team = null; // { at, members: [{ id, name, first, email }] }
export const _clearTeamCache = () => {
  _team = null;
};

const lower = (s) => String(s || "").trim().toLowerCase();

async function team(env) {
  if (_team && Date.now() - _team.at < TEAM_CACHE_MS) return _team.members;
  const body = await clickup(env, "GET", "/team");
  const ws = (body.teams || []).find((t) => String(t.id) === String(env.CLICKUP_TEAM_ID || "9017029478")) || (body.teams || [])[0] || {};
  const members = (ws.members || [])
    .map((m) => m.user)
    .filter((u) => u && u.email)
    .map((u) => ({ id: Number(u.id), name: u.username || u.email, first: lower(String(u.username || "").split(/\s+/)[0]), email: lower(u.email) }));
  _team = { at: Date.now(), members };
  return members;
}

/**
 * "Gaby", "gabriela", "Cass", an email, or a ClickUp id -> { email, name } or null.
 * The team's nicknames (config PEOPLE) win; otherwise short names match the
 * start of a ClickUp first name, and only when exactly one person matches.
 */
export async function resolvePerson(env, who) {
  const w = lower(who);
  if (!w) return null;
  if (w.includes("@")) {
    const m = (await team(env)).find((x) => x.email === w);
    return { email: w, name: m?.name || w };
  }
  const members = await team(env);
  // The team's own names first: "Gaby" is Gabriela in ClickUp, "Cass" is Cassandra.
  if (PEOPLE[w]) {
    const m = members.find((x) => x.id === PEOPLE[w]);
    if (m) return { email: m.email, name: m.name };
  }
  if (/^\d+$/.test(w)) {
    const m = members.find((x) => x.id === Number(w));
    return m ? { email: m.email, name: m.name } : null;
  }
  const exact = members.filter((x) => x.first === w || lower(x.name) === w);
  const pool = exact.length ? exact : members.filter((x) => x.first.startsWith(w) || w.startsWith(x.first));
  return pool.length === 1 ? { email: pool[0].email, name: pool[0].name } : null;
}

export function fallbackNames(env) {
  return String(env.PM_FALLBACK || "Avery,Mark").split(/[\s,;]+/).filter(Boolean);
}

// The first fallback person Flo can actually reach: where scheduled digests
// and non-approvers' proposals go.
export async function findLead(env) {
  for (const n of fallbackNames(env)) {
    const p = await resolvePerson(env, n).catch(() => null);
    if (!p) continue;
    const row = await getPerson(env, p.email);
    if (row?.dm_space) return { ...p, dmSpace: row.dm_space };
  }
  return null;
}

async function dmTo(env, email) {
  const row = await getPerson(env, email);
  return row?.dm_space || null;
}

async function deliver(env, { dmSpace, text }) {
  await postChatMessage(env, { space: dmSpace, thread: null, text });
  // So a reply in that DM has the context of what Flo said.
  await appendHistory(env, { space: dmSpace, thread: "" }, [{ role: "assistant", text }]);
}

const firstOf = (name) => String(name || "").trim().split(/\s+/)[0] || "them";

/**
 * Send `text` to a person. Returns a short, honest result for the model/page.
 * kind: check_in | escalation | calendar | approval | digest | other
 */
export async function messagePerson(env, { person, text, taskId = null, kind = "other", tz = DEFAULT_TZ, from = "Flo" }) {
  const day = todayYmd(tz);
  const body = String(text || "").trim();
  if (!body) return { sent: false, message: "Nothing to send." };
  if (body.length > 1500) return { sent: false, message: "Keep it under 1500 characters." };

  const p = await resolvePerson(env, person);
  if (!p) return { sent: false, message: `I don't know who "${person}" is. Use a first name from ClickUp.` };

  // Out today on the Team Out calendar: leave them alone.
  const out = await teamOut(env, { tz, today: day }).catch(() => null);
  const away = out ? isOut(out.away, p, day) : null;
  if (away) return { sent: false, message: `${away.name} is out (Team Out calendar, through ${weekdayName(away.to)}). Not sent.` };

  if (kind !== "digest" && kind !== "approval") {
    if (taskId && (await countOutreach(env, day, { toEmail: p.email, taskId })) > 0) {
      return { sent: false, message: `Already messaged ${firstOf(p.name)} about this card today. Not sending again.` };
    }
    if ((await countOutreach(env, day, { toEmail: p.email })) >= MAX_PER_PERSON) {
      return { sent: false, message: `Already sent ${firstOf(p.name)} ${MAX_PER_PERSON} messages today. Holding off.` };
    }
    if ((await countOutreach(env, day)) >= MAX_PER_DAY) {
      return { sent: false, message: `Hit today's limit of ${MAX_PER_DAY} messages. Holding off.` };
    }
  }

  const base = { day, to_email: p.email, to_name: p.name, task_id: taskId, kind, text: body };
  const direct = await dmTo(env, p.email);
  if (direct) {
    try {
      await deliver(env, { dmSpace: direct, text: body });
      await logOutreach(env, { ...base, delivered_to: p.email, status: "sent" });
      return { sent: true, to: firstOf(p.name), message: `Sent to ${firstOf(p.name)}.` };
    } catch (e) {
      await logOutreach(env, { ...base, status: "failed", detail: e.message });
      // fall through to the fallback people
      return fallback(env, { base, p, body, reason: `I couldn't post to ${firstOf(p.name)}'s chat (${e.message})` });
    }
  }
  return fallback(env, { base, p, body, reason: `${firstOf(p.name)} hasn't opened a chat with me yet, so I can't message them directly` });
}

async function fallback(env, { base, p, body, reason }) {
  const reached = [];
  const errors = [];
  for (const n of fallbackNames(env)) {
    const f = await resolvePerson(env, n).catch(() => null);
    if (!f || f.email === p.email) continue;
    const dm = await dmTo(env, f.email);
    if (!dm) continue;
    const text = `For *${firstOf(p.name)}* (can't reach them yet, please pass on):\n${body}`;
    try {
      await deliver(env, { dmSpace: dm, text });
      reached.push(firstOf(f.name));
    } catch (e) {
      errors.push(`${firstOf(f.name)}: ${e.message}`);
    }
  }
  if (reached.length) {
    await logOutreach(env, { ...base, delivered_to: reached.join(", "), status: "fallback", detail: reason });
    return { sent: true, via_fallback: true, to: reached.join(" and "), message: `${reason}. Sent it to ${reached.join(" and ")} to pass on.` };
  }
  await logOutreach(env, { ...base, status: "undelivered", detail: [reason, ...errors].join("; ") });
  await logAction(env, { conv: null, event: "outreach_undelivered", action: { to: p.email, kind: base.kind, task_id: base.task_id }, result: reason });
  return {
    sent: false,
    message: `${reason}, and none of the fallback people (${fallbackNames(env).join(", ")}) have opened a chat with me either. Nothing was sent.`,
  };
}
