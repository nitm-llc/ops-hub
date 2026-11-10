// ============================================================================
// FLO — scheduled runs, driven by the Worker's existing 2-minute cron.
//
//   daily   weekdays 9am-noon New York: the daily check
//   weekly  Wednesdays 10am-1pm: next week's due dates + calendar balance
//
// Each runs once per day: (kind, day) is claimed in pm_runs before any work,
// so overlapping cron ticks can't double-run. The run happens in the lead's
// DM (Avery, else Mark: the first fallback person Flo can reach), so their
// "approve 1 and 3" applies the proposals directly. Check-ins and escalations
// go out through message_person.
//
// Off switch: pm_settings proactive = "off" (the Flo page toggle).
// "Run now" on the page sets pm_settings run_request; the next tick runs it.
// Scheduled invocations may run 15 minutes, so a run gets RUN_DEADLINE_MS.
// ============================================================================

import { runTurn } from "./brain.js";
import { findLead, resolvePerson } from "./outreach.js";
import { postChatMessage } from "./google.js";
import { toChatFormat } from "./index.js";
import { ensureTables, expireOld, expireScheduledPending, claimRun, finishRun, getSetting, deleteSetting, appendHistory, loadHistory, listPendingRows, logAction, getPerson, linkConversation } from "./store.js";
import { cardNameOf } from "./approvals.js";
import { DEFAULT_TZ, todayYmd, addDays, weekdayName } from "./config.js";
import { summaryCard, stageInfo } from "./cards.js";
import { DEFAULT_APP_HOSTNAME } from "../access.js";

export const DAILY_HOUR = 9;
export const WEEKLY_HOUR = 10;
// Only inside a window: a deploy at 3pm, or an outage, doesn't fire a late run.
const WINDOW_HOURS = 3;
const RUN_DEADLINE_MS = 8 * 60 * 1000;

function nyClock(tz, now = Date.now()) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", { timeZone: tz, weekday: "short", hour: "numeric", hour12: false })
      .formatToParts(new Date(now))
      .map((p) => [p.type, p.value])
  );
  return { weekday: parts.weekday, hour: Number(parts.hour) % 24 };
}

export function dueRuns(tz, now = Date.now()) {
  const { weekday, hour } = nyClock(tz, now);
  const weekdayOk = !["Sat", "Sun"].includes(weekday);
  const out = [];
  const inWindow = (h) => hour >= h && hour < h + WINDOW_HOURS;
  if (weekdayOk && inWindow(DAILY_HOUR)) out.push("daily");
  if (weekday === "Wed" && inWindow(WEEKLY_HOUR)) out.push("weekly");
  return out;
}

function nextWeek(today) {
  const dow = new Date(today + "T12:00:00Z").getUTCDay(); // 0 Sun
  const monday = addDays(today, ((8 - dow) % 7) || 7);
  return { from: monday, to: addDays(monday, 6) };
}

// What every summary looks like. One line per card, no proposal numbers, no
// talk about earlier runs: the card (cards.js) groups the lines by Stage.
const SHAPE =
  `Then reply in this shape, as briefly as possible. Skip any part that's empty. No proposal numbers, and don't mention earlier runs or older proposals.\n` +
  `*Done:* then one line per card: <url|short name>: everything you changed on it, in plain words ("due today, priority High, comment asking Gaby if anything's blocking it").\n` +
  `*Needs a person:* then one line per card for changes waiting for approval or anything you escalated: <url|short name>: what and why, in a few words.\n` +
  `*Messaged:* who, and about which cards, in one line.\n` +
  `If nothing needed doing, one line.`;

export function promptFor(kind, { leadFirst, today }) {
  if (kind === "weekly") {
    const w = nextWeek(today);
    return (
      `Scheduled run: the weekly routine. Your reply goes to ${leadFirst} as the planning summary.\n` +
      `1. Find every card with a Post Date next week (${weekdayName(w.from)} to ${weekdayName(w.to)}, ${w.from} to ${w.to}) that isn't done.\n` +
      `2. Set due dates (propose_change type due_date): Post Date minus 8 to 9 days, never on a weekend (use the Friday before), spread so no day piles up. Nothing beyond next week. Skip cards whose due date already fits.\n` +
      `3. Check the calendar balance for the next 3 weeks (calendar tool). If anything breaks a rule, send Cass ONE message (message_person, kind calendar) with the problems and suggested swaps. Don't propose Post Date changes.\n` +
      SHAPE
    );
  }
  return (
    `Scheduled run: the weekday daily check. Your reply goes to ${leadFirst} as the morning summary.\n` +
    `Follow the daily routine: find_cards with overdue=true and with_comments=true, then decide per card.\n` +
    `- ClickUp changes the routine calls for (due date to today, priority High, giving the card to the owner the Stage table names, the public check-in comment): propose_change. Routine ones are made right away.\n` +
    `- Check-ins the routine calls for (Post Date within the follow-up window and no active conversation on the card): message_person to the card's owner, kind check_in.\n` +
    `- Escalations (posts within 3 workdays and still stuck after an earlier follow-up, a card in an unused stage, or a workload question): message_person to Avery, kind escalation.\n` +
    SHAPE
  );
}

const TITLES = { daily: "Daily check", weekly: "Weekly planning" };

// The summary goes out as a card (same words, laid out like ClickUp). If Chat
// refuses the card, the plain text goes instead, so the summary always arrives.
async function postSummary(env, space, { title, subtitle, reply, text, stages, approveNames }) {
  const pageUrl = `https://${env.APP_HOSTNAME || DEFAULT_APP_HOSTNAME}/flo/`;
  try {
    return await postChatMessage(env, { space, thread: null, card: summaryCard({ title, subtitle, body: reply, stages, pageUrl, approveNames }) });
  } catch (e) {
    await logAction(env, { conv: null, event: "card_post_failed", action: { space }, result: e.message });
    return postChatMessage(env, { space, thread: null, text });
  }
}

export async function runScheduled(env, kind, { trigger = "schedule", tz = env.PM_TIMEZONE || DEFAULT_TZ } = {}) {
  const today = todayYmd(tz);
  const lead = await findLead(env);
  if (!lead) {
    const msg = "No one to send it to: none of the fallback people (Avery, Mark) has opened a chat with Flo yet.";
    await logAction(env, { conv: null, event: "run_skipped", action: { kind, trigger }, result: msg });
    return { ok: false, message: msg };
  }
  const leadFirst = String(lead.name).split(/\s+/)[0];
  const conv = { space: lead.dmSpace, thread: "" };
  // Earlier days' unapproved suggestions are replaced by today's run.
  const cleared = await expireScheduledPending(env, conv, new Date(Date.parse(`${today}T05:00:00Z`)).toISOString());
  if (cleared) await logAction(env, { conv, actor: "flo-scheduled", event: "pending_cleared", action: { kind }, result: `${cleared} unapproved suggestion(s) from earlier runs` });
  const reply = await runTurn(env, {
    conv,
    sender: { email: "flo-scheduled", displayName: `Flo (${TITLES[kind]})` },
    text: promptFor(kind, { leadFirst, today }),
    tz,
    today,
    history: [],
    approvedNumbers: [],
    deadline: Date.now() + RUN_DEADLINE_MS,
    scheduled: true,
  });
  const title = TITLES[kind];
  const text = toChatFormat(`*${title}, ${weekdayName(today)}*\n${reply}`);
  const stages = await stageInfo(env, reply).catch(() => new Map());
  const approveNames = [...new Set((await listPendingRows(env, conv)).map(cardNameOf))];
  try {
    await postSummary(env, lead.dmSpace, { title, subtitle: weekdayName(today), reply: toChatFormat(reply), text, stages, approveNames });
    await appendHistory(env, conv, [{ role: "assistant", text }]);
  } catch (e) {
    await logAction(env, { conv, event: "async_post_failed", action: { kind }, result: e.message });
    return { ok: false, message: `Ran, but couldn't post to ${leadFirst}: ${e.message}` };
  }
  // Copies (PM_DIGEST_CC, e.g. "Tia"), linked to the lead's conversation so
  // they can approve the same numbers. Whoever approves first makes the change.
  const copied = [];
  for (const name of String(env.PM_DIGEST_CC || "").split(/[\s,;]+/).filter(Boolean)) {
    try {
      const p = await resolvePerson(env, name);
      if (!p || p.email === lead.email) continue;
      const dm = (await getPerson(env, p.email))?.dm_space;
      if (!dm) {
        await logAction(env, { conv: null, event: "digest_copy_skipped", action: { to: p.email, kind }, result: "hasn't opened a chat with Flo" });
        continue;
      }
      const copy = toChatFormat(`*${TITLES[kind]}, ${weekdayName(today)}* (copy: ${leadFirst} has it too, either of you can approve)\n${reply}`);
      await postSummary(env, dm, { title, subtitle: `${weekdayName(today)} · copy, ${leadFirst} has it too, either of you can approve`, reply: toChatFormat(reply), text: copy, stages, approveNames });
      await appendHistory(env, { space: dm, thread: "" }, [{ role: "assistant", text: copy }]);
      await linkConversation(env, dm, conv);
      copied.push(String(p.name).split(/\s+/)[0]);
    } catch (e) {
      await logAction(env, { conv: null, event: "digest_copy_failed", action: { to: name, kind }, result: e.message });
    }
  }
  const to = [leadFirst, ...copied].join(" and ");
  await logAction(env, { conv, actor: "flo-scheduled", event: `run_${kind}`, action: { trigger }, result: `sent to ${to}` });
  return { ok: true, message: `Sent to ${to}.`, text };
}

/**
 * "Send me a preview" on the Flo page: the latest summary Flo sent the lead,
 * re-sent as a card to the person asking. Nothing runs and nothing changes.
 */
export async function sendPreview(env, email) {
  const me = await getPerson(env, email);
  if (!me?.dm_space) return { ok: false, message: "Send Flo a message in Google Chat first, so it can reach you." };
  const lead = await findLead(env);
  const history = lead ? await loadHistory(env, { space: lead.dmSpace, thread: "" }) : [];
  const last = [...history].reverse().find((m) => m.role === "assistant" && /^\*(Daily check|Weekly planning), /.test(m.text || ""));
  if (!last) return { ok: false, message: "There's no summary yet to preview." };
  const [first, ...rest] = last.text.split("\n");
  const m = first.match(/^\*(Daily check|Weekly planning), ([^*]+)\*/);
  const stages = await stageInfo(env, rest.join("\n")).catch(() => new Map());
  const approveNames = [...new Set((await listPendingRows(env, { space: lead.dmSpace, thread: "" })).map(cardNameOf))];
  await postSummary(env, me.dm_space, { title: m[1], subtitle: `${m[2]} · preview`, reply: rest.join("\n"), text: last.text, stages, approveNames });
  return { ok: true, message: "Sent to your chat with Flo." };
}

/** Called from the Worker's scheduled() every 2 minutes. Cheap when idle. */
export async function floCron(env, now = Date.now()) {
  if (!env.DB || !env.ANTHROPIC_API_KEY || !env.GOOGLE_CHAT_SA_KEY) return;
  await ensureTables(env);
  const tz = env.PM_TIMEZONE || DEFAULT_TZ;

  // A "run now" from the Flo page runs even when the schedule is switched off.
  const req = await getSetting(env, "run_request");
  if (req) {
    await deleteSetting(env, "run_request");
    const { kind, by } = JSON.parse(req);
    if (kind === "daily" || kind === "weekly") {
      const key = new Date(now).toISOString();
      await claimRun(env, `${kind}:manual`, key, `manual by ${by || "?"}`);
      await expireOld(env);
      const r = await runScheduled(env, kind, { trigger: "manual", tz }).catch((e) => ({ ok: false, message: e.message }));
      await finishRun(env, `${kind}:manual`, key, r.message);
    }
    return;
  }

  if ((await getSetting(env, "proactive", "on")) === "off") return;
  const day = todayYmd(tz, now);
  for (const kind of dueRuns(tz, now)) {
    if (!(await claimRun(env, kind, day, "schedule"))) continue;
    await expireOld(env);
    const r = await runScheduled(env, kind, { tz }).catch((e) => ({ ok: false, message: e.message }));
    await finishRun(env, kind, day, r.message);
    return; // one run per tick; the weekly one follows on the next tick
  }
}
