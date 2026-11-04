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
import { ensureTables, expireOld, claimRun, finishRun, getSetting, deleteSetting, appendHistory, logAction, getPerson, linkConversation } from "./store.js";
import { DEFAULT_TZ, todayYmd, addDays, weekdayName } from "./config.js";

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

export function promptFor(kind, { leadFirst, today }) {
  if (kind === "weekly") {
    const w = nextWeek(today);
    return (
      `Scheduled run: the weekly routine. Your reply goes to ${leadFirst} as the planning summary, and they approve proposals here.\n` +
      `1. Find every card with a Post Date next week (${weekdayName(w.from)} to ${weekdayName(w.to)}, ${w.from} to ${w.to}) that isn't done.\n` +
      `2. Propose due dates (propose_change type due_date): Post Date minus 8 to 9 days, never on a weekend (use the Friday before), spread so no day piles up. Nothing beyond next week. Skip cards whose due date already fits.\n` +
      `3. Check the calendar balance for the next 3 weeks (calendar tool). If anything breaks a rule, send Cass ONE message (message_person, kind calendar) with the problems and suggested swaps. Don't propose Post Date changes.\n` +
      `Then reply, as briefly as possible: the numbered proposals (one line each) and, in one line, what you sent Cass. If nothing needs doing, one line.`
    );
  }
  return (
    `Scheduled run: the weekday daily check. Your reply goes to ${leadFirst} as the morning summary, and they approve proposals here.\n` +
    `Follow the daily routine: find_cards with overdue=true and with_comments=true, then decide per card.\n` +
    `- ClickUp changes (due date to today, priority High, fixing an owner who doesn't match the stage, the public check-in comment): propose_change.\n` +
    `- Check-ins the routine calls for (Post Date within the follow-up window and no active conversation on the card): message_person to the card's owner, kind check_in.\n` +
    `- Escalations (posts within 3 workdays and still stuck after an earlier follow-up, a card in an unused stage, or a workload question): message_person to Avery, kind escalation.\n` +
    `Then reply, as briefly as possible: the numbered proposals (one line each), one line for who you messaged, one line for cards left alone. If nothing is stuck, one line.`
  );
}

const TITLES = { daily: "Daily check", weekly: "Weekly planning" };

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
  const text = toChatFormat(`*${TITLES[kind]}, ${weekdayName(today)}*\n${reply}`);
  try {
    await postChatMessage(env, { space: lead.dmSpace, thread: null, text });
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
      await postChatMessage(env, { space: dm, thread: null, text: copy });
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
