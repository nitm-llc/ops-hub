// ============================================================================
// FLO — who's out, from the shared "Team Out" Google Calendar (Mark, Nov 11).
//
// People add an event when they're out ("Gaby out", "Diana PTO", or just
// "Gaby"). The calendar is shared with Flo's service account ("See all event
// details"); its id is saved on the Flo page (pm_settings out_calendar_id).
//
// Who an event is about: a team first name in its title, else whoever created
// it. All-day events cover their days (Google's end date is exclusive); timed
// events cover the days they touch.
//
// Short time off is NOT out (Mark, Nov 11): the HR sync writes titles like
// "FREEMAN, TIA - Time Off (1 hours)" as all-day events. Under FULL_DAY_HOURS
// (from "(N hours)" in the title, else a timed event's length) the person is
// still around: listed as short time off, but messaged and chased as usual.
//
// Flo uses this to leave people alone while they're out: the runs and answers
// see an "Out" line, and message_person won't DM someone who's out today.
// ============================================================================

import { getAccessToken, CALENDAR_SCOPE } from "./google.js";
import { getSetting, listPeople } from "./store.js";
import { PEOPLE, DEFAULT_TZ, todayYmd, addDays, ymd, weekdayName } from "./config.js";

const CAL_API = "https://www.googleapis.com/calendar/v3/calendars";
const CACHE_MS = 10 * 60 * 1000;
const LOOK_AHEAD_DAYS = 14;
export const FULL_DAY_HOURS = 6;

/** Hours off, from "(8 hours)" / "(2 hrs)" in the title, else a timed event's length; null = whole day(s). */
function hoursOff(ev) {
  const m = String(ev.summary || "").match(/\(\s*(\d+(?:\.\d+)?)\s*(?:hours?|hrs?|h)\s*\)/i);
  if (m) return Number(m[1]);
  if (ev.start?.dateTime && ev.end?.dateTime) return Math.round(((Date.parse(ev.end.dateTime) - Date.parse(ev.start.dateTime)) / 3600e3) * 10) / 10;
  return null;
}

let cache = null; // { key, at, value }
export const _clearTeamOutCache = () => {
  cache = null;
};

const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);

// First name → email, from ClickUp nicknames and everyone who has messaged Flo.
async function teamNames(env) {
  const names = new Map(Object.keys(PEOPLE).map((k) => [k, { name: cap(k), email: null }]));
  try {
    for (const p of await listPeople(env)) {
      const first = String(p.display_name || p.email).split(/\s+/)[0].toLowerCase();
      const local = String(p.email).split("@")[0].split(/[._-]/)[0].toLowerCase();
      for (const key of new Set([first, local])) {
        const hit = names.get(key);
        names.set(key, { name: hit?.name || cap(first), email: p.email });
      }
    }
  } catch {
    // People table missing: names from ClickUp still work.
  }
  // Nicknames people use for each other.
  if (names.has("cass")) names.set("cassandra", names.get("cass"));
  if (names.has("gaby")) names.set("gabriela", names.get("gaby"));
  if (names.has("samantha")) names.set("sam", names.get("samantha"));
  return names;
}

/** Days an event covers, as [firstYmd, lastYmd] in New York time. */
function eventDays(ev, tz) {
  if (ev.start?.date) return [ev.start.date, addDays(ev.end?.date || addDays(ev.start.date, 1), -1)];
  const from = ymd(Date.parse(ev.start?.dateTime), tz);
  const to = ymd(Date.parse(ev.end?.dateTime || ev.start?.dateTime) - 1, tz);
  return from ? [from, to || from] : null;
}

/** Who an event is about: every team first name in the title, else its creator. */
function eventPeople(ev, names) {
  const words = String(ev.summary || "").toLowerCase().match(/[a-z]+/g) || [];
  const hits = [...new Set(words.filter((w) => names.has(w)).map((w) => names.get(w)))];
  if (hits.length) return hits;
  const creator = String(ev.creator?.email || "").toLowerCase();
  for (const v of names.values()) if (v.email && v.email.toLowerCase() === creator) return [v];
  return [];
}

/**
 * Everyone out in the next two weeks:
 *   { configured, error?, away: [{ name, email, from, to, title }] }
 */
export async function teamOut(env, { tz = env.PM_TIMEZONE || DEFAULT_TZ, today = todayYmd(tz) } = {}) {
  const calId = await getSetting(env, "out_calendar_id").catch(() => null);
  if (!calId) return { configured: false, away: [] };
  const key = `${calId}|${today}`;
  if (cache?.key === key && Date.now() - cache.at < CACHE_MS) return cache.value;

  let value;
  try {
    const token = await getAccessToken(env, CALENDAR_SCOPE);
    const qs = new URLSearchParams({
      timeMin: new Date(Date.parse(`${today}T00:00:00Z`) - 86400e3).toISOString(),
      timeMax: new Date(Date.parse(`${addDays(today, LOOK_AHEAD_DAYS)}T23:59:59Z`)).toISOString(),
      singleEvents: "true",
      orderBy: "startTime",
      maxResults: "250",
    });
    const res = await fetch(`${CAL_API}/${encodeURIComponent(calId)}/events?${qs}`, { headers: { Authorization: `Bearer ${token}` } });
    const text = await res.text();
    if (!res.ok) {
      const hint = res.status === 404 || res.status === 403 ? " (is the calendar shared with Flo's service account?)" : "";
      throw new Error(`Google Calendar ${res.status}${hint}: ${text.slice(0, 160)}`);
    }
    const names = await teamNames(env);
    const away = [];
    for (const ev of JSON.parse(text).items || []) {
      if (ev.status === "cancelled" || ev.transparency === "transparent") continue;
      const days = eventDays(ev, tz);
      if (!days || days[1] < today) continue;
      const hours = hoursOff(ev);
      const partial = hours != null && hours < FULL_DAY_HOURS;
      for (const p of eventPeople(ev, names)) away.push({ name: p.name, email: p.email, from: days[0], to: days[1], title: String(ev.summary || "").slice(0, 80), hours, partial });
    }
    away.sort((a, b) => (a.from < b.from ? -1 : a.from > b.from ? 1 : 0));
    value = { configured: true, away };
  } catch (e) {
    value = { configured: true, error: e.message, away: [] };
  }
  cache = { key, at: Date.now(), value };
  return value;
}

const NICKNAMES = { gabriela: "gaby", cassandra: "cass", samantha: "samantha", sam: "samantha" };

/** Is this person out on `day`? Matches by email, else first name (or its nickname). */
export function isOut(away, { email, name }, day) {
  const raw = String(name || "").split(/\s+/)[0].toLowerCase();
  const first = NICKNAMES[raw] || raw;
  return away.find(
    (a) => !a.partial && a.from <= day && a.to >= day && ((email && a.email && a.email.toLowerCase() === String(email).toLowerCase()) || (first && (NICKNAMES[a.name.toLowerCase()] || a.name.toLowerCase()) === first))
  );
}

/** One line for Flo's context: "Gaby (today through Mon, Nov 16), Diana (Thu, Nov 12)". */
export function outLine(away, today) {
  const within = away.filter((a) => a.from <= addDays(today, 7));
  const when = (a) => {
    const from = a.from <= today ? "today" : weekdayName(a.from);
    return a.from === a.to || (a.from <= today && a.to === today) ? from : `${from} through ${weekdayName(a.to)}`;
  };
  const out = within.filter((a) => !a.partial).map((a) => `${a.name} (${when(a)})`);
  const short = within.filter((a) => a.partial).map((a) => `${a.name} (${when(a)}, ${a.hours} hr)`);
  return (out.length ? out.join(", ") : "nobody in the next week") + (short.length ? `. Short time off, still working those days: ${short.join(", ")}` : "");
}
