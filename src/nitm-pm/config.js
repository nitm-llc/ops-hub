// ============================================================================
// NITM PM — list config, stage map and date helpers. Pure, no I/O.
//
// This is the CURRENT TikTok list (TikTok space > Production), where the real
// stage is the "Stage" dropdown and the ClickUp status means nothing. The
// TikTok router works on the NEW 12-stage list (Organic Social > TikTok
// Production). When the team moves over, swap LIST_ID / FIELDS / STAGE_MAP here.
// ============================================================================

export const LIST_ID = "901704217808";
export const LIST_URL = "https://app.clickup.com/9017029478/v/li/901704217808";

export const FIELDS = {
  stage: "540a20f7-53de-4cb4-b256-b9990cbea8b5", // dropdown
  type: "4c0d2e72-7b03-4a8f-97b3-f5382549c8a5", // dropdown
  postDate: "f95ca719-29ba-4599-a99d-20b993a100ae", // date
  googleDoc: "807486c6-be93-4d6f-8e38-2af7bd4b56f7", // url
};

// ClickUp user ids. Config, not secrets (they are in every task payload).
export const PEOPLE = {
  diana: 89106214,
  karli: 101381768,
  kristine: 89106213,
  tia: 89106216,
  gaby: 89133528,
  cass: 89173497,
  avery: 89106215,
};
export const PERSON_NAME = Object.fromEntries(
  Object.entries(PEOPLE).map(([k, id]) => [id, k[0].toUpperCase() + k.slice(1)])
);

const norm = (s) => String(s || "").trim().toLowerCase();

export const DONE_STAGES = ["Scheduled 📅", "Posted", "Posted To All Platforms"];
export const PAUSED_STAGES = ["Schedule On Hold", "⚠️CAN NOT FINISH⚠️"];
export const UNUSED_STAGES = ["T: Tia Review", "T: TikTok Edit", "T: Kristine - Changes", "To Be Posted Manually"];
const REVIEW_STAGES = ["T: Nurses Outline Review", "T: Drive Review", "T: TikTok Review", "Review / QA"];
const KRISTINE_STAGES = ["T: Kristine To Film", "T: Only Voiceover"];
const EDIT_STAGES = ["Ready To Edit / Design", "Editing / Designing", "Changes", "Rework"];
const TIA_TYPES = ["Funny", "Sim Funny", "Trial Reel", "Inspirational"];

export const isDoneStage = (s) => DONE_STAGES.map(norm).includes(norm(s));
export const isPausedStage = (s) => PAUSED_STAGES.map(norm).includes(norm(s));
export const isUnusedStage = (s) => UNUSED_STAGES.map(norm).includes(norm(s));
export const isKristineStage = (s) => KRISTINE_STAGES.map(norm).includes(norm(s));

// Who should own a card at this stage, per the instructions' stage table.
// null = "leave the owner as is" (or done/paused/unused: nobody to assign).
export function expectedOwner(stage, type) {
  const s = norm(stage);
  if (REVIEW_STAGES.map(norm).includes(s)) return PEOPLE.diana;
  if (KRISTINE_STAGES.map(norm).includes(s)) return PEOPLE.kristine;
  if (EDIT_STAGES.map(norm).includes(s)) return TIA_TYPES.map(norm).includes(norm(type)) ? PEOPLE.tia : PEOPLE.gaby;
  if (s === norm("Ready to Schedule")) return PEOPLE.cass;
  return null;
}

// ---------------------------------------------------------------------------
// Dates. The team works in New York time; ClickUp date-only values are
// timestamps, so always compare calendar days in that zone, never raw ms.
// ---------------------------------------------------------------------------
export const DEFAULT_TZ = "America/New_York";

export function ymd(ms, tz = DEFAULT_TZ) {
  if (ms === null || ms === undefined || ms === "") return null;
  const d = new Date(Number(ms));
  if (Number.isNaN(d.getTime())) return null;
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
}

export const todayYmd = (tz = DEFAULT_TZ, now = Date.now()) => ymd(now, tz);

export function isYmd(s) {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(s + "T00:00:00Z"));
}

export function addDays(dateYmd, n) {
  const d = new Date(dateYmd + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

const dow = (dateYmd) => new Date(dateYmd + "T12:00:00Z").getUTCDay(); // 0 Sun .. 6 Sat
export const isWeekend = (dateYmd) => [0, 6].includes(dow(dateYmd));
export const weekdayName = (dateYmd) =>
  new Date(dateYmd + "T12:00:00Z").toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", timeZone: "UTC" });

// Workdays strictly after `from` up to and including `to`. Negative if `to` is past.
export function workdaysBetween(from, to) {
  if (!from || !to) return null;
  if (to === from) return 0;
  const sign = to > from ? 1 : -1;
  let n = 0;
  let d = from;
  while (d !== to) {
    d = addDays(d, sign);
    if (!isWeekend(d)) n += sign;
  }
  return n;
}

// A calendar day as a ClickUp date-only timestamp. 16:00 UTC is mid-day in
// New York in both EST and EDT, so the day can't slip in either direction.
export const ymdToClickUpMs = (dateYmd) => Date.parse(dateYmd + "T16:00:00Z");

// Start/end of a New York calendar day as ms, for API range filters. Generous
// by a few hours on each side; callers re-check the day with ymd().
export const rangeStartMs = (dateYmd) => Date.parse(dateYmd + "T00:00:00Z") - 6 * 3600e3;
export const rangeEndMs = (dateYmd) => Date.parse(dateYmd + "T23:59:59Z") + 6 * 3600e3;

export const PRIORITY = { urgent: 1, high: 2, normal: 3, low: 4 };
export const PRIORITY_NAME = { 1: "urgent", 2: "high", 3: "normal", 4: "low" };
