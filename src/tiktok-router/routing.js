// ============================================================================
// TIKTOK PRODUCTION ROUTING TABLE — the one file to edit to change a rule.
// ----------------------------------------------------------------------------
// Who owns each stage of Organic Social > TikTok Production. Change a rule
// here, run `npm test`, commit, push, `npx wrangler deploy`. Nothing else in
// src/tiktok-router/ needs to change for a routing change.
//
// Kept as plain data + tiny functions (no I/O) so it can be lifted into a
// bigger system — the creative engine's ClickUp sync — without rewriting it.
// ============================================================================

export const WORKSPACE_ID = "9017029478";
export const LIST_ID = "1600440000001184"; // Organic Social > TikTok Production

export const FIELDS = {
  writer: "53eaec4d-cf19-405b-9c68-e4ee30f63285", // People field
  format: "1b7ca213-f3de-40f4-8f40-ef2d6e2cfdbd", // Dropdown field
};

// ClickUp user ids.
export const PEOPLE = {
  tia: 89106216,
  diana: 89106214,
  karli: 101381768,
  kristine: 89106213,
  gaby: 89133528,
  cass: 89173497,
  avery: 89106215,
  // Not in ClickUp yet. Put her id here once she's invited; until then Prep is
  // skipped (logged, not errored) and the card keeps whoever it had.
  amber: null,
};

// Formats Tia edits. Everything else — including no Format set — goes to Gaby.
const TIA_EDIT_FORMATS = ["funny", "trend"];

// Reviews go to the other nurse: Diana's scripts to Karli, everyone else's to Diana.
const reviewer = (card) => (card.writerId === PEOPLE.diana ? PEOPLE.karli : PEOPLE.diana);

const editor = (card) =>
  card.format && TIA_EDIT_FORMATS.includes(card.format.toLowerCase()) ? PEOPLE.tia : PEOPLE.gaby;

// Stage name (lowercase, as ClickUp reports it) -> owner.
// `card` is { writerId: number|null, format: string|null } (format = option NAME).
// Stages not listed are left alone: draft, film (weekly rotation — the AI PM
// will own that), posted, cancelled.
export const ROUTES = {
  "package": () => PEOPLE.tia,
  "script review": reviewer,
  "prep": () => PEOPLE.amber,
  "voiceover": () => PEOPLE.kristine,
  "edit": editor,
  "accuracy check": reviewer,
  "titles": editor,
  "final review": reviewer,
  "schedule": () => PEOPLE.cass,
};

// Adding this tag raises priority to High and pings Avery in an assigned comment.
export const ROADBLOCK = {
  tag: "roadblock",
  priority: 2, // ClickUp: 1 urgent, 2 high, 3 normal, 4 low
  notify: PEOPLE.avery,
  comment: "Roadblock added. Avery, can you help get this unstuck?",
};
