// ============================================================================
// TIKTOK PRODUCTION ROUTING RULES
// ----------------------------------------------------------------------------
// Who owns each stage of Organic Social > TikTok Production.
//
// The rules are DATA. Day to day they are edited at /tiktok-router/ and stored
// in D1 (tiktok_router_rules); DEFAULT_RULES below is only the starting point,
// used until someone saves from the page, and restored by "Reset to defaults".
// If you find yourself hardcoding a person into a function here, you have
// rebuilt the bottleneck the page exists to remove.
//
// Pure functions, no I/O, so this can move into the creative engine's ClickUp
// sync unchanged.
// ============================================================================

export const WORKSPACE_ID = "9017029478";
export const LIST_ID = "1600440000001184"; // Organic Social > TikTok Production

export const FIELDS = {
  writer: "53eaec4d-cf19-405b-9c68-e4ee30f63285", // People field
  format: "1b7ca213-f3de-40f4-8f40-ef2d6e2cfdbd", // Dropdown field
};

// ClickUp user ids — used only to build the defaults.
export const PEOPLE = {
  tia: 89106216,
  diana: 89106214,
  karli: 101381768,
  kristine: 89106213,
  gaby: 89133528,
  cass: 89173497,
  avery: 89106215,
  amber: null, // not in ClickUp yet
};

// The list's stages in order, lowercase as ClickUp reports them. The page reads
// the live list too; this is the fallback and the order for anything unknown.
export const STAGES = [
  "draft", "package", "script review", "prep", "film", "voiceover", "edit",
  "accuracy check", "titles", "final review", "schedule", "posted", "cancelled",
];

// Per stage, one of:
//   { mode: "off" }                      leave the card alone
//   { mode: "person", owner: <id|null> } always this person (null = skip)
//   { mode: "review" }                   the review rule below
//   { mode: "edit" }                     the edit rule below
export const MODES = ["off", "person", "review", "edit"];

export const DEFAULT_RULES = {
  stages: {
    "draft": { mode: "off" },
    "package": { mode: "person", owner: PEOPLE.tia },
    "script review": { mode: "review" },
    "prep": { mode: "person", owner: PEOPLE.amber },
    "film": { mode: "off" }, // weekly rotation — the AI PM will own this
    "voiceover": { mode: "person", owner: PEOPLE.kristine },
    "edit": { mode: "edit" },
    "accuracy check": { mode: "review" },
    "titles": { mode: "edit" },
    "final review": { mode: "review" },
    "schedule": { mode: "person", owner: PEOPLE.cass },
    "posted": { mode: "off" },
    "cancelled": { mode: "off" },
  },
  // Reviews go to the other nurse: Diana's scripts to Karli, everyone else's to Diana.
  review: { writer: PEOPLE.diana, then: PEOPLE.karli, otherwise: PEOPLE.diana },
  // Funny/Trend are edited by Tia; everything else, including no Format, by Gaby.
  edit: { formats: ["Funny", "Trend"], then: PEOPLE.tia, otherwise: PEOPLE.gaby },
  // Adding this tag raises priority to High and pings `notify` in an assigned comment.
  roadblock: {
    enabled: true,
    tag: "roadblock",
    priority: 2, // ClickUp: 1 urgent, 2 high, 3 normal, 4 low
    notify: PEOPLE.avery,
    notify_name: "Avery Pless", // display name, so "{person}" can be filled without a ClickUp call
    // "{person}" becomes notify_name's first name when the comment is posted.
    comment: "Roadblock added. {person}, can you help get this unstuck?",
  },
};

// Who should own `stage` for this card?
//   { action: "assign", owner }  |  { action: "skip", reason }
// `card` is { writerId: number|null, format: string|null } (format = option NAME).
export function ownerFor(rules, stage, card) {
  const s = rules.stages?.[stage];
  if (!s || s.mode === "off") return { action: "skip", reason: `left alone: "${stage}" is not routed` };

  let owner = null;
  if (s.mode === "person") owner = s.owner;
  else if (s.mode === "review") {
    const r = rules.review;
    owner = card.writerId != null && card.writerId === r.writer ? r.then : r.otherwise;
  } else if (s.mode === "edit") {
    const e = rules.edit;
    const wanted = (e.formats || []).map((f) => String(f).toLowerCase());
    owner = card.format && wanted.includes(card.format.toLowerCase()) ? e.then : e.otherwise;
  }
  if (owner == null) return { action: "skip", reason: `skipped: no owner set for "${stage}"` };
  return { action: "assign", owner: Number(owner) };
}

// Validate + normalise rules arriving from the page. Throws Error(message) on
// anything malformed, so a bad save can never reach the router.
export function normalizeRules(input) {
  if (!input || typeof input !== "object") throw new Error("Rules must be an object.");
  const userId = (v, what) => {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) throw new Error(`${what} must be a ClickUp user.`);
    return n;
  };

  const stages = {};
  for (const [rawName, cfg] of Object.entries(input.stages || {})) {
    const name = String(rawName).trim().toLowerCase();
    if (!name || name.length > 80) throw new Error("Stage names must be 1-80 characters.");
    const mode = cfg?.mode;
    if (!MODES.includes(mode)) throw new Error(`Unknown setting for "${name}".`);
    stages[name] = mode === "person" ? { mode, owner: userId(cfg.owner, `Owner of "${name}"`) } : { mode };
  }

  const r = input.review || {};
  const e = input.edit || {};
  const b = input.roadblock || {};
  const formats = Array.isArray(e.formats) ? e.formats.map((f) => String(f).trim()).filter(Boolean) : [];
  const comment = String(b.comment ?? "").trim();
  if (comment.length > 1000) throw new Error("The roadblock comment is too long.");

  return {
    stages,
    review: {
      writer: userId(r.writer, "Review rule writer"),
      then: userId(r.then, "Review rule reviewer"),
      otherwise: userId(r.otherwise, "Review rule default reviewer"),
    },
    edit: {
      formats: [...new Set(formats)].slice(0, 50),
      then: userId(e.then, "Edit rule editor"),
      otherwise: userId(e.otherwise, "Edit rule default editor"),
    },
    roadblock: {
      enabled: b.enabled !== false,
      tag: String(b.tag || "roadblock").trim().toLowerCase(),
      priority: [1, 2, 3, 4].includes(Number(b.priority)) ? Number(b.priority) : 2,
      notify: userId(b.notify, "Roadblock person"),
      notify_name: b.notify_name ? String(b.notify_name).trim().slice(0, 120) : null,
      comment: comment || DEFAULT_RULES.roadblock.comment,
    },
  };
}
