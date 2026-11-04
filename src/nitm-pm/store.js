// ============================================================================
// NITM PM — D1 storage (env.DB, content-calendar). Mirrors migrations/0010.
//
// A "conversation" is a space + thread. In a DM the thread is stored as ""
// because Chat gives every DM message its own thread; keying DMs by thread
// would lose the history and the pending numbers between messages.
// ============================================================================

export const PENDING_TTL_MS = 48 * 3600 * 1000;
const HISTORY_KEEP = 16;
const HISTORY_CHARS = 4000;

let _ready = false;
export async function ensureTables(env) {
  if (_ready) return;
  const stmts = [
    `CREATE TABLE IF NOT EXISTS pm_threads (
       space      TEXT NOT NULL,
       thread     TEXT NOT NULL,
       messages   TEXT NOT NULL,
       updated_at TEXT NOT NULL,
       PRIMARY KEY (space, thread)
     )`,
    `CREATE TABLE IF NOT EXISTS pm_pending_actions (
       id          INTEGER PRIMARY KEY AUTOINCREMENT,
       number      INTEGER NOT NULL,
       space       TEXT NOT NULL,
       thread      TEXT NOT NULL,
       proposed_by TEXT,
       action      TEXT NOT NULL,
       summary     TEXT,
       status      TEXT NOT NULL DEFAULT 'pending',
       created_at  TEXT NOT NULL,
       decided_by  TEXT,
       decided_at  TEXT,
       result      TEXT,
       UNIQUE (space, thread, number)
     )`,
    `CREATE INDEX IF NOT EXISTS idx_pm_pending_conv ON pm_pending_actions (space, thread, status)`,
    `CREATE TABLE IF NOT EXISTS pm_instructions (
       id         INTEGER PRIMARY KEY CHECK (id = 1),
       text       TEXT NOT NULL,
       updated_at TEXT NOT NULL,
       updated_by TEXT
     )`,
    `CREATE TABLE IF NOT EXISTS pm_instruction_changes (
       id    INTEGER PRIMARY KEY AUTOINCREMENT,
       at    TEXT NOT NULL,
       by    TEXT,
       note  TEXT,
       text  TEXT
     )`,
    `CREATE TABLE IF NOT EXISTS pm_action_log (
       id      INTEGER PRIMARY KEY AUTOINCREMENT,
       at      TEXT NOT NULL,
       space   TEXT,
       thread  TEXT,
       number  INTEGER,
       actor   TEXT,
       event   TEXT NOT NULL,
       action  TEXT,
       result  TEXT
     )`,
  ];
  for (const s of stmts) await env.DB.prepare(s).run();
  _ready = true;
}
export const _resetReady = () => {
  _ready = false;
};

const nowIso = () => new Date().toISOString();

// ---- conversation history -------------------------------------------------
export async function loadHistory(env, conv) {
  const row = await env.DB.prepare("SELECT messages FROM pm_threads WHERE space = ? AND thread = ?")
    .bind(conv.space, conv.thread)
    .first();
  if (!row) return [];
  try {
    return JSON.parse(row.messages);
  } catch {
    return [];
  }
}

export async function appendHistory(env, conv, entries) {
  const prev = await loadHistory(env, conv);
  const next = prev
    .concat(entries.map((e) => ({ ...e, text: String(e.text || "").slice(0, HISTORY_CHARS), at: nowIso() })))
    .slice(-HISTORY_KEEP);
  await env.DB.prepare(
    `INSERT INTO pm_threads (space, thread, messages, updated_at) VALUES (?, ?, ?, ?)
     ON CONFLICT(space, thread) DO UPDATE SET messages = excluded.messages, updated_at = excluded.updated_at`
  )
    .bind(conv.space, conv.thread, JSON.stringify(next), nowIso())
    .run();
}

// ---- pending actions --------------------------------------------------------
export async function expireOld(env, now = Date.now()) {
  const cutoff = new Date(now - PENDING_TTL_MS).toISOString();
  await env.DB.prepare(
    "UPDATE pm_pending_actions SET status = 'expired', decided_at = ? WHERE status = 'pending' AND created_at < ?"
  )
    .bind(nowIso(), cutoff)
    .run();
}

// Number is the next one in this conversation, allocated in the same statement
// as the insert so parallel proposals can't collide.
export async function createPending(env, conv, { proposedBy, action, summary }) {
  const row = await env.DB.prepare(
    `INSERT INTO pm_pending_actions (number, space, thread, proposed_by, action, summary, status, created_at)
     SELECT COALESCE(MAX(number), 0) + 1, ?, ?, ?, ?, ?, 'pending', ?
       FROM pm_pending_actions WHERE space = ? AND thread = ?
     RETURNING id, number`
  )
    .bind(conv.space, conv.thread, proposedBy, JSON.stringify(action), summary, nowIso(), conv.space, conv.thread)
    .first();
  return row;
}

export async function listPending(env, conv) {
  const { results } = await env.DB.prepare(
    "SELECT number, summary, created_at FROM pm_pending_actions WHERE space = ? AND thread = ? AND status = 'pending' ORDER BY number"
  )
    .bind(conv.space, conv.thread)
    .all();
  return results || [];
}

export async function getPending(env, conv, number) {
  return env.DB.prepare("SELECT * FROM pm_pending_actions WHERE space = ? AND thread = ? AND number = ?")
    .bind(conv.space, conv.thread, number)
    .first();
}

// Claim a pending action for applying. Returns true only for the one caller
// whose UPDATE flipped it, so two approvals racing can't apply it twice.
export async function claimPending(env, id, by, cutoffIso) {
  const r = await env.DB.prepare(
    "UPDATE pm_pending_actions SET status = 'approved', decided_by = ?, decided_at = ? WHERE id = ? AND status = 'pending' AND created_at >= ?"
  )
    .bind(by, nowIso(), id, cutoffIso)
    .run();
  return Number(r?.meta?.changes || 0) === 1;
}

export async function finishPending(env, id, status, result) {
  await env.DB.prepare("UPDATE pm_pending_actions SET status = ?, result = ? WHERE id = ?")
    .bind(status, String(result).slice(0, 1000), id)
    .run();
}

export async function rejectPending(env, id, by) {
  const r = await env.DB.prepare(
    "UPDATE pm_pending_actions SET status = 'rejected', decided_by = ?, decided_at = ? WHERE id = ? AND status = 'pending'"
  )
    .bind(by, nowIso(), id)
    .run();
  return Number(r?.meta?.changes || 0) === 1;
}

export async function markExpired(env, id) {
  await env.DB.prepare("UPDATE pm_pending_actions SET status = 'expired', decided_at = ? WHERE id = ? AND status = 'pending'")
    .bind(nowIso(), id)
    .run();
}

// ---- action log -------------------------------------------------------------
// Never throws: the log must not be what breaks a reply.
export async function logAction(env, { conv, number = null, actor = null, event, action = null, result = null }) {
  try {
    await env.DB.prepare(
      "INSERT INTO pm_action_log (at, space, thread, number, actor, event, action, result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
      .bind(
        nowIso(),
        conv?.space ?? null,
        conv?.thread ?? null,
        number,
        actor,
        event,
        action === null ? null : typeof action === "string" ? action : JSON.stringify(action),
        result === null ? null : String(result).slice(0, 1000)
      )
      .run();
  } catch (e) {
    console.error("nitm-pm log write failed:", e);
  }
}

// ---- instructions (edited at /flo/) ----------------------------------------
// One row, id = 1. No row = use the version in code (ai-pm-instructions.md).
export async function loadInstructions(env, defaultText) {
  const row = await env.DB.prepare("SELECT text, updated_at, updated_by FROM pm_instructions WHERE id = 1").first();
  if (!row) return { text: defaultText, isDefault: true, updatedAt: null, updatedBy: null };
  return { text: row.text, isDefault: false, updatedAt: row.updated_at, updatedBy: row.updated_by };
}

export async function saveInstructions(env, text, by, note) {
  const at = nowIso();
  await env.DB.prepare(
    `INSERT INTO pm_instructions (id, text, updated_at, updated_by) VALUES (1, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET text = excluded.text, updated_at = excluded.updated_at, updated_by = excluded.updated_by`
  ).bind(text, at, by).run();
  await env.DB.prepare("INSERT INTO pm_instruction_changes (at, by, note, text) VALUES (?, ?, ?, ?)")
    .bind(at, by, note || null, text).run();
  return at;
}

export async function resetInstructions(env, by) {
  const at = nowIso();
  await env.DB.prepare("DELETE FROM pm_instructions WHERE id = 1").run();
  await env.DB.prepare("INSERT INTO pm_instruction_changes (at, by, note, text) VALUES (?, ?, ?, NULL)")
    .bind(at, by, "Reset to the version in code").run();
  return at;
}

export async function listInstructionChanges(env, limit = 30) {
  const { results } = await env.DB.prepare(
    "SELECT id, at, by, note, LENGTH(text) AS chars FROM pm_instruction_changes ORDER BY id DESC LIMIT ?"
  ).bind(limit).all();
  return results || [];
}

export async function getInstructionChange(env, id) {
  return env.DB.prepare("SELECT id, at, by, note, text FROM pm_instruction_changes WHERE id = ?").bind(id).first();
}
