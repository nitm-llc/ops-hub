-- Strategy module: the strategic plan, task backlogs, readiness/gap ratings, and
-- the state needed for two-way sync with the Strat Mgmt Google Sheet.
--
-- Applied against the existing `content-calendar` D1 database (binding: DB):
--   npx wrangler d1 migrations apply content-calendar --remote
--
-- SCHEMA ONLY. The repo is public: no plan content, people or emails here. The
-- only rows seeded are the four generic default task states.
--
-- Ids are app-generated TEXT ("ini_…", "tsk_…") so they can be written into the
-- sheet's hidden id columns before the row exists here.

CREATE TABLE IF NOT EXISTS strategy_objectives (
  id    TEXT PRIMARY KEY,
  name  TEXT NOT NULL,
  sort  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS strategy_strategies (
  id            TEXT PRIMARY KEY,
  objective_id  TEXT NOT NULL REFERENCES strategy_objectives(id),
  name          TEXT NOT NULL,
  sort          INTEGER NOT NULL DEFAULT 0
);

-- Everyone who can be a Driver, Doer or Trainee. A planned hire is a row with
-- planned = 1 and no login.
CREATE TABLE IF NOT EXISTS strategy_people (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT UNIQUE,
  role       TEXT NOT NULL DEFAULT 'none' CHECK (role IN ('admin','driver','none')),
  can_login  INTEGER NOT NULL DEFAULT 0,
  planned    INTEGER NOT NULL DEFAULT 0,
  active     INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS strategy_initiatives (
  id              TEXT PRIMARY KEY,
  strategy_id     TEXT NOT NULL REFERENCES strategy_strategies(id),
  name            TEXT NOT NULL,
  driver_id       TEXT REFERENCES strategy_people(id),
  start_date      TEXT,             -- ISO date, typed by the driver
  projected_date  TEXT,             -- ISO date
  projected_raw   TEXT,             -- non-date text from the sheet, e.g. HOLD
  notes           TEXT,
  on_hold         INTEGER NOT NULL DEFAULT 0,
  done            INTEGER NOT NULL DEFAULT 0,
  archived        INTEGER NOT NULL DEFAULT 0,
  sheet_row_hint  INTEGER,          -- last seen row; never trusted for writes
  updated_at      TEXT,
  updated_by      TEXT
);

-- Editable list; behaviour comes from `kind`.
CREATE TABLE IF NOT EXISTS strategy_task_states (
  id    TEXT PRIMARY KEY,
  name  TEXT NOT NULL UNIQUE,
  kind  TEXT NOT NULL CHECK (kind IN ('backlog','next','done','dropped')),
  sort  INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO strategy_task_states (id, name, kind, sort) VALUES
  ('st_backlog', 'Backlog', 'backlog', 1),
  ('st_next',    'Next',    'next',    2),
  ('st_done',    'Done',    'done',    3),
  ('st_dropped', 'Dropped', 'dropped', 4);

CREATE TABLE IF NOT EXISTS strategy_tasks (
  id             TEXT PRIMARY KEY,
  initiative_id  TEXT NOT NULL REFERENCES strategy_initiatives(id),
  text           TEXT NOT NULL,
  state_id       TEXT NOT NULL REFERENCES strategy_task_states(id),
  next_date      TEXT,
  doer_id        TEXT REFERENCES strategy_people(id),
  readiness      TEXT CHECK (readiness IN ('Confident','Has an idea','In training','Can''t do it')),
  gap_action     TEXT CHECK (gap_action IN ('None','Train me','Train someone else','Hire','Outsource','Undecided')),
  trainee_id     TEXT REFERENCES strategy_people(id),
  done_on        TEXT,
  flags          TEXT,              -- JSON array, e.g. ["unknown_doer"]
  sort           INTEGER NOT NULL DEFAULT 0,
  updated_at     TEXT,
  updated_by     TEXT
);
CREATE INDEX IF NOT EXISTS idx_strategy_tasks_initiative ON strategy_tasks(initiative_id);

-- Sync bookkeeping: Drive version, last pull/push, last error, alert state.
CREATE TABLE IF NOT EXISTS strategy_sync_state (
  key    TEXT PRIMARY KEY,
  value  TEXT
);

-- The last value synced for each cell, for conflict detection.
CREATE TABLE IF NOT EXISTS strategy_sync_shadow (
  entity     TEXT NOT NULL,          -- 'initiative' | 'task'
  entity_id  TEXT NOT NULL,
  field      TEXT NOT NULL,
  value      TEXT,
  PRIMARY KEY (entity, entity_id, field)
);

-- Same field changed in both places between syncs: the sheet's value stood,
-- the app's is kept here for one-click re-apply.
CREATE TABLE IF NOT EXISTS strategy_conflicts (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  entity       TEXT NOT NULL,
  entity_id    TEXT NOT NULL,
  field        TEXT NOT NULL,
  sheet_value  TEXT,
  app_value    TEXT,
  app_by       TEXT,
  detected_at  TEXT NOT NULL DEFAULT (datetime('now')),
  resolved     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS strategy_audit (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  at         TEXT NOT NULL DEFAULT (datetime('now')),
  actor      TEXT,
  entity     TEXT NOT NULL,
  entity_id  TEXT NOT NULL,
  field      TEXT,
  old        TEXT,
  new        TEXT,
  source     TEXT NOT NULL CHECK (source IN ('app','sheet','import'))
);
CREATE INDEX IF NOT EXISTS idx_strategy_audit_entity ON strategy_audit(entity, entity_id);
