-- CX scorecard audit (src/cx-audit). All tables prefixed cxa_; nothing joins other modules.
-- Generated from SCHEMA in src/cx-audit/store.js (ensureTables also creates these); keep in sync.
-- Guideline versions (v1, v2 ...). The text lives here, not in the public repo.
CREATE TABLE IF NOT EXISTS cxa_guideline_versions (
  version     TEXT PRIMARY KEY,
  text        TEXT NOT NULL,
  note        TEXT,
  is_active   INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- Key/value settings: toggles (checks + voice sub-rules), schedule.
CREATE TABLE IF NOT EXISTS cxa_settings (
  key         TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  updated_by  TEXT
);

-- Editable exclusion list (seeded from DEFAULT_EXCLUSIONS on first use).
CREATE TABLE IF NOT EXISTS cxa_exclusions (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  kind        TEXT NOT NULL,
  value       TEXT NOT NULL,
  note        TEXT,
  enabled     INTEGER NOT NULL DEFAULT 1,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- Fin change log, filled in by hand; marked on the trend chart.
CREATE TABLE IF NOT EXISTS cxa_fin_changes (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  day         TEXT NOT NULL,
  what        TEXT NOT NULL,
  who         TEXT,
  created_at  TEXT NOT NULL,
  created_by  TEXT
);

-- One row per audit run (scheduled or manual).
CREATE TABLE IF NOT EXISTS cxa_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  kind          TEXT NOT NULL,
  day_from      TEXT NOT NULL,
  day_to        TEXT NOT NULL,
  force         INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL,
  settings_json TEXT,
  searched      INTEGER,
  scored        INTEGER,
  skipped       INTEGER,
  excluded_json TEXT,
  errors        INTEGER,
  error         TEXT,
  usage_json    TEXT,
  lease_until   INTEGER,
  created_at    TEXT NOT NULL,
  created_by    TEXT,
  started_at    TEXT,
  finished_at   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS cxa_runs_scheduled ON cxa_runs (kind, day_from) WHERE kind IN ('daily', 'weekly');

-- Work list for a run, worked through a batch per cron tick.
-- stage: fetch (day = '*') -> score | skipped | excluded -> done | error
CREATE TABLE IF NOT EXISTS cxa_queue (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  day             TEXT NOT NULL,
  stage           TEXT NOT NULL,
  reason          TEXT,
  record_json     TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  error           TEXT,
  PRIMARY KEY (run_id, conversation_id, day)
);
CREATE INDEX IF NOT EXISTS cxa_queue_stage ON cxa_queue (run_id, stage);

-- One row per conversation x audit day x author group x guidelines version.
CREATE TABLE IF NOT EXISTS cxa_scores (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id    TEXT NOT NULL,
  audit_day          TEXT NOT NULL,
  author_type        TEXT NOT NULL,
  guidelines_version TEXT NOT NULL,
  last_reply_id      TEXT,
  reply_ids          TEXT,
  link               TEXT NOT NULL,
  channel            TEXT,
  issue_type         TEXT,
  issue_type_source  TEXT,
  passed             INTEGER NOT NULL,
  failed             INTEGER NOT NULL,
  applicable         INTEGER NOT NULL,
  note               TEXT,
  overall_note       TEXT,
  scorer_key         TEXT,
  model              TEXT,
  run_id             INTEGER,
  scored_at          TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS cxa_scores_key ON cxa_scores (conversation_id, audit_day, author_type, guidelines_version);
CREATE INDEX IF NOT EXISTS cxa_scores_day ON cxa_scores (audit_day);

-- Per-check results. evidence and rewrite are scrubbed text.
CREATE TABLE IF NOT EXISTS cxa_check_results (
  score_id     INTEGER NOT NULL,
  check_id     TEXT NOT NULL,
  result       TEXT NOT NULL,
  evidence     TEXT,
  rewrite      TEXT,
  reply_id     TEXT,
  failed_rules TEXT,
  PRIMARY KEY (score_id, check_id)
);
CREATE INDEX IF NOT EXISTS cxa_check_results_check ON cxa_check_results (check_id, result);
