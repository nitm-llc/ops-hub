-- CX scorecard audit, Phase 6: golden set + calibration runs (src/cx-audit/calibration.js).
-- Generated from SCHEMA_CALIBRATION in src/cx-audit/store.js (ensureTables also creates these); keep in sync.
-- One row per golden conversation/day (what to fetch and re-score).
CREATE TABLE IF NOT EXISTS cxa_golden_items (
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  note            TEXT,
  added_by        TEXT,
  added_at        TEXT NOT NULL,
  PRIMARY KEY (conversation_id, audit_day)
);

-- Mark's hand scores.
CREATE TABLE IF NOT EXISTS cxa_golden_labels (
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  author_type     TEXT NOT NULL,
  check_id        TEXT NOT NULL,
  expected        TEXT NOT NULL,
  labeled_by      TEXT,
  labeled_at      TEXT NOT NULL,
  PRIMARY KEY (conversation_id, audit_day, author_type, check_id)
);

CREATE TABLE IF NOT EXISTS cxa_calibration_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  status        TEXT NOT NULL,
  settings_json TEXT,
  total         INTEGER,
  done          INTEGER,
  errors        INTEGER,
  report_json   TEXT,
  error         TEXT,
  lease_until   INTEGER,
  created_at    TEXT NOT NULL,
  created_by    TEXT,
  finished_at   TEXT
);

CREATE TABLE IF NOT EXISTS cxa_calibration_queue (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  stage           TEXT NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  error           TEXT,
  PRIMARY KEY (run_id, conversation_id, audit_day)
);

-- What the model said for each labeled check, next to the hand label.
CREATE TABLE IF NOT EXISTS cxa_calibration_results (
  run_id          INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  audit_day       TEXT NOT NULL,
  author_type     TEXT NOT NULL,
  check_id        TEXT NOT NULL,
  expected        TEXT NOT NULL,
  got             TEXT NOT NULL,
  evidence        TEXT,
  PRIMARY KEY (run_id, conversation_id, audit_day, author_type, check_id)
);
