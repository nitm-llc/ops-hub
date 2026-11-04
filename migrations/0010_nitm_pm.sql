-- NITM PM Google Chat app (src/nitm-pm). Also created on first use by
-- src/nitm-pm/store.js ensureTables(); keep the two in sync.
CREATE TABLE IF NOT EXISTS pm_threads (
  space      TEXT NOT NULL,
  thread     TEXT NOT NULL,           -- "" for DMs (one conversation per DM)
  messages   TEXT NOT NULL,           -- JSON: last 16 {role, who, text, at}
  updated_at TEXT NOT NULL,
  PRIMARY KEY (space, thread)
);

CREATE TABLE IF NOT EXISTS pm_pending_actions (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  number      INTEGER NOT NULL,       -- the number people approve ("approve 3")
  space       TEXT NOT NULL,
  thread      TEXT NOT NULL,
  proposed_by TEXT,
  action      TEXT NOT NULL,          -- JSON, validated again before applying
  summary     TEXT,
  status      TEXT NOT NULL DEFAULT 'pending', -- pending|approved|rejected|expired|failed
  created_at  TEXT NOT NULL,
  decided_by  TEXT,
  decided_at  TEXT,
  result      TEXT,
  UNIQUE (space, thread, number)
);
CREATE INDEX IF NOT EXISTS idx_pm_pending_conv ON pm_pending_actions (space, thread, status);

CREATE TABLE IF NOT EXISTS pm_action_log (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  at      TEXT NOT NULL,
  space   TEXT,
  thread  TEXT,
  number  INTEGER,
  actor   TEXT,
  event   TEXT NOT NULL,              -- proposed|applied|apply_failed|rejected|approval_refused|...
  action  TEXT,
  result  TEXT
);
