-- Flo reaching people + scheduled runs (src/nitm-pm/outreach.js, scheduler.js).
-- Also created on first use by store.js ensureTables(); keep in sync.
CREATE TABLE IF NOT EXISTS pm_people (      -- learned when someone messages Flo
  email        TEXT PRIMARY KEY,
  display_name TEXT,
  chat_user    TEXT,
  dm_space     TEXT,                        -- their DM with Flo; NULL = can't DM yet
  last_seen    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pm_outreach (    -- every DM Flo sent (or couldn't)
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  at           TEXT NOT NULL,
  day          TEXT NOT NULL,               -- New York day, for the daily limits
  to_email     TEXT,
  to_name      TEXT,
  task_id      TEXT,
  kind         TEXT,                        -- check_in|escalation|calendar|approval|other
  text         TEXT,
  delivered_to TEXT,
  status       TEXT NOT NULL,               -- sent|fallback|undelivered|failed
  detail       TEXT
);
CREATE INDEX IF NOT EXISTS idx_pm_outreach_day ON pm_outreach (day, to_email);
CREATE TABLE IF NOT EXISTS pm_runs (        -- one row per scheduled run per day
  kind        TEXT NOT NULL,                -- daily|weekly|daily:manual|weekly:manual
  day         TEXT NOT NULL,
  started_at  TEXT NOT NULL,
  finished_at TEXT,
  trigger     TEXT,
  result      TEXT,
  PRIMARY KEY (kind, day)
);
CREATE TABLE IF NOT EXISTS pm_settings (    -- proactive on/off, run_request
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);
