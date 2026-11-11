-- Coaching Flo (src/nitm-pm/coach.js, /flo/coach/). Also created on first use
-- by store.js ensureTables(); keep in sync.
CREATE TABLE IF NOT EXISTS pm_coaching (     -- a verdict on one thing Flo would have done
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  log_id   INTEGER NOT NULL UNIQUE,          -- pm_action_log row (event 'shadow')
  verdict  TEXT NOT NULL,                    -- right | wrong | skip
  note     TEXT,
  by       TEXT,
  at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pm_lessons (      -- what the team taught Flo; goes into its instructions
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  text          TEXT NOT NULL,
  by            TEXT,
  at            TEXT NOT NULL,
  about         TEXT,                        -- what Flo wanted to do, when it came from a review
  source_log_id INTEGER,                     -- the shadow item it came from, if any
  active        INTEGER NOT NULL DEFAULT 1
);
