-- ClickUp Automation module — replaces the Zapier Zap whose entire configuration
-- lived in a JavaScript object literal inside a code step, so only one person
-- could add a marketing list. The whole point of these tables is that the config
-- is DATA, editable from /clickup-automation/ by anyone on the team.
--
-- Applied against the existing `content-calendar` D1 database (binding: DB):
--   npx wrangler d1 migrations apply content-calendar --remote
--
-- Kept in sync with ensureClickUpAutomationTables() in src/clickup-automation.js.
-- Change one, change the other.
--
-- Table names are prefixed because this database is shared with ~30 tables from
-- the calendar, tracker, 3PL, med-supplies and video-review modules.

-- ---------------------------------------------------------------------------
-- One row per ClickUp list. Duplication over sharing: three lists needing the
-- same setup is three rows, and the UI has a Duplicate button for the
-- ergonomics. Deliberately NO join table for many-lists-to-one-config.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickup_automations (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  name                  TEXT    NOT NULL,
  status                TEXT    NOT NULL DEFAULT 'draft'
                          CHECK (status IN ('draft','live','archived')),

  -- Where it fires. Matched on list ID ONLY, never name: the old Zap matched on
  -- list name with an includes() fallback, so a task in the KPIs list "YouTube"
  -- would have been given an ads folder.
  clickup_list_id       TEXT    NOT NULL UNIQUE,
  -- Cached display names so the list view renders with zero external API calls.
  clickup_list_name     TEXT,
  clickup_folder_name   TEXT,
  clickup_space_name    TEXT,

  -- What it does. Each action independently switchable.
  act_rename            INTEGER NOT NULL DEFAULT 1,
  act_create_folder     INTEGER NOT NULL DEFAULT 1,
  act_create_subfolders INTEGER NOT NULL DEFAULT 1,
  act_copy_template     INTEGER NOT NULL DEFAULT 0,
  act_write_back        INTEGER NOT NULL DEFAULT 1,

  -- Where folders go.
  drive_id              TEXT,                 -- shared drive id, informational
  drive_name            TEXT,
  drive_parent_id       TEXT,
  drive_parent_name     TEXT,                 -- the folder's own name, e.g. "Google"
  drive_parent_path     TEXT,                 -- human breadcrumb, e.g. "Ads > Google"
  folder_name_template  TEXT    NOT NULL DEFAULT '{code} - {name}',
  -- JSON array of subfolder names, no cap. Names may contain {code} / {name}.
  -- Editing this affects FUTURE runs only; folders already in Drive are never
  -- renamed or deleted by this system.
  subfolders            TEXT    NOT NULL DEFAULT '[]',
  template_file_id      TEXT,                 -- Google Doc to copy into the folder
  canva_link            TEXT,                 -- static per-list link (the Zap did this for Email)
  -- The Zap shared every folder and copied doc with the domain. Without this the
  -- team cannot open what the automation creates.
  share_domain          TEXT    DEFAULT 'anurseinthemaking.com',

  -- ClickUp custom fields. The ID is primary; the name is a human label and a
  -- fallback. The old Zap fuzzy-matched on name
  -- (includes('Drive') && !includes('Link')), which is one rename away from
  -- silently writing nothing.
  field_drive_id        TEXT,
  field_drive           TEXT,
  field_doc_id          TEXT,
  field_doc             TEXT,
  field_canva_id        TEXT,
  field_canva           TEXT,

  -- Cached preflight result, so the list view can show a stale/failing dot
  -- without firing 2N Google + ClickUp calls on page load.
  last_check_at         TEXT,
  last_check_ok         INTEGER,
  last_check_detail     TEXT,

  created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
  updated_at            TEXT    NOT NULL DEFAULT (datetime('now')),
  updated_by            TEXT
);

CREATE INDEX IF NOT EXISTS idx_clickup_autom_list
  ON clickup_automations (clickup_list_id, status);

-- ---------------------------------------------------------------------------
-- Idempotency ledger AND the log the team debugs from. The UNIQUE constraint on
-- clickup_task_id is the lock: a redelivered webhook loses the INSERT race and
-- that constraint violation IS the signal to bail. Never SELECT-then-INSERT.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickup_automation_runs (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  clickup_task_id  TEXT    NOT NULL UNIQUE,
  automation_id    INTEGER REFERENCES clickup_automations(id),
  clickup_list_id  TEXT,
  status           TEXT    NOT NULL
                     CHECK (status IN ('running','ok','skipped','error')),
  -- Machine codes that drive the plain-language sentence shown in the UI.
  reason_code      TEXT,                      -- why it was skipped
  error_code       TEXT,                      -- what class of failure
  code             TEXT,
  task_name        TEXT,
  folder_id        TEXT,
  folder_url       TEXT,
  doc_url          TEXT,
  fields_written   TEXT,                      -- JSON array of field names
  -- JSON map of completed steps, so a retry resumes instead of creating a
  -- second folder. "Never 5xx" means ClickUp never retries, so retries are ours.
  steps            TEXT    NOT NULL DEFAULT '{}',
  attempts         INTEGER NOT NULL DEFAULT 0,
  error            TEXT,
  payload          TEXT,                      -- truncated raw webhook body
  -- NOT named `trigger`: that is a reserved word in SQLite.
  trigger_source   TEXT    NOT NULL DEFAULT 'webhook'
                     CHECK (trigger_source IN ('webhook','test','retry')),
  next_attempt_at  TEXT,                      -- set for recoverable failures; cron picks these up
  started_at       TEXT,
  finished_at      TEXT,
  created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
  updated_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_clickup_runs_created
  ON clickup_automation_runs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_clickup_runs_retry
  ON clickup_automation_runs (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_clickup_runs_autom
  ON clickup_automation_runs (automation_id, id DESC);

-- ---------------------------------------------------------------------------
-- Sequential codes, YYYY.NNNN. Replaces the Zap's
-- '2026.' + Math.floor(1000 + Math.random() * 9000), which collides.
-- last_value = the highest number handed out this year. Allocation is a single
-- atomic upsert with RETURNING, because D1 gives us no cross-statement
-- transaction to hold.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickup_automation_codes (
  year        TEXT    PRIMARY KEY,
  last_value  INTEGER NOT NULL DEFAULT 0,
  updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ---------------------------------------------------------------------------
-- Registered ClickUp webhooks. ClickUp returns the signing secret ONLY in the
-- create response, so the register endpoint writes it here directly and it
-- never passes through a human or a config file. Multiple active rows are
-- allowed so verification can accept any active secret during a rotation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickup_automation_webhooks (
  webhook_id   TEXT    PRIMARY KEY,
  secret       TEXT    NOT NULL,
  endpoint     TEXT,
  events       TEXT,
  active       INTEGER NOT NULL DEFAULT 1,
  created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
  created_by   TEXT
);

-- ---------------------------------------------------------------------------
-- Key/value scratch for this module (cached Google access token, cached ClickUp
-- workspace tree). Mirrors the shopifyToken / amzState pattern in src/index.js.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clickup_automation_state (
  key         TEXT    PRIMARY KEY,
  value       TEXT,
  updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
