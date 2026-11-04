-- Flo (src/nitm-pm): instructions edited at /flo/. No row = use the version
-- in code (src/nitm-pm/ai-pm-instructions.md). Also created on first use by
-- store.js ensureTables(); keep the two in sync.
CREATE TABLE IF NOT EXISTS pm_instructions (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  text       TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  updated_by TEXT
);

-- Every save and reset, with the full text (NULL text = reset to code).
CREATE TABLE IF NOT EXISTS pm_instruction_changes (
  id    INTEGER PRIMARY KEY AUTOINCREMENT,
  at    TEXT NOT NULL,
  by    TEXT,
  note  TEXT,
  text  TEXT
);
