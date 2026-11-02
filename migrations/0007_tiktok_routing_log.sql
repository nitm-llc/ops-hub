-- TikTok router run log — one row per action OR skip OR error, so "why did
-- this card get (or not get) reassigned?" is always answerable.
--
-- Applied against content-calendar (binding: DB):
--   npx wrangler d1 migrations apply content-calendar --remote
--
-- Kept in sync with ensureTable() in src/tiktok-router/index.js.
-- Change one, change the other.
--
-- status = the card's stage when the event was processed.
CREATE TABLE IF NOT EXISTS tiktok_routing_log (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  at      TEXT NOT NULL,
  task_id TEXT NOT NULL,
  event   TEXT NOT NULL,
  status  TEXT,
  action  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tiktok_routing_log_task
  ON tiktok_routing_log (task_id, id DESC);
