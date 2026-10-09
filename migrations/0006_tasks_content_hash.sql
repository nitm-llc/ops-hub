-- Content calendar: fingerprint of each synced task row, so the 2-minute ClickUp
-- sync can skip rows that haven't changed instead of rewriting all of them.
--
-- Applied against the existing `content-calendar` D1 database (binding: DB):
--   npx wrangler d1 migrations apply content-calendar --remote

ALTER TABLE tasks ADD COLUMN content_hash TEXT;
