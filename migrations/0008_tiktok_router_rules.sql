-- TikTok router rules, editable from /tiktok-router/. One row (id = 1); no row
-- means "use DEFAULT_RULES from src/tiktok-router/routing.js".
--
-- Kept in sync with ensureTable() in src/tiktok-router/index.js.
CREATE TABLE IF NOT EXISTS tiktok_router_rules (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  rules      TEXT NOT NULL,          -- JSON, validated by normalizeRules()
  updated_at TEXT NOT NULL,
  updated_by TEXT                    -- Cloudflare Access email of whoever saved
);

-- Every save and reset, so "who changed Edit to Karli?" has an answer.
CREATE TABLE IF NOT EXISTS tiktok_router_rule_changes (
  id    INTEGER PRIMARY KEY AUTOINCREMENT,
  at    TEXT NOT NULL,
  by    TEXT,
  rules TEXT NOT NULL
);
