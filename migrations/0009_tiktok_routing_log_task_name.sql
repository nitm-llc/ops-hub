-- Card names in the TikTok router log, so the page can say "Insulin myths"
-- instead of "86e3mkhyh". Older rows stay NULL and show the id.
ALTER TABLE tiktok_routing_log ADD COLUMN task_name TEXT;
