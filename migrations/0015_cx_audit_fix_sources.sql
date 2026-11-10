-- CX scorecard audit: where each failed check comes from + a suggested change (fix list).
-- Mirrors SCHEMA_FIX_SOURCES in src/cx-audit/store.js; ensureTables adds the columns only if missing.
ALTER TABLE cxa_check_results ADD COLUMN source TEXT;
ALTER TABLE cxa_check_results ADD COLUMN fix TEXT;
