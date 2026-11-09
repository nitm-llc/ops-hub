#!/usr/bin/env node
// Upload a guidelines version into D1 (cxa_guideline_versions) and make it the
// active one. The text never goes into this public repo; guidelines/ is
// gitignored and this is how it reaches the Worker.
//
//   node tools/cx-audit/upload-guidelines.mjs v1 "first draft, Nov 6"
//
// Writes a temporary SQL file outside the repo and runs
// `wrangler d1 execute content-calendar --remote --file`.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { root } from "./env.mjs";

const [version, note = ""] = process.argv.slice(2);
if (!/^v\d+$/.test(version || "")) { console.error('usage: upload-guidelines.mjs vN ["note"]'); process.exit(1); }
const file = path.join(root, "guidelines", `${version}.md`);
if (!fs.existsSync(file)) { console.error(`No ${file}`); process.exit(1); }
const text = fs.readFileSync(file, "utf8");
const q = (s) => `'${String(s).replace(/'/g, "''")}'`;
const sql = [
  // Same statements as ensureTables, so the upload works before the first deploy too.
  fs.readFileSync(path.join(root, "migrations/0013_cx_audit.sql"), "utf8"),
  "UPDATE cxa_guideline_versions SET is_active = 0;",
  `INSERT INTO cxa_guideline_versions (version, text, note, is_active, created_at, created_by) VALUES (${q(version)}, ${q(text)}, ${q(note)}, 1, ${q(new Date().toISOString())}, 'upload-guidelines.mjs')
   ON CONFLICT(version) DO UPDATE SET text = excluded.text, note = excluded.note, is_active = 1;`,
].join("\n");
const tmp = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "cxa-")), "guidelines.sql");
fs.writeFileSync(tmp, sql);
try {
  execFileSync("npx", ["wrangler", "d1", "execute", "content-calendar", "--remote", "--file", tmp, "--yes"], { cwd: root, stdio: "inherit" });
  console.log(`\nUploaded guidelines ${version} (${text.length} chars) and made it active.`);
} finally {
  fs.rmSync(path.dirname(tmp), { recursive: true, force: true });
}
