// Local env for the CX audit tools: .dev.vars (gitignored) overlaid by the
// real environment. Also loads the newest guidelines/vN.md (gitignored).
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export function loadEnv() {
  const out = {};
  try {
    for (const line of fs.readFileSync(path.join(root, ".dev.vars"), "utf8").split("\n")) {
      const m = /^\s*([A-Z0-9_]+)\s*=\s*"?(.*?)"?\s*$/.exec(line);
      if (m) out[m[1]] = m[2];
    }
  } catch { /* no .dev.vars */ }
  return { ...out, ...process.env };
}

export function requireVars(env, names) {
  const missing = names.filter((n) => !env[n]);
  if (missing.length) {
    console.error(`Missing ${missing.join(", ")}. Put ${missing.length > 1 ? "them" : "it"} in .dev.vars (gitignored) or the environment.`);
    process.exit(1);
  }
}

// guidelines/v1.md, v2.md ... -> the highest, or CX_AUDIT_GUIDELINES=v1.
export function loadGuidelines(env) {
  const dir = path.join(root, "guidelines");
  const files = fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => /^v\d+\.md$/.test(f)) : [];
  if (!files.length) { console.error("No guidelines/vN.md found."); process.exit(1); }
  const want = env.CX_AUDIT_GUIDELINES ? `${env.CX_AUDIT_GUIDELINES}.md` : files.sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1))).at(-1);
  return { version: want.replace(/\.md$/, ""), text: fs.readFileSync(path.join(dir, want), "utf8") };
}
