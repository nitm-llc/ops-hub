// ============================================================================
// Strategy module: the NITM strategic plan, its task backlogs, and the
// train/hire gaps, in two-way sync with the Strat Mgmt Google Sheet.
//
// Routes (ALL under /strategy/: the Cloudflare Access path app covers exactly
// that prefix, and anything outside it falls back to the whole-staff policy):
//   /strategy             -> 301 to /strategy/
//   /strategy/            -> public/strategy/index.html (via ASSETS)
//   /strategy/api/whoami  -> who the gate thinks you are
//
// SECRETS: STRATEGY_ACCESS_AUD (the Strategy Access app's AUD tag),
//          ACCESS_TEAM_DOMAIN (e.g. nitm.cloudflareaccess.com).
// D1:      DB (content-calendar), tables from migrations/0005_strategy.sql.
//
// The repo is PUBLIC: no plan content, emails or sheet ids in this file, its
// migration, or its tests. People rows are created in the app (or by a one-off
// `wrangler d1 execute`), never committed.
// ============================================================================

import { checkStrategyAccess } from "./strategy/gate.js";

const API = "/strategy/api/";

export async function handleStrategyRoutes(request, env, ctx, path) {
  if (path === "/strategy") return Response.redirect(new URL(request.url).origin + "/strategy/", 301);
  if (!path.startsWith(API)) return null; // the page itself loads from ASSETS

  const gate = await checkStrategyAccess(request, env, { findPerson: (email) => findPerson(env, email) });
  if (!gate.ok) return json({ error: gate.error, reason: gate.reason }, gate.status);

  const sub = path.slice(API.length).replace(/\/+$/, "");
  if (sub === "whoami" && request.method === "GET") {
    const p = gate.person;
    return json({ email: gate.email, name: p.name, role: p.role });
  }
  return json({ error: "Not found" }, 404);
}

async function findPerson(env, email) {
  return env.DB.prepare(
    "SELECT id, name, email, role, can_login, active FROM strategy_people WHERE lower(email) = ?1 LIMIT 1"
  ).bind(email).first();
}

// Same-origin only: no CORS headers at all, unlike most of this repo.
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store", "X-Robots-Tag": "noindex, nofollow" },
  });
}
