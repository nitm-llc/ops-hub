// The Strategy module's door. Every /strategy/api/* request passes through
// checkStrategyAccess() before anything else runs.
//
// Two layers, both required:
//   1. Cloudflare Access, as a path app on `/strategy` whose policy admits only
//      the drivers and admins. Proved here by the JWT carrying THAT app's AUD
//      (STRATEGY_ACCESS_AUD), not the whole-host app's. If someone deletes or
//      mis-scopes the path app, requests fall back to the host app, carry the
//      wrong AUD, and are refused: a config slip fails closed.
//   2. The People list in D1. This copy can only refuse people, never admit
//      anyone Access didn't.
//
// Unlike clickup-automation's gate, a missing AUD/team secret is a refusal, not
// a skip.
//
// No Worker globals: the D1 lookup is passed in, so tests run under `node --test`.

import { isTrustedHost, verifyAccessJwt } from "../access.js";

// Returns { ok: true, email, person } or { ok: false, status, error, reason }.
export async function checkStrategyAccess(request, env, { findPerson, verify = verifyAccessJwt }) {
  const url = new URL(request.url);
  if (!isTrustedHost(env, url)) {
    return deny(403, "wrong_host", "Strategy is only served on the Cloudflare Access hostname.");
  }
  if (!env.STRATEGY_ACCESS_AUD || !env.ACCESS_TEAM_DOMAIN) {
    return deny(503, "not_configured",
      "Strategy login isn't set up yet: STRATEGY_ACCESS_AUD and ACCESS_TEAM_DOMAIN must both be set.");
  }

  const v = await verify(env, request.headers.get("Cf-Access-Jwt-Assertion"), env.STRATEGY_ACCESS_AUD);
  if (!v.ok) return deny(401, v.reason, `Cloudflare Access check failed: ${v.reason}`);

  // The email comes from the verified token, never the raw
  // Cf-Access-Authenticated-User-Email header, which a client could set on any
  // route that skips Access.
  const email = String(v.email || "").trim().toLowerCase();
  if (!email) return deny(401, "no_email", "The Access token carried no email.");

  let person;
  try {
    person = await findPerson(email);
  } catch {
    return deny(503, "people_unavailable", "Couldn't read the People list.");
  }
  if (!person || !person.can_login || !person.active) {
    return deny(403, "not_on_people_list", `${email} isn't on the Strategy People list.`);
  }
  return { ok: true, email, person };
}

function deny(status, reason, error) {
  return { ok: false, status, reason, error };
}
