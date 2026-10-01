// Tests for the Strategy gate's refusal paths. verify() and findPerson() are
// stubbed; access.test.js covers real token verification. Invented fixtures only.
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkStrategyAccess } from "../src/strategy/gate.js";

const HOST = "https://ops.anurseinthemaking.com/strategy/api/whoami";
const ENV = { STRATEGY_ACCESS_AUD: "aud-strategy", ACCESS_AUD: "aud-whole-host", ACCESS_TEAM_DOMAIN: "example.cloudflareaccess.com" };
const ADMIN = { id: "p1", name: "Ada", email: "ada@example.com", role: "admin", can_login: 1, active: 1 };

const req = (url = HOST, headers = { "Cf-Access-Jwt-Assertion": "tok" }) => new Request(url, { headers });
const ok = (email = "ada@example.com") => async () => ({ ok: true, email });
const people = (...rows) => async (email) => rows.find((r) => r.email === email) || null;

test("lets in a verified email that is on the People list", async () => {
  const r = await checkStrategyAccess(req(), ENV, { verify: ok(), findPerson: people(ADMIN) });
  assert.equal(r.ok, true);
  assert.equal(r.email, "ada@example.com");
  assert.equal(r.person, ADMIN);
});

test("checks the token against the STRATEGY app's AUD, not the whole-host one", async () => {
  let seen;
  await checkStrategyAccess(req(), ENV, { verify: async (_e, tok, aud) => { seen = { tok, aud }; return { ok: true, email: "ada@example.com" }; }, findPerson: people(ADMIN) });
  assert.deepEqual(seen, { tok: "tok", aud: "aud-strategy" });
});

test("refuses the workers.dev host before anything else", async () => {
  let called = false;
  const r = await checkStrategyAccess(req("https://ops-hub.x.workers.dev/strategy/api/whoami"), ENV,
    { verify: async () => { called = true; return { ok: true, email: "ada@example.com" }; }, findPerson: people(ADMIN) });
  assert.deepEqual([r.ok, r.status, r.reason, called], [false, 403, "wrong_host", false]);
});

test("fails closed when the Strategy AUD or team domain is not configured", async () => {
  for (const env of [{ ...ENV, STRATEGY_ACCESS_AUD: "" }, { ...ENV, ACCESS_TEAM_DOMAIN: undefined }]) {
    const r = await checkStrategyAccess(req(), env, { verify: ok(), findPerson: people(ADMIN) });
    assert.deepEqual([r.ok, r.status, r.reason], [false, 503, "not_configured"]);
  }
});

test("a failed Access check is a 401 carrying the reason", async () => {
  const r = await checkStrategyAccess(req(), ENV, { verify: async () => ({ ok: false, reason: "aud_mismatch" }), findPerson: people(ADMIN) });
  assert.deepEqual([r.ok, r.status, r.reason], [false, 401, "aud_mismatch"]);
});

test("ignores the raw email header; only the verified token counts", async () => {
  const r = await checkStrategyAccess(
    req(HOST, { "Cf-Access-Jwt-Assertion": "tok", "Cf-Access-Authenticated-User-Email": "ada@example.com" }),
    ENV, { verify: async () => ({ ok: true, email: null }), findPerson: people(ADMIN) });
  assert.deepEqual([r.ok, r.status, r.reason], [false, 401, "no_email"]);
});

test("normalises the email before the People lookup", async () => {
  const r = await checkStrategyAccess(req(), ENV, { verify: ok("  Ada@Example.COM "), findPerson: people(ADMIN) });
  assert.equal(r.ok, true);
});

test("refuses anyone not on the People list, or not allowed to log in, or inactive", async () => {
  const cases = [
    people(),                                   // not listed
    people({ ...ADMIN, can_login: 0 }),         // e.g. a planned hire
    people({ ...ADMIN, active: 0 }),            // removed
  ];
  for (const findPerson of cases) {
    const r = await checkStrategyAccess(req(), ENV, { verify: ok(), findPerson });
    assert.deepEqual([r.ok, r.status, r.reason], [false, 403, "not_on_people_list"]);
  }
});

test("a People-list read failure refuses rather than throws", async () => {
  const r = await checkStrategyAccess(req(), ENV, { verify: ok(), findPerson: async () => { throw new Error("D1 down"); } });
  assert.deepEqual([r.ok, r.status, r.reason], [false, 503, "people_unavailable"]);
});
