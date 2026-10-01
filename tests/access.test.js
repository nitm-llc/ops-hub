// Tests for src/access.js. Signs real RS256 tokens with a throwaway key and
// stubs fetch() for the Access certs endpoint. Invented fixtures only.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { isTrustedHost, verifyAccessJwt } from "../src/access.js";

const TEAM = "example.cloudflareaccess.com";
const AUD = "aud-strategy";
const env = { ACCESS_TEAM_DOMAIN: TEAM };
let keys, jwk, realFetch, certsOk = true;

const b64url = (bytes) => Buffer.from(bytes).toString("base64url");
async function sign(payload, { kid = "k1", alg = "RS256", key = keys.privateKey } = {}) {
  const h = b64url(JSON.stringify({ alg, kid }));
  const p = b64url(JSON.stringify(payload));
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(`${h}.${p}`));
  return `${h}.${p}.${b64url(new Uint8Array(sig))}`;
}
const now = () => Math.floor(Date.now() / 1000);
const good = (over = {}) => ({ iss: `https://${TEAM}`, aud: [AUD], email: "driver@example.com", exp: now() + 600, ...over });

before(async () => {
  keys = await crypto.subtle.generateKey(
    { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
    true, ["sign", "verify"]);
  jwk = { ...(await crypto.subtle.exportKey("jwk", keys.publicKey)), kid: "k1" };
  realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    assert.equal(url, `https://${TEAM}/cdn-cgi/access/certs`);
    if (!certsOk) return new Response("nope", { status: 500 });
    return Response.json({ keys: [jwk] });
  };
});
after(() => { globalThis.fetch = realFetch; });

test("accepts a valid token for the expected AUD", async () => {
  assert.deepEqual(await verifyAccessJwt(env, await sign(good()), AUD), { ok: true, email: "driver@example.com" });
});

test("a token minted for another Access app is refused", async () => {
  const r = await verifyAccessJwt(env, await sign(good({ aud: ["aud-whole-host"] })), AUD);
  assert.deepEqual(r, { ok: false, reason: "aud_mismatch" });
});

test("refusal reasons", async () => {
  const other = await crypto.subtle.generateKey(
    { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
    true, ["sign", "verify"]);
  const cases = [
    [await sign(good({ exp: now() - 10 })), "expired"],
    [await sign(good({ nbf: now() + 3600 })), "not_yet_valid"],
    [await sign(good({ iss: "https://evil.example" })), "iss_mismatch"],
    [await sign(good(), { kid: "k2" }), "unknown_kid"],
    [await sign(good(), { key: other.privateKey }), "bad_signature"],
    [await sign(good(), { alg: "HS256" }), "bad_alg"],
    ["not-a-jwt", "malformed"],
    ["a.b.c", "malformed"],
    [null, "missing"],
  ];
  for (const [tok, reason] of cases) {
    assert.deepEqual(await verifyAccessJwt(env, tok, AUD), { ok: false, reason }, reason);
  }
});

test("not configured without a team domain or an AUD", async () => {
  const tok = await sign(good());
  assert.equal((await verifyAccessJwt({}, tok, AUD)).reason, "not_configured");
  assert.equal((await verifyAccessJwt(env, tok, undefined)).reason, "not_configured");
});

test("an unreachable certs endpoint is a refusal, not a throw", async () => {
  certsOk = false;
  try {
    assert.deepEqual(await verifyAccessJwt(env, await sign(good()), AUD), { ok: false, reason: "certs_unavailable" });
  } finally { certsOk = true; }
});

test("isTrustedHost: only the Access hostname (and localhost)", () => {
  const t = (u, e = {}) => isTrustedHost(e, new URL(u));
  assert.equal(t("https://ops.anurseinthemaking.com/strategy/api/whoami"), true);
  assert.equal(t("https://OPS.anurseinthemaking.com/x"), true);
  assert.equal(t("https://ops-hub.someone.workers.dev/strategy/api/whoami"), false);
  assert.equal(t("http://localhost:8787/x"), true);
  assert.equal(t("https://other.example/x", { APP_HOSTNAME: "other.example" }), true);
});
