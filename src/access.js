// Cloudflare Access primitives shared by every module that gates its own API.
//
// Pure ESM with no Worker globals beyond fetch/crypto/atob, so `node --test`
// can import it directly (see tests/).

export const DEFAULT_APP_HOSTNAME = "ops.anurseinthemaking.com";

// The *.workers.dev hostname skips Cloudflare Access completely, so anything
// arriving on another hostname has not been authenticated by anybody.
export function isTrustedHost(env, url) {
  const expected = (env.APP_HOSTNAME || DEFAULT_APP_HOSTNAME).toLowerCase();
  const host = url.hostname.toLowerCase();
  // localhost keeps `wrangler dev` usable.
  return host === expected || host === "localhost" || host === "127.0.0.1";
}

function b64urlToBytes(s) {
  const pad = s.replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(pad + "=".repeat((4 - (pad.length % 4)) % 4));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function b64urlToString(s) {
  return new TextDecoder().decode(b64urlToBytes(s));
}

// Verify a Cloudflare Access JWT against ONE application's AUD. Each Access app
// has its own AUD, so the caller says which app it expects: a token minted for
// the whole-host app must not open a path app with a stricter policy.
//
// Never throws: every failure is { ok: false, reason }.
export async function verifyAccessJwt(env, token, aud) {
  const team = env.ACCESS_TEAM_DOMAIN;
  if (!team || !aud) return { ok: false, reason: "not_configured" };
  if (!token) return { ok: false, reason: "missing" };

  const parts = token.split(".");
  if (parts.length !== 3) return { ok: false, reason: "malformed" };
  const [h, p, s] = parts;

  let header, payload;
  try {
    header = JSON.parse(b64urlToString(h));
    payload = JSON.parse(b64urlToString(p));
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (header.alg !== "RS256") return { ok: false, reason: "bad_alg" };

  let certs;
  try {
    const res = await fetch(`https://${team}/cdn-cgi/access/certs`);
    if (!res.ok) return { ok: false, reason: "certs_unavailable" };
    certs = await res.json();
  } catch {
    return { ok: false, reason: "certs_unavailable" };
  }
  const jwk = (certs.keys || []).find((k) => k.kid === header.kid);
  if (!jwk) return { ok: false, reason: "unknown_kid" };

  let valid;
  try {
    const key = await crypto.subtle.importKey(
      "jwk",
      { kty: jwk.kty, n: jwk.n, e: jwk.e, alg: "RS256", ext: true },
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"]
    );
    valid = await crypto.subtle.verify(
      "RSASSA-PKCS1-v1_5",
      key,
      b64urlToBytes(s),
      new TextEncoder().encode(`${h}.${p}`)
    );
  } catch {
    return { ok: false, reason: "bad_signature" };
  }
  if (!valid) return { ok: false, reason: "bad_signature" };

  const now = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < now) return { ok: false, reason: "expired" };
  if (payload.nbf && payload.nbf > now + 60) return { ok: false, reason: "not_yet_valid" };
  if (payload.iss !== `https://${team}`) return { ok: false, reason: "iss_mismatch" };
  const auds = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!auds.includes(aud)) return { ok: false, reason: "aud_mismatch" };

  return { ok: true, email: payload.email || null };
}
