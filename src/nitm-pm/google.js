// ============================================================================
// NITM PM — Google Chat plumbing: request verification, service-account token,
// posting messages. WebCrypto only (no Node libraries), so it runs in Workers
// and under `node --test` alike.
//
// VERIFICATION (developers.google.com/workspace/chat/verify-requests-from-chat,
// checked 2026-11-04). Chat sends `Authorization: Bearer <JWT>`. Two shapes are
// accepted, and both are tied to OUR Google Cloud project, so another
// project's Chat app pointed at this URL is refused:
//
//   1. Add-on format (the default for apps created since July 2025):
//      Google-signed OIDC ID token. iss accounts.google.com, aud = the
//      endpoint URL, email = service-<PROJECT_NUMBER>@gcp-sa-gsuiteaddons.
//      iam.gserviceaccount.com, email_verified = true.
//   2. Legacy format with "Project Number" audience: a JWT self-signed by
//      chat@system.gserviceaccount.com, iss = that address, aud = the project
//      number.
//
// Deliberately NOT accepted: legacy "HTTP endpoint URL" audience tokens
// (email chat@system.gserviceaccount.com). Any Chat app in any project can
// mint one of those for our URL. Configure the app with the add-on format or
// the Project Number audience instead.
//
// Fails closed: without PM_CHAT_PROJECT_NUMBER nothing verifies.
// ============================================================================

const GOOGLE_OIDC_JWKS = "https://www.googleapis.com/oauth2/v3/certs";
const CHAT_SYSTEM = "chat@system.gserviceaccount.com";
const CHAT_SYSTEM_JWKS = `https://www.googleapis.com/service_accounts/v1/jwk/${CHAT_SYSTEM}`;
const GOOGLE_ISSUERS = ["https://accounts.google.com", "accounts.google.com"];
const TOKEN_URL = "https://oauth2.googleapis.com/token";
const CHAT_API = "https://chat.googleapis.com/v1";
const CHAT_SCOPE = "https://www.googleapis.com/auth/chat.bot";

// ---- base64url -------------------------------------------------------------
function b64urlToBytes(s) {
  const pad = s.replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(pad + "=".repeat((4 - (pad.length % 4)) % 4));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
const b64urlToJson = (s) => JSON.parse(new TextDecoder().decode(b64urlToBytes(s)));
function bytesToB64url(bytes) {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
const strToB64url = (s) => bytesToB64url(new TextEncoder().encode(s));

// ---- JWKS cache (per isolate) ---------------------------------------------
const jwksCache = new Map(); // url -> { at, keys }
const JWKS_TTL_MS = 60 * 60 * 1000;

async function getJwks(url, { refresh = false } = {}) {
  const hit = jwksCache.get(url);
  if (!refresh && hit && Date.now() - hit.at < JWKS_TTL_MS) return hit.keys;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`certs ${res.status}`);
  const body = await res.json();
  const keys = body.keys || [];
  jwksCache.set(url, { at: Date.now(), keys });
  return keys;
}
export const _clearJwksCache = () => jwksCache.clear();

async function verifyRs256(token, jwksUrl) {
  const parts = String(token || "").split(".");
  if (parts.length !== 3) return { ok: false, reason: "malformed" };
  let header, payload;
  try {
    header = b64urlToJson(parts[0]);
    payload = b64urlToJson(parts[1]);
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (header.alg !== "RS256") return { ok: false, reason: "bad_alg" };

  let keys;
  try {
    keys = await getJwks(jwksUrl);
    if (!keys.some((k) => k.kid === header.kid)) keys = await getJwks(jwksUrl, { refresh: true });
  } catch {
    return { ok: false, reason: "certs_unavailable" };
  }
  const jwk = keys.find((k) => k.kid === header.kid);
  if (!jwk) return { ok: false, reason: "unknown_kid" };

  try {
    const key = await crypto.subtle.importKey(
      "jwk",
      { kty: jwk.kty, n: jwk.n, e: jwk.e, alg: "RS256", ext: true },
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"]
    );
    const valid = await crypto.subtle.verify(
      "RSASSA-PKCS1-v1_5",
      key,
      b64urlToBytes(parts[2]),
      new TextEncoder().encode(`${parts[0]}.${parts[1]}`)
    );
    if (!valid) return { ok: false, reason: "bad_signature" };
  } catch {
    return { ok: false, reason: "bad_signature" };
  }

  const now = Math.floor(Date.now() / 1000);
  if (!payload.exp || payload.exp < now - 30) return { ok: false, reason: "expired" };
  if (payload.iat && payload.iat > now + 300) return { ok: false, reason: "not_yet_valid" };
  return { ok: true, payload };
}

const audIncludes = (payload, want) =>
  (Array.isArray(payload.aud) ? payload.aud : [payload.aud]).map(String).includes(String(want));

/**
 * Verify a Chat request. Never throws.
 * @returns {Promise<{ok: true, mode: string} | {ok: false, reason: string}>}
 */
export async function verifyChatRequest(env, request, endpointUrl) {
  const project = String(env.PM_CHAT_PROJECT_NUMBER || "").trim();
  if (!/^\d+$/.test(project)) return { ok: false, reason: "not_configured" };
  const auth = request.headers.get("Authorization") || "";
  const m = auth.match(/^Bearer\s+(\S+)$/i);
  if (!m) return { ok: false, reason: "missing" };
  const token = m[1];

  let iss;
  try {
    iss = b64urlToJson(token.split(".")[1] || "").iss;
  } catch {
    return { ok: false, reason: "malformed" };
  }

  if (iss === CHAT_SYSTEM) {
    const v = await verifyRs256(token, CHAT_SYSTEM_JWKS);
    if (!v.ok) return v;
    if (!audIncludes(v.payload, project)) return { ok: false, reason: "aud_mismatch" };
    return { ok: true, mode: "project_number" };
  }

  if (GOOGLE_ISSUERS.includes(iss)) {
    const v = await verifyRs256(token, GOOGLE_OIDC_JWKS);
    if (!v.ok) return v;
    if (!audIncludes(v.payload, endpointUrl)) return { ok: false, reason: "aud_mismatch" };
    const addonSa = `service-${project}@gcp-sa-gsuiteaddons.iam.gserviceaccount.com`;
    if (v.payload.email !== addonSa) return { ok: false, reason: "email_mismatch" };
    if (v.payload.email_verified !== true && v.payload.email_verified !== "true") {
      return { ok: false, reason: "email_unverified" };
    }
    return { ok: true, mode: "addon" };
  }

  return { ok: false, reason: "iss_mismatch" };
}

// ---------------------------------------------------------------------------
// Service-account access token (chat.bot scope), RS256-signed with WebCrypto.
// ---------------------------------------------------------------------------
let tokenCache = null; // { token, exp, email }

function pemToPkcs8(pem) {
  const body = String(pem)
    .replace(/-----BEGIN [^-]+-----/, "")
    .replace(/-----END [^-]+-----/, "")
    .replace(/\\n/g, "")
    .replace(/\s+/g, "");
  const bin = atob(body);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out.buffer;
}

export function parseServiceAccount(env) {
  if (!env.GOOGLE_CHAT_SA_KEY) throw new Error("GOOGLE_CHAT_SA_KEY is not set");
  let sa;
  try {
    sa = JSON.parse(env.GOOGLE_CHAT_SA_KEY);
  } catch {
    throw new Error("GOOGLE_CHAT_SA_KEY is not valid JSON");
  }
  if (!sa.client_email || !sa.private_key) throw new Error("GOOGLE_CHAT_SA_KEY is missing client_email or private_key");
  return sa;
}

export async function signServiceAccountJwt(sa, nowSec = Math.floor(Date.now() / 1000)) {
  const header = { alg: "RS256", typ: "JWT", ...(sa.private_key_id ? { kid: sa.private_key_id } : {}) };
  const claims = { iss: sa.client_email, sub: sa.client_email, scope: CHAT_SCOPE, aud: TOKEN_URL, iat: nowSec, exp: nowSec + 3600 };
  const input = `${strToB64url(JSON.stringify(header))}.${strToB64url(JSON.stringify(claims))}`;
  const key = await crypto.subtle.importKey(
    "pkcs8",
    pemToPkcs8(sa.private_key),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(input));
  return `${input}.${bytesToB64url(new Uint8Array(sig))}`;
}

export async function getChatAccessToken(env) {
  const sa = parseServiceAccount(env);
  if (tokenCache && tokenCache.email === sa.client_email && tokenCache.exp - 60 > Date.now() / 1000) return tokenCache.token;
  const assertion = await signServiceAccountJwt(sa);
  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=${encodeURIComponent("urn:ietf:params:oauth:grant-type:jwt-bearer")}&assertion=${assertion}`,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Google token ${res.status}: ${text.slice(0, 200)}`);
  const body = JSON.parse(text);
  tokenCache = { token: body.access_token, exp: Date.now() / 1000 + Number(body.expires_in || 3600), email: sa.client_email };
  return tokenCache.token;
}
export const _clearTokenCache = () => {
  tokenCache = null;
};

// Post a message as the app. `thread` = a thread resource name, or null for a
// DM / new thread. The fallback option means a vanished thread still gets the
// answer instead of an error.
export async function postChatMessage(env, { space, thread, text }) {
  const token = await getChatAccessToken(env);
  const qs = thread ? "?messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD" : "";
  const body = thread ? { text, thread: { name: thread } } : { text };
  const res = await fetch(`${CHAT_API}/${space}/messages${qs}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const out = await res.text();
  if (!res.ok) throw new Error(`Chat API ${res.status}: ${out.slice(0, 300)}`);
  return out ? JSON.parse(out) : {};
}
