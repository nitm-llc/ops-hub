// ============================================================================
// CLICKUP AUTOMATION  —  ops-hub feature module
// ----------------------------------------------------------------------------
// Replaces a Zapier Zap. When a task is created in a configured ClickUp list:
//   1. give it the next sequential code (YYYY.NNNN)
//   2. rename the task to "CODE - Task Name"
//   3. create a Drive folder of that name inside a per-list parent folder
//   4. create subfolders inside it
//   5. optionally copy a template doc in
//   6. share both with the company domain
//   7. write the resulting links back into ClickUp custom fields
//
// WHY THIS EXISTS:
//   The Zap kept every list's configuration in a JavaScript object literal
//   inside a code step, so adding a marketing list meant editing code and only
//   one person could do it. Here the configuration is DATA in D1, editable by
//   anyone from /clickup-automation/. That is the entire point — if you find
//   yourself hardcoding a list, a folder id or a subfolder name in this file,
//   you have rebuilt the thing we were replacing.
//
// HOW IT WIRES INTO src/index.js:
//   1. import { handleClickUpAutomationRoutes, clickUpAutomationCron }
//        from "./clickup-automation.js";                        (top of file)
//   2. Inside fetch(), AFTER `const path = url.pathname;` and BEFORE the global
//      OPTIONS handler (this module answers its own preflight):
//          const cuaResp = await handleClickUpAutomationRoutes(request, env, ctx, path);
//          if (cuaResp) return cuaResp;
//      Returns null when the path isn't ours, so it falls through harmlessly.
//   3. Inside scheduled(), to retry recoverable failures:
//          await clickUpAutomationCron(env);
//
// DB:      env.DB (content-calendar). Tables mirror migrations/0004_clickup_automation.sql
//          — change one, change the other.
// SECRETS: CLICKUP_TOKEN (already deployed, shared with stage-tracker + calendar),
//          GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY,
//          CLICKUP_AUTOMATION_ADMIN_SECRET (gates the dangerous endpoints).
//          The ClickUp webhook signing secret is NOT a secret binding — ClickUp
//          returns it once, at registration, and we store it straight into D1 so
//          it never passes through a human.
// VARS:    CLICKUP_TEAM_ID, APP_HOSTNAME, optionally ACCESS_TEAM_DOMAIN + ACCESS_AUD.
//
// SECURITY POSTURE (read before changing anything in here):
//   * /clickup-automation/webhook MUST be excluded from Cloudflare Access at the
//     edge, because ClickUp cannot log in. The exclusion must be that ONE path,
//     never /clickup-automation/* — that wildcard would publish the admin API.
//     The HMAC signature is the only thing guarding this endpoint, so it is
//     mandatory and fails closed.
//   * The *.workers.dev hostname bypasses Cloudflare Access entirely. The admin
//     API therefore refuses to serve any request that did not arrive on
//     APP_HOSTNAME. Belt and braces: set "workers_dev": false in wrangler.jsonc.
//   * No wildcard CORS here, unlike the rest of this repo. Same-origin only.
// ============================================================================

const CLICKUP_API = "https://api.clickup.com/api/v2";
const DRIVE_API = "https://www.googleapis.com/drive/v3";
const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";

const DEFAULT_APP_HOSTNAME = "ops.anurseinthemaking.com";
const ADMIN_HEADER = "X-Ops-Admin-Secret";

// A task already carrying a code keeps it and is not renamed.
const CODE_RE = /^(\d{4}\.\d+)\s*-\s*(.+)$/;
// Google/ClickUp resource ids. Validated before ever reaching a query string.
const ID_RE = /^[A-Za-z0-9_-]{8,128}$/;

const MAX_FOLDER_NAME = 180;      // Drive allows far more; this keeps names usable
const MAX_SUBFOLDERS = 25;        // a real ceiling: each one is a sequential API call
const PAYLOAD_KEEP_BYTES = 4000;  // truncated webhook body kept for debugging
const STALE_RUN_MINUTES = 5;      // a 'running' row older than this is reclaimable
const TREE_CACHE_SECONDS = 600;

// ---------------------------------------------------------------------------
// Schema — mirrors migrations/0004_clickup_automation.sql
// ---------------------------------------------------------------------------
let _tablesReady = false;

async function ensureClickUpAutomationTables(env) {
  if (_tablesReady) return;
  await env.DB.batch([
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS clickup_automations (
      id                    INTEGER PRIMARY KEY AUTOINCREMENT,
      name                  TEXT    NOT NULL,
      status                TEXT    NOT NULL DEFAULT 'draft'
                              CHECK (status IN ('draft','live','archived')),
      clickup_list_id       TEXT    NOT NULL UNIQUE,
      clickup_list_name     TEXT,
      clickup_folder_name   TEXT,
      clickup_space_name    TEXT,
      act_rename            INTEGER NOT NULL DEFAULT 1,
      act_create_folder     INTEGER NOT NULL DEFAULT 1,
      act_create_subfolders INTEGER NOT NULL DEFAULT 1,
      act_copy_template     INTEGER NOT NULL DEFAULT 0,
      act_write_back        INTEGER NOT NULL DEFAULT 1,
      drive_id              TEXT,
      drive_name            TEXT,
      drive_parent_id       TEXT,
      drive_parent_name     TEXT,
      drive_parent_path     TEXT,
      folder_name_template  TEXT    NOT NULL DEFAULT '{code} - {name}',
      subfolders            TEXT    NOT NULL DEFAULT '[]',
      template_file_id      TEXT,
      canva_link            TEXT,
      share_domain          TEXT    DEFAULT 'anurseinthemaking.com',
      field_drive_id        TEXT,
      field_drive           TEXT,
      field_doc_id          TEXT,
      field_doc             TEXT,
      field_canva_id        TEXT,
      field_canva           TEXT,
      last_check_at         TEXT,
      last_check_ok         INTEGER,
      last_check_detail     TEXT,
      created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
      updated_at            TEXT    NOT NULL DEFAULT (datetime('now')),
      updated_by            TEXT
    )`),
    env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_clickup_autom_list ON clickup_automations (clickup_list_id, status)`
    ),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS clickup_automation_runs (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      clickup_task_id  TEXT    NOT NULL UNIQUE,
      automation_id    INTEGER REFERENCES clickup_automations(id),
      clickup_list_id  TEXT,
      status           TEXT    NOT NULL
                         CHECK (status IN ('running','ok','skipped','error')),
      reason_code      TEXT,
      error_code       TEXT,
      code             TEXT,
      task_name        TEXT,
      folder_id        TEXT,
      folder_url       TEXT,
      doc_url          TEXT,
      fields_written   TEXT,
      steps            TEXT    NOT NULL DEFAULT '{}',
      attempts         INTEGER NOT NULL DEFAULT 0,
      error            TEXT,
      payload          TEXT,
      trigger_source   TEXT    NOT NULL DEFAULT 'webhook'
                         CHECK (trigger_source IN ('webhook','test','retry')),
      next_attempt_at  TEXT,
      started_at       TEXT,
      finished_at      TEXT,
      created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
      updated_at       TEXT    NOT NULL DEFAULT (datetime('now'))
    )`),
    env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_clickup_runs_created ON clickup_automation_runs (created_at DESC)`
    ),
    env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_clickup_runs_retry ON clickup_automation_runs (status, next_attempt_at)`
    ),
    env.DB.prepare(
      `CREATE INDEX IF NOT EXISTS idx_clickup_runs_autom ON clickup_automation_runs (automation_id, id DESC)`
    ),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS clickup_automation_codes (
      year        TEXT    PRIMARY KEY,
      last_value  INTEGER NOT NULL DEFAULT 0,
      updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
    )`),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS clickup_automation_webhooks (
      webhook_id   TEXT    PRIMARY KEY,
      secret       TEXT    NOT NULL,
      endpoint     TEXT,
      events       TEXT,
      active       INTEGER NOT NULL DEFAULT 1,
      created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
      created_by   TEXT
    )`),
    env.DB.prepare(`CREATE TABLE IF NOT EXISTS clickup_automation_state (
      key         TEXT    PRIMARY KEY,
      value       TEXT,
      updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
    )`),
  ]);
  _tablesReady = true;
}

// ---------------------------------------------------------------------------
// Responses. Same-origin only — deliberately NOT the repo's wildcard CORS.
// ---------------------------------------------------------------------------
function corsFor(request) {
  const h = { "Content-Type": "application/json", "Cache-Control": "no-store" };
  const origin = request.headers.get("Origin");
  if (origin) {
    try {
      if (new URL(origin).origin === new URL(request.url).origin) {
        h["Access-Control-Allow-Origin"] = origin;
        h["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS";
        h["Access-Control-Allow-Headers"] = `Content-Type, ${ADMIN_HEADER}`;
        h["Vary"] = "Origin";
      }
    } catch {
      /* malformed Origin — send no CORS headers at all */
    }
  }
  return h;
}

function json(request, data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: corsFor(request) });
}

// ---------------------------------------------------------------------------
// Security primitives
// ---------------------------------------------------------------------------

// Constant-time string compare. The repo's existing signature check uses `===`
// (src/index.js:4142); this does not.
function timingSafeEqual(a, b) {
  const x = String(a ?? "");
  const y = String(b ?? "");
  if (x.length !== y.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x.charCodeAt(i) ^ y.charCodeAt(i);
  return diff === 0;
}

async function hmacHex(secret, body) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const mac = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
  return [...new Uint8Array(mac)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// ClickUp signs the RAW request body with HMAC-SHA256, hex, in X-Signature.
// Hash the raw text — never a re-serialized object, because key order differs
// and you get intermittent failures that look like flakiness.
// Fails CLOSED: no stored secret means no valid request.
async function verifyClickUpSignature(env, rawBody, signature) {
  if (!signature) return false;
  const { results } = await env.DB.prepare(
    "SELECT secret FROM clickup_automation_webhooks WHERE active = 1"
  ).all();
  const secrets = (results || []).map((r) => r.secret).filter(Boolean);
  // Accept any active secret so a rotation has an overlap window.
  if (env.CLICKUP_WEBHOOK_SECRET) secrets.push(env.CLICKUP_WEBHOOK_SECRET);
  if (!secrets.length) return false;
  for (const s of secrets) {
    if (timingSafeEqual(await hmacHex(s, rawBody), signature)) return true;
  }
  return false;
}

// The *.workers.dev hostname skips Cloudflare Access completely, so anything
// arriving on another hostname has not been authenticated by anybody.
function isTrustedHost(env, url) {
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

// Verify the Cloudflare Access JWT properly, when configured. This is the only
// real defence against a request that reached the Worker without passing the
// Access gate. Optional because it needs the team domain + application AUD.
async function verifyAccessJwt(env, token) {
  const team = env.ACCESS_TEAM_DOMAIN;
  const aud = env.ACCESS_AUD;
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

  const certs = await fetchJson(`https://${team}/cdn-cgi/access/certs`);
  const jwk = (certs.keys || []).find((k) => k.kid === header.kid);
  if (!jwk) return { ok: false, reason: "unknown_kid" };

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
    b64urlToBytes(s),
    new TextEncoder().encode(`${h}.${p}`)
  );
  if (!valid) return { ok: false, reason: "bad_signature" };

  const now = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < now) return { ok: false, reason: "expired" };
  if (payload.nbf && payload.nbf > now + 60) return { ok: false, reason: "not_yet_valid" };
  if (payload.iss !== `https://${team}`) return { ok: false, reason: "iss_mismatch" };
  const auds = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!auds.includes(aud)) return { ok: false, reason: "aud_mismatch" };

  return { ok: true, email: payload.email || null };
}

// Gate for every /api/ route. Layered:
//   1. must have arrived on the Access-protected hostname
//   2. if Access JWT verification is configured, it must pass
// Returns null when allowed, or a Response to return immediately.
async function guardAdminApi(request, env, url) {
  if (!isTrustedHost(env, url)) {
    return json(
      request,
      {
        error:
          "This API is only served on the Cloudflare Access hostname. " +
          "The *.workers.dev route bypasses the login gate and is refused.",
      },
      403
    );
  }
  if (env.ACCESS_TEAM_DOMAIN && env.ACCESS_AUD) {
    const v = await verifyAccessJwt(env, request.headers.get("Cf-Access-Jwt-Assertion"));
    if (!v.ok) return json(request, { error: `Cloudflare Access check failed: ${v.reason}` }, 401);
  }
  return null;
}

// Extra gate for the genuinely dangerous, once-ever, Mark-only operations:
// registering or deleting the workspace webhook, and hard deletes. Header, not
// query param (query params leak into logs and referrers). Fails closed.
function guardDangerous(request, env) {
  const expected = env.CLICKUP_AUTOMATION_ADMIN_SECRET;
  if (!expected) {
    return json(
      request,
      {
        error:
          "CLICKUP_AUTOMATION_ADMIN_SECRET is not set, so this operation is refused. " +
          "Set it with: npx wrangler secret put CLICKUP_AUTOMATION_ADMIN_SECRET",
      },
      503
    );
  }
  if (!timingSafeEqual(request.headers.get(ADMIN_HEADER), expected)) {
    return json(request, { error: `Missing or wrong ${ADMIN_HEADER} header.` }, 401);
  }
  return null;
}

function actor(request) {
  return request.headers.get("Cf-Access-Authenticated-User-Email") || null;
}

// Registering the webhook is a setup step, not a destructive one: it refuses to
// create a second webhook for an endpoint that already has one, and it writes a
// signing secret nobody ever sees. So a human Cloudflare Access has already
// authenticated is enough.
//
// Demanding the shared secret as well meant the only way to finish setup was a
// terminal plus a secret only one person held — which is precisely the problem
// this module exists to remove. DEPLOY.md documented a curl for it that could
// never have worked: Access rejects an unauthenticated request before the Worker
// sees the header at all.
//
// Anything destructive — force-replacing a webhook, deleting one, hard-deleting
// an automation — still demands the secret. Being logged in is authentication,
// not a licence to do the irreversible thing by accident.
function guardSetup(request, env) {
  if (actor(request)) return null;
  return guardDangerous(request, env);
}

// Every id is checked before it can reach a URL or a Drive `q=` search string.
// The starter code interpolated a caller-supplied parent id straight into a
// Drive query, which is a query-injection hole.
function assertId(value, what) {
  const v = String(value || "");
  if (!ID_RE.test(v)) throw new HttpError(400, `That ${what} doesn't look valid.`);
  return v;
}

class HttpError extends Error {
  constructor(status, message, code) {
    super(message);
    this.status = status;
    this.code = code || null;
  }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJson(url, init) {
  const res = await fetch(url, init);
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { _raw: text };
  }
  if (!res.ok) {
    const err = new HttpError(res.status, `${url.split("?")[0]} -> ${res.status}: ${text.slice(0, 300)}`);
    err.upstreamStatus = res.status;
    throw err;
  }
  return data;
}

async function getState(env, key) {
  try {
    const row = await env.DB.prepare("SELECT value FROM clickup_automation_state WHERE key = ?")
      .bind(key)
      .first();
    return row ? row.value : null;
  } catch {
    return null;
  }
}

async function setState(env, key, value) {
  await env.DB.prepare(
    `INSERT INTO clickup_automation_state (key, value, updated_at)
     VALUES (?, ?, datetime('now'))
     ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')`
  )
    .bind(key, value)
    .run();
}

function safeParse(text, fallback) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

// ---------------------------------------------------------------------------
// Google service-account auth (WebCrypto — no Node built-ins in Workers)
// ---------------------------------------------------------------------------
let _googleTok = null; // per-isolate cache, same pattern as shopifyToken()

function b64urlFromBytes(buf) {
  const arr = new Uint8Array(buf);
  let bin = "";
  // Chunked rather than String.fromCharCode(...arr) — spreading a large buffer
  // blows the argument limit.
  for (let i = 0; i < arr.length; i += 0x8000) {
    bin += String.fromCharCode.apply(null, arr.subarray(i, i + 0x8000));
  }
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function b64urlFromString(s) {
  return b64urlFromBytes(new TextEncoder().encode(s));
}

// Wrangler stores a PEM with literal backslash-n, and pasting one by hand can
// give you real newlines. Handle both.
function pemToPkcs8(pem) {
  const body = String(pem || "")
    .replace(/-----BEGIN [A-Z ]*PRIVATE KEY-----/, "")
    .replace(/-----END [A-Z ]*PRIVATE KEY-----/, "")
    .replace(/\\n/g, "")
    .replace(/\s/g, "");
  if (!body) throw new HttpError(503, "GOOGLE_PRIVATE_KEY is not set or is empty.");
  const bin = atob(body);
  const buf = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
  return buf.buffer;
}

async function googleAccessToken(env) {
  const now = Date.now();
  if (_googleTok && _googleTok.expiresAt > now) return _googleTok.token;

  // Cross-isolate cache, so a burst of webhooks doesn't mint a token each time.
  const cached = safeParse(await getState(env, "google_token"), null);
  if (cached && cached.expiresAt > now) {
    _googleTok = cached;
    return cached.token;
  }

  if (!env.GOOGLE_CLIENT_EMAIL) throw new HttpError(503, "GOOGLE_CLIENT_EMAIL is not set.");

  const iat = Math.floor(now / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const claims = {
    iss: env.GOOGLE_CLIENT_EMAIL,
    scope: "https://www.googleapis.com/auth/drive",
    aud: GOOGLE_TOKEN_URL,
    iat,
    exp: iat + 3600,
  };
  const signingInput = `${b64urlFromString(JSON.stringify(header))}.${b64urlFromString(
    JSON.stringify(claims)
  )}`;

  const key = await crypto.subtle.importKey(
    "pkcs8",
    pemToPkcs8(env.GOOGLE_PRIVATE_KEY),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    key,
    new TextEncoder().encode(signingInput)
  );
  const assertion = `${signingInput}.${b64urlFromBytes(sig)}`;

  const res = await fetch(GOOGLE_TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error || !data.access_token) {
    throw new HttpError(
      502,
      `Google auth failed: ${data.error_description || data.error || res.status}`,
      "google_auth"
    );
  }

  // Refresh 300s early, matching shopifyToken().
  const entry = {
    token: data.access_token,
    expiresAt: now + (Number(data.expires_in || 3600) - 300) * 1000,
  };
  _googleTok = entry;
  await setState(env, "google_token", JSON.stringify(entry));
  return entry.token;
}

// ---------------------------------------------------------------------------
// Drive client. Every call carries supportsAllDrives — the parent folders live
// in a shared drive, and omitting it is the classic cause of baffling 404s.
// ---------------------------------------------------------------------------
async function driveFetch(env, path, init = {}, attempt = 0) {
  const token = await googleAccessToken(env);
  const res = await fetch(`${DRIVE_API}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();

  if ((res.status === 429 || res.status >= 500) && attempt < 4) {
    await sleep(Math.min(8000, 500 * Math.pow(2, attempt)));
    return driveFetch(env, path, init, attempt + 1);
  }

  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { _raw: text };
  }
  if (!res.ok || data.error) {
    const msg = data.error?.message || text.slice(0, 300) || `HTTP ${res.status}`;
    const err = new HttpError(502, `Drive ${res.status}: ${msg}`, driveErrorCode(res.status, msg));
    err.upstreamStatus = res.status;
    throw err;
  }
  return data;
}

function driveErrorCode(status, msg) {
  if (status === 403 || /permission|insufficient|forbidden/i.test(msg)) return "drive_permission";
  if (status === 404 || /not found/i.test(msg)) return "drive_parent_missing";
  if (status === 429) return "drive_rate_limit";
  return "drive_error";
}

async function driveCreateFolder(env, name, parentId) {
  assertId(parentId, "Drive folder id");
  return driveFetch(env, "/files?supportsAllDrives=true&fields=id,name,webViewLink", {
    method: "POST",
    body: JSON.stringify({
      name,
      mimeType: "application/vnd.google-apps.folder",
      parents: [parentId],
    }),
  });
}

async function driveCopyFile(env, fileId, name, parentId) {
  assertId(fileId, "template file id");
  assertId(parentId, "Drive folder id");
  return driveFetch(
    env,
    `/files/${fileId}/copy?supportsAllDrives=true&fields=id,name,webViewLink,mimeType`,
    { method: "POST", body: JSON.stringify({ name, parents: [parentId] }) }
  );
}

async function driveDelete(env, fileId) {
  assertId(fileId, "Drive file id");
  await driveFetch(env, `/files/${fileId}?supportsAllDrives=true`, { method: "DELETE" });
}

// Non-fatal: a shared drive's sharing policy can forbid domain permissions, and
// that must not fail the whole run.
async function driveShareWithDomain(env, fileId, domain) {
  if (!domain) return false;
  try {
    await driveFetch(env, `/files/${fileId}/permissions?supportsAllDrives=true`, {
      method: "POST",
      body: JSON.stringify({ type: "domain", domain, role: "writer" }),
    });
    return true;
  } catch {
    return false;
  }
}

async function driveGetFile(env, fileId) {
  assertId(fileId, "Drive folder id");
  return driveFetch(
    env,
    `/files/${fileId}?supportsAllDrives=true&fields=id,name,mimeType,driveId,parents,trashed,capabilities(canAddChildren)`
  );
}

async function driveListSharedDrives(env) {
  const data = await driveFetch(env, "/drives?pageSize=100&fields=drives(id,name)");
  return data.drives || [];
}

async function driveListChildFolders(env, parentId, pageToken) {
  const safe = assertId(parentId, "Drive folder id");
  const q = encodeURIComponent(
    `'${safe}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`
  );
  const page = pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "";
  const data = await driveFetch(
    env,
    `/files?q=${q}&supportsAllDrives=true&includeItemsFromAllDrives=true` +
      `&corpora=allDrives&orderBy=name&pageSize=100` +
      `&fields=nextPageToken,files(id,name,capabilities(canAddChildren))${page}`
  );
  return { folders: data.files || [], nextPageToken: data.nextPageToken || null };
}

// Build the human breadcrumb ("Ads > Campaigns > Google"). This is what makes a
// pasted folder link safe to accept — the user sees where they actually pointed.
async function driveFolderPath(env, fileId, maxDepth = 8) {
  const names = [];
  let id = fileId;
  let driveName = null;
  for (let i = 0; i < maxDepth && id; i++) {
    let f;
    try {
      f = await driveGetFile(env, id);
    } catch {
      break;
    }
    names.unshift(f.name);
    const parent = (f.parents || [])[0];
    if (!parent || parent === f.driveId) {
      if (f.driveId) {
        try {
          const d = await driveFetch(env, `/drives/${f.driveId}?fields=name`);
          driveName = d.name || null;
        } catch {
          /* the service account may not be able to read the drive record */
        }
      }
      break;
    }
    id = parent;
  }
  if (driveName) names.unshift(driveName);
  return names.join(" › ");
}

// Accepts a pasted Drive URL or a bare id. Never fetches a user-supplied URL —
// only extracts an id from it.
function extractDriveFolderId(input) {
  const s = String(input || "").trim();
  if (!s) return null;
  if (ID_RE.test(s) && !s.includes("/")) return s;
  const patterns = [
    /\/folders\/([A-Za-z0-9_-]{8,128})/,
    /[?&]id=([A-Za-z0-9_-]{8,128})/,
    /\/drive\/u\/\d+\/folders\/([A-Za-z0-9_-]{8,128})/,
    /\/d\/([A-Za-z0-9_-]{8,128})/,
  ];
  for (const re of patterns) {
    const m = s.match(re);
    if (m) return m[1];
  }
  return null;
}

// ---------------------------------------------------------------------------
// ClickUp client
// ---------------------------------------------------------------------------
async function cuFetch(env, path, init = {}, attempt = 0) {
  if (!env.CLICKUP_TOKEN) throw new HttpError(503, "CLICKUP_TOKEN is not set.", "clickup_auth");
  const res = await fetch(`${CLICKUP_API}${path}`, {
    ...init,
    headers: {
      Authorization: env.CLICKUP_TOKEN,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();

  if ((res.status === 429 || res.status >= 500) && attempt < 4) {
    await sleep(Math.min(8000, 500 * Math.pow(2, attempt)));
    return cuFetch(env, path, init, attempt + 1);
  }

  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { _raw: text };
  }
  if (!res.ok) {
    const msg = data.err || data.error || text.slice(0, 300) || `HTTP ${res.status}`;
    const code =
      res.status === 401 || res.status === 403
        ? "clickup_auth"
        : res.status === 429
        ? "clickup_rate_limit"
        : res.status === 404
        ? "clickup_not_found"
        : "clickup_error";
    const err = new HttpError(502, `ClickUp ${res.status}: ${msg}`, code);
    err.upstreamStatus = res.status;
    throw err;
  }
  return data;
}

function teamId(env) {
  const t = env.CLICKUP_TEAM_ID;
  if (!t) throw new HttpError(503, "CLICKUP_TEAM_ID is not configured.");
  return String(t);
}

const cuGetTask = (env, id) => cuFetch(env, `/task/${encodeURIComponent(id)}`);

const cuRenameTask = (env, id, name) =>
  cuFetch(env, `/task/${encodeURIComponent(id)}`, {
    method: "PUT",
    body: JSON.stringify({ name }),
  });

async function cuListFields(env, listId) {
  const data = await cuFetch(env, `/list/${encodeURIComponent(listId)}/field`);
  return data.fields || [];
}

const cuSetField = (env, taskId, fieldId, value) =>
  cuFetch(env, `/task/${encodeURIComponent(taskId)}/field/${encodeURIComponent(fieldId)}`, {
    method: "POST",
    body: JSON.stringify({ value }),
  });

// Exact, case-insensitive name match. The old Zap used
// includes('Drive') && !includes('Link'), which is case-sensitive and one
// rename away from silently writing nothing.
function findFieldByName(fields, name) {
  if (!name) return null;
  const target = String(name).trim().toLowerCase();
  return fields.find((f) => String(f.name || "").trim().toLowerCase() === target) || null;
}

// Resolve a configured field to an id: stored id first, name as the fallback.
function resolveField(fields, id, name) {
  if (id) {
    const byId = fields.find((f) => f.id === id);
    if (byId) return byId;
  }
  return findFieldByName(fields, name);
}

async function cuWorkspaceTree(env, refresh = false) {
  if (!refresh) {
    const cached = safeParse(await getState(env, "tree"), null);
    if (cached && Date.now() - cached.at < TREE_CACHE_SECONDS * 1000) return cached.tree;
  }

  const team = teamId(env);
  const spacesData = await cuFetch(env, `/team/${team}/space?archived=false`);
  const spaces = [];

  for (const sp of spacesData.spaces || []) {
    const [foldersData, looseData] = await Promise.all([
      cuFetch(env, `/space/${sp.id}/folder?archived=false`).catch(() => ({ folders: [] })),
      cuFetch(env, `/space/${sp.id}/list?archived=false`).catch(() => ({ lists: [] })),
    ]);
    spaces.push({
      id: sp.id,
      name: sp.name,
      folders: (foldersData.folders || []).map((f) => ({
        id: f.id,
        name: f.name,
        lists: (f.lists || []).map((l) => ({ id: l.id, name: l.name })),
      })),
      lists: (looseData.lists || []).map((l) => ({ id: l.id, name: l.name })),
    });
  }

  const tree = { team_id: team, spaces, fetched_at: new Date().toISOString() };
  await setState(env, "tree", JSON.stringify({ at: Date.now(), tree }));
  return tree;
}

function findListInTree(tree, listId) {
  for (const sp of tree.spaces || []) {
    for (const l of sp.lists || []) {
      if (String(l.id) === String(listId)) {
        return { list: l, space: sp.name, folder: null };
      }
    }
    for (const fo of sp.folders || []) {
      for (const l of fo.lists || []) {
        if (String(l.id) === String(listId)) {
          return { list: l, space: sp.name, folder: fo.name };
        }
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Codes — YYYY.NNNN, from a counter. The Zap used Math.random() and would
// eventually hand two tasks the same code.
// ---------------------------------------------------------------------------
function currentYear() {
  return String(new Date().getUTCFullYear());
}

async function nextCode(env) {
  const year = currentYear();
  const row = await env.DB.prepare(
    `INSERT INTO clickup_automation_codes (year, last_value, updated_at)
     VALUES (?, 1, datetime('now'))
     ON CONFLICT(year) DO UPDATE SET last_value = last_value + 1, updated_at = datetime('now')
     RETURNING last_value`
  )
    .bind(year)
    .first();
  // No silent fallback: minting 0001 forever because RETURNING came back empty
  // would be far worse than a loud failure.
  if (!row || row.last_value == null) {
    throw new HttpError(500, "Could not allocate a code (counter returned nothing).", "code_sequence");
  }
  return `${year}.${String(row.last_value).padStart(4, "0")}`;
}

// Read-only: what the next code WOULD be. Used by previews and preflight, which
// must never consume a code or the numbering develops gaps people report as bugs.
async function peekCode(env) {
  const year = currentYear();
  const row = await env.DB.prepare(
    "SELECT last_value FROM clickup_automation_codes WHERE year = ?"
  )
    .bind(year)
    .first();
  const next = (row?.last_value || 0) + 1;
  return `${year}.${String(next).padStart(4, "0")}`;
}

// ---------------------------------------------------------------------------
// Naming
// ---------------------------------------------------------------------------
function parseTaskName(name) {
  const raw = String(name || "").trim();
  const m = raw.match(CODE_RE);
  if (m) return { code: m[1], clean: m[2].trim(), hadCode: true };
  return { code: null, clean: raw, hadCode: false };
}

// Task names are written by anyone in the workspace, so treat them as untrusted
// input before they become a filesystem-ish name.
function sanitizeName(s) {
  let t = String(s || "")
    // Control characters and newlines out; slashes are not usable in a name.
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/[\\/]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
  if (t.length > MAX_FOLDER_NAME) t = t.slice(0, MAX_FOLDER_NAME).trim();
  return t;
}

// Everything a name template may refer to, in one place. ClickUp's own task id
// is offered so a folder can be named after the task instead of a code we mint:
// it is stable, never has gaps, and survives a retry unchanged, which a
// counter does not.
//
// `custom_id` is ClickUp's human-readable id (e.g. GOOG-123) and is null unless
// the Custom Task IDs ClickApp is switched on for the workspace.
function buildVars({ code = null, name = "", task = null } = {}) {
  return {
    code: code ?? "",
    name,
    task_id: task?.id ? String(task.id) : "",
    custom_id: task?.custom_id ? String(task.custom_id) : "",
    url: task?.url ? String(task.url) : "",
    year: String(currentYear()),
  };
}

// Does anything in play actually ask for a minted code? Allocation is a write to
// a counter, so doing it when no template mentions {code} burns a number for
// nothing — which is how the sequence developed gaps.
function templatesWantCode(automation) {
  const raw = Array.isArray(automation.subfolders)
    ? automation.subfolders
    : safeParse(automation.subfolders, []);
  const all = [automation.folder_name_template || "{code} - {name}"]
    .concat(Array.isArray(raw) ? raw : [])
    .join(" ");
  return /\{code\}/.test(all);
}

function renderTemplate(template, vars) {
  return String(template || "").replace(/\{(\w+)\}/g, (m, k) => (k in vars ? vars[k] : m));
}

function subfolderNames(automation, vars) {
  // Callers hand us either a raw D1 row (subfolders is a JSON string) or a
  // shaped one (already an array). Accept both — re-parsing an array silently
  // yields [], which made previews claim no subfolders would be created.
  const raw = Array.isArray(automation.subfolders)
    ? automation.subfolders
    : safeParse(automation.subfolders, []);
  if (!Array.isArray(raw)) return [];
  return raw
    .map((n) => sanitizeName(renderTemplate(n, vars)))
    .filter(Boolean)
    .slice(0, MAX_SUBFOLDERS);
}

function folderNameFor(automation, vars) {
  const name = sanitizeName(renderTemplate(automation.folder_name_template || "{code} - {name}", vars));
  return name || vars.code || vars.task_id || "Untitled";
}

// ---------------------------------------------------------------------------
// Run ledger
// ---------------------------------------------------------------------------
function isUniqueViolation(e) {
  return /UNIQUE constraint failed/i.test(String(e?.message || e));
}

// The UNIQUE index on clickup_task_id IS the lock. Two near-simultaneous
// deliveries both used to pass a SELECT-then-act check and both create a
// folder; here exactly one wins the INSERT and the loser's constraint violation
// is the signal to stand down.
async function claimRun(env, { taskId, listId, taskName, rawBody, trigger = "webhook", force = false }) {
  try {
    await env.DB.prepare(
      `INSERT INTO clickup_automation_runs
         (clickup_task_id, clickup_list_id, status, task_name, attempts, steps,
          payload, trigger_source, started_at, created_at, updated_at)
       VALUES (?, ?, 'running', ?, 1, '{}', ?, ?, datetime('now'), datetime('now'), datetime('now'))`
    )
      .bind(
        taskId,
        listId ? String(listId) : null,
        taskName || null,
        rawBody ? String(rawBody).slice(0, PAYLOAD_KEEP_BYTES) : null,
        trigger
      )
      .run();
    return { claimed: true, steps: {} };
  } catch (e) {
    if (!isUniqueViolation(e)) throw e;
  }

  const existing = await env.DB.prepare(
    "SELECT * FROM clickup_automation_runs WHERE clickup_task_id = ?"
  )
    .bind(taskId)
    .first();
  if (!existing) return { claimed: false, reason: "race" };

  // Already done. This is the idempotency short-circuit.
  if (existing.status === "ok" && !force) {
    return { claimed: false, reason: "already_done", run: existing };
  }
  // Someone else is mid-flight, unless they died and left the row behind.
  if (existing.status === "running" && !force) {
    const stale = await env.DB.prepare(
      `SELECT 1 AS stale FROM clickup_automation_runs
        WHERE clickup_task_id = ?
          AND started_at < datetime('now', ?)`
    )
      .bind(taskId, `-${STALE_RUN_MINUTES} minutes`)
      .first();
    if (!stale) return { claimed: false, reason: "in_flight", run: existing };
  }

  // A previous 'skipped' or 'error' row must be reclaimed, not re-INSERTed.
  // Otherwise a task that logged 'skipped' while the automation was a draft can
  // never succeed after it goes live — which reads as "I turned it on and
  // nothing happened", the exact failure this rebuild exists to remove.
  await env.DB.prepare(
    `UPDATE clickup_automation_runs
        SET status = 'running', attempts = attempts + 1, error = NULL, error_code = NULL,
            reason_code = NULL, next_attempt_at = NULL, trigger_source = ?,
            started_at = datetime('now'), finished_at = NULL, updated_at = datetime('now')
      WHERE clickup_task_id = ?`
  )
    .bind(trigger, taskId)
    .run();

  return { claimed: true, steps: safeParse(existing.steps, {}) || {}, previous: existing };
}

async function finishRun(env, taskId, patch) {
  await env.DB.prepare(
    `UPDATE clickup_automation_runs
        SET status = ?, automation_id = COALESCE(?, automation_id),
            clickup_list_id = COALESCE(?, clickup_list_id),
            reason_code = ?, error_code = ?, code = COALESCE(?, code),
            task_name = COALESCE(?, task_name), folder_id = COALESCE(?, folder_id),
            folder_url = COALESCE(?, folder_url), doc_url = COALESCE(?, doc_url),
            fields_written = COALESCE(?, fields_written), steps = ?, error = ?,
            next_attempt_at = ?, finished_at = datetime('now'), updated_at = datetime('now')
      WHERE clickup_task_id = ?`
  )
    .bind(
      patch.status,
      patch.automationId ?? null,
      patch.listId ?? null,
      patch.reasonCode ?? null,
      patch.errorCode ?? null,
      patch.code ?? null,
      patch.taskName ?? null,
      patch.folderId ?? null,
      patch.folderUrl ?? null,
      patch.docUrl ?? null,
      patch.fieldsWritten ? JSON.stringify(patch.fieldsWritten) : null,
      JSON.stringify(patch.steps || {}),
      patch.error ? String(patch.error).slice(0, 900) : null,
      patch.nextAttemptAt ?? null,
      taskId
    )
    .run();
}

// A run that logs a skip before it ever claimed anything.
async function logSkip(env, { taskId, listId, taskName, rawBody, reasonCode }) {
  await env.DB.prepare(
    `INSERT INTO clickup_automation_runs
       (clickup_task_id, clickup_list_id, status, reason_code, task_name, payload,
        started_at, finished_at, created_at, updated_at)
     VALUES (?, ?, 'skipped', ?, ?, ?, datetime('now'), datetime('now'), datetime('now'), datetime('now'))
     ON CONFLICT(clickup_task_id) DO UPDATE SET
       status = 'skipped', reason_code = excluded.reason_code,
       clickup_list_id = COALESCE(excluded.clickup_list_id, clickup_list_id),
       task_name = COALESCE(excluded.task_name, task_name),
       finished_at = datetime('now'), updated_at = datetime('now')`
  )
    .bind(
      taskId,
      listId ? String(listId) : null,
      reasonCode,
      taskName || null,
      rawBody ? String(rawBody).slice(0, PAYLOAD_KEEP_BYTES) : null
    )
    .run();
}

// Which failures are worth retrying automatically on the cron.
function isRecoverable(code) {
  return ["drive_rate_limit", "clickup_rate_limit", "drive_error", "clickup_error", "google_auth"].includes(
    code
  );
}

// ---------------------------------------------------------------------------
// The pipeline. `steps` carries what already succeeded, so a retry resumes
// instead of creating a second folder.
// ---------------------------------------------------------------------------
async function runAutomation(env, automation, task, steps = {}) {
  const parsed = parseTaskName(task.name);
  const code =
    steps.code ||
    (parsed.hadCode ? parsed.code : templatesWantCode(automation) ? await nextCode(env) : null);
  steps.code = code;

  const vars = buildVars({ code, name: sanitizeName(parsed.clean), task });
  const folderName = folderNameFor(automation, vars);

  // --- main folder ---------------------------------------------------------
  if (automation.act_create_folder && !steps.folder) {
    if (!automation.drive_parent_id) {
      throw new HttpError(400, "No Drive parent folder is configured.", "drive_parent_missing");
    }
    const folder = await driveCreateFolder(env, folderName, automation.drive_parent_id);
    steps.folder = {
      id: folder.id,
      url: folder.webViewLink || `https://drive.google.com/drive/folders/${folder.id}`,
    };
    if (automation.share_domain) {
      steps.folder.shared = await driveShareWithDomain(env, folder.id, automation.share_domain);
    }
  }
  const folderId = steps.folder?.id || null;

  // --- subfolders ---------------------------------------------------------
  if (automation.act_create_subfolders && folderId) {
    const want = subfolderNames(automation, vars);
    steps.subfolders = steps.subfolders || [];
    for (const name of want) {
      if (steps.subfolders.includes(name)) continue; // already made on an earlier attempt
      await driveCreateFolder(env, name, folderId);
      steps.subfolders.push(name);
    }
  }

  // --- template doc -------------------------------------------------------
  if (automation.act_copy_template && automation.template_file_id && folderId && !steps.doc) {
    const doc = await driveCopyFile(env, automation.template_file_id, folderName, folderId);
    steps.doc = {
      id: doc.id,
      url: doc.webViewLink || `https://docs.google.com/document/d/${doc.id}/edit`,
    };
    if (automation.share_domain) {
      await driveShareWithDomain(env, doc.id, automation.share_domain);
    }
  }

  // --- rename the task ----------------------------------------------------
  if (automation.act_rename && !parsed.hadCode && !steps.renamed && task.name !== folderName) {
    await cuRenameTask(env, task.id, folderName);
    steps.renamed = true;
  }

  // --- write the links back ----------------------------------------------
  if (automation.act_write_back) {
    const fields = await cuListFields(env, automation.clickup_list_id);
    const wanted = [
      ["drive", automation.field_drive_id, automation.field_drive, steps.folder?.url],
      ["doc", automation.field_doc_id, automation.field_doc, steps.doc?.url],
      ["canva", automation.field_canva_id, automation.field_canva, automation.canva_link],
    ];
    steps.fields = steps.fields || [];
    for (const [kind, id, name, value] of wanted) {
      if (!value || (!id && !name)) continue;
      if (steps.fields.some((f) => f.kind === kind)) continue;
      const field = resolveField(fields, id, name);
      if (!field) {
        // Not fatal — but recorded, because a silently missing field is the bug
        // that made the old version untrustworthy.
        steps.fields.push({ kind, skipped: "field_not_found", wanted: name || id });
        continue;
      }
      await cuSetField(env, task.id, field.id, value);
      steps.fields.push({ kind, name: field.name, id: field.id });
    }
  }

  return {
    code,
    folderName,
    folderId,
    folderUrl: steps.folder?.url || null,
    docUrl: steps.doc?.url || null,
    steps,
  };
}

// ---------------------------------------------------------------------------
// Webhook handler. Never returns 5xx: repeated 5xx makes ClickUp disable the
// webhook, which would silently kill every automation at once. The single
// exception is 401 for a bad signature, which ClickUp does not count.
// ---------------------------------------------------------------------------
async function handleWebhook(request, env, ctx) {
  const rawBody = await request.text();

  // Verify BEFORE parsing and before any database write, so an unsigned flood
  // costs one HMAC and cannot fill the runs table or burn codes.
  const ok = await verifyClickUpSignature(env, rawBody, request.headers.get("X-Signature"));
  if (!ok) {
    return new Response(JSON.stringify({ error: "invalid signature" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  const reply = (body, status = 200) =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });

  let payload;
  try {
    payload = JSON.parse(rawBody);
  } catch {
    return reply({ ok: false, error: "invalid json" });
  }

  if (payload.event !== "taskCreated") {
    return reply({ ok: true, skipped: "event", event: payload.event || null });
  }
  const taskId = payload.task_id;
  if (!taskId) return reply({ ok: true, skipped: "no task id" });

  // Acknowledge first, then work.
  //
  // Building a folder tree takes several Drive round-trips — longer than
  // ClickUp is willing to wait. When ClickUp gave up it disconnected, and
  // Cloudflare cancelled the Worker mid-flight: outcome "canceled", no
  // exception, and a run row frozen at 'running' forever. ClickUp then retried,
  // saw the frozen row, and skipped. Both sides looked fine. Nothing ran.
  //
  // waitUntil keeps the work alive after the response has gone, which is the
  // only way to be both fast enough for the caller and slow enough for Drive.
  ctx.waitUntil(
    processTask(env, taskId, { rawBody, trigger: "webhook" }).catch((err) => {
      // processTask records its own failures; this is the backstop for one that
      // fails before it can.
      console.error("clickup-automation processTask:", taskId, err);
    })
  );
  return reply({ ok: true, queued: taskId });
}

// Shared by the webhook, the retry button and the cron.
async function processTask(env, taskId, { rawBody = null, trigger = "webhook", force = false } = {}) {
  await ensureClickUpAutomationTables(env);

  // Cheap pre-check so a redelivery doesn't even hit the ClickUp API.
  if (!force) {
    const done = await env.DB.prepare(
      "SELECT status FROM clickup_automation_runs WHERE clickup_task_id = ? AND status = 'ok'"
    )
      .bind(taskId)
      .first();
    if (done) return { skipped: "already_done" };
  }

  let task;
  try {
    task = await cuGetTask(env, taskId);
  } catch (e) {
    await logSkip(env, { taskId, rawBody, reasonCode: "task_unreadable" });
    return { skipped: "task_unreadable", error: String(e.message || e) };
  }

  const listId = task?.list?.id;
  if (!listId) {
    await logSkip(env, { taskId, taskName: task?.name, rawBody, reasonCode: "no_list" });
    return { skipped: "no_list" };
  }

  const automation = await env.DB.prepare(
    "SELECT * FROM clickup_automations WHERE clickup_list_id = ?"
  )
    .bind(String(listId))
    .first();

  if (!automation) {
    await logSkip(env, {
      taskId,
      listId,
      taskName: task.name,
      rawBody,
      reasonCode: "no_automation",
    });
    return { skipped: "no_automation", list: task?.list?.name || listId };
  }
  if (automation.status !== "live") {
    await logSkip(env, {
      taskId,
      listId,
      taskName: task.name,
      rawBody,
      reasonCode: automation.status === "draft" ? "automation_draft" : "automation_archived",
    });
    return { skipped: `automation_${automation.status}` };
  }

  const claim = await claimRun(env, {
    taskId,
    listId,
    taskName: task.name,
    rawBody,
    trigger,
    force,
  });
  if (!claim.claimed) return { skipped: claim.reason };

  try {
    const result = await runAutomation(env, automation, task, claim.steps);
    await finishRun(env, taskId, {
      status: "ok",
      automationId: automation.id,
      listId,
      code: result.code,
      taskName: result.folderName,
      folderId: result.folderId,
      folderUrl: result.folderUrl,
      docUrl: result.docUrl,
      fieldsWritten: (result.steps.fields || []).map((f) => f.name || f.wanted),
      steps: result.steps,
    });
    return {
      code: result.code,
      folder: result.folderUrl,
      doc: result.docUrl,
      name: result.folderName,
    };
  } catch (err) {
    const code = err?.code || "unknown";
    // We own retries now: we never 5xx, so ClickUp never retries us.
    const retryable = isRecoverable(code);
    await finishRun(env, taskId, {
      status: "error",
      automationId: automation.id,
      listId,
      errorCode: code,
      taskName: task.name,
      steps: claim.steps || {},
      error: err?.message || String(err),
      nextAttemptAt: null,
    });
    if (retryable) {
      await env.DB.prepare(
        `UPDATE clickup_automation_runs
            SET next_attempt_at = datetime('now', '+5 minutes')
          WHERE clickup_task_id = ?`
      )
        .bind(taskId)
        .run();
    }
    return { error: err?.message || String(err), error_code: code };
  }
}

// ---------------------------------------------------------------------------
// Preflight — "will this actually work?" in plain language.
// ---------------------------------------------------------------------------
async function preflight(env, cfg, { deep = false } = {}) {
  const checks = [];
  const add = (key, label, status, detail, fixCode, meta) =>
    checks.push({ key, label, status, detail: detail || null, fix_code: fixCode || null, meta: meta || null });

  // --- the workspace webhook ---------------------------------------------
  const hook = await env.DB.prepare(
    "SELECT webhook_id FROM clickup_automation_webhooks WHERE active = 1 LIMIT 1"
  ).first();
  if (hook) {
    add("webhook", "ClickUp is sending us new tasks", "ok");
  } else {
    add(
      "webhook",
      "ClickUp is sending us new tasks",
      "fail",
      "No ClickUp webhook is registered, so nothing will ever run.",
      "reconnect_webhook"
    );
  }

  // --- ClickUp reachable --------------------------------------------------
  // Same reasoning as the Google check below: if the connection is down, say so
  // once and skip the downstream checks rather than blaming the list or the
  // field, which would send someone off reconfiguring something that is fine.
  let fields = [];
  let clickupOk = true;
  try {
    await cuFetch(env, `/team/${teamId(env)}`);
    add("clickup_auth", "We can talk to ClickUp", "ok");
  } catch (e) {
    clickupOk = false;
    add("clickup_auth", "We can talk to ClickUp", "fail", e.message, "contact_owner");
  }

  // --- the list -----------------------------------------------------------
  if (!clickupOk) {
    add("list_exists", "The chosen ClickUp list still exists", "skip",
      "Can't check this until the ClickUp connection is fixed.");
  } else if (!cfg.clickup_list_id) {
    add("list_exists", "A ClickUp list is chosen", "fail", "No list chosen yet.", "remap_field");
  } else {
    try {
      const list = await cuFetch(env, `/list/${encodeURIComponent(cfg.clickup_list_id)}`);
      add("list_exists", `The list "${list.name}" is still there`, "ok");
      fields = await cuListFields(env, cfg.clickup_list_id);
    } catch (e) {
      add(
        "list_exists",
        "The chosen ClickUp list still exists",
        "fail",
        "That list can't be read — it may have been deleted or archived.",
        "pick_new_list",
        { error: e.message }
      );
    }

    const clash = await env.DB.prepare(
      `SELECT id, name FROM clickup_automations
        WHERE clickup_list_id = ? AND status != 'archived' AND id != ?`
    )
      .bind(String(cfg.clickup_list_id), cfg.id || -1)
      .first();
    if (clash) {
      add(
        "list_unique",
        "No other automation uses this list",
        "fail",
        `"${clash.name}" is already set up for this list.`,
        "open_existing_automation",
        { automation_id: clash.id }
      );
    } else {
      add("list_unique", "No other automation uses this list", "ok");
    }
  }

  // --- the custom fields --------------------------------------------------
  if (cfg.act_write_back && !clickupOk) {
    add("fields_exist", "The chosen custom fields exist on this list", "skip",
      "Can't check this until the ClickUp connection is fixed.");
  } else if (cfg.act_write_back) {
    const mapped = [
      ["Drive folder link", cfg.field_drive_id, cfg.field_drive],
      ["Doc link", cfg.field_doc_id, cfg.field_doc],
      ["Canva link", cfg.field_canva_id, cfg.field_canva],
    ].filter(([, id, name]) => id || name);

    if (!mapped.length) {
      add(
        "fields_exist",
        "The link has somewhere to go",
        "warn",
        "No custom field is mapped, so no link will be written anywhere.",
        "remap_field"
      );
    } else {
      const missing = mapped.filter(([, id, name]) => !resolveField(fields, id, name));
      if (missing.length) {
        add(
          "fields_exist",
          "The chosen custom fields exist on this list",
          "fail",
          `Not found on this list: ${missing.map(([l]) => l).join(", ")}. Someone may have renamed or removed the field.`,
          "remap_field"
        );
      } else {
        add(
          "fields_exist",
          `The field "${mapped.map(([, id, name]) => resolveField(fields, id, name).name).join('", "')}" exists on this list`,
          "ok"
        );
      }
    }
  }

  // --- Google reachable ---------------------------------------------------
  // Checked separately so a broken credential doesn't get misreported as a
  // problem with the folder. Telling someone to pick a different folder when
  // the real fault is a missing key sends them down the wrong path entirely.
  const saEmail = env.GOOGLE_CLIENT_EMAIL || null;
  let googleOk = true;
  try {
    await googleAccessToken(env);
    add("google_auth", "We can talk to Google Drive", "ok");
  } catch (e) {
    googleOk = false;
    add(
      "google_auth",
      "We can talk to Google Drive",
      "fail",
      "The automation's Google connection isn't working, so nothing can be created in Drive.",
      "contact_owner",
      { error: e.message }
    );
  }

  // --- the Drive parent ---------------------------------------------------
  if (!googleOk) {
    add("drive_parent_exists", "We can see the chosen Drive folder", "skip",
      "Can't check this until the Google connection is fixed.");
    add("drive_writable", "We can create folders in it", "skip",
      "Can't check this until the Google connection is fixed.");
  } else if (!cfg.drive_parent_id) {
    add("drive_parent_exists", "A Drive folder is chosen", "fail", "No parent folder chosen yet.", "pick_new_parent");
  } else {
    let parent = null;
    try {
      parent = await driveGetFile(env, cfg.drive_parent_id);
      if (parent.trashed) {
        add(
          "drive_parent_exists",
          "We can see the chosen Drive folder",
          "fail",
          "That folder is in the trash.",
          "pick_new_parent"
        );
      } else {
        add("drive_parent_exists", `We can see the folder "${parent.name}"`, "ok", cfg.drive_parent_path);
      }
    } catch (e) {
      const permission = e.code === "drive_permission";
      add(
        "drive_parent_exists",
        "We can see the chosen Drive folder",
        "fail",
        permission
          ? "The automation's Google account can't see this folder."
          : "That folder was deleted, moved, or is not shared with the automation.",
        permission ? "share_folder_with_service_account" : "pick_new_parent",
        { service_account_email: saEmail, error: e.message }
      );
    }

    if (parent && !parent.trashed) {
      const shallowOk = parent.capabilities?.canAddChildren !== false;
      if (!deep) {
        add(
          "drive_writable",
          "We can create folders in it",
          shallowOk ? "ok" : "fail",
          shallowOk
            ? null
            : "The automation's Google account can't add folders here.",
          shallowOk ? null : "share_folder_with_service_account",
          { service_account_email: saEmail }
        );
      } else {
        // canAddChildren lies for restricted folders inside a shared drive, so
        // the real check is to create something and remove it again.
        let probeId = null;
        try {
          const probe = await driveCreateFolder(
            env,
            `.opshub-preflight-${Math.random().toString(36).slice(2, 8)}`,
            cfg.drive_parent_id
          );
          probeId = probe.id;
          add(
            "drive_writable",
            "We can create folders in it",
            "ok",
            "Checked for real: a test folder was created and deleted again."
          );
        } catch (e) {
          add(
            "drive_writable",
            "We can create folders in it",
            "fail",
            "The automation's Google account can't add folders here.",
            "share_folder_with_service_account",
            { service_account_email: saEmail, error: e.message }
          );
        } finally {
          // Always clean up, so a failure part-way doesn't litter the drive.
          if (probeId) {
            try {
              await driveDelete(env, probeId);
            } catch {
              /* nothing more we can do; it is named so it's obvious what it was */
            }
          }
        }
      }
    }
  }

  // --- the counter (informational, and it explains where codes come from) --
  const next = await peekCode(env);
  add("code_sequence", `The next code will be ${next}`, "ok");

  const hardFail = checks.some((c) => c.status === "fail");
  const cleanName = sanitizeName("Spring Sale Video");
  const vars = buildVars({ code: next, name: cleanName, task: { id: "86abc1def", custom_id: null, url: "https://app.clickup.com/t/86abc1def" } });

  return {
    ok: !hardFail,
    hard_fail: hardFail,
    next_code: next,
    service_account_email: saEmail,
    preview: {
      task_name_example: cleanName,
      folder_name: folderNameFor(cfg, vars),
      subfolder_names: subfolderNames(cfg, vars),
      parent_path: cfg.drive_parent_path || null,
    },
    checks,
  };
}

// ---------------------------------------------------------------------------
// Automation row shaping
// ---------------------------------------------------------------------------
const AUTOMATION_FIELDS = [
  "name",
  "clickup_list_id",
  "clickup_list_name",
  "clickup_folder_name",
  "clickup_space_name",
  "act_rename",
  "act_create_folder",
  "act_create_subfolders",
  "act_copy_template",
  "act_write_back",
  "drive_id",
  "drive_name",
  "drive_parent_id",
  "drive_parent_name",
  "drive_parent_path",
  "folder_name_template",
  "template_file_id",
  "canva_link",
  "share_domain",
  "field_drive_id",
  "field_drive",
  "field_doc_id",
  "field_doc",
  "field_canva_id",
  "field_canva",
];

const BOOL_FIELDS = new Set([
  "act_rename",
  "act_create_folder",
  "act_create_subfolders",
  "act_copy_template",
  "act_write_back",
]);

function shapeAutomation(row) {
  if (!row) return null;
  const out = { ...row };
  out.subfolders = safeParse(row.subfolders, []);
  if (!Array.isArray(out.subfolders)) out.subfolders = [];
  for (const f of BOOL_FIELDS) out[f] = row[f] === 1 || row[f] === true;
  out.last_check_ok = row.last_check_ok == null ? null : row.last_check_ok === 1;
  return out;
}

function readAutomationBody(body) {
  const vals = {};
  for (const f of AUTOMATION_FIELDS) {
    if (!(f in body)) continue;
    let v = body[f];
    if (BOOL_FIELDS.has(f)) v = v ? 1 : 0;
    else if (v === "" ) v = null;
    vals[f] = v;
  }
  if ("subfolders" in body) {
    const list = Array.isArray(body.subfolders) ? body.subfolders : [];
    const clean = list
      .map((n) => String(n || "").trim())
      .filter(Boolean)
      .slice(0, MAX_SUBFOLDERS);
    vals.subfolders = JSON.stringify(clean);
  }
  if (vals.drive_parent_id) assertId(vals.drive_parent_id, "Drive folder id");
  if (vals.template_file_id) assertId(vals.template_file_id, "template file id");
  return vals;
}

// ---------------------------------------------------------------------------
// Admin API
// ---------------------------------------------------------------------------
async function handleApi(request, env, ctx, url, sub) {
  const method = request.method;
  await ensureClickUpAutomationTables(env);
  const body =
    method === "POST" || method === "PUT" || method === "PATCH"
      ? await request.json().catch(() => ({}))
      : {};

  // ---- health ----------------------------------------------------------
  if (sub === "health" && method === "GET") {
    const out = { webhook: null, clickup: null, google: null, code_sequence: null };

    const hook = await env.DB.prepare(
      "SELECT webhook_id, endpoint, events, created_at FROM clickup_automation_webhooks WHERE active = 1 LIMIT 1"
    ).first();
    out.webhook = hook
      ? { registered: true, id: hook.webhook_id, endpoint: hook.endpoint, events: safeParse(hook.events, null), created_at: hook.created_at }
      : { registered: false };

    try {
      const team = await cuFetch(env, `/team/${teamId(env)}`);
      out.clickup = { ok: true, team_name: team?.team?.name || null };
    } catch (e) {
      out.clickup = { ok: false, error: e.message };
    }

    try {
      await googleAccessToken(env);
      out.google = { ok: true, service_account_email: env.GOOGLE_CLIENT_EMAIL || null };
    } catch (e) {
      // Never 500 here — this endpoint powers the banner that explains outages.
      out.google = { ok: false, service_account_email: env.GOOGLE_CLIENT_EMAIL || null, error: e.message };
    }

    out.code_sequence = { year: currentYear(), next: await peekCode(env) };
    out.access_jwt_verification = Boolean(env.ACCESS_TEAM_DOMAIN && env.ACCESS_AUD);
    return json(request, out);
  }

  // ---- automations -----------------------------------------------------
  if (sub === "automations" && method === "GET") {
    const includeArchived = url.searchParams.get("archived") === "1";
    const { results } = await env.DB.prepare(
      `SELECT a.*,
              (SELECT COUNT(*) FROM clickup_automation_runs r
                WHERE r.automation_id = a.id AND r.status = 'ok'
                  AND r.created_at > datetime('now','-7 days')) AS runs_ok_7d,
              (SELECT COUNT(*) FROM clickup_automation_runs r
                WHERE r.automation_id = a.id AND r.status = 'error'
                  AND r.created_at > datetime('now','-7 days')) AS runs_error_7d,
              (SELECT MAX(created_at) FROM clickup_automation_runs r
                WHERE r.automation_id = a.id) AS last_run_at
         FROM clickup_automations a
        WHERE (? = 1 OR a.status != 'archived')
        ORDER BY CASE a.status WHEN 'live' THEN 0 WHEN 'draft' THEN 1 ELSE 2 END, a.name`
    )
      .bind(includeArchived ? 1 : 0)
      .all();

    const automations = (results || []).map(shapeAutomation);
    const counts = { live: 0, draft: 0, archived: 0 };
    for (const a of automations) counts[a.status] = (counts[a.status] || 0) + 1;
    return json(request, { automations, counts, next_code: await peekCode(env) });
  }

  if (sub === "automations" && method === "POST") {
    const vals = readAutomationBody(body);
    if (!vals.clickup_list_id) return json(request, { error: "Choose a ClickUp list first." }, 400);
    if (!vals.name) vals.name = vals.clickup_list_name || `List ${vals.clickup_list_id}`;

    const cols = Object.keys(vals);
    try {
      const row = await env.DB.prepare(
        `INSERT INTO clickup_automations (${cols.join(", ")}, status, updated_by, created_at, updated_at)
         VALUES (${cols.map(() => "?").join(", ")}, 'draft', ?, datetime('now'), datetime('now'))
         RETURNING *`
      )
        .bind(...cols.map((c) => vals[c]), actor(request))
        .first();
      return json(request, { automation: shapeAutomation(row) }, 201);
    } catch (e) {
      if (isUniqueViolation(e)) {
        return json(request, { error: "That list already has an automation." }, 409);
      }
      throw e;
    }
  }

  const autoMatch = sub.match(/^automations\/(\d+)(?:\/(.+))?$/);
  if (autoMatch) {
    const id = Number(autoMatch[1]);
    const action = autoMatch[2] || null;
    const row = await env.DB.prepare("SELECT * FROM clickup_automations WHERE id = ?")
      .bind(id)
      .first();
    if (!row) return json(request, { error: "No such automation." }, 404);

    if (!action && method === "PUT") {
      const vals = readAutomationBody(body);
      if (!Object.keys(vals).length) return json(request, { error: "Nothing to update." }, 400);
      const sets = Object.keys(vals).map((c) => `${c} = ?`);
      const updated = await env.DB.prepare(
        `UPDATE clickup_automations
            SET ${sets.join(", ")}, updated_by = ?, updated_at = datetime('now')
          WHERE id = ? RETURNING *`
      )
        .bind(...Object.keys(vals).map((c) => vals[c]), actor(request), id)
        .first();
      return json(request, { automation: shapeAutomation(updated) });
    }

    if (!action && method === "DELETE") {
      if (url.searchParams.get("hard") === "1") {
        const gate = guardDangerous(request, env);
        if (gate) return gate;
        await env.DB.prepare("DELETE FROM clickup_automations WHERE id = ?").bind(id).run();
        return json(request, { deleted: id });
      }
      await env.DB.prepare(
        `UPDATE clickup_automations SET status = 'archived', updated_by = ?, updated_at = datetime('now') WHERE id = ?`
      )
        .bind(actor(request), id)
        .run();
      return json(request, { archived: id });
    }

    if (action === "status" && method === "PATCH") {
      const status = String(body.status || "");
      if (!["draft", "live", "archived"].includes(status)) {
        return json(request, { error: "status must be draft, live or archived." }, 400);
      }
      // Going live re-runs the hard checks server-side. Never trust the client's
      // gating — that is what makes "live" mean something.
      if (status === "live") {
        const pf = await preflight(env, shapeAutomation(row), { deep: true });
        if (pf.hard_fail) {
          return json(
            request,
            { error: "Some checks failed, so this can't go live yet.", checks: pf.checks },
            409
          );
        }
        await env.DB.prepare(
          `UPDATE clickup_automations
              SET last_check_at = datetime('now'), last_check_ok = 1, last_check_detail = NULL
            WHERE id = ?`
        )
          .bind(id)
          .run();
      }
      const updated = await env.DB.prepare(
        `UPDATE clickup_automations SET status = ?, updated_by = ?, updated_at = datetime('now')
          WHERE id = ? RETURNING *`
      )
        .bind(status, actor(request), id)
        .first();
      return json(request, { automation: shapeAutomation(updated) });
    }

    if (action === "dry-run" && method === "POST") {
      // Read-only. Deliberately does NOT consume a code.
      const cfg = shapeAutomation(row);
      const next = await peekCode(env);
      const example = sanitizeName(body.task_name || "Spring Sale Video");
      const vars = buildVars({ code: next, name: example, task: { id: "86abc1def", custom_id: null, url: "https://app.clickup.com/t/86abc1def" } });
      const fields = await cuListFields(env, cfg.clickup_list_id).catch(() => []);
      const writes = [
        ["Drive folder link", cfg.field_drive_id, cfg.field_drive, "the new folder's URL"],
        ["Doc link", cfg.field_doc_id, cfg.field_doc, "the copied doc's URL"],
        ["Canva link", cfg.field_canva_id, cfg.field_canva, cfg.canva_link],
      ]
        .filter(([, fid, fname, value]) => (fid || fname) && value)
        .map(([label, fid, fname, value]) => {
          const f = resolveField(fields, fid, fname);
          return { label, field_name: f?.name || fname || null, field_id: f?.id || null, found: Boolean(f), value_example: value };
        });
      return json(request, {
        preview: {
          task_name_example: example,
          would_rename_to: cfg.act_rename ? folderNameFor(cfg, vars) : null,
          folder_name: folderNameFor(cfg, vars),
          subfolder_names: subfolderNames(cfg, vars),
          parent_path: cfg.drive_parent_path,
        },
        next_code: next,
        writes,
      });
    }

    if (action === "preflight" && method === "POST") {
      const pf = await preflight(env, shapeAutomation(row), { deep: body.deep === true });
      await env.DB.prepare(
        `UPDATE clickup_automations
            SET last_check_at = datetime('now'), last_check_ok = ?, last_check_detail = ?
          WHERE id = ?`
      )
        .bind(
          pf.hard_fail ? 0 : 1,
          pf.hard_fail
            ? (pf.checks.find((c) => c.status === "fail")?.detail || "A check failed.")
            : null,
          id
        )
        .run();
      return json(request, pf);
    }
  }

  // Preflight an unsaved draft.
  if (sub === "preflight" && method === "POST") {
    const cfg = { ...body, subfolders: Array.isArray(body.subfolders) ? body.subfolders : [] };
    cfg.subfolders = JSON.stringify(cfg.subfolders);
    const pf = await preflight(env, shapeAutomation(cfg), { deep: body.deep === true });
    return json(request, pf);
  }

  // ---- ClickUp lookups -------------------------------------------------
  if (sub === "clickup/tree" && method === "GET") {
    const tree = await cuWorkspaceTree(env, url.searchParams.get("refresh") === "1");
    // Mark lists that already have an automation, so the picker can say so
    // rather than letting someone create a duplicate that races.
    const { results } = await env.DB.prepare(
      "SELECT id, name, clickup_list_id FROM clickup_automations WHERE status != 'archived'"
    ).all();
    const taken = {};
    for (const r of results || []) taken[String(r.clickup_list_id)] = { id: r.id, name: r.name };
    return json(request, { ...tree, taken });
  }

  const fieldsMatch = sub.match(/^clickup\/lists\/([A-Za-z0-9_-]+)\/fields$/);
  if (fieldsMatch && method === "GET") {
    const listId = fieldsMatch[1];
    const [fields, list] = await Promise.all([
      cuListFields(env, listId),
      cuFetch(env, `/list/${encodeURIComponent(listId)}`).catch(() => null),
    ]);
    return json(request, {
      list: list ? { id: list.id, name: list.name } : null,
      fields: fields.map((f) => ({ id: f.id, name: f.name, type: f.type })),
    });
  }

  // ---- Drive browsing --------------------------------------------------
  if (sub === "drive/roots" && method === "GET") {
    const drives = await driveListSharedDrives(env);
    return json(request, {
      drives: drives.map((d) => ({ id: d.id, name: d.name, kind: "shared" })),
      note: drives.length
        ? null
        : "The automation's Google account isn't a member of any shared drive yet.",
    });
  }

  if (sub === "drive/folders" && method === "GET") {
    const parent = url.searchParams.get("parent");
    if (!parent) return json(request, { error: "parent is required." }, 400);
    const [{ folders, nextPageToken }, meta, path] = await Promise.all([
      driveListChildFolders(env, parent, url.searchParams.get("page")),
      driveGetFile(env, parent).catch(() => null),
      driveFolderPath(env, parent).catch(() => null),
    ]);
    return json(request, {
      parent: meta
        ? {
            id: meta.id,
            name: meta.name,
            path,
            drive_id: meta.driveId || null,
            can_add_children: meta.capabilities?.canAddChildren !== false,
          }
        : null,
      folders: folders.map((f) => ({
        id: f.id,
        name: f.name,
        can_add_children: f.capabilities?.canAddChildren !== false,
      })),
      next_page: nextPageToken,
    });
  }

  if (sub === "drive/resolve" && method === "GET") {
    const q = url.searchParams.get("q");
    const id = extractDriveFolderId(q);
    if (!id) {
      return json(
        request,
        { error: "That doesn't look like a Google Drive folder link. Copy the address bar while the folder is open." },
        400
      );
    }
    const meta = await driveGetFile(env, id);
    if (meta.mimeType !== "application/vnd.google-apps.folder") {
      return json(request, { error: "That link points at a file, not a folder." }, 400);
    }
    let driveName = null;
    if (meta.driveId) {
      driveName = await driveFetch(env, `/drives/${meta.driveId}?fields=name`)
        .then((d) => d.name)
        .catch(() => null);
    }
    return json(request, {
      id: meta.id,
      name: meta.name,
      path: await driveFolderPath(env, meta.id).catch(() => meta.name),
      drive_id: meta.driveId || null,
      drive_name: driveName,
      can_add_children: meta.capabilities?.canAddChildren !== false,
      trashed: Boolean(meta.trashed),
    });
  }

  // ---- runs ------------------------------------------------------------
  if (sub === "runs" && method === "GET") {
    const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit") || 50)));
    const cursor = Number(url.searchParams.get("cursor") || 0);
    const status = url.searchParams.get("status");
    const automationId = url.searchParams.get("automation_id");

    const where = ["(? = 0 OR r.id < ?)"];
    const binds = [cursor, cursor || 0];
    if (status === "problems") where.push("r.status = 'error'");
    else if (status) {
      where.push("r.status = ?");
      binds.push(status);
    }
    if (automationId) {
      where.push("r.automation_id = ?");
      binds.push(Number(automationId));
    }

    const { results } = await env.DB.prepare(
      `SELECT r.*, a.name AS automation_name
         FROM clickup_automation_runs r
         LEFT JOIN clickup_automations a ON a.id = r.automation_id
        WHERE ${where.join(" AND ")}
        ORDER BY r.id DESC LIMIT ?`
    )
      .bind(...binds, limit)
      .all();

    const rows = results || [];
    const summary = await env.DB.prepare(
      `SELECT
         SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS in_flight,
         SUM(CASE WHEN status = 'error' AND created_at > datetime('now','-1 day') THEN 1 ELSE 0 END) AS problems_24h
       FROM clickup_automation_runs`
    ).first();

    return json(request, {
      runs: rows.map((r) => ({
        ...r,
        steps: safeParse(r.steps, {}),
        fields_written: safeParse(r.fields_written, []),
        payload: undefined, // not needed by the UI; keeps the response small
        task_url: `https://app.clickup.com/t/${r.clickup_task_id}`,
      })),
      next_cursor: rows.length === limit ? rows[rows.length - 1].id : null,
      in_flight: Number(summary?.in_flight || 0),
      problems_24h: Number(summary?.problems_24h || 0),
    });
  }

  const retryMatch = sub.match(/^runs\/(\d+)\/retry$/);
  if (retryMatch && method === "POST") {
    const run = await env.DB.prepare("SELECT * FROM clickup_automation_runs WHERE id = ?")
      .bind(Number(retryMatch[1]))
      .first();
    if (!run) return json(request, { error: "No such run." }, 404);
    const result = await processTask(env, run.clickup_task_id, { trigger: "retry", force: true });
    return json(request, { result });
  }

  // ---- webhook registration (dangerous: Mark-only) ---------------------
  if (sub === "webhook/list" && method === "GET") {
    const gate = guardDangerous(request, env);
    if (gate) return gate;
    const data = await cuFetch(env, `/team/${teamId(env)}/webhook`);
    const stored = await env.DB.prepare(
      "SELECT webhook_id, active, created_at FROM clickup_automation_webhooks"
    ).all();
    return json(request, {
      // Never echo the secret, not even to an authenticated caller.
      clickup: (data.webhooks || []).map((w) => ({
        id: w.id,
        endpoint: w.endpoint,
        events: w.events,
        health: w.health,
      })),
      stored: stored.results || [],
    });
  }

  if (sub === "webhook/register" && method === "POST") {
    // force replaces an existing webhook, so that path keeps the stricter guard.
    const gate = body.force ? guardDangerous(request, env) : guardSetup(request, env);
    if (gate) return gate;

    const endpoint = `https://${env.APP_HOSTNAME || DEFAULT_APP_HOSTNAME}/clickup-automation/webhook`;

    // Re-registering blindly leaves duplicates that each fire, so look first.
    const existing = await cuFetch(env, `/team/${teamId(env)}/webhook`).catch(() => ({ webhooks: [] }));
    const dupe = (existing.webhooks || []).find((w) => w.endpoint === endpoint);
    let replaced = null;

    if (dupe && !body.force) {
      // A webhook we hold no signing secret for is dead weight, however healthy
      // ClickUp believes it to be: every delivery fails the signature check and
      // is rejected. ClickUp keeps sending, we keep refusing, and nothing says
      // so — which is precisely the state a registration that half-completed
      // leaves behind. ClickUp reveals a secret only at creation, so the only
      // way to hold one is to create the webhook ourselves.
      const held = await env.DB.prepare(
        `SELECT 1 AS ok FROM clickup_automation_webhooks
          WHERE webhook_id = ? AND active = 1 AND secret IS NOT NULL AND secret != ''`
      )
        .bind(String(dupe.id))
        .first();

      if (held) {
        // Genuinely set up already. Saying so beats an error the reader has to
        // interpret, and re-registering would only double-fire every task.
        return json(request, {
          webhook: { id: dupe.id, endpoint, events: ["taskCreated"] },
          already: true,
        });
      }

      // Orphaned. Replace it — adding a second would make both fire on every task.
      await cuFetch(env, `/webhook/${encodeURIComponent(dupe.id)}`, { method: "DELETE" }).catch(() => null);
      await env.DB.prepare("UPDATE clickup_automation_webhooks SET active = 0 WHERE webhook_id = ?")
        .bind(String(dupe.id))
        .run();
      replaced = String(dupe.id);
    }

    const created = await cuFetch(env, `/team/${teamId(env)}/webhook`, {
      method: "POST",
      body: JSON.stringify({ endpoint, events: ["taskCreated"] }),
    });
    // ClickUp nests the created webhook under `webhook` and puts the signing
    // secret inside it. Older responses put the secret at the top level, so take
    // whichever is present rather than betting on one shape.
    const hook = created.webhook || created;
    const secret = created.secret || hook?.secret || null;
    if (!hook?.id || !secret) {
      // Never echo `created` itself: on the shapes that do work it contains the
      // signing secret, and this response is read by a browser and a log.
      return json(
        request,
        {
          error:
            "ClickUp did not return a webhook id and secret. It replied with: " +
            (Object.keys(created || {}).join(", ") || "nothing") +
            (created?.webhook ? " (webhook: " + Object.keys(created.webhook).join(", ") + ")" : ""),
        },
        502
      );
    }

    // The secret goes straight from ClickUp into D1 — it never passes through a
    // human, a config file or a log line.
    await env.DB.prepare(
      `INSERT INTO clickup_automation_webhooks (webhook_id, secret, endpoint, events, active, created_by)
       VALUES (?, ?, ?, ?, 1, ?)
       ON CONFLICT(webhook_id) DO UPDATE SET secret = excluded.secret, active = 1`
    )
      .bind(String(hook.id), secret, endpoint, JSON.stringify(["taskCreated"]), actor(request))
      .run();

    return json(request, { webhook: { id: hook.id, endpoint, events: ["taskCreated"] }, replaced });
  }

  if (sub === "webhook/delete" && method === "POST") {
    const gate = guardDangerous(request, env);
    if (gate) return gate;
    const id = String(body.webhook_id || "");
    if (!id) return json(request, { error: "webhook_id is required." }, 400);
    await cuFetch(env, `/webhook/${encodeURIComponent(id)}`, { method: "DELETE" }).catch(() => null);
    await env.DB.prepare("UPDATE clickup_automation_webhooks SET active = 0 WHERE webhook_id = ?")
      .bind(id)
      .run();
    return json(request, { deleted: id });
  }

  return json(request, { error: `Unknown endpoint: ${sub}` }, 404);
}

// ---------------------------------------------------------------------------
// Cron — retry the failures that are worth retrying. We never 5xx, so ClickUp
// never retries us; if we don't do this, nobody does.
// ---------------------------------------------------------------------------
export async function clickUpAutomationCron(env) {
  if (!env.DB) return;
  try {
    await ensureClickUpAutomationTables(env);

    // A run stuck at 'running' is one that died mid-flight: a cancelled Worker,
    // an eviction, a deploy landing between two awaits. It records no error
    // because nothing survived to write one, and claimRun will not re-enter it
    // until it goes stale — so without this sweep the work is simply abandoned,
    // silently, with the UI showing a task that is forever "running".
    //
    // Marking it failed is what makes it visible AND retryable: the query below
    // then picks it up like any other failure.
    await env.DB.prepare(
      `UPDATE clickup_automation_runs
          SET status = 'error',
              error = COALESCE(NULLIF(error, ''),
                'Stopped part-way through and never reported why — usually the Worker '
                || 'was cancelled mid-run. Picked up automatically and retried.'),
              error_code = COALESCE(error_code, 'abandoned'),
              next_attempt_at = datetime('now'),
              updated_at = datetime('now')
        WHERE status = 'running'
          AND started_at < datetime('now', ?)`
    )
      .bind(`-${STALE_RUN_MINUTES} minutes`)
      .run();

    const { results } = await env.DB.prepare(
      `SELECT clickup_task_id FROM clickup_automation_runs
        WHERE status = 'error' AND next_attempt_at IS NOT NULL
          AND next_attempt_at <= datetime('now') AND attempts < 5
        ORDER BY next_attempt_at LIMIT 5`
    ).all();
    for (const r of results || []) {
      // Clear the marker first so a failure doesn't spin every two minutes.
      await env.DB.prepare(
        "UPDATE clickup_automation_runs SET next_attempt_at = NULL WHERE clickup_task_id = ?"
      )
        .bind(r.clickup_task_id)
        .run();
      await processTask(env, r.clickup_task_id, { trigger: "retry", force: true });
    }
  } catch (e) {
    console.error("clickUpAutomationCron:", e);
  }
}

// ---------------------------------------------------------------------------
// Router. Returns a Response for our paths, or null so everything else falls
// through to the existing routes and the static assets.
// ---------------------------------------------------------------------------
export async function handleClickUpAutomationRoutes(request, env, ctx, path) {
  if (!path.startsWith("/clickup-automation")) return null;

  // The webhook: public by necessity, guarded by the HMAC signature alone.
  if (path === "/clickup-automation/webhook") {
    if (request.method !== "POST") {
      return new Response(JSON.stringify({ error: "POST only" }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
      });
    }
    if (!env.DB) {
      return new Response(JSON.stringify({ ok: false, error: "DB not configured" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    await ensureClickUpAutomationTables(env);
    return handleWebhook(request, env, ctx);
  }

  if (!path.startsWith("/clickup-automation/api/")) return null; // let the page load

  const url = new URL(request.url);

  // This module answers its own preflight, so it is mounted above the global
  // OPTIONS handler in src/index.js.
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsFor(request) });
  }

  if (!env.DB) return json(request, { error: "DB not configured" }, 503);

  const gate = await guardAdminApi(request, env, url);
  if (gate) return gate;

  const sub = path.slice("/clickup-automation/api/".length).replace(/\/+$/, "");
  try {
    return await handleApi(request, env, ctx, url, sub);
  } catch (e) {
    const status = e instanceof HttpError ? e.status : 500;
    return json(request, { error: String(e?.message || e), code: e?.code || null }, status);
  }
}
