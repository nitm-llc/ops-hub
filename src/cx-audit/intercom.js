// ============================================================================
// CX SCORECARD AUDIT — Intercom REST client (read-only)
// ----------------------------------------------------------------------------
// Two calls:
//   POST /conversations/search   updated_at >= window start, 150 per page,
//                                cursor in pages.next.starting_after
//   GET  /conversations/{id}?display_as=plaintext   full parts (Intercom caps
//                                at the 500 most recent; ours run 5-60)
//
// The search has NO upper bound on updated_at on purpose: the job runs at 7am,
// so a conversation answered yesterday and touched again this morning has
// updated_at = today. Which replies count is decided per part, by created_at.
//
// Secret: INTERCOM_ACCESS_TOKEN. Config: INTERCOM_API_BASE (region),
// INTERCOM_API_VERSION.
// ============================================================================

import { DEFAULT_API_BASE, DEFAULT_API_VERSION } from "./config.js";

export class IntercomError extends Error {
  constructor(message, status) { super(message); this.status = status; }
}

function headers(env) {
  if (!env.INTERCOM_ACCESS_TOKEN) throw new IntercomError("INTERCOM_ACCESS_TOKEN is not set", 0);
  return {
    Authorization: `Bearer ${env.INTERCOM_ACCESS_TOKEN}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    "Intercom-Version": env.INTERCOM_API_VERSION || DEFAULT_API_VERSION,
  };
}

const base = (env) => (env.INTERCOM_API_BASE || DEFAULT_API_BASE).replace(/\/$/, "");

async function call(env, path, init = {}, attempt = 0) {
  const r = await fetch(base(env) + path, { ...init, headers: headers(env) });
  if (r.status === 429 && attempt < 3) {
    const reset = Number(r.headers.get("X-RateLimit-Reset")) * 1000;
    const wait = reset ? Math.min(Math.max(reset - Date.now(), 1000), 10000) : 2000 * (attempt + 1);
    await new Promise((res) => setTimeout(res, wait));
    return call(env, path, init, attempt + 1);
  }
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    throw new IntercomError(`Intercom ${init.method || "GET"} ${path} -> ${r.status}: ${text.slice(0, 300)}`, r.status);
  }
  return r.json();
}

// Workspace id for inbox links (app.id_code). Falls back to "_", which the
// Intercom inbox resolves to the signed-in workspace.
export async function getAppId(env) {
  try {
    const me = await call(env, "/me");
    return me?.app?.id_code || "_";
  } catch {
    return "_";
  }
}

export async function searchConversationIds(env, { start }) {
  const ids = [];
  let cursor = null;
  for (let page = 0; page < 50; page++) {
    const body = {
      query: { operator: "AND", value: [{ field: "updated_at", operator: ">=", value: start }] },
      pagination: { per_page: 150, ...(cursor ? { starting_after: cursor } : {}) },
    };
    const r = await call(env, "/conversations/search", { method: "POST", body: JSON.stringify(body) });
    for (const c of r.conversations || []) ids.push(String(c.id));
    cursor = r.pages?.next?.starting_after;
    if (!cursor) break;
  }
  // Stateless cursors can repeat a row if something updates mid-paging.
  return [...new Set(ids)];
}

export function getConversation(env, id) {
  return call(env, `/conversations/${encodeURIComponent(id)}?display_as=plaintext`);
}

// Fetch many with bounded concurrency. Per-item failures are returned, not thrown.
export async function getConversations(env, ids, { concurrency = 4 } = {}) {
  const out = new Array(ids.length);
  let next = 0;
  async function worker() {
    while (next < ids.length) {
      const i = next++;
      try { out[i] = { id: ids[i], conversation: await getConversation(env, ids[i]) }; }
      catch (e) { out[i] = { id: ids[i], error: e.message }; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, ids.length) }, worker));
  return out;
}
