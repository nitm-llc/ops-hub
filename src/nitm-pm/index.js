// ============================================================================
// FLO  —  marketing project-management helper in Google Chat
// (step 1: Q&A + approvals; TikTok Production is the first list connected)
// ----------------------------------------------------------------------------
// People DM "Flo" or @mention it. (Folder and endpoint keep the original
// nitm-pm name so the Google Chat config never has to change.) It answers from ClickUp (TikTok space >
// Production), proposes changes as numbered pending actions, and only changes
// a card when someone on PM_APPROVERS says yes. See README.md in this folder.
//
// ENDPOINT: POST /clickup-automation/webhook/nitm-pm
//   Under the existing Cloudflare Access bypass for /clickup-automation/webhook
//   (subpaths included), because Google can't log in to Access. The Google
//   token check in google.js is the only guard, and it fails closed.
//
// RESPONDING: Chat wants an answer within 30 s. We race the work against
//   SYNC_MS; if it isn't done we reply "On it…" and finish in ctx.waitUntil,
//   posting the answer in the same thread with the service account. Workers
//   allow waitUntil ~30 s after the response, so a turn's hard deadline is
//   DEADLINE_MS from the request; past that the bot says it stopped.
//
// Both Chat event formats are handled: Workspace add-on ({chat:{...}}, the
//   default for new apps) and the legacy ({type:"MESSAGE", ...}) format.
// ============================================================================

import { verifyChatRequest, postChatMessage } from "./google.js";
import { ensureTables, expireOld, loadHistory, appendHistory, logAction, recordPerson } from "./store.js";
import { parseApproval, handlePureApproval, isApprover } from "./approvals.js";
import { runTurn } from "./brain.js";
import { DEFAULT_TZ } from "./config.js";
import { DEFAULT_APP_HOSTNAME } from "../access.js";

export const CHAT_PATH = "/clickup-automation/webhook/nitm-pm";
// Reply "On it…" after this long. Google allows ~30 s; 10 s leaves a wide
// margin for D1, verification and network time before and after the race.
export const SYNC_MS = 10000;
// After the "On it" reply Cloudflare allows ~30 s of waitUntil work, so a
// turn must finish (and post) by SYNC_MS + ~30 s. Leave a few seconds to post.
export const DEADLINE_MS = SYNC_MS + 26000;
const MAX_CHARS = 3900; // Chat's text limit is 4,096 characters

export const endpointUrl = (env) => `https://${env.APP_HOSTNAME || DEFAULT_APP_HOSTNAME}${CHAT_PATH}`;
const allowedDomain = (env) => String(env.PM_ALLOWED_DOMAIN || "anurseinthemaking.com").toLowerCase();

// ---------------------------------------------------------------------------
// Event parsing (both formats) -> one shape
// ---------------------------------------------------------------------------
export function parseEvent(body) {
  if (body && body.chat) {
    const c = body.chat;
    const p = c.messagePayload || c.addedToSpacePayload || c.removedFromSpacePayload || c.appCommandPayload || {};
    const type = c.messagePayload
      ? "MESSAGE"
      : c.addedToSpacePayload
        ? "ADDED_TO_SPACE"
        : c.removedFromSpacePayload
          ? "REMOVED_FROM_SPACE"
          : c.appCommandPayload
            ? "MESSAGE"
            : "OTHER";
    return { format: "addon", type, user: c.user || {}, space: p.space || c.space || {}, message: p.message || null };
  }
  return { format: "legacy", type: body?.type || "OTHER", user: body?.user || {}, space: body?.space || {}, message: body?.message || null };
}

export function isDm(space) {
  return space?.spaceType === "DIRECT_MESSAGE" || space?.type === "DM" || space?.singleUserBotDm === true;
}

// DMs: one conversation per space (Chat threads every DM message separately).
export function conversationOf(ev) {
  const space = ev.space?.name || ev.message?.space?.name || "";
  const thread = isDm(ev.space) ? "" : ev.message?.thread?.name || "";
  return { space, thread, dm: isDm(ev.space) };
}

function messageText(message) {
  const t = message?.argumentText ?? message?.text ?? "";
  return String(t).replace(/^\s*@?(?:Flo|NITM PM)\b[:,]?/i, "").trim();
}

// ---------------------------------------------------------------------------
// Replies
// ---------------------------------------------------------------------------
// Belt and braces: the prompt asks for Chat formatting; fix common Markdown.
export function toChatFormat(text) {
  let t = String(text || "").trim();
  t = t.replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, "<$2|$1>");
  t = t.replace(/\*\*([^*\n]+)\*\*/g, "*$1*");
  t = t.replace(/^#{1,6}\s+(.+)$/gm, "*$1*");
  if (t.length > MAX_CHARS) t = t.slice(0, MAX_CHARS - 40).replace(/\n[^\n]*$/, "") + "\n…(cut short; ask for the rest)";
  return t;
}

function syncReply(ev, text) {
  const message = { text: toChatFormat(text) };
  const body = ev.format === "addon" ? { hostAppDataAction: { chatDataAction: { createMessageAction: { message } } } } : message;
  return json(body);
}
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

export const HELLO =
  "Hi, I'm *Flo*, marketing project management in ClickUp (TikTok first, more coming).\n" +
  "Try: \"What's stuck today?\" · \"What's posting next week?\" · \"Run the daily check\"\n" +
  "I propose changes; an approver replies \"yes\" or \"approve 1 and 3\".";

// ---------------------------------------------------------------------------
// The turn
// ---------------------------------------------------------------------------
async function handleMessage(env, ev, started) {
  const conv = conversationOf(ev);
  const sender = {
    email: String(ev.message?.sender?.email || ev.user?.email || "").toLowerCase(),
    displayName: ev.message?.sender?.displayName || ev.user?.displayName || "",
  };
  const text = messageText(ev.message);
  const tz = env.PM_TIMEZONE || DEFAULT_TZ;

  if (!sender.email || !sender.email.endsWith("@" + allowedDomain(env))) {
    await logAction(env, { conv, actor: sender.email || null, event: "refused_sender", result: "outside the allowed domain" });
    return { text: "Sorry, I only work with the NITM team." };
  }
  // Remember how to reach this person: an app can't open a DM itself, so
  // their DM with Flo is only known once they've messaged it.
  try {
    await recordPerson(env, {
      email: sender.email,
      displayName: sender.displayName,
      chatUser: ev.message?.sender?.name || ev.user?.name || null,
      dmSpace: conv.dm ? conv.space : null,
    });
  } catch (e) {
    console.error("nitm-pm: recordPerson failed:", e);
  }
  if (!text) return { text: HELLO };

  await expireOld(env);
  const parsed = parseApproval(text);
  const quick = await handlePureApproval(env, conv, sender, parsed);
  if (quick !== null) {
    await appendHistory(env, conv, [
      { role: "user", who: sender.displayName, text },
      { role: "assistant", text: quick },
    ]);
    return { text: quick };
  }

  const history = await loadHistory(env, conv);
  const work = runTurn(env, {
    conv,
    sender,
    text,
    tz,
    history,
    approvedNumbers: parsed.kind === "approve" && isApprover(env, sender.email) ? parsed.numbers : [],
    deadline: started + DEADLINE_MS,
  }).then(async (reply) => {
    try {
      await appendHistory(env, conv, [
        { role: "user", who: sender.displayName, text },
        { role: "assistant", text: reply },
      ]);
    } catch (e) {
      console.error("nitm-pm history write failed:", e);
    }
    return reply;
  });
  return { work };
}

export async function handleNitmPmRoutes(request, env, ctx, path, { syncMs = SYNC_MS } = {}) {
  if (path !== CHAT_PATH) return null;
  if (request.method !== "POST") return json({ error: "POST only" }, 405);
  const started = Date.now();

  const v = await verifyChatRequest(env, request, endpointUrl(env));
  if (!v.ok) {
    console.warn("nitm-pm: rejected request:", v.reason);
    return json({ error: "unauthorized" }, 401);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "invalid json" }, 400);
  }
  const ev = parseEvent(body);
  if (!env.DB) return syncReply(ev, "I'm not set up yet (no database binding).");

  try {
    await ensureTables(env);
    if (ev.type === "ADDED_TO_SPACE") {
      const c = conversationOf(ev);
      await logAction(env, { conv: c, actor: ev.user?.email || null, event: "added_to_space" });
      const email = String(ev.user?.email || "").toLowerCase();
      if (c.dm && email.endsWith("@" + allowedDomain(env))) {
        await recordPerson(env, { email, displayName: ev.user?.displayName, chatUser: ev.user?.name, dmSpace: c.space }).catch(() => {});
      }
      return syncReply(ev, HELLO);
    }
    if (ev.type === "REMOVED_FROM_SPACE") {
      await logAction(env, { conv: conversationOf(ev), actor: ev.user?.email || null, event: "removed_from_space" });
      return json({});
    }
    if (ev.type !== "MESSAGE" || !ev.message) return json({});

    const r = await handleMessage(env, ev, started);
    if (r.text !== undefined) return syncReply(ev, r.text);

    // Race the answer against the sync window. Timing marks go to the action
    // log (event "turn") so a "not responding" can be diagnosed afterwards.
    const conv0 = conversationOf(ev);
    const mark = (result) => logAction(env, { conv: conv0, actor: ev.message?.sender?.email || ev.user?.email || null, event: "turn", result });
    const t0 = Date.now();
    let handle;
    const timer = new Promise((resolve) => {
      // Counted from when the request ARRIVED, not from here: verification and
      // table setup already used part of Google's 30 seconds.
      handle = setTimeout(() => resolve(null), Math.max(0, syncMs - (Date.now() - started)));
    });
    const winner = await Promise.race([r.work.then((t) => ({ t })), timer]);
    clearTimeout(handle);
    if (winner) {
      ctx.waitUntil(mark(`answered directly after ${((Date.now() - started) / 1000).toFixed(1)}s (pre-work ${((t0 - started) / 1000).toFixed(1)}s)`));
      return syncReply(ev, winner.t);
    }

    const conv = conversationOf(ev);
    ctx.waitUntil(mark(`replied "On it" after ${((Date.now() - started) / 1000).toFixed(1)}s (pre-work ${((t0 - started) / 1000).toFixed(1)}s)`));
    ctx.waitUntil(
      r.work
        .then((t) => postChatMessage(env, { space: conv.space, thread: conv.dm ? null : conv.thread, text: toChatFormat(t) }))
        .then(() => mark(`posted the answer after ${((Date.now() - started) / 1000).toFixed(1)}s`))
        .catch(async (e) => {
          console.error("nitm-pm async post failed:", e);
          await logAction(env, { conv, event: "async_post_failed", result: String(e.message || e) });
        })
    );
    return syncReply(ev, "On it…");
  } catch (e) {
    console.error("nitm-pm error:", e);
    return syncReply(ev, `Sorry, something went wrong on my side: ${e.message}`);
  }
}
