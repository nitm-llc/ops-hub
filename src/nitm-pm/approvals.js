// ============================================================================
// NITM PM — approvals. Parsed and enforced in code: the model can ask to
// apply things, but only numbers the approver typed in THIS message, from a
// person on PM_APPROVERS, ever reach applyAction().
// ============================================================================

import { applyAction, RefusedError } from "./clickup.js";
import { PENDING_TTL_MS, getPending, claimPending, finishPending, rejectPending, markExpired, listPending, listPendingRows, logAction, getLinkedConv } from "./store.js";

// This conversation first; then the conversation it's linked to (a copy of
// the lead's morning summary), so either person can approve its numbers.
async function findPending(env, conv, n) {
  const own = await getPending(env, conv, n);
  if (own) return own;
  const linked = conv.thread === "" ? await getLinkedConv(env, conv.space) : null;
  return linked ? getPending(env, linked, n) : null;
}

export function approvers(env) {
  return String(env.PM_APPROVERS || "")
    .split(/[\s,;]+/)
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}
export const isApprover = (env, email) => !!email && approvers(env).includes(String(email).toLowerCase());

const NUM_LIST = String.raw`#?\d+(?:\s*(?:,|&|\band\b|\+)\s*#?\d+)*`;
const numbersIn = (s) => [...new Set((String(s).match(/\d+/g) || []).map(Number))].filter((n) => n > 0 && n < 10000);

/**
 * What the message says about approvals.
 *  { kind: "approve", numbers: [..] }    "approve 1 and 3", "approve #2"
 *  { kind: "approve_all" }               "approve all"
 *  { kind: "yes" }                       "yes" / "go ahead" (single proposal)
 *  { kind: "reject", numbers: [..] }     "reject 2", "cancel 1 and 4"
 *  { kind: "no" }                        "no" (single proposal)
 *  { kind: "none" }
 * `pure` is true when the whole message is just the approval, so it can be
 * handled without calling Claude at all.
 */
export function parseApproval(text) {
  const t = String(text || "").trim().replace(/[.!]+$/, "").trim();
  const low = t.toLowerCase();

  const all = low.match(/^(?:please\s+)?approve\s+(?:them\s+)?all$/);
  if (all) return { kind: "approve_all", numbers: [], pure: true };

  const approveRe = new RegExp(String.raw`\bapprove[sd]?\s+(?:numbers?\s+|changes?\s+|items?\s+)?(${NUM_LIST})`, "i");
  const rejectRe = new RegExp(String.raw`\b(?:reject|cancel|skip|drop)\s+(?:numbers?\s+|changes?\s+|items?\s+)?(${NUM_LIST})`, "i");
  const a = t.match(approveRe);
  const r = t.match(rejectRe);
  if (a || r) {
    const approve = a ? numbersIn(a[1]) : [];
    const reject = r ? numbersIn(r[1]) : [];
    const pureRe = new RegExp(
      String.raw`^(?:please\s+)?(?:(?:approve[sd]?|reject|cancel|skip|drop)\s+(?:numbers?\s+|changes?\s+|items?\s+)?${NUM_LIST}\s*(?:,|and|;)?\s*)+$`,
      "i"
    );
    return { kind: approve.length ? "approve" : "reject", numbers: approve, reject, pure: pureRe.test(t) };
  }

  // By card name: "approve Spot the Med and IV Fluids", "skip Lupus",
  // "approve all except Lupus". Matched against pending changes in code.
  const except = low.match(/^(?:please\s+)?approve\s+(?:them\s+)?all\s+(?:except|but(?:\s+not)?|other than)\s+(.+)$/);
  if (except) return { kind: "approve_all", numbers: [], except: splitNames(except[1]), pure: true };
  const named = t.match(/^(?:please\s+)?(approve|yes to|go ahead with|skip|reject|drop|cancel)\s+(.+)$/i);
  if (named) {
    const names = splitNames(named[2]);
    if (names.length) {
      const verb = named[1].toLowerCase();
      return { kind: ["skip", "reject", "drop", "cancel"].includes(verb) ? "reject_names" : "approve_names", numbers: [], names, pure: true };
    }
  }

  if (/^(?:yes|yep|yeah|yup|y|ok|okay|approve|approved|go ahead|do it|sounds good|confirm|confirmed|ship it)$/i.test(t)) {
    return { kind: "yes", numbers: [], pure: true };
  }
  if (/^(?:no|nope|don'?t|do not|reject|cancel)$/i.test(t)) return { kind: "no", numbers: [], pure: true };
  return { kind: "none", numbers: [], pure: false };
}

// Words that aren't part of a card name: "approve the changes for Lupus".
const FILLER = new Set(["the", "a", "an", "change", "changes", "card", "cards", "for", "on", "to", "one", "ones", "please", "it", "them", "those", "these", "that", "this"]);
const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
function splitNames(text) {
  return String(text)
    .split(/\s*(?:,|&|\+|;|\band\b)\s*/i)
    .map((p) => norm(p).split(" ").filter((w) => w && !FILLER.has(w)).join(" "))
    .filter(Boolean);
}
export const cardNameOf = (row) => String(row.summary || "").match(/\*([^*]+)\*/)?.[1] || "that card";
const taskOf = (row) => {
  try {
    return JSON.parse(row.action).task_id;
  } catch {
    return row.id;
  }
};

// Pending rows here, plus the linked conversation's (a copy of the lead's
// morning summary), each tagged with the conversation it belongs to.
async function pendingHereAndLinked(env, conv) {
  const own = (await listPendingRows(env, conv)).map((r) => ({ ...r, conv }));
  const linked = conv.thread === "" ? await getLinkedConv(env, conv.space) : null;
  const theirs = linked ? (await listPendingRows(env, linked)).map((r) => ({ ...r, conv: linked })) : [];
  return [...own, ...theirs];
}

/**
 * Which pending changes each typed name means. A name matches a card when
 * every word starts a word of the card's name ("spot med" → "Spot the Med
 * Error"). All of that card's changes go together. Two different cards for
 * one name is ambiguous and nothing is picked.
 */
export function matchNames(rows, names) {
  const picked = [];
  const problems = [];
  for (const name of names) {
    const words = name.split(" ");
    const hits = rows.filter((r) => {
      const cw = norm(cardNameOf(r)).split(" ");
      return words.every((w) => cw.some((c) => c.startsWith(w)));
    });
    let tasks = [...new Set(hits.map(taskOf))];
    if (tasks.length > 1) {
      const exact = hits.filter((r) => norm(cardNameOf(r)).startsWith(name));
      const t2 = [...new Set(exact.map(taskOf))];
      if (t2.length === 1) tasks = t2;
    }
    if (!tasks.length) problems.push(`I don't see a pending change for "${name}".`);
    else if (tasks.length > 1) {
      const cards = [...new Set(hits.map(cardNameOf))];
      problems.push(`"${name}" could be ${cards.join(" or ")}. Say a bit more of the name.`);
    } else picked.push(...hits.filter((r) => taskOf(r) === tasks[0] && !picked.includes(r)));
  }
  return { picked, problems };
}

const firstName = (sender) => String(sender.displayName || sender.email || "someone").trim().split(/\s+/)[0];

/**
 * Apply the given pending numbers for an approver. Returns Chat-ready lines,
 * one per number, saying exactly what happened. Never claims success it didn't get.
 */
export async function applyNumbers(env, conv, sender, numbers) {
  const lines = [];
  const who = firstName(sender);
  if (!isApprover(env, sender.email)) {
    await logAction(env, { conv, actor: sender.email, event: "approval_refused", action: { numbers }, result: "not an approver" });
    return [
      `Thanks, ${who}. Only approvers can approve, so ${numbers.map((n) => `#${n}`).join(", ")} stays pending.`,
    ];
  }
  const cutoff = new Date(Date.now() - PENDING_TTL_MS).toISOString();

  for (const n of numbers) {
    const row = await findPending(env, conv, n);
    if (!row) {
      lines.push(`#${n}: I don't have a proposal with that number in this conversation.`);
      continue;
    }
    lines.push(await applyRow(env, conv, sender, row, `#${n}`));
  }
  return lines;
}

/** Apply one pending row and say what happened. `tag` is "#3" or null (by name). */
async function applyRow(env, conv, sender, row, tag) {
  const who = firstName(sender);
  const n = row.number;
  const what = tag ? `${tag}: ` : `${cardNameOf(row)}: `;
  const cutoff = new Date(Date.now() - PENDING_TTL_MS).toISOString();
  if (row.status !== "pending") return `${what}already ${row.status}${row.decided_by ? ` (by ${row.decided_by})` : ""}, so I left it alone.`;
  if (row.created_at < cutoff) {
    await markExpired(env, row.id);
    await logAction(env, { conv, number: n, actor: sender.email, event: "approval_expired", action: row.action, result: "older than 48 hours" });
    return `${what}expired (proposals last 48 hours). Ask me again and I'll re-check the card.`;
  }
  if (!(await claimPending(env, row.id, sender.email, cutoff))) return `${what}someone else just handled this one, so I left it alone.`;
  let action;
  try {
    action = JSON.parse(row.action);
    const r = await applyAction(env, action, who);
    await finishPending(env, row.id, "approved", r.detail);
    await logAction(env, { conv, number: n, actor: sender.email, event: "applied", action, result: r.detail });
    if (!r.commented) return `⚠️ ${tag ? `${tag} done, but` : `${row.summary}: done, but`} ${r.detail.replace(/^changed, but /, "")}`;
    return tag ? `✅ ${tag} done: ${row.summary}` : `✅ ${row.summary}`;
  } catch (e) {
    const msg = e instanceof RefusedError ? e.message : `ClickUp error: ${e.message}`;
    await finishPending(env, row.id, "failed", msg);
    await logAction(env, { conv, number: n, actor: sender.email, event: "apply_failed", action: action ?? row.action, result: msg });
    return `❌ ${tag ? `${tag}: ` : ""}${row.summary}. *Not changed.* ${msg}`;
  }
}

/** "approve Spot the Med and IV Fluids" / "skip Lupus" / "approve all except Lupus". */
export async function handleNames(env, conv, sender, parsed) {
  const rows = await pendingHereAndLinked(env, conv);
  if (!rows.length) return parsed.kind === "approve_all" ? "There's nothing pending in this conversation." : null;
  const reject = parsed.kind === "reject_names";
  if (!isApprover(env, sender.email)) {
    await logAction(env, { conv, actor: sender.email, event: "approval_refused", action: { names: parsed.names || parsed.except }, result: "not an approver" });
    return `Thanks, ${firstName(sender)}. Only approvers can ${reject ? "turn down" : "approve"} changes, so they stay pending.`;
  }
  let picked;
  const problems = [];
  if (parsed.kind === "approve_all") {
    const skip = matchNames(rows, parsed.except || []);
    if (skip.problems.length) return skip.problems.join("\n") + "\nNothing changed yet.";
    picked = rows.filter((r) => !skip.picked.includes(r));
  } else {
    const m = matchNames(rows, parsed.names);
    picked = m.picked;
    problems.push(...m.problems);
  }
  const lines = [];
  for (const row of picked) {
    if (reject) {
      if (await rejectPending(env, row.id, sender.email)) {
        await logAction(env, { conv: row.conv, number: row.number, actor: sender.email, event: "rejected", action: row.action });
        lines.push(`Skipped: ${row.summary}. Nothing changed.`);
      } else lines.push(`${cardNameOf(row)}: already handled.`);
    } else lines.push(await applyRow(env, row.conv, sender, row, null));
  }
  // Nothing matched and it reads like a sentence, not a name: let Flo read it.
  if (!picked.length && parsed.kind !== "approve_all" && parsed.names.join(" ").split(" ").length > 4) return null;
  return [...lines, ...problems].join("\n") || "Nothing matched, so nothing changed.";
}

export async function rejectNumbers(env, conv, sender, numbers) {
  if (!isApprover(env, sender.email)) {
    return [`Only people on the approver list can turn down proposals, so ${numbers.map((n) => `#${n}`).join(", ")} stay pending (they expire after 48 hours).`];
  }
  const lines = [];
  for (const n of numbers) {
    const row = await findPending(env, conv, n);
    if (!row) lines.push(`#${n}: no proposal with that number here.`);
    else if (await rejectPending(env, row.id, sender.email)) {
      await logAction(env, { conv, number: n, actor: sender.email, event: "rejected", action: row.action });
      lines.push(`#${n}: dropped. Nothing changed.`);
    } else lines.push(`#${n}: already ${row.status}.`);
  }
  return lines;
}

/**
 * Handle a message that is ONLY an approval/rejection, without Claude.
 * Returns reply text, or null if this message should go to Claude instead.
 */
export async function handlePureApproval(env, conv, sender, parsed) {
  if (!parsed.pure || parsed.kind === "none") return null;
  const pending = await listPending(env, conv);

  if (parsed.kind === "yes" || parsed.kind === "no") {
    if (!pending.length) return null; // probably answering a question; let Claude read it
    if (pending.length > 1) {
      const cards = [...new Set(pending.map(cardNameOf))];
      return `Which ones? ${cards.length} cards have changes waiting. Reply "approve all", or name them, like "approve ${cards[0]}".`;
    }
    const lines = parsed.kind === "yes"
      ? await applyNumbers(env, conv, sender, [pending[0].number])
      : await rejectNumbers(env, conv, sender, [pending[0].number]);
    return lines.join("\n");
  }

  if (parsed.kind === "approve_all" || parsed.kind === "approve_names" || parsed.kind === "reject_names") {
    return handleNames(env, conv, sender, parsed);
  }

  const lines = [];
  if (parsed.numbers.length) lines.push(...(await applyNumbers(env, conv, sender, parsed.numbers)));
  if (parsed.reject?.length) lines.push(...(await rejectNumbers(env, conv, sender, parsed.reject)));
  return lines.join("\n");
}
