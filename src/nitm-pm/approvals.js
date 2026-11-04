// ============================================================================
// NITM PM — approvals. Parsed and enforced in code: the model can ask to
// apply things, but only numbers the approver typed in THIS message, from a
// person on PM_APPROVERS, ever reach applyAction().
// ============================================================================

import { applyAction, RefusedError } from "./clickup.js";
import { PENDING_TTL_MS, getPending, claimPending, finishPending, rejectPending, markExpired, listPending, logAction } from "./store.js";

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

  if (/^(?:yes|yep|yeah|yup|y|ok|okay|approve|approved|go ahead|do it|sounds good|confirm|confirmed|ship it)$/i.test(t)) {
    return { kind: "yes", numbers: [], pure: true };
  }
  if (/^(?:no|nope|don'?t|do not|reject|cancel)$/i.test(t)) return { kind: "no", numbers: [], pure: true };
  return { kind: "none", numbers: [], pure: false };
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
    const row = await getPending(env, conv, n);
    if (!row) {
      lines.push(`#${n}: I don't have a proposal with that number in this conversation.`);
      continue;
    }
    if (row.status !== "pending") {
      lines.push(`#${n}: already ${row.status}${row.decided_by ? ` (by ${row.decided_by})` : ""}, so I left it alone.`);
      continue;
    }
    if (row.created_at < cutoff) {
      await markExpired(env, row.id);
      await logAction(env, { conv, number: n, actor: sender.email, event: "approval_expired", action: row.action, result: "older than 48 hours" });
      lines.push(`#${n}: expired (proposals last 48 hours). Ask me again and I'll re-check the card.`);
      continue;
    }
    if (!(await claimPending(env, row.id, sender.email, cutoff))) {
      lines.push(`#${n}: someone else just handled this one, so I left it alone.`);
      continue;
    }
    let action;
    try {
      action = JSON.parse(row.action);
      const r = await applyAction(env, action, who);
      await finishPending(env, row.id, "approved", r.detail);
      await logAction(env, { conv, number: n, actor: sender.email, event: "applied", action, result: r.detail });
      lines.push(r.commented ? `✅ #${n} done: ${row.summary}` : `⚠️ #${n} done, but ${r.detail.replace(/^changed, but /, "")}`);
    } catch (e) {
      const msg = e instanceof RefusedError ? e.message : `ClickUp error: ${e.message}`;
      await finishPending(env, row.id, "failed", msg);
      await logAction(env, { conv, number: n, actor: sender.email, event: "apply_failed", action: action ?? row.action, result: msg });
      lines.push(`❌ #${n}: ${row.summary}. *Not changed.* ${msg}`);
    }
  }
  return lines;
}

export async function rejectNumbers(env, conv, sender, numbers) {
  if (!isApprover(env, sender.email)) {
    return [`Only people on the approver list can turn down proposals, so ${numbers.map((n) => `#${n}`).join(", ")} stay pending (they expire after 48 hours).`];
  }
  const lines = [];
  for (const n of numbers) {
    const row = await getPending(env, conv, n);
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
      return `Which ones? ${pending.map((p) => `#${p.number}`).join(", ")} are pending. Reply "approve ${pending[0].number}" or "approve all".`;
    }
    const lines = parsed.kind === "yes"
      ? await applyNumbers(env, conv, sender, [pending[0].number])
      : await rejectNumbers(env, conv, sender, [pending[0].number]);
    return lines.join("\n");
  }

  if (parsed.kind === "approve_all") {
    if (!pending.length) return "There's nothing pending in this conversation.";
    return (await applyNumbers(env, conv, sender, pending.map((p) => p.number))).join("\n");
  }

  const lines = [];
  if (parsed.numbers.length) lines.push(...(await applyNumbers(env, conv, sender, parsed.numbers)));
  if (parsed.reject?.length) lines.push(...(await rejectNumbers(env, conv, sender, parsed.reject)));
  return lines.join("\n");
}
