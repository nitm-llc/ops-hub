// ============================================================================
// NITM PM — the Claude tool loop. Raw fetch to the Messages API, matching the
// rest of ops-hub (no npm dependencies in this Worker).
//
// Model: env.PM_MODEL (default claude-sonnet-5-5). Thinking is left at the
// model's default (adaptive); effort from env.PM_EFFORT (default "medium").
// Thinking blocks are passed back unchanged within a turn, as the API requires.
// The system prompt + tools are a stable prefix with a cache breakpoint, so
// repeat questions only pay for the conversation.
// ============================================================================

import INSTRUCTIONS from "./ai-pm-instructions.md";
import { findCards, getCard, calendar, prepareAction, describeAction, loadListTask, readCard, RefusedError } from "./clickup.js";
import { createPending, listPending, logAction, loadInstructions } from "./store.js";
import { applyNumbers, isApprover } from "./approvals.js";
import { messagePerson, findLead } from "./outreach.js";
import { todayYmd, weekdayName, LIST_URL } from "./config.js";

export const DEFAULT_MODEL = "claude-sonnet-5-5";
const API = "https://api.anthropic.com/v1/messages";
const MAX_ROUNDS = 8;

const GUARDRAILS = `
# How you work in Google Chat (these override anything above)

You are "Flo", the NITM marketing team's project-management helper in Google Chat (named after Florence Nightingale, and keeping work flowing). Right now you're connected to one ClickUp list, TikTok Production; if someone asks about other marketing work, say it isn't connected yet. Today's date, the sender and any pending proposals arrive in a <context> block with each message.

Ground rules, enforced by the tools as well:
- You are read-only on your own. To change anything, call propose_change. It records a numbered proposal and changes NOTHING. Then tell the person that an approver can reply "yes" (one proposal), "approve all", or name the card ("approve Peds quiz"). Numbers ("approve 1 and 3") also work.
- Only call apply_approved when the sender's CURRENT message explicitly approves numbered proposals, and only for those numbers. The tool checks this and refuses anything else.
- You can propose: a due date, priority, assignees, a comment, the Post Date, the Stage, or another custom field on the card.
- Post Dates and Stages: this OVERRIDES "never change a post date or a card's Stage" above. When a person explicitly asks for one, propose exactly what they asked; it's on purpose. Don't propose Post Date or Stage changes on your own initiative (daily check, balance check): there you suggest them in words, and Post Dates stay Cass's call.
- Never say a change was made unless apply_approved reported it done. If a tool returns an error, say so plainly in one line. Never guess card data: look it up.
- "Run the daily check": follow the daily routine in suggest mode. Call find_cards with overdue=true and with_comments=true, decide per card, then call propose_change once per suggested change (in parallel). Reply with one numbered line per proposal, plus a short list of cards you're leaving alone and why.
- "What's stuck": overdue cards (find_cards overdue=true, with_comments=true), each with the likely reason from the routine (active comment thread, owner/stage mismatch, post date close, unused stage). No proposals unless asked.
- Calendar balance: use the calendar tool, apply the rules, and suggest swaps for Cass in words. Only propose the swap as changes if the person asks you to make it.
- Reaching people: message_person sends someone a Google Chat DM (by first name). Use it for the routine's check-ins with a card's owner (Post Date close, daily step 5), escalations to Avery (step 6), calendar suggestions for Cass, and when a person asks you to tell or ask someone something. One card per message: friendly, 1 to 3 short lines, with the card link and the ask. If they can't be reached directly, the tool sends it to Avery and Mark to pass on, so just report what it says. Don't message the person you're already talking to. The ClickUp comment the routine asks for is still a propose_change.
- If the sender isn't an approver, their proposals are sent to an approver automatically; tell them who it went to.
- Things you can't do (Instagram, Metricool): say so in a line.

Be as concise as possible. People read you on their phones between tasks.
- Answer first. No greeting, no preamble ("Sure!", "Here's..."), no restating the question, no recap at the end.
- Normal answers: 1 to 4 lines. Lists: one line per card, and the note after the card is at most ~8 words.
- Don't explain your reasoning or the rules unless asked. Don't pad with "Let me know if...".
- Offer a next step only when there's a real decision, in a few words ("Send to Cass?").
- Proposals: one line each: "*#1* <url|Peds quiz>: due → Fri Nov 6". Then once, at the end: Reply "approve all", or name the cards ("approve Peds quiz").
- Say every change in plain words a teammate understands. Never the bare word "comment": say who it asks and what ("comment asking Gaby for an update"). Same for owners ("give it to Diana"), not shorthand.
- Nothing to report: one line ("Nothing stuck today.").
- Messages to teammates (message_person): 1 to 2 lines, the card link and the ask. Example: "<url|Peds quiz> posts Fri. Anything blocking it?"

Formatting (Google Chat, not Markdown):
- No headings, no tables, no Markdown links or **double asterisks**.
- *single asterisks* for bold. Links as <url|text>.
- Card lines: <card url|short card name> · Stage · Post Date · the issue. Drop parts that don't matter for the question. Shorten long card names (drop the ID prefix like "86e3g380g - " or "2026.3298 - ").
- Dates as "Fri Nov 6", not ISO.
- The list itself: <${LIST_URL}|TikTok Production>.
`.trim();

// The instructions are editable at /flo/ (stored in D1); the file is the
// default and what "Reset" goes back to. GUARDRAILS are code-only on purpose.
export const DEFAULT_INSTRUCTIONS = INSTRUCTIONS.trim();
export { GUARDRAILS };
export const buildSystemPrompt = (instructions) => `${String(instructions || DEFAULT_INSTRUCTIONS).trim()}\n\n${GUARDRAILS}`;
export const SYSTEM_PROMPT = buildSystemPrompt(DEFAULT_INSTRUCTIONS);

// Stable order: changing this list invalidates the prompt cache.
export const TOOLS = [
  {
    name: "find_cards",
    description:
      "Search TikTok Production cards. Returns compact cards with stage, type, post_date, due_date, assignees, priority, overdue/days_overdue, workdays_to_post, follow_up_window (5, or 7 for Kristine), expected_owner and owner_matches. Without overdue or a date range it looks at Post Dates from 2 weeks ago to 6 weeks ahead. Done stages are hidden unless include_done.",
    input_schema: {
      type: "object",
      properties: {
        overdue: { type: "boolean", description: "Only cards whose due date has passed and whose Stage isn't done or paused." },
        stage: { type: "array", items: { type: "string" }, description: "Stage names, e.g. [\"Ready To Edit / Design\"]" },
        type: { type: "array", items: { type: "string" }, description: "Type names, e.g. [\"Quiz\"]" },
        post_date_from: { type: "string", description: "YYYY-MM-DD" },
        post_date_to: { type: "string", description: "YYYY-MM-DD" },
        assignee: { type: "string", description: "First name (Diana, Karli, Kristine, Tia, Gaby, Cass, Avery) or ClickUp id" },
        name_contains: { type: "string", description: "Case-insensitive text in the card name" },
        include_done: { type: "boolean" },
        with_comments: { type: "boolean", description: "Attach each card's 3 most recent comments (slower). Use for stuck/daily checks." },
        limit: { type: "integer", description: "Max cards, default 40" },
      },
      additionalProperties: false,
    },
  },
  {
    name: "get_card",
    description: "One card in full: fields, assignees, due date, priority, script link, description and the 10 most recent comments.",
    input_schema: {
      type: "object",
      properties: { task_id: { type: "string" } },
      required: ["task_id"],
      additionalProperties: false,
    },
  },
  {
    name: "calendar",
    description: "TikTok cards by Post Date for a date range (max ~2 months), day by day, with Type and Stage. Use for 'what's posting' and balance checks.",
    input_schema: {
      type: "object",
      properties: { from: { type: "string", description: "YYYY-MM-DD" }, to: { type: "string", description: "YYYY-MM-DD" } },
      required: ["from", "to"],
      additionalProperties: false,
    },
  },
  {
    name: "propose_change",
    description:
      "Record ONE proposed change as a numbered pending action. Changes nothing. type is one of: due_date (date YYYY-MM-DD), priority (urgent|high|normal|low), assignee (add/remove: first names), comment (text, posted publicly on the card), post_date (date YYYY-MM-DD, or empty to clear), stage (stage: the Stage option name), custom_field (field: the field name, value: an option name, date YYYY-MM-DD, text, number or yes/no; empty clears it). Values are checked against the list; an error lists the valid options. Returns the proposal number.",
    input_schema: {
      type: "object",
      properties: {
        type: { type: "string", enum: ["due_date", "priority", "assignee", "comment", "post_date", "stage", "custom_field"] },
        task_id: { type: "string" },
        date: { type: "string" },
        priority: { type: "string", enum: ["urgent", "high", "normal", "low"] },
        add: { type: "array", items: { type: "string" } },
        remove: { type: "array", items: { type: "string" } },
        text: { type: "string" },
        stage: { type: "string", description: "For type=stage: the Stage option name" },
        field: { type: "string", description: "For type=custom_field: the field name, e.g. \"Who's Filming\"" },
        value: { description: "For type=custom_field: the new value (option name, YYYY-MM-DD, text, number or yes/no). Empty clears it." },
        reason: { type: "string", description: "One short line: why" },
      },
      required: ["type", "task_id"],
      additionalProperties: false,
    },
  },
  {
    name: "message_person",
    description:
      "Send a teammate a Google Chat DM. person: first name (Diana, Karli, Kristine, Tia, Gaby, Cass, Avery, Mark...). If they haven't opened a chat with Flo yet, it goes to Avery and Mark to pass on. Limited to one message per person per card per day. Returns what happened.",
    input_schema: {
      type: "object",
      properties: {
        person: { type: "string" },
        text: { type: "string", description: "The message, in Chat formatting (*bold*, <url|text>)" },
        task_id: { type: "string", description: "The card it's about, if any" },
        kind: { type: "string", enum: ["check_in", "escalation", "calendar", "other"] },
      },
      required: ["person", "text"],
      additionalProperties: false,
    },
  },
  {
    name: "apply_approved",
    description:
      "Carry out pending proposals the sender approved in their current message. Only works for approvers and only for the numbers they typed. Returns what happened to each.",
    input_schema: {
      type: "object",
      properties: { numbers: { type: "array", items: { type: "integer" } } },
      required: ["numbers"],
      additionalProperties: false,
    },
  },
];

// ---------------------------------------------------------------------------
// Tool execution
// ---------------------------------------------------------------------------
async function runTool(env, turn, name, input) {
  const opts = { tz: turn.tz, today: turn.today };
  switch (name) {
    case "find_cards":
      return findCards(env, input, opts);
    case "get_card":
      return getCard(env, input, opts);
    case "calendar":
      return calendar(env, input, opts);
    case "propose_change": {
      let action, resolved;
      try {
        const { reason, ...raw } = input;
        ({ action, resolved } = await prepareAction(env, raw));
      } catch (e) {
        if (e instanceof RefusedError) {
          await logAction(env, { conv: turn.conv, actor: turn.sender.email, event: "proposal_refused", action: input, result: e.message });
          return { refused: true, message: e.message };
        }
        throw e;
      }
      const task = await loadListTask(env, action.task_id);
      const card = readCard(task, turn.tz, turn.today);
      const summary = describeAction(action, card.name, resolved) + (input.reason ? ` (${String(input.reason).slice(0, 140)})` : "");
      // A non-approver's proposal can't be approved in their own DM (no
      // approver is in it), so it's filed in the lead approver's DM and sent
      // to them. Scheduled runs already happen in the lead's DM.
      if (!turn.scheduled && !isApprover(env, turn.sender.email)) {
        const lead = await findLead(env);
        if (lead) {
          const leadConv = { space: lead.dmSpace, thread: "" };
          const asker = String(turn.sender.displayName || turn.sender.email).split(/\s+/)[0];
          const row = await createPending(env, leadConv, { proposedBy: turn.sender.email, action, summary });
          await logAction(env, { conv: leadConv, number: row.number, actor: turn.sender.email, event: "proposed", action, result: summary });
          const sent = await messagePerson(env, {
            person: lead.email,
            kind: "approval",
            taskId: action.task_id,
            tz: turn.tz,
            text: `${asker} asks: *#${row.number}* ${summary} <${card.url}|card>\nReply "approve ${String(card.name).split(/\s+/).slice(0, 3).join(" ")}" or "skip ${String(card.name).split(/\s+/).slice(0, 3).join(" ")}".`,
          });
          turn.proposed.push(row.number);
          return {
            number: row.number,
            summary,
            card_url: card.url,
            status: sent.sent
              ? `pending — sent to ${String(lead.name).split(/\s+/)[0]} to approve (${sent.message}). Nothing has changed yet.`
              : `pending, but I couldn't reach an approver: ${sent.message}`,
          };
        }
      }
      const row = await createPending(env, turn.conv, { proposedBy: turn.sender.email, action, summary });
      turn.proposed.push(row.number);
      await logAction(env, { conv: turn.conv, number: row.number, actor: turn.sender.email, event: "proposed", action, result: summary });
      return { number: row.number, summary, card_url: card.url, status: "pending — nothing has changed yet" };
    }
    case "message_person": {
      return messagePerson(env, {
        person: input.person,
        text: input.text,
        taskId: input.task_id || null,
        kind: input.kind || "other",
        tz: turn.tz,
      });
    }
    case "apply_approved": {
      const asked = (input.numbers || []).map(Number);
      const allowed = asked.filter((n) => turn.approvedNumbers.includes(n));
      const blocked = asked.filter((n) => !turn.approvedNumbers.includes(n));
      if (!isApprover(env, turn.sender.email)) {
        return { refused: true, message: "The sender isn't on the approver list. Nothing was changed." };
      }
      const lines = allowed.length ? await applyNumbers(env, turn.conv, turn.sender, allowed) : [];
      if (blocked.length) {
        lines.push(`Not applied: ${blocked.map((n) => `#${n}`).join(", ")} (the sender didn't approve these numbers in this message).`);
      }
      return { results: lines };
    }
    default:
      throw new Error(`Unknown tool ${name}`);
  }
}

// ---------------------------------------------------------------------------
// Claude call
// ---------------------------------------------------------------------------
export class ClaudeError extends Error {}

async function callClaude(env, messages, { signal, system }) {
  if (!env.ANTHROPIC_API_KEY) throw new ClaudeError("ANTHROPIC_API_KEY is not set");
  const body = {
    model: env.PM_MODEL || DEFAULT_MODEL,
    max_tokens: 8000,
    system: [{ type: "text", text: system || SYSTEM_PROMPT, cache_control: { type: "ephemeral" } }],
    tools: TOOLS,
    messages,
    output_config: { effort: env.PM_EFFORT || "medium" },
  };
  const headers = {
    "Content-Type": "application/json",
    "x-api-key": env.ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
  };
  // Server-side refusal fallback: on a policy decline the API re-runs the
  // request on a fallback model inside the same call. PM_FALLBACKS=off disables.
  if (String(env.PM_FALLBACKS || "on").toLowerCase() !== "off") {
    body.fallbacks = "default";
    headers["anthropic-beta"] = "server-side-fallback-2026-07-01";
  }
  let res;
  try {
    res = await fetch(API, { method: "POST", headers, body: JSON.stringify(body), signal });
  } catch (e) {
    if (e?.name === "TimeoutError" || e?.name === "AbortError") throw new ClaudeError("timed out");
    throw new ClaudeError(`network error: ${e.message}`);
  }
  const text = await res.text();
  if (!res.ok) throw new ClaudeError(`${res.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

// ---------------------------------------------------------------------------
// One turn: history + new message -> reply text. Never throws.
// ---------------------------------------------------------------------------
function contextBlock(env, turn, pending) {
  const p = pending.length
    ? pending.map((x) => `#${x.number}: ${x.summary}`).join("\n")
    : "none";
  return [
    "<context>",
    `Today: ${weekdayName(turn.today)} (${turn.today}), ${turn.tz}`,
    `Sender: ${turn.sender.displayName || "?"} <${turn.sender.email}>; approver: ${isApprover(env, turn.sender.email) ? "yes" : "no"}`,
    `Numbers this message approves: ${turn.approvedNumbers.length ? turn.approvedNumbers.join(", ") : "none"}`,
    ...(turn.scheduled ? ["This is a scheduled run, not a person: nobody approves anything in this turn. Propose and message people as the task says."] : []),
    `Pending proposals in this conversation:\n${p}`,
    "</context>",
  ].join("\n");
}

function historyToMessages(history) {
  const msgs = [];
  for (const h of history) {
    const role = h.role === "assistant" ? "assistant" : "user";
    const text = role === "user" && h.who ? `${h.who}: ${h.text}` : h.text;
    if (!text) continue;
    const last = msgs[msgs.length - 1];
    if (last && last.role === role) last.content += `\n\n${text}`;
    else msgs.push({ role, content: text });
  }
  while (msgs.length && msgs[0].role !== "user") msgs.shift();
  return msgs;
}

export async function runTurn(env, turn) {
  turn.today = turn.today || todayYmd(turn.tz);
  turn.proposed = [];
  let pending;
  try {
    pending = await listPending(env, turn.conv);
  } catch {
    pending = [];
  }

  let system = SYSTEM_PROMPT;
  try {
    system = buildSystemPrompt((await loadInstructions(env, DEFAULT_INSTRUCTIONS)).text);
  } catch (e) {
    console.error("nitm-pm: instructions load failed, using the version in code:", e);
  }

  const messages = historyToMessages(turn.history || []);
  const userText = `${contextBlock(env, turn, pending)}\n\n${turn.text}`;
  const last = messages[messages.length - 1];
  if (last && last.role === "user") last.content += `\n\n${userText}`;
  else messages.push({ role: "user", content: userText });

  const remaining = () => turn.deadline - Date.now();
  try {
    for (let round = 0; round < MAX_ROUNDS; round++) {
      if (remaining() < 3000) return timeoutReply(turn);
      const resp = await callClaude(env, messages, { system, signal: AbortSignal.timeout(Math.max(1000, remaining() - 1500)) });

      if (resp.stop_reason === "refusal") return "Sorry, I can't help with that one.";
      if (resp.stop_reason !== "tool_use") {
        const text = (resp.content || []).filter((b) => b.type === "text").map((b) => b.text).join("\n").trim();
        if (resp.stop_reason === "max_tokens") return (text ? text + "\n\n" : "") + "(I ran out of room. Ask me something narrower for the rest.)";
        return text || "Done.";
      }

      messages.push({ role: "assistant", content: resp.content });
      const uses = resp.content.filter((b) => b.type === "tool_use");
      const results = await Promise.all(
        uses.map(async (u) => {
          try {
            const out = await runTool(env, turn, u.name, u.input || {});
            return { type: "tool_result", tool_use_id: u.id, content: JSON.stringify(out) };
          } catch (e) {
            return { type: "tool_result", tool_use_id: u.id, is_error: true, content: String(e.message || e).slice(0, 500) };
          }
        })
      );
      messages.push({ role: "user", content: results });
    }
    return "That needed more steps than I'm allowed in one go. Try a narrower question." + proposedNote(turn);
  } catch (e) {
    if (e instanceof ClaudeError) {
      if (/timed out/.test(e.message)) return timeoutReply(turn);
      return `Sorry, my AI service (Claude) returned an error, so I couldn't answer: ${e.message}${proposedNote(turn)}`;
    }
    return `Sorry, something went wrong: ${e.message}${proposedNote(turn)}`;
  }
}

function proposedNote(turn) {
  if (!turn.proposed?.length) return "";
  const nums = turn.proposed.map((n) => `#${n}`).join(", ");
  return `\n\nI did record proposal${turn.proposed.length > 1 ? "s" : ""} ${nums} before stopping. Nothing has changed; reply "approve ${turn.proposed.join(" and ")}" to apply, or ask me to list them.`;
}

function timeoutReply(turn) {
  return "That took longer than I can spend on one message (about 35 seconds), so I stopped. Try a narrower question, like one stage or one week." + proposedNote(turn);
}
