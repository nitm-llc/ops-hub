// ============================================================================
// CX SCORECARD AUDIT — the 9 checks and the "Right voice" sub-rules.
//
// Every check and sub-rule can be switched off. A disabled check is left out
// of the prompt and recorded as "off" (never pass/fail), so turning one on
// later doesn't rewrite history. Defaults below; from Phase 3 the switches
// live in D1 and the /cx-audit/ page.
//
// Sub-rules marked OFF wait on an open decision in the guidelines
// ("Open decisions": who's speaking, Ivy's disclosure + sign-off, emoji).
// ============================================================================

export const CHECKS = [
  { id: "answered", n: 1, name: "Answered the question", passes: "Every question the customer asked is answered, in the first reply that could answer it." },
  { id: "didnt_ask_for_what_we_had", n: 2, name: "Didn't ask for what we had", passes: "No request for information already in the email, subject line, order record, or earlier messages. We can look customers up by the email they wrote from, so asking for an order number before trying that is a fail." },
  { id: "reassured_first", n: 3, name: "Reassured first", passes: "A worried or upset customer gets reassurance before any request. N/A when the customer is not worried or upset." },
  { id: "accurate", n: 4, name: "Accurate", passes: "No claim beyond our content and policies: no invented social proof, page counts, timelines, policies or product facts. Fail only on a claim the guidelines contradict or that is clearly made up; a plausible claim you can't verify passes, and you say so in the evidence." },
  { id: "no_silent_handoff", n: 5, name: "No silent handoff", passes: "Every handoff tells the customer what was done, who takes over, and when. A handoff with no message to the customer at all is a fail. N/A when there is no handoff." },
  { id: "right_voice", n: 6, name: "Right voice", passes: "The enabled voice rules below all hold." },
  { id: "encouragement_placed_right", n: 7, name: "Encouragement placed right", passes: "\"Future nurse\" / \"You've got this\" / \"Happy studying\" style lines only close a resolved or study-related reply, never one to someone worried, upset or still waiting on a fix, and never in place of an answer. N/A when there is no encouragement line." },
  { id: "one_message", n: 8, name: "One message", passes: "No separate message whose only content is \"Anything else I can help with?\" or similar. A follow-up that adds information or asks for something needed is fine." },
  { id: "every_no_has_next_step", n: 9, name: "Every no has a next step", passes: "Each refusal or \"we can't\" comes with an alternative or next step. N/A when nothing is refused." },
];

export const VOICE_RULES = [
  { id: "banned_phrases", name: "No banned phrases", rule: "None of: \"your query\", \"I'm well trained to answer\", \"I apologize for any inconvenience\", \"I'd be more than happy to\", the stock closer \"Please let me know if this helps or if you have any further questions\", or \"Thank you for your patience\" when the customer did not actually wait." },
  { id: "greeting", name: "Greets by first name", rule: "Greets the customer by first name when it is known (from the message, signature or form); \"Hi there\" only when it isn't." },
  { id: "product_names", name: "Product names right", rule: "\"The Complete Nursing School Bundle\" (with \"The\"), \"The NCLEX Study Notebook\", \"eBook\" with a capital B, \"VitalSource Bookshelf\"." },
  { id: "ivy_not_kristine", name: "Ivy never speaks as Kristine", rule: "Ivy never writes as if she were Kristine (\"my products\", \"my YouTube channel\"). Applies to Ivy only." },
  { id: "speaker_identity", name: "Speaks as the team", rule: "Uses \"we\"/\"our\", not \"my products\", unless Kristine herself is writing; names Kristine in the third person; nobody signs as Kristine or \"for Kristine\" unless they are Kristine." },
  { id: "sign_off", name: "Consistent sign-off", rule: "Each person signs their own first name; Ivy signs as Ivy, the same way every time." },
  { id: "ivy_disclosure", name: "Ivy's disclosure line", rule: "Ivy discloses once per conversation, after the greeting, with one short line like \"I'm Ivy, Nurse In The Making's AI assistant. If you'd rather talk to someone on our team, just reply and ask.\" Applies to Ivy only." },
  { id: "emoji", name: "Emoji rule", rule: "People may use at most one emoji, in the sign-off only. Ivy uses none." },
];

// Defaults. Waiting on open decisions: speaker_identity, sign_off, ivy_disclosure, emoji.
export const DEFAULT_TOGGLES = {
  checks: Object.fromEntries(CHECKS.map((c) => [c.id, true])),
  voice: {
    banned_phrases: true,
    greeting: true,
    product_names: true,
    ivy_not_kristine: true,
    speaker_identity: false,
    sign_off: false,
    ivy_disclosure: false,
    emoji: false,
  },
};

export function normalizeToggles(t = {}) {
  const out = { checks: { ...DEFAULT_TOGGLES.checks }, voice: { ...DEFAULT_TOGGLES.voice } };
  for (const c of CHECKS) if (typeof t.checks?.[c.id] === "boolean") out.checks[c.id] = t.checks[c.id];
  for (const r of VOICE_RULES) if (typeof t.voice?.[r.id] === "boolean") out.voice[r.id] = t.voice[r.id];
  // Right voice with every sub-rule off has nothing to judge.
  if (!VOICE_RULES.some((r) => out.voice[r.id])) out.checks.right_voice = false;
  return out;
}

export const enabledChecks = (toggles) => CHECKS.filter((c) => toggles.checks[c.id]);
export const enabledVoiceRules = (toggles) => VOICE_RULES.filter((r) => toggles.voice[r.id]);

// Plain-language "what went wrong" per check, for the fix list and digest:
// "<who> <problem>", e.g. "Ivy handed off without saying what happens next".
export const PROBLEM = {
  answered: "didn't fully answer the question",
  didnt_ask_for_what_we_had: "asked for information we already had",
  reassured_first: "didn't reassure a worried customer first",
  accurate: "said something we can't back up",
  no_silent_handoff: "handed off without saying what happens next and when",
  right_voice: "used off-voice wording",
  encouragement_placed_right: "used encouragement at the wrong moment",
  one_message: "sent an extra message that only checks in",
  every_no_has_next_step: "said no without offering an alternative",
};

// "Right voice" is split by the rule that broke, so the fix list says what
// actually went wrong ("used a banned phrase", not "off-voice wording").
export const VOICE_PROBLEM = {
  banned_phrases: "used a banned phrase",
  greeting: "didn't greet the customer by first name",
  product_names: "got a product name wrong",
  ivy_not_kristine: "spoke as if she were Kristine",
  speaker_identity: "didn't speak as the team",
  sign_off: "signed off inconsistently",
  ivy_disclosure: "skipped or changed the AI disclosure line",
  emoji: "broke the emoji rule",
};
export const problemFor = (checkId, rule) => (rule && VOICE_PROBLEM[rule]) || PROBLEM[checkId] || checkId;
