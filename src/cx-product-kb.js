// Pure pieces of the CX agent's product knowledge base: model pricing, the per-chunk
// reading prompt, and the Fin profile prompt. Kept out of index.js so they can be tested
// word-for-word — the two prompts must match NITM's existing Fin-profile tool exactly.

// Per-MTok pricing, matched on the specific model ID (not the family). Haiku 5.5 has two
// rate cards: a request whose prompt goes over 100k input tokens is billed at the higher one.
export const HAIKU_55_TIER_TOKENS = 100000;
const PRICES = [
  ['claude-opus-5-5',   { in: 4e-6,   out: 20e-6 }],
  ['claude-opus-5',     { in: 5e-6,   out: 25e-6 }],
  ['claude-opus-4-8',   { in: 5e-6,   out: 25e-6 }],
  ['claude-opus-4-7',   { in: 5e-6,   out: 25e-6 }],
  ['claude-sonnet-5-5', { in: 2e-6,   out: 10e-6 }],
  ['claude-sonnet-5',   { in: 2e-6,   out: 10e-6 }],
  ['claude-haiku-4-5',  { in: 1e-6,   out: 5e-6  }],
];
export function cxModelPricing(model, promptTokens = 0) {
  const m = (model || '').toLowerCase();
  if (m.startsWith('claude-haiku-5-5')) {
    return promptTokens > HAIKU_55_TIER_TOKENS ? { in: 0.5e-6, out: 2.5e-6 } : { in: 0.1e-6, out: 0.5e-6 };
  }
  // Longest ID first, so 'claude-opus-5' never swallows 'claude-opus-5-5'.
  for (const [id, p] of PRICES) if (m === id || m.startsWith(id + '-')) return p;
  return { in: 5e-6, out: 25e-6 }; // unknown model: assume Opus-tier rather than under-report
}

// Read-step batch sizes. Each request must stay under Haiku 5.5's 100k-token tier. A
// high-res page/image costs up to ~4.8k image tokens, and a PDF page also carries its text
// layer (~1–3k), so: 8 PDF pages ≤ ~64k, 10 images ≤ ~48k. The server also counts tokens
// before each Haiku read and refuses anything over the line.
export const PDF_PAGES_PER_CHUNK = 8;
export const IMAGES_PER_BATCH = 10;

export function cxReadPrompt(kind, a, b, total) {
  const unit = kind === 'images' ? 'images' : 'pages';
  return `These are ${unit} ${a}–${b} of ${total} from one Nurse In The Making product (study guide pages, flashcards, or badge reference cards). For each card or page, write its title, then list every section heading, topic, body system, drug class, scale, procedure, and named concept it covers. Topic names only: do not copy numeric values, ranges, doses, or formulas. Use short plain-text bullets grouped under each title. No intro or closing text.`;
}

export const LENGTH_RULES = {
  short: "Keep the whole entry around 150–250 words. 'What's inside' should be one line per card/section.",
  standard: "Keep the whole entry around 300–450 words. 'What's inside' should be one concise line per card/section.",
  detailed: "Keep the whole entry around 550–800 words. 'What's inside' can name the main sub-topics within each card/section.",
};
export function cxProfileLength(v) {
  return Object.prototype.hasOwnProperty.call(LENGTH_RULES, v) ? v : 'detailed';
}

// source: { pageNotes } for products read through the form, or { coverageOutline } for the
// backfill of older products that were never re-uploaded.
export function cxFinProfilePrompt({ name, description, length, pageNotes, coverageOutline }) {
  const LENGTH_RULE = LENGTH_RULES[cxProfileLength(length)];
  const desc = (description || '').trim();
  const sourceBlock = pageNotes != null
    ? `Here are notes taken from every page/card of the product, in order:\n\n${pageNotes}`
    : `Here is the product's coverage map, listing every card/section and the topics it covers:\n\n${coverageOutline || ''}`;
  const extra = desc ? `\nExtra notes from the NITM team (treat as accurate):\n${desc}` : '';
  return `You are writing a product knowledge-base entry for Fin, the AI customer-support agent for Nurse In The Making (NITM). NITM is a creator-led brand that sells physical study guides, flashcards, badge reference cards, eBooks, and bundles to nursing students and new nurses. Fin reads these entries to recommend the right product when a shopper asks a question.

${sourceBlock}

Product name: ${name}${extra}

Rules:
- Describe what's covered at the topic level: name the topics, body systems, drug classes, scales, procedures, and concepts so Fin can match shopper questions to this product. Do NOT reproduce clinical values, normal ranges, doses, formulas, drug-antidote pairings, or answers. Fin must not be able to use this entry as a clinical reference.
- Only describe what is actually in the materials or the notes. Don't invent page counts, prices, sizes, materials, or features. If something is unclear, leave it out.
- If the product mentions The Complete Nursing School Bundle, always write it exactly as "The Complete Nursing School Bundle".
- Plain, friendly, accurate language. No hype, no exclamation points.
- ${LENGTH_RULE}

Output Markdown in exactly this structure and nothing else:

# <Product name>

<One or two sentences: what it is, its format, and how it's used.>

## What's inside
- <Card or section name>: <what it covers, topic level>

## Who it's for
<One or two sentences.>

## When to recommend it
- <A shopper situation, in plain terms>
(3–5 bullets)

## Questions this answers
- <A question a shopper might type, in their words>
(5–8 bullets)`;
}
