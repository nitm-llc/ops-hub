// Shared PII scrubbing. cxScrubPII moved here unchanged from src/index.js so the
// CX scorecard audit (src/cx-audit) can reuse it; tests/pii.test.js pins its behaviour.
// Pure ESM, no Worker globals, so node --test imports it directly.

// v5.1: strip customer-identifying detail from text that leaves this system. Export files get
// pasted into third-party tools, so they must carry the SHAPE of a request, never an identity.
// Order matters: order numbers before the generic 5-digit (zip) rule, or they get half-eaten.
export function cxScrubPII(s) {
  let t = String(s || '');
  t = t.replace(/[\w.+-]+@[\w-]+\.[\w.-]+/g, '[email]');
  t = t.replace(/\bE-\d{6,}\b/gi, '[order]');
  t = t.replace(/#?\b\d{1,2}-\d{4,8}\b/g, '[order]');
  // Phone: consume any wrapping parens so we don't leave an orphan bracket.
  t = t.replace(/\(?\b(?:\+?\d{1,2}[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b\)?/g, '[phone]');
  // Addresses are the highest-risk field, so catch them three ways.
  // 1) Context-led: everything after an address cue, up to sentence end. This is what catches
  //    forms with no recognisable suffix (e.g. "6321 Via Venetia N, Delray Beach").
  t = t.replace(
    /\b(address\s+is|address\s*:|ship(?:ping)?\s+(?:to|address)|send\s+(?:it\s+)?to|mail\s+(?:it\s+)?to|deliver(?:ed)?\s+to)\s*:?\s*([^.!?\n]{5,140})/gi,
    (m, cue, tail) => (/\d/.test(tail) && !/https?:\/\//i.test(tail)) ? `${cue} [address]` : m);
  // 2) Street-suffix led.
  t = t.replace(/\b\d{1,6}\s+[A-Za-z][A-Za-z.'-]*(?:\s+[A-Za-z][A-Za-z.'-]*){0,4}\s+(?:st|street|ave|avenue|rd|road|dr|drive|ln|lane|blvd|boulevard|way|ct|court|pl|place|cir|circle|ter|terrace|trl|trail|loop|path|row|walk|sq|square|plaza|pkwy|parkway|hwy|highway|via|crossing|bend|run|pass|point|ridge|alley|park)\b\.?/gi, '[address]');
  // 3) Secondary address lines.
  t = t.replace(/\b(?:apt|apartment|unit|suite|ste)\b\.?\s*#?\s*(?=[\w-]{0,7}\d)[\w-]{1,8}\b/gi, '[unit]');
  t = t.replace(/\b\d{5}(?:-\d{4})?\b/g, '[postcode]');
  // Keep our own links and the tool consoles our process rules point at — blanking those
  // turns a usable rule ("check ShipMonk under Customer Information") into noise, and none of
  // them are secrets. Anything else a customer pasted gets dropped.
  const KEEP_LINK = /(?:a)?nurseinthemaking\.com|youtube\.com\/@NurseInTheMaking|(?:app\.)?shipmonk\.com|myshopify\.com|admin\.shopify\.com|vitalsource\.com|zendesk\.com|intercom\.com/i;
  t = t.replace(/https?:\/\/\S+/g, (m) => KEEP_LINK.test(m) ? m : '[link]');
  return t.replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}

// ---------------------------------------------------------------------------
// CX scorecard audit: text goes to the Claude API and scrubbed excerpts go into
// D1, so on top of cxScrubPII it removes the secrets customers paste in: eBook
// redemption codes, one-time passcodes and discount codes. Placeholders (not
// deletion) on purpose: the "didn't ask for what we had" check needs to see that
// an order number or code WAS given, just not what it was. Product names stay.
//
// Runs BEFORE cxScrubPII: its 5-digit postcode and phone rules would otherwise
// half-eat 6-8 digit codes, leaving fragments.
// ---------------------------------------------------------------------------
const CODE_CUE = String.raw`(?:verification|security|confirmation|one[- ]time|access|login|sign[- ]in|redemption|redeem|ebook|e-book|vitalsource|activation|promo|discount|coupon|gift ?card)`;

export function scrubCodes(s) {
  let t = String(s || "");
  // Cued discount codes: "use code NURSE15", "promo code: SAVE10", "discount code is X".
  t = t.replace(/\b((?:promo|discount|coupon)\s*code|use\s+(?:the\s+)?code|code)(\s*(?:is|:|=)?\s*)["“'‘]?([A-Za-z0-9][A-Za-z0-9_-]{2,30})["”'’]?(?=\s+(?:for|at|to)\s+\d{1,2}\s*%|\s*(?:at checkout|for \d))/gi,
    (m, cue, gap) => `${cue}${gap}[discount code]`);
  t = t.replace(/\b((?:promo|discount|coupon)\s*code)(\s*(?:is|:|=)?\s*)["“'‘]?([A-Za-z0-9][A-Za-z0-9_-]{2,30})["”'’]?/gi,
    (m, cue, gap) => `${cue}${gap}[discount code]`);
  // Cued codes of any other kind: "your eBook code is ABCD-1234", "verification code: 482913".
  t = t.replace(new RegExp(String.raw`\b(${CODE_CUE}\s*(?:code|key|pin|passcode)|passcode|otp|pin)(\s*(?:is|was|:|=|#)?\s*)["“'‘]?([A-Za-z0-9](?:[A-Za-z0-9-]{2,40}[A-Za-z0-9])?)["”'’]?`, "gi"),
    (m, cue, gap, code) => /\d/.test(code) || /^[A-Z0-9-]{6,}$/.test(code) ? `${cue}${gap}[${/promo|discount|coupon/i.test(cue) ? "discount code" : /\d{4,8}/.test(code) && /^\d+$/.test(code) ? "one-time code" : "code"}]` : m);
  t = t.replace(/\b(code)(\s*(?:is|:|=)\s*)["“'‘]?([A-Za-z0-9](?:[A-Za-z0-9-]{2,40}[A-Za-z0-9])?)["”'’]?/gi,
    (m, cue, gap, code) => /\d/.test(code) || /^[A-Z0-9-]{6,}$/.test(code) ? `${cue}${gap}[code]` : m);
  // Uncued redemption-style codes: dashed groups (ABCD-EFGH-1234) or 8+ upper-case
  // letters and digits mixed. Real words and product names are never both.
  t = t.replace(/\b(?=[A-Z0-9-]*\d)(?=[A-Z0-9-]*[A-Z])[A-Z0-9]{3,}(?:-[A-Z0-9]{3,}){1,5}\b/g, "[code]");
  t = t.replace(/\b(?=[A-Z0-9]*\d)(?=[A-Z0-9]*[A-Z])[A-Z0-9]{8,}\b/g, "[code]");
  // Short promo-style codes: SAVE15, NURSE10.
  t = t.replace(/\b[A-Z]{3,}\d{1,3}\b/g, "[discount code]");
  // A bare 6-8 digit number is a passcode or account number, never prose.
  t = t.replace(/(?<![#\w-])\d{6,8}(?![\w-])/g, "[code]");
  return t;
}

// Order numbers: placeholder, so the scorer knows one was given.
function scrubOrderNumbers(t) {
  return t
    .replace(/#\s?\d{1,2}-\d{4,8}\b/g, "[order number]")
    .replace(/#\s?\d{4,8}\b/g, "[order number]")
    .replace(/\b(order\s*(?:number|no\.?|#)?\s*(?:is|:)?\s*)#?\d{4,10}\b/gi, (m, cue) => `${cue}[order number]`);
}

// Names: keep a person's first name (the greeting check needs it), drop the rest.
// `names` are full names known from the conversation (customer author names).
export function scrubNames(s, names = []) {
  let t = String(s || "");
  for (const full of names) {
    const tokens = String(full || "").trim().split(/\s+/).filter(Boolean);
    if (tokens.length < 2) continue;
    for (const tok of tokens.slice(1)) {
      if (tok.length < 3) continue;
      const esc = tok.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      t = t.replace(new RegExp(`\\b${esc}\\b`, "gi"), "[surname]");
    }
  }
  // Contact-form style "Name: First Last" (Shopify contact form, signatures).
  t = t.replace(/^(\s*(?:full\s+)?name\s*:\s*)([A-Za-z'’-]+)((?:\s+[A-Za-z'’-]+)+)\s*$/gim, (m, cue, first) => `${cue}${first} [surname]`);
  return t;
}

export function scrubForAudit(s, { names = [] } = {}) {
  let t = scrubOrderNumbers(String(s || ""));
  t = scrubCodes(t);
  t = scrubNames(t, names);
  return cxScrubPII(t);
}
