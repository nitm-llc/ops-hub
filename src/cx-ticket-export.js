// Pure pieces of the CX agent's Zendesk ticket export: the search query for a date range,
// the return-pattern analysis prompt and how its answer is normalized, the summary rollup,
// and CSV. Kept out of index.js so they can be tested — the Worker routes that call
// Zendesk, Claude and D1 live in index.js.

const DAY = /^\d{4}-\d{2}-\d{2}$/;

// Validated, inclusive YYYY-MM-DD range, plus the UTC bounds used to filter stored rows.
export function zdDateRange(start, end) {
  if (!DAY.test(start || '') || !DAY.test(end || '')) throw new Error('start and end must be YYYY-MM-DD');
  if (start > end) throw new Error('start must be on or before end');
  const next = new Date(end + 'T00:00:00Z');
  next.setUTCDate(next.getUTCDate() + 1);
  return { start, end, from: start + 'T00:00:00Z', to: next.toISOString().slice(0, 10) + 'T00:00:00Z' };
}

// Zendesk search syntax; date-only values cover the whole day, both ends inclusive.
export function zdSearchQuery(range) {
  return `type:ticket created>=${range.start} created<=${range.end}`;
}

// What a ticket is about, from a returns point of view. The model picks exactly one.
export const ZD_CATEGORIES = {
  return_request:  'Wants to send an item back',
  exchange:        'Wants a different item, edition or size instead',
  damaged:         'Item arrived damaged',
  wrong_item:      'Received the wrong item',
  missing_item:    'Item missing from the order',
  defective:       'Product defect or quality problem (printing, binding, pages, broken parts)',
  not_as_expected: "Product isn't what they expected, isn't useful, or was ordered by mistake",
  refund_request:  'Wants money back, no return mentioned',
  cancellation:    'Wants to cancel an order',
  lost_or_late:    'Package lost, late, or tracking stuck',
  digital_access:  'Ebook code, login or digital access',
  other:           'Anything else',
};
// Counted as "return-related" in the summary. Decided here, not by the model.
export const ZD_RETURN_CATEGORIES = new Set([
  'return_request', 'exchange', 'damaged', 'wrong_item', 'missing_item',
  'defective', 'not_as_expected', 'refund_request', 'cancellation',
]);
export const ZD_RESOLUTIONS = [
  'refunded', 'partial_refund', 'replacement_sent', 'return_label_sent', 'store_credit',
  'denied', 'no_action_needed', 'unresolved', 'unknown',
];

export function zdAnalysisPrompt(productNames = []) {
  const cats = Object.entries(ZD_CATEGORIES).map(([k, v]) => `- ${k}: ${v}`).join('\n');
  const products = productNames.length
    ? `Known product names — use one of these exactly when it matches:\n${productNames.map(n => `- ${n}`).join('\n')}\nIf the product isn't on the list, give a short plain name for it.`
    : 'Give a short plain name for the product.';
  return `You review customer support tickets for Nurse In The Making, a company that sells nursing study products (printed books, flashcards, clinical cards, bundles, ebooks). Your job is to tag each ticket so the team can spot patterns in returns, refunds and product problems.

Pick exactly ONE category for the customer's main issue:
${cats}

${products}
Use null for product when no specific product is mentioned.

resolution — what the team ended up doing, from the conversation: ${ZD_RESOLUTIONS.join(', ')}. Use "unknown" if the thread doesn't say.

Respond with only this JSON:
{"category": "...", "product": "..." or null, "reason": "the customer's stated reason in under 12 words" or null, "resolution": "...", "summary": "one sentence describing the ticket"}`;
}

export function zdAnalysisUserPrompt(row) {
  return `Subject: ${row.subject || '(none)'}\nChannel: ${row.channel || 'unknown'}\nTags: ${row.tags || ''}\n\nConversation:\n${String(row.transcript || row.description || '').slice(0, 6000)}`;
}

// Never trust the model's labels as-is: unknown values fall back to safe defaults.
export function zdNormalizeAnalysis(parsed) {
  const p = parsed || {};
  const category = Object.hasOwn(ZD_CATEGORIES, p.category) ? p.category : 'other';
  const resolution = ZD_RESOLUTIONS.includes(p.resolution) ? p.resolution : 'unknown';
  const clean = (v, max) => (typeof v === 'string' && v.trim() && v.trim().toLowerCase() !== 'null') ? v.trim().slice(0, max) : null;
  return {
    category,
    is_return: ZD_RETURN_CATEGORIES.has(category) ? 1 : 0,
    product: clean(p.product, 120),
    reason: clean(p.reason, 200),
    resolution,
    summary: clean(p.summary, 400),
  };
}

// Monday of the row's week (UTC), as YYYY-MM-DD.
function weekOf(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return 'unknown';
  d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7));
  return d.toISOString().slice(0, 10);
}

const bump = (obj, key, n = 1) => { obj[key] = (obj[key] || 0) + n; };
const ranked = (obj) => Object.entries(obj).sort((a, b) => b[1] - a[1]).map(([key, count]) => ({ key, count }));

// Product names come back with small spelling differences; group on a loose key, show the
// most common spelling.
const productKey = (p) => String(p).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();

export function zdSummarize(rows) {
  const analyzed = rows.filter(r => r.category);
  const returns = analyzed.filter(r => r.is_return);
  const byCategory = {}, byResolution = {}, byChannel = {}, byWeek = {}, reasons = {};
  const products = {};
  for (const r of analyzed) {
    bump(byCategory, r.category);
    const w = weekOf(r.created_at);
    byWeek[w] = byWeek[w] || { week: w, total: 0, returns: 0 };
    byWeek[w].total++;
    if (r.is_return) byWeek[w].returns++;
    if (r.product) {
      const k = productKey(r.product);
      const p = products[k] = products[k] || { names: {}, total: 0, returns: 0, categories: {} };
      bump(p.names, r.product);
      p.total++;
      if (r.is_return) { p.returns++; bump(p.categories, r.category); }
    }
  }
  for (const r of returns) {
    bump(byResolution, r.resolution || 'unknown');
    bump(byChannel, r.channel || 'unknown');
    if (r.reason) bump(reasons, r.reason.toLowerCase().replace(/[.!]+$/, ''));
  }
  return {
    total: rows.length,
    analyzed: analyzed.length,
    returns: returns.length,
    by_category: ranked(byCategory),
    return_resolutions: ranked(byResolution),
    return_channels: ranked(byChannel),
    by_week: Object.values(byWeek).sort((a, b) => a.week.localeCompare(b.week)),
    products: Object.values(products)
      .map(p => ({ name: ranked(p.names)[0].key, total: p.total, returns: p.returns, categories: ranked(p.categories) }))
      .sort((a, b) => b.returns - a.returns || b.total - a.total),
    top_reasons: ranked(reasons).slice(0, 25),
  };
}

export function zdToCsv(rows, columns) {
  const cell = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\r\n]/.test(s) ? '"' + s.replaceAll('"', '""') + '"' : s;
  };
  return [columns.join(','), ...rows.map(r => columns.map(c => cell(r[c])).join(','))].join('\r\n');
}
