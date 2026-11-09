// ============================================================================
// CX SCORECARD AUDIT — defaults and time windows
// ----------------------------------------------------------------------------
// DEFAULT_EXCLUSIONS seeds the editable exclusion list. From Phase 3 it lives
// in D1 (cxa_exclusions) and is edited on the /cx-audit/ page; this array is
// only the starting point and the fallback when the table is empty.
//
// A rule matches the conversation's real sender (for Shopify contact forms, the
// customer in reply_to, not mailer@shopify.com) or its subject:
//   { kind: "domain",  value: "docusign.net" }   also matches subdomains
//   { kind: "email",   value: "someone@vendor.com" }
//   { kind: "subject", value: "regex, case-insensitive" }
//   { kind: "issue_type", value: "Collaboration / Donation" }   exact match
//
// Deliberately NOT here: shopify.com (contact forms are real customers) and
// marketplace.amazon.com (Amazon buyer messages are real customers).
// ============================================================================

export const DEFAULT_EXCLUSIONS = [
  { kind: "domain", value: "docusign.net", note: "DocuSign envelopes" },
  { kind: "domain", value: "docusign.com", note: "DocuSign" },
  { kind: "domain", value: "business-updates.facebook.com", note: "Meta business notices" },
  { kind: "domain", value: "notifications.tiktok.com", note: "TikTok notifications" },
  { kind: "domain", value: "email.manychat.com", note: "ManyChat newsletter" },
  { kind: "domain", value: "news.paypal.com", note: "PayPal newsletter" },
  { kind: "domain", value: "em.target.com", note: "Target marketing" },
  { kind: "domain", value: "rakuten.com", note: "Rakuten marketing" },
  { kind: "domain", value: "avalara.com", note: "Avalara (vendor)" },
  { kind: "domain", value: "anurseinthemaking.com", note: "Our own team writing in (tests)" },
  { kind: "domain", value: "wa.gov", note: "Government notices (WA)" },
  { kind: "subject", value: "\\bunsubscribe\\b|\\bnewsletter\\b|\\bwebinar\\b", note: "Bulk mail" },
  // Added 2026-11-09 after the first live run (Mark confirmed).
  { kind: "domain", value: "virtualexponent.com", note: "Fin test conversations (TEST n - ...)" },
  { kind: "domain", value: "editorder.net", note: "Cleverific order-edit notices" },
  { kind: "domain", value: "zapier.com", note: "Zapier notices" },
  { kind: "domain", value: "klaviyo.com", note: "Klaviyo digests" },
  { kind: "domain", value: "beacons.ai", note: "Beacons link-in-bio stats" },
  { kind: "domain", value: "ccsend.com", note: "Constant Contact newsletters" },
  { kind: "domain", value: "craftspring.com", note: "Marketing" },
  { kind: "domain", value: "elmbiosciences.com", note: "Marketing" },
  { kind: "domain", value: "sok-it.com", note: "Marketing" },
  { kind: "domain", value: "get-charmed.com", note: "Vendor pitch" },
  { kind: "domain", value: "channelreply.com", note: "ChannelReply invoices" },
  { kind: "domain", value: "epidemicsound.com", note: "Epidemic Sound notices" },
  { kind: "domain", value: "google.com", note: "Google Analytics / Ads notices" },
];

export const PREVIEW_EMAIL = /^preview-user\+.*@intercom\.io$/i;
export const SHOPIFY_MAILER = /^mailer@shopify\.com$/i;

export const DEFAULT_TZ = "America/New_York";
export const DEFAULT_API_BASE = "https://api.intercom.io"; // EU: https://api.eu.intercom.io, AU: https://api.au.intercom.io
export const DEFAULT_API_VERSION = "2.14";

// Offset (ms) of `tz` from UTC at instant `ms`.
function tzOffsetMs(tz, ms) {
  const p = Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone: tz, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    }).formatToParts(new Date(ms)).map((x) => [x.type, x.value])
  );
  const asUtc = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
  return asUtc - Math.floor(ms / 1000) * 1000;
}

// Unix seconds of local midnight starting `ymd` in `tz` (DST-safe).
export function localMidnight(ymd, tz = DEFAULT_TZ) {
  const [y, m, d] = ymd.split("-").map(Number);
  let guess = Date.UTC(y, m - 1, d);
  for (let i = 0; i < 2; i++) guess = Date.UTC(y, m - 1, d) - tzOffsetMs(tz, guess);
  return Math.floor(guess / 1000);
}

export function addDaysYmd(ymd, n) {
  const d = new Date(ymd + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

export function todayYmd(tz = DEFAULT_TZ, now = Date.now()) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(now));
}

// The audit window for calendar days [fromYmd .. toYmd] inclusive, local time.
export function windowFor(fromYmd, toYmd = fromYmd, tz = DEFAULT_TZ) {
  return { from: fromYmd, to: toYmd, tz, start: localMidnight(fromYmd, tz), end: localMidnight(addDaysYmd(toYmd, 1), tz) };
}

export function yesterdayWindow(tz = DEFAULT_TZ, now = Date.now()) {
  const y = addDaysYmd(todayYmd(tz, now), -1);
  return windowFor(y, y, tz);
}
