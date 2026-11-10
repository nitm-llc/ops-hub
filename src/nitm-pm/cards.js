// ============================================================================
// FLO — the morning summary as a Google Chat card.
//
// The words are exactly what Flo wrote; this only lays them out:
//   * a header ("Daily check" / "Mon, Nov 9")
//   * one section per paragraph, titled by its lead-in ("Proposals",
//     "Messaged", "Left alone")
//   * cards are grouped under their Stage (a square in ClickUp's own Stage
//     color), so each Stage is said once; each row opens its card
//   * no proposal numbers: people approve by name ("approve Spot the Med"),
//     or "approve all"; approvals.js matches the names in code
//   * long lists fold after a few rows
// If Chat rejects the card, the scheduler falls back to the plain text.
// ============================================================================

import { clickup } from "./clickup.js";
import { FIELDS } from "./config.js";

const CARD_LINK = /<(https:\/\/app\.clickup\.com\/t\/([A-Za-z0-9_-]+))\|([^>]+)>/;
const MAX_LOOKUPS = 25;
const FOLD_AFTER = 5;

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/** Chat's "#fff" style colors as #RRGGBB; white/blank shown as grey so it's visible. */
export function stageColor(c) {
  let h = String(c || "").trim().toLowerCase();
  if (/^#[0-9a-f]{3}$/.test(h)) h = "#" + [...h.slice(1)].map((x) => x + x).join("");
  if (!/^#[0-9a-f]{6}$/.test(h) || h === "#ffffff") return "#9e9e9e";
  return h;
}

/** Chat text markup (*bold*, _italic_, <url|label>) → card HTML. */
export function toCardHtml(line) {
  const links = [];
  let t = String(line).replace(/<(https?:\/\/[^|>\s]+)(?:\|([^>]+))?>/g, (_, url, label) => {
    links.push(`<a href="${esc(url)}">${esc(label || url)}</a>`);
    return `\u0000${links.length - 1}\u0000`;
  });
  t = esc(t)
    .replace(/^- /, "• ")
    .replace(/\*([^*\n]+)\*/g, "<b>$1</b>")
    .replace(/(^|\s)_([^_\n]+)_(?=\s|$|[.,;:!?])/g, "$1<i>$2</i>");
  return t.replace(/\u0000(\d+)\u0000/g, (_, i) => links[Number(i)]);
}

/** Stage name + color for each linked card, read live from ClickUp. Misses are skipped. */
export async function stageInfo(env, text) {
  const ids = [...new Set([...String(text).matchAll(new RegExp(CARD_LINK, "g"))].map((m) => m[2]))].slice(0, MAX_LOOKUPS);
  const out = new Map();
  await Promise.all(
    ids.map(async (id) => {
      try {
        const task = await clickup(env, "GET", `/task/${id}`);
        const f = (task.custom_fields || []).find((x) => x.id === FIELDS.stage);
        const v = f?.value;
        if (v === undefined || v === null || v === "") return;
        const opts = f.type_config?.options || [];
        const opt = opts.find((o) => o.id === v) || opts.find((o) => o.orderindex != null && String(o.orderindex) === String(v));
        if (opt) out.set(id, { stage: String(opt.name).trim(), color: stageColor(opt.color) });
      } catch {
        // No Stage tag for this row; the row still shows.
      }
    })
  );
  return out;
}

// "*Proposals* (reply …):", "Messaged: Gaby …", "Left alone: …", "Cards left alone:"
function splitHeading(line) {
  let m = line.match(/^\*([^*#][^*]{0,39})\*\s*(.*)$/);
  if (m) return { header: m[1].replace(/:\s*$/, "").trim(), rest: m[2].replace(/^:\s*/, "").trim() };
  m = line.match(/^([A-Z][A-Za-z ]{1,25}?)\s*(\([^)]*\))?:\s*(.*)$/);
  if (m) return { header: m[1].trim(), rest: [m[2], m[3]].filter(Boolean).join(" ").trim() };
  return { header: null, rest: line };
}

/**
 * The summary as a cardsV2 message body.
 *   title     "Daily check"
 *   subtitle  "Mon, Nov 9" (plus the copy note on Tia's copy)
 *   body      Flo's reply, unchanged
 *   stages    Map from stageInfo()
 */
export function summaryCard({ title, subtitle, body, stages = new Map(), pageUrl, approveNames = null }) {
  const sections = [];
  const proposed = []; // card names with numbered changes, for the reply hint (older summaries)
  for (const block of String(body || "").trim().split(/\n\s*\n/)) {
    const lines = block.split("\n").map((l) => l.trimEnd()).filter((l) => l.trim());
    if (!lines.length) continue;
    let { header, rest } = splitHeading(lines[0]);
    // Flo's "(reply "approve 16 to 29", or pick numbers):" and "Reply "yes" or
    // "approve 1 and 3"…" lines are about numbers, which the card doesn't
    // show; a name-based hint replaces them.
    if (/^\(.*\):?$/.test(rest)) rest = "";
    const before = [];
    const after = [];
    const groups = new Map(); // stage → { color, rows: [] }, in order seen
    const loose = [];
    let numbered = false;
    for (let line of [rest, ...lines.slice(1)].filter(Boolean)) {
      if (APPROVE_HINT.test(line)) continue;
      // "- *#30* <card>…" / "• #30 <card>…": drop the bullet and any leading numbers.
      line = line.replace(/^\s*[-•]\s+/, "").replace(/^\*?(#\d+(?:\s*(?:,|–|-|and)\s*#?\d+)*)\*?\s+(?=<)/, (m0, nums) => `*${nums}* `);
      const link = line.match(CARD_LINK);
      if (!link) {
        (groups.size || loose.length ? after : before).push(toCardHtml(line));
        continue;
      }
      // The card's words without the proposal numbers. The whole row opens the card.
      const lead = line.match(/^\*([^*]+)\*\s*/);
      if (lead && /#\d/.test(lead[1])) {
        numbered = true;
        proposed.push(link[3]);
      }
      const words = lead && /#\d/.test(lead[1]) ? line.slice(lead[0].length) : line;
      const row = { decoratedText: { text: toCardHtml(words), wrapText: true, onClick: { openLink: { url: link[1] } } } };
      const s = stages.get(link[2]);
      if (!s) loose.push(row);
      else {
        if (!groups.has(s.stage)) groups.set(s.stage, { color: s.color, rows: [] });
        groups.get(s.stage).rows.push(row);
      }
    }
    const widgets = [];
    if (before.length) widgets.push({ textParagraph: { text: before.join("<br>") } });
    // Cards grouped under their Stage, so each Stage is said once.
    for (const [stage, g] of groups) {
      widgets.push({ textParagraph: { text: `<font color="${g.color}">■</font> <b>${esc(stage)}</b>` } });
      widgets.push(...g.rows);
    }
    widgets.push(...loose);
    if (after.length) widgets.push({ textParagraph: { text: after.join("<br>") } });
    if (numbered && !approveNames) widgets.push({ textParagraph: { text: "{{HINT}}" } });
    if (!widgets.length) widgets.push({ textParagraph: { text: "—" } });
    const section = header ? { header: esc(header), widgets } : { widgets };
    // Long lists fold; "Show more" opens the rest.
    if (widgets.length > FOLD_AFTER + 2) Object.assign(section, { collapsible: true, uncollapsibleWidgetsCount: FOLD_AFTER + 1 });
    sections.push(section);
  }
  // The hint names cards really waiting for approval (today's run), or, for
  // older summaries, the cards that had numbered proposals.
  if (approveNames?.length) sections.push({ widgets: [{ textParagraph: { text: "{{HINT}}" } }] });
  const names = [...new Set(approveNames?.length ? approveNames : proposed)].slice(0, 2).map(shortName);
  const hint = `<font color="#80868b">Reply "approve all", or name the cards${names.length ? `: "approve ${esc(names.join(" and "))}"` : ""}</font>`;
  for (const sec of sections) for (const w of sec.widgets) if (w.textParagraph?.text === "{{HINT}}") w.textParagraph.text = hint;
  if (pageUrl) {
    sections.push({ widgets: [{ buttonList: { buttons: [{ text: "Open Flo page", onClick: { openLink: { url: pageUrl } } }] } }] });
  }
  return {
    fallbackText: `${title}, ${subtitle}`,
    cardsV2: [{ cardId: "flo-summary", card: { header: { title: esc(title), subtitle: esc(subtitle) }, sections } }],
  };
}

// "Don't make these sterile field mistakes" → "Don't make these sterile": enough to match.
const shortName = (n) => String(n).split(/\s+/).slice(0, 3).join(" ").replace(/[:,.]$/, "");
const APPROVE_HINT = /^reply\b.*\bapprove\s+#?\d/i;
