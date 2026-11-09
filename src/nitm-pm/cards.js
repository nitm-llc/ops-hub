// ============================================================================
// FLO — the morning summary as a Google Chat card.
//
// The words are exactly what Flo wrote; this only lays them out:
//   * a header ("Daily check" / "Mon, Nov 9")
//   * one section per paragraph, titled by its lead-in ("Proposals",
//     "Messaged", "Left alone")
//   * every line that links a ClickUp card becomes a row that opens the card,
//     with its Stage underneath in ClickUp's own Stage color
// Approving is unchanged: people still type "approve 1 and 3".
// If Chat rejects the card, the scheduler falls back to the plain text.
// ============================================================================

import { clickup } from "./clickup.js";
import { FIELDS } from "./config.js";

const CARD_LINK = /<(https:\/\/app\.clickup\.com\/t\/([A-Za-z0-9_-]+))\|([^>]+)>/;
const MAX_LOOKUPS = 25;

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
export function summaryCard({ title, subtitle, body, stages = new Map(), pageUrl }) {
  const sections = [];
  for (const block of String(body || "").trim().split(/\n\s*\n/)) {
    const lines = block.split("\n").map((l) => l.trimEnd()).filter((l) => l.trim());
    if (!lines.length) continue;
    const { header, rest } = splitHeading(lines[0]);
    const widgets = [];
    let para = [];
    const flush = () => {
      if (para.length) widgets.push({ textParagraph: { text: para.join("<br>") } });
      para = [];
    };
    for (const line of [rest, ...lines.slice(1)].filter(Boolean)) {
      const link = line.match(CARD_LINK);
      if (!link) {
        para.push(toCardHtml(line));
        continue;
      }
      flush();
      const s = stages.get(link[2]);
      const tag = s ? `<br><font color="${s.color}">■</font> ${esc(s.stage)}` : "";
      widgets.push({
        decoratedText: {
          text: toCardHtml(line) + tag,
          wrapText: true,
          button: { text: "Open", onClick: { openLink: { url: link[1] } } },
        },
      });
    }
    flush();
    if (!widgets.length) widgets.push({ textParagraph: { text: "—" } });
    sections.push(header ? { header: esc(header), widgets } : { widgets });
  }
  if (pageUrl) {
    sections.push({ widgets: [{ buttonList: { buttons: [{ text: "Open Flo page", onClick: { openLink: { url: pageUrl } } }] } }] });
  }
  return {
    fallbackText: `${title}, ${subtitle}`,
    cardsV2: [{ cardId: "flo-summary", card: { header: { title: esc(title), subtitle: esc(subtitle) }, sections } }],
  };
}
