#!/usr/bin/env node
// build_seed.mjs — regenerate the Medical Supplies module's D1 schema + seed
// from the 4 AppSheet CSV exports ("Medical Supplies NEW - *.csv").
//
// Usage:  node tools/med-supplies/build_seed.mjs [csv-dir] [out-dir]
//         defaults: csv-dir=~/Downloads, out-dir=tools/med-supplies/out
//
// Outputs: schema.sql, seed.sql (rerunnable: DELETEs then INSERTs),
//          image_map.csv (drive filename -> R2 key), and a printed report.
//
// Item IDs are TEXT: the AppSheet export mixes numeric ids ("100") with
// 8-hex hash ids ("94fcbb3b") — both are real items and both appear as
// refs in the usage log. Coercing to INTEGER drops 78 items; don't.
//
// Legacy usage-log rows are imported with counted=0: the export's Office Qty
// is ALREADY net of past checkouts (its "Checkout Out" column is 0 for every
// item and Remaining == Office Qty), so counting legacy rows against stock
// would double-subtract. Only moves made in the new app (counted=1) affect
// remaining stock.

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

const SRC = process.argv[2] || join(homedir(), "Downloads");
const OUT = process.argv[3] || join(import.meta.dirname, "out");
mkdirSync(OUT, { recursive: true });

// ---------- RFC-4180 CSV parser (fields contain commas, quotes, newlines) ----------
function parseCSV(text) {
  const rows = [];
  let row = [], field = "", inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += c;
    } else if (c === '"') inQuotes = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); field = "";
      rows.push(row); row = [];
    } else field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows;
}
function readCsv(name) {
  const text = readFileSync(join(SRC, name), "utf-8").replace(/^﻿/, "");
  const [header, ...rows] = parseCSV(text);
  return rows
    .filter((r) => r.some((v) => (v || "").trim() !== ""))
    .map((r) => Object.fromEntries(header.map((h, i) => [h, (r[i] || "").trim()])));
}

const q = (v) => (v === null || v === undefined || v === "" ? "NULL" : `'${String(v).replace(/'/g, "''")}'`);
const n = (v) => (v === null || v === undefined || v === "" ? "NULL" : String(v));
const int = (v) => {
  const m = String(v ?? "").trim().match(/^-?\d+(\.\d+)?$/);
  return m ? Math.trunc(parseFloat(v)) : null;
};

// ---------- Category icons (translated from the AppSheet FontAwesome format rules) ----------
const CATEGORY_ICONS = {
  "Medications": "💊", "Cardiac": "🫀", "Clinical & PPE": "🧑‍⚕️",
  "Diabetic Monitoring": "📈", "Fluid & Blood Products": "🩸",
  "GI/NG Tubes & Suction": "🧪", "IV Supplies / Blood Draw": "💧",
  "Misc.": "🏥", "Mother Baby": "👶", "Needles": "💉", "Organs": "🫘",
  "Ostomies & Stoma": "🩹", "Oxygenation": "🫁", "Patient Care": "🛏️",
  "Trach": "⚕️", "Urinary / Catheters / Foley": "🚻", "Vital Signs": "💓",
  "DIY Simulation": "🛠️", "Meal/Dietary": "🍽️",
};
// "diana" -> "Diana", "Molly Savitzky" -> "Molly" — first name, capitalized
const normUser = (u) => {
  const first = (u || "").trim().split(/\s+/)[0];
  return first ? first[0].toUpperCase() + first.slice(1).toLowerCase() : null;
};

const report = [];

// ---------- Categories ----------
const catRows = readCsv("Medical Supplies NEW - Categories.csv");
const categories = catRows.map((r) => r["Category Name"]).filter(Boolean);
report.push(`categories: ${categories.length}`);

// ---------- Inventory items ----------
const invRows = readCsv("Medical Supplies NEW - Inventory.csv");
const items = [];
const imageMap = []; // { drive_filename, r2_key, content_type, item_id, slot }
const CT = { png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg", gif: "image/gif", webp: "image/webp", heic: "image/heic" };

function imageKey(appsheetPath, itemId, slot) {
  const p = (appsheetPath || "").trim();
  if (!p) return null;
  const m = p.match(/^Inventory_Images\/(.+?)\.(Primary|Alt) Image\.\d+\.(\w+)$/);
  const ext = (m ? m[3] : (p.split(".").pop() || "png")).toLowerCase();
  const key = `items/${itemId}-${slot}.${ext}`;
  imageMap.push({
    drive_filename: p.replace(/^Inventory_Images\//, ""),
    r2_key: key,
    content_type: CT[ext] || "application/octet-stream",
    item_id: itemId,
    slot,
  });
  return key;
}

for (const r of invRows) {
  const id = r[""];
  if (!id) continue; // header profiling showed all no-id rows are fully empty
  const needToOrder = int(r["Need To Order"]);
  let status = { "To Order": "to_order", "Ordered": "ordered" }[r["Order Status"]] || "none";
  if (status === "none" && needToOrder > 0) status = "to_order";
  if (!r["Item Name"]) report.push(`  ⚠ item ${id} (${r["Category"] || "no category"}) has no name — seeded as "(unnamed)", rename in the app`);
  items.push({
    id,
    name: r["Item Name"] || "(unnamed)",
    category: r["Category"] || null,
    dose: r["Dose / Strength"] || null,
    size: r["Size / Volume"] || null,
    variant: r["Model / Variant"] || null,
    sublocation: r["Sublocation"] || null,
    office_qty: int(r["Office Qty"]), // blank -> NULL = "never counted"
    purchase_link: r["Purchase Link"] || null,
    primary_image_key: imageKey(r["Primary Image"], id, "primary"),
    alt_image_key: imageKey(r["Alt Image"], id, "alt"),
    notes: r["Notes"] || null,
    order_status: status,
    qty_to_order: needToOrder > 0 ? needToOrder : null,
    restock_level: int(r["Restock Level"]),
    simulated_label: r["Simulated Label"] || null,
    shared_supply: r["Shared Supply"] === "TRUE" ? 1 : r["Shared Supply"] === "FALSE" ? 0 : null,
  });
}
if (items.length !== 415) throw new Error(`expected 415 items, got ${items.length}`);
const itemIds = new Set(items.map((i) => i.id));
const hashIds = items.filter((i) => !/^\d+$/.test(i.id)).length;
report.push(`items: ${items.length} (${hashIds} hash-id, ${items.length - hashIds} numeric); ` +
  `uncounted office_qty: ${items.filter((i) => i.office_qty === null).length}; ` +
  `image refs: ${imageMap.length}`);

// ---------- Usage log -> moves ----------
const logRows = readCsv("Medical Supplies NEW - Usage_Log.csv");
const moves = [];
let linked = 0, orphan = 0, blankRef = 0, qtyDefaulted = 0;
for (const r of logRows) {
  const logId = r["Log ID"];
  if (!logId) continue;
  const ref = r["Item Name"];
  const itemId = itemIds.has(ref) ? ref : null;
  if (itemId) linked++; else if (ref) orphan++; else blankRef++;
  let qty = int(r["Qty Taken"]);
  if (qty === null) { qty = 1; qtyDefaulted++; }
  const tsMatch = (r["Timestamp"] || "").match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  const ts = tsMatch
    ? `${tsMatch[3]}-${tsMatch[1].padStart(2, "0")}-${tsMatch[2].padStart(2, "0")} ` +
      `${tsMatch[4].padStart(2, "0")}:${tsMatch[5]}:${tsMatch[6] || "00"}`
    : null;
  moves.push({
    id: logId,
    item_id: itemId,
    raw_item_ref: itemId ? null : ref || null,
    type: "checkout",
    qty_delta: -qty,
    user_name: normUser(r["User"]),
    video_name: r["Video Name"] || null,
    note: null,
    created_at: ts,
  });
}
if (moves.length !== 308) throw new Error(`expected 308 moves, got ${moves.length}`);
report.push(`moves: ${moves.length} (linked ${linked}, orphan ${orphan}, blank-ref ${blankRef}, qty defaulted to 1: ${qtyDefaulted})`);

// ---------- Videos (CSV names ∪ names only in the usage log — don't drop any) ----------
const vidRows = readCsv("Medical Supplies NEW - Videos.csv");
const videos = new Set(vidRows.map((r) => r["Video Name"]).filter(Boolean));
const fromCsv = videos.size;
for (const m of moves) if (m.video_name) videos.add(m.video_name);
report.push(`videos: ${videos.size} (${fromCsv} from Videos.csv, ${videos.size - fromCsv} only in usage log)`);

// ---------- schema.sql ----------
// NOTE: this DDL is duplicated as a CREATE TABLE IF NOT EXISTS safety net at the
// top of handleMedSuppliesAPI in src/index.js — keep the two in sync.
const schema = `-- Medical Supplies module — tables live in the shared content-calendar D1 (binding DB).
-- Remaining stock is NEVER stored: remaining = COALESCE(office_qty,0) + SUM(med_moves.qty_delta WHERE counted=1).
-- (Imported AppSheet history has counted=0 — the export's Office Qty already reflects it.)
-- Kept in sync with the lazy CREATE TABLE block in src/index.js handleMedSuppliesAPI.

CREATE TABLE IF NOT EXISTS med_items (
  id TEXT PRIMARY KEY,                 -- AppSheet id ("100" or "94fcbb3b") or UUID for new items
  name TEXT NOT NULL,
  category TEXT,
  dose TEXT,
  size TEXT,
  variant TEXT,
  sublocation TEXT,
  office_qty INTEGER,                  -- imported baseline; NULL = never counted; immutable (recount = adjust move)
  purchase_link TEXT,
  primary_image_key TEXT,              -- R2 key like items/100-primary.png (served via Worker)
  alt_image_key TEXT,
  notes TEXT,
  order_status TEXT NOT NULL DEFAULT 'none',   -- none | to_order | ordered
  qty_to_order INTEGER,
  restock_level INTEGER,
  simulated_label TEXT,
  shared_supply INTEGER,               -- 1 / 0 / NULL(unknown)
  archived INTEGER NOT NULL DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS med_moves (
  id TEXT PRIMARY KEY,                 -- legacy Log ID or UUID
  item_id TEXT,                        -- NULL for legacy rows whose item was deleted
  raw_item_ref TEXT,                   -- original ref preserved when item_id is NULL
  type TEXT NOT NULL,                  -- checkout | receive | adjust
  qty_delta INTEGER NOT NULL,          -- checkout negative, receive positive, adjust signed
  counted INTEGER NOT NULL DEFAULT 1,  -- 0 = imported AppSheet history (already reflected in office_qty)
  user_name TEXT,
  video_name TEXT,
  note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_med_moves_item ON med_moves(item_id);
CREATE INDEX IF NOT EXISTS idx_med_moves_created ON med_moves(created_at DESC);

CREATE TABLE IF NOT EXISTS med_videos (
  name TEXT PRIMARY KEY,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS med_categories (
  name TEXT PRIMARY KEY,
  icon TEXT,
  sort_order INTEGER DEFAULT 100
);

-- Sim-lab packing: each video has a "bag"; supplies are assigned per video.
-- Packing a line records a checkout move (packed_move_id); returning the bag
-- after filming records 'return' moves for what came back, so consumption =
-- packed - returned and stock stays true without recounting.
CREATE TABLE IF NOT EXISTS med_bags (
  video_name TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active',   -- active | done (returned after filming)
  created_at TEXT DEFAULT (datetime('now')),
  done_at TEXT
);

CREATE TABLE IF NOT EXISTS med_bag_items (
  video_name TEXT NOT NULL,
  item_id TEXT NOT NULL,
  qty_needed INTEGER NOT NULL DEFAULT 1,
  packed_move_id TEXT,                 -- NULL = not packed yet
  qty_returned INTEGER,                -- set when the bag is returned
  return_move_id TEXT,                 -- 'return' move restocking what came back
  note TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (video_name, item_id)
);
`;

// ---------- seed.sql (rerunnable; no BEGIN/COMMIT — wrangler d1 execute manages its own txn) ----------
const seed = [
  "-- Generated by tools/med-supplies/build_seed.mjs — do not hand-edit.",
  "-- Full reset: wipes bags too. Never run against a live DB with real activity.",
  "DELETE FROM med_bag_items;",
  "DELETE FROM med_bags;",
  "DELETE FROM med_moves;",
  "DELETE FROM med_items;",
  "DELETE FROM med_videos;",
  "DELETE FROM med_categories;",
];
function batchInsert(table, cols, rows, toValues, batchSize = 40) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const vals = rows.slice(i, i + batchSize).map(toValues).join(",\n");
    seed.push(`INSERT INTO ${table} (${cols}) VALUES\n${vals};`);
  }
}
batchInsert("med_categories", "name, icon, sort_order", categories,
  (c) => `(${q(c)}, ${q(CATEGORY_ICONS[c] || "📦")}, ${categories.indexOf(c) * 10})`);
batchInsert("med_videos", "name", [...videos], (v) => `(${q(v)})`);
batchInsert("med_items",
  "id, name, category, dose, size, variant, sublocation, office_qty, purchase_link, " +
  "primary_image_key, alt_image_key, notes, order_status, qty_to_order, restock_level, " +
  "simulated_label, shared_supply",
  items,
  (i) => `(${q(i.id)}, ${q(i.name)}, ${q(i.category)}, ${q(i.dose)}, ${q(i.size)}, ` +
    `${q(i.variant)}, ${q(i.sublocation)}, ${n(i.office_qty)}, ${q(i.purchase_link)}, ` +
    `${q(i.primary_image_key)}, ${q(i.alt_image_key)}, ${q(i.notes)}, ${q(i.order_status)}, ` +
    `${n(i.qty_to_order)}, ${n(i.restock_level)}, ${q(i.simulated_label)}, ${n(i.shared_supply)})`);
batchInsert("med_moves",
  "id, item_id, raw_item_ref, type, qty_delta, counted, user_name, video_name, note, created_at",
  moves,
  (m) => `(${q(m.id)}, ${q(m.item_id)}, ${q(m.raw_item_ref)}, ${q(m.type)}, ${m.qty_delta}, 0, ` +
    `${q(m.user_name)}, ${q(m.video_name)}, ${q(m.note)}, ${m.created_at ? q(m.created_at) : "datetime('now')"})`);

// ---------- image_map.csv ----------
const mapCsv = ["drive_filename,r2_key,content_type,item_id,slot"];
for (const r of imageMap) {
  mapCsv.push([r.drive_filename, r.r2_key, r.content_type, r.item_id, r.slot]
    .map((v) => (/[",]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v)).join(","));
}

// ---------- remaining-stock check vs CSV ----------
// Legacy moves are counted=0, so at import time remaining == office_qty for every
// item. Verify that against the CSV's Remaining Stock column (blank ≡ 0/blank noise).
let stockMismatch = 0, blankFormula = 0;
for (const i of items) {
  const csvVal = int(invRows.find((r) => r[""] === i.id)["Remaining Stock"]);
  if (csvVal === null) { if (i.office_qty !== null) blankFormula++; continue; } // formula cell blank on newest rows — Office Qty is authoritative
  if ((i.office_qty ?? 0) !== csvVal) {
    stockMismatch++;
    report.push(`  ✗ remaining mismatch ${i.id} "${i.name}": office_qty ${i.office_qty}, CSV Remaining "${csvVal}"`);
  }
}
report.push(`remaining-stock check vs CSV: ${items.length - stockMismatch}/${items.length} match` +
  ` (${blankFormula} rows with blank Remaining formula, Office Qty used)` +
  (stockMismatch ? ` — ${stockMismatch} MISMATCHES above` : " ✓"));

writeFileSync(join(OUT, "schema.sql"), schema);
writeFileSync(join(OUT, "seed.sql"), seed.join("\n\n") + "\n");
writeFileSync(join(OUT, "image_map.csv"), mapCsv.join("\n") + "\n");
console.log(report.join("\n"));
console.log(`\nwrote ${OUT}/{schema.sql, seed.sql, image_map.csv}`);
