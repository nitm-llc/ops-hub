// ===== OPS HUB WORKER — v5 with 3PL API consolidated =====
const CLICKUP_API = "https://api.clickup.com/api/v2";
const SHEET_BASE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSCO2_B3HitEVQIJE71RL357tdUPErxkhG4AdwXapyhOWtry_-czGMVg_HpZ0paQQ/pub";
const GIDS = { lists: 601447197, settings: 370298211, customFields: 1218915327, statusOptions: 1580123029, brandColors: 1121511065 };
function parseCSV(text) {
  const rows = []; let row = []; let cell = ""; let inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '"') { if (inQ && text[i+1] === '"') { cell += '"'; i++; } else inQ = !inQ; }
    else if (c === ',' && !inQ) { row.push(cell); cell = ""; }
    else if ((c === '\n' || c === '\r') && !inQ) { if (c === '\r' && text[i+1] === '\n') i++; row.push(cell); cell = ""; rows.push(row); row = []; }
    else cell += c;
  }
  if (cell || row.length) { row.push(cell); rows.push(row); }
  if (rows.length < 2) return [];
  const headers = rows[0].map(h => h.trim());
  return rows.slice(1).filter(r => r.some(c => c.trim())).map(r => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = (r[i] || "").trim(); });
    return obj;
  });
}
async function fetchSheet(gid) {
  const res = await fetch(`${SHEET_BASE}?gid=${gid}&single=true&output=csv`);
  return parseCSV(await res.text());
}
async function fetchListTasks(token, list) {
  const tasks = []; let page = 0;
  while (page < 10) {
    try {
      const res = await fetch(`${CLICKUP_API}/list/${list["List ID"]}/task?include_closed=true&subtasks=true&page=${page}`, { headers: { Authorization: token } });
      if (!res.ok) break;
      const data = await res.json(); const pageTasks = data.tasks || [];
      pageTasks.forEach(t => {
        const cf = t.custom_fields?.find(f => f.name === "Post Date"); let postDate = null;
        if (cf?.value) { const utc = new Date(parseInt(cf.value)); postDate = `${utc.getUTCFullYear()}-${String(utc.getUTCMonth()+1).padStart(2,'0')}-${String(utc.getUTCDate()).padStart(2,'0')}`; }
        tasks.push({ id: t.id, name: t.name, status: t.status?.status || "", post_date: postDate, list_id: list["List ID"], list_name: list["List Name"], brand: list.Brand, platform: list.Platform, color: list["Platform Color (Hex)"] || "#666", url: t.url || "", custom_fields: JSON.stringify(t.custom_fields || []) });
      });
      if (pageTasks.length < 100) break; page++;
    } catch { break; }
  }
  return tasks;
}
async function fullSync(env) {
  const [lists, settings, customFields, statusOptions, brandColors] = await Promise.all([
    fetchSheet(GIDS.lists), fetchSheet(GIDS.settings), fetchSheet(GIDS.customFields), fetchSheet(GIDS.statusOptions), fetchSheet(GIDS.brandColors),
  ]);
  await env.DB.batch([
    env.DB.prepare("INSERT OR REPLACE INTO config_cache (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind("lists", JSON.stringify(lists)),
    env.DB.prepare("INSERT OR REPLACE INTO config_cache (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind("settings", JSON.stringify(settings)),
    env.DB.prepare("INSERT OR REPLACE INTO config_cache (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind("customFields", JSON.stringify(customFields)),
    env.DB.prepare("INSERT OR REPLACE INTO config_cache (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind("statusOptions", JSON.stringify(statusOptions)),
    env.DB.prepare("INSERT OR REPLACE INTO config_cache (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind("brandColors", JSON.stringify(brandColors)),
  ]);
  const activeLists = lists.filter(l => (l["Active (TRUE/FALSE)"] || "").toUpperCase() === "TRUE");
  const allTasks = [];
  for (let i = 0; i < activeLists.length; i += 4) {
    const batch = activeLists.slice(i, i + 4);
    const results = await Promise.all(batch.map(list => fetchListTasks(env.CLICKUP_TOKEN, list)));
    results.forEach(t => allTasks.push(...t));
  }
  const batchSize = 50;
  for (let i = 0; i < allTasks.length; i += batchSize) {
    const batch = allTasks.slice(i, i + batchSize);
    const stmts = batch.map(t => env.DB.prepare(`INSERT OR REPLACE INTO tasks (id, name, status, post_date, list_id, list_name, brand, platform, color, url, custom_fields, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`).bind(t.id, t.name, t.status, t.post_date, t.list_id, t.list_name, t.brand, t.platform, t.color, t.url, t.custom_fields));
    await env.DB.batch(stmts);
  }
  const currentTaskIds = new Set(allTasks.map(t => t.id));
  for (const list of activeLists) {
    const { results: dbTasks } = await env.DB.prepare("SELECT id FROM tasks WHERE list_id = ?").bind(list["List ID"]).all();
    const toDelete = dbTasks.filter(t => !currentTaskIds.has(t.id)).map(t => t.id);
    for (let i = 0; i < toDelete.length; i += 50) { const batch = toDelete.slice(i, i + 50); await env.DB.batch(batch.map(id => env.DB.prepare("DELETE FROM tasks WHERE id = ?").bind(id))); }
  }
  return allTasks.length;
}
async function handleShipFusionAPI(request, env) {
  const corsHeaders = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json" };
  if (request.method === "OPTIONS") { return new Response(null, { headers: corsHeaders }); }
  if (!env.SHIPFUSION_USERNAME || !env.SHIPFUSION_PASSWORD) { return new Response(JSON.stringify({ error: "ShipFusion not configured" }), { status: 400, headers: corsHeaders }); }
  try {
    const auth = btoa(`${env.SHIPFUSION_USERNAME}:${env.SHIPFUSION_PASSWORD}`);
    const headers = { "Authorization": `Basic ${auth}`, "Content-Type": "application/json", "Accept": "application/json" };
    async function fetchAllPages(warehouse) { const allItems = []; let page = 1; let totalPages = 1; while (page <= totalPages) { const url = `https://api.shipfusion.com/v1/inventory?warehouse=${warehouse}&perPage=100&page=${page}`; const response = await fetch(url, { method: "GET", headers }); if (!response.ok) break; const data = await response.json(); totalPages = data.totalPages || 1; allItems.push(...(data.items || [])); page++; } return allItems; }
    async function fetchSkuVelocity(sku, warehouse) { try { const url = `https://api.shipfusion.com/v1/inventory/${encodeURIComponent(sku)}?warehouse=${warehouse}`; const response = await fetch(url, { method: "GET", headers }); if (!response.ok) return { velocity30: 0, velocity60: 0 }; const data = await response.json(); return { velocity30: parseFloat(data['30DayAverage'] || 0), velocity60: parseFloat(data['60DayAverage'] || 0), _rawResponse: data, _url: url }; } catch (e) { return { velocity30: 0, velocity60: 0, _error: e.message }; } }
    const [ilItems, lvItems] = await Promise.all([fetchAllPages('IL'), fetchAllPages('LV')]);
    const inventoryMap = new Map();
    for (const item of ilItems) { const sku = item.SKU || item.sku || ''; if (!sku) continue; inventoryMap.set(sku, { id: sku, sku, name: item.name || item.productName || sku, sfChicago: parseInt(item.ready||0), sfChicagoAllocated: parseInt(item.allocated||0), sfChicagoOnHand: parseInt(item.onHand||0), sfLasVegas: 0, sfLasVegasAllocated: 0, sfLasVegasOnHand: 0, velocityChicago: parseFloat(item['30DayAverage']||item['thirtyDayAverage']||item['velocity']||0), velocityVegas: 0, velocity: parseFloat(item['30DayAverage']||item['thirtyDayAverage']||item['velocity']||0), incoming: parseInt(item.incoming||0), backOrder: parseInt(item.backOrder||0), _rawFields: Object.keys(item).join(',') }); }
    for (const item of lvItems) { const sku = item.SKU || item.sku || ''; if (!sku) continue; const lvV = parseFloat(item['30DayAverage']||item['thirtyDayAverage']||item['velocity']||0); if (inventoryMap.has(sku)) { const e = inventoryMap.get(sku); e.sfLasVegas=parseInt(item.ready||0); e.sfLasVegasAllocated=parseInt(item.allocated||0); e.sfLasVegasOnHand=parseInt(item.onHand||0); e.incoming+=parseInt(item.incoming||0); e.backOrder+=parseInt(item.backOrder||0); e.velocityVegas=lvV; e.velocity=e.velocityChicago+lvV; } else { inventoryMap.set(sku, { id:sku, sku, name:item.name||item.productName||sku, sfChicago:0, sfChicagoAllocated:0, sfChicagoOnHand:0, sfLasVegas:parseInt(item.ready||0), sfLasVegasAllocated:parseInt(item.allocated||0), sfLasVegasOnHand:parseInt(item.onHand||0), velocityChicago:0, velocityVegas:lvV, velocity:lvV, incoming:parseInt(item.incoming||0), backOrder:parseInt(item.backOrder||0), _rawFields:Object.keys(item).join(',') }); } }
    const allSkus = Array.from(inventoryMap.keys());
    const velocityResults = await Promise.all(allSkus.map(sku => fetchSkuVelocity(sku, 'IL').then(vel => ({ sku, ...vel }))));
    for (const vel of velocityResults) { if (inventoryMap.has(vel.sku)) { const item = inventoryMap.get(vel.sku); item.velocity = vel.velocity30; item.velocity60 = vel.velocity60; } }
    const inventory = Array.from(inventoryMap.values()).map(item => ({ ...item, totalAvailable: item.sfChicago + item.sfLasVegas }));
    const sampleRawItem = ilItems[0] || lvItems[0] || null;
    return new Response(JSON.stringify({ success: true, source: "shipfusion", count: inventory.length, inventory, _debug: { ilCount: ilItems.length, lvCount: lvItems.length, sampleRawItem, sampleFields: sampleRawItem ? Object.keys(sampleRawItem) : [], velocityDebug: velocityResults.slice(0,3) } }), { headers: corsHeaders });
  } catch (error) { return new Response(JSON.stringify({ error: "Failed to fetch from ShipFusion", details: error.message }), { status: 500, headers: corsHeaders }); }
}

// ===== 3PL API (D1-backed) =====
async function handle3plAPI(request, env, path) {
  const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json" };
  if (request.method === "OPTIONS") return new Response(null, { headers: cors });

  // Auto-create table
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS tpl_orders (
    shipment_id TEXT PRIMARY KEY,
    order_number TEXT,
    ship_date TEXT,
    order_date TEXT,
    carrier_delivery_date TEXT,
    carrier TEXT,
    service TEXT,
    carrier_status TEXT,
    zone TEXT,
    state TEXT,
    country TEXT,
    num_products INTEGER DEFAULT 0,
    shipping_cost REAL DEFAULT 0,
    base_price REAL DEFAULT 0,
    residential_fee REAL DEFAULT 0,
    das_fee REAL DEFAULT 0,
    peak_fee REAL DEFAULT 0,
    fuel_fee REAL DEFAULT 0,
    services_cost REAL DEFAULT 0,
    packaging_cost REAL DEFAULT 0,
    first_pick_fee REAL DEFAULT 0,
    additional_pick_fee REAL DEFAULT 0,
    insert_pick_fee REAL DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now'))
  )`).run();

  // GET /3pl/api/orders — return all orders
  if (path === "/3pl/api/orders" && request.method === "GET") {
    try {
      const { results } = await env.DB.prepare("SELECT * FROM tpl_orders ORDER BY ship_date DESC").all();
      return new Response(JSON.stringify({ success: true, data: results, count: results.length }), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: cors });
    }
  }

  // POST /3pl/api/orders — upsert orders
  if (path === "/3pl/api/orders" && request.method === "POST") {
    try {
      const body = await request.json();
      const orders = body.orders || [];
      if (orders.length === 0) return new Response(JSON.stringify({ success: true, added: 0, updated: 0 }), { headers: cors });

      let added = 0, updated = 0;
      const batchSize = 25;
      for (let i = 0; i < orders.length; i += batchSize) {
        const batch = orders.slice(i, i + batchSize);
        const stmts = batch.map(o => {
          return env.DB.prepare(`INSERT INTO tpl_orders (shipment_id, order_number, ship_date, order_date, carrier_delivery_date, carrier, service, carrier_status, zone, state, country, num_products, shipping_cost, base_price, residential_fee, das_fee, peak_fee, fuel_fee, services_cost, packaging_cost, first_pick_fee, additional_pick_fee, insert_pick_fee, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(shipment_id) DO UPDATE SET
              order_number=excluded.order_number, ship_date=excluded.ship_date, order_date=excluded.order_date,
              carrier_delivery_date=excluded.carrier_delivery_date, carrier=excluded.carrier, service=excluded.service,
              carrier_status=excluded.carrier_status, zone=excluded.zone, state=excluded.state, country=excluded.country,
              num_products=excluded.num_products, shipping_cost=excluded.shipping_cost, base_price=excluded.base_price,
              residential_fee=excluded.residential_fee, das_fee=excluded.das_fee, peak_fee=excluded.peak_fee,
              fuel_fee=excluded.fuel_fee, services_cost=excluded.services_cost, packaging_cost=excluded.packaging_cost,
              first_pick_fee=excluded.first_pick_fee, additional_pick_fee=excluded.additional_pick_fee,
              insert_pick_fee=excluded.insert_pick_fee, updated_at=datetime('now')
          `).bind(
            o.shipmentId, o.orderNumber, o.shipDate, o.orderDate, o.carrierDeliveryDate,
            o.carrier, o.service, o.carrierStatus, o.zone, o.state, o.country,
            o.numProducts || 0, o.shippingCost || 0, o.basePrice || 0,
            o.residentialFee || 0, o.dasFee || 0, o.peakFee || 0, o.fuelFee || 0,
            o.servicesCost || 0, o.packagingCost || 0, o.firstPickFee || 0,
            o.additionalPickFee || 0, o.insertPickFee || 0
          );
        });
        await env.DB.batch(stmts);
        added += batch.length; // Simplified — D1 upsert doesn't easily distinguish
      }
      return new Response(JSON.stringify({ success: true, added, updated: 0 }), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: cors });
    }
  }

  // DELETE /3pl/api/orders — clear all
  if (path === "/3pl/api/orders" && request.method === "DELETE") {
    try {
      await env.DB.prepare("DELETE FROM tpl_orders").run();
      return new Response(JSON.stringify({ success: true }), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: e.message }), { status: 500, headers: cors });
    }
  }

  return new Response(JSON.stringify({ error: "Not found" }), { status: 404, headers: cors });
}

// ===== CONTENT TRACKER API (D1-backed) =====
async function initTrackerTables(db) {
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_posts (
      id INTEGER PRIMARY KEY, date TEXT, caption TEXT, media_type TEXT, ig_url TEXT,
      engagement_rate REAL DEFAULT 0, views INTEGER DEFAULT 0, likes INTEGER DEFAULT 0,
      saves INTEGER DEFAULT 0, shares INTEGER DEFAULT 0, comments INTEGER DEFAULT 0,
      auto_content_type TEXT DEFAULT ''
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_daily (
      date TEXT PRIMARY KEY, orders INTEGER DEFAULT 0, revenue REAL DEFAULT 0,
      lt_views INTEGER DEFAULT 0, lt_clicks INTEGER DEFAULT 0
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_overrides (
      post_id INTEGER PRIMARY KEY, content_type TEXT, intent TEXT,
      reviewed INTEGER DEFAULT 0, last_edited TEXT
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_sale_days (date TEXT PRIMARY KEY)`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_clickup_tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, task_id TEXT, task_name TEXT,
      content_type TEXT, UNIQUE(date, task_id)
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_settings (key TEXT PRIMARY KEY, value TEXT)`),
    db.prepare(`CREATE TABLE IF NOT EXISTS tracker_presets (
      id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, weights TEXT, caps TEXT
    )`),
  ]);
}

async function handleTrackerAPI(request, env) {
  const cors = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type", "Content-Type": "application/json" };
  if (request.method === "OPTIONS") return new Response(null, { headers: cors });

  const db = env.DB;
  await initTrackerTables(db);

  // GET — assemble the full blob from D1 tables
  if (request.method === "GET") {
    try {
      const [postsR, dailyR, overridesR, saleDaysR, clickupR, settingsR, presetsR] = await db.batch([
        db.prepare("SELECT * FROM tracker_posts ORDER BY date DESC"),
        db.prepare("SELECT * FROM tracker_daily ORDER BY date ASC"),
        db.prepare("SELECT * FROM tracker_overrides"),
        db.prepare("SELECT date FROM tracker_sale_days ORDER BY date ASC"),
        db.prepare("SELECT * FROM tracker_clickup_tasks ORDER BY date ASC"),
        db.prepare("SELECT key, value FROM tracker_settings"),
        db.prepare("SELECT name, weights, caps FROM tracker_presets ORDER BY id ASC"),
      ]);

      const posts = postsR.results || [];
      const daily = (dailyR.results || []).map(d => ({ ...d, lt_views: d.lt_views || 0, lt_clicks: d.lt_clicks || 0 }));

      // Rebuild overrides as { postId: { content_type, intent, reviewed, lastEdited } }
      const overrides = {};
      for (const row of (overridesR.results || [])) {
        overrides[row.post_id] = { content_type: row.content_type || '', intent: row.intent || '', reviewed: !!row.reviewed, lastEdited: row.last_edited || '' };
      }

      const saleDays = (saleDaysR.results || []).map(r => r.date);

      // Rebuild clickupMap as { date: [{ taskId, taskName, contentType }] }
      const clickupMap = {};
      for (const row of (clickupR.results || [])) {
        if (!clickupMap[row.date]) clickupMap[row.date] = [];
        clickupMap[row.date].push({ taskId: row.task_id, taskName: row.task_name, contentType: row.content_type });
      }

      // Rebuild settings
      const settingsMap = {};
      for (const row of (settingsR.results || [])) {
        try { settingsMap[row.key] = JSON.parse(row.value); } catch { settingsMap[row.key] = row.value; }
      }

      // Rebuild presets
      const presets = (presetsR.results || []).map(r => {
        try { return { name: r.name, weights: JSON.parse(r.weights), caps: JSON.parse(r.caps), builtIn: false }; }
        catch { return null; }
      }).filter(Boolean);

      const result = {
        posts,
        daily,
        overrides,
        saleDays,
        caps: settingsMap.caps || null,
        weights: settingsMap.weights || null,
        ltMax: settingsMap.ltMax ?? null,
        ltMid: settingsMap.ltMid ?? null,
        clickupMap,
        goals: settingsMap.goals || null,
        sheetData: settingsMap.sheetData || null,
        sheetName: settingsMap.sheetName || '',
        percentileMode: settingsMap.percentileMode || false,
        presets,
      };

      return new Response(JSON.stringify(result), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: cors });
    }
  }

  // POST — decompose the blob into D1 tables
  if (request.method === "POST") {
    try {
      const body = await request.json();
      const { posts, daily, overrides, saleDays, caps, weights, ltMax, ltMid, clickupMap, goals, sheetData, sheetName, percentileMode, presets } = body;

      // Clear all tracker tables
      await db.batch([
        db.prepare("DELETE FROM tracker_posts"),
        db.prepare("DELETE FROM tracker_daily"),
        db.prepare("DELETE FROM tracker_overrides"),
        db.prepare("DELETE FROM tracker_sale_days"),
        db.prepare("DELETE FROM tracker_clickup_tasks"),
        db.prepare("DELETE FROM tracker_presets"),
      ]);

      // Insert posts in batches of 50
      if (posts && posts.length) {
        for (let i = 0; i < posts.length; i += 50) {
          const batch = posts.slice(i, i + 50);
          await db.batch(batch.map(p =>
            db.prepare("INSERT INTO tracker_posts (id, date, caption, media_type, ig_url, engagement_rate, views, likes, saves, shares, comments, auto_content_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
              .bind(p.id, p.date, p.caption || '', p.media_type || '', p.ig_url || '', p.engagement_rate || 0, p.views || 0, p.likes || 0, p.saves || 0, p.shares || 0, p.comments || 0, p.auto_content_type || '')
          ));
        }
      }

      // Insert daily in batches of 50
      if (daily && daily.length) {
        for (let i = 0; i < daily.length; i += 50) {
          const batch = daily.slice(i, i + 50);
          await db.batch(batch.map(d =>
            db.prepare("INSERT INTO tracker_daily (date, orders, revenue, lt_views, lt_clicks) VALUES (?, ?, ?, ?, ?)")
              .bind(d.date, d.orders || 0, d.revenue || 0, d.lt_views || 0, d.lt_clicks || 0)
          ));
        }
      }

      // Insert overrides
      if (overrides && Object.keys(overrides).length) {
        const ovEntries = Object.entries(overrides);
        for (let i = 0; i < ovEntries.length; i += 50) {
          const batch = ovEntries.slice(i, i + 50);
          await db.batch(batch.map(([pid, ov]) =>
            db.prepare("INSERT INTO tracker_overrides (post_id, content_type, intent, reviewed, last_edited) VALUES (?, ?, ?, ?, ?)")
              .bind(parseInt(pid), ov.content_type || '', ov.intent || '', ov.reviewed ? 1 : 0, ov.lastEdited || ov.last_edited || '')
          ));
        }
      }

      // Insert sale days
      if (saleDays && saleDays.length) {
        for (let i = 0; i < saleDays.length; i += 50) {
          const batch = saleDays.slice(i, i + 50);
          await db.batch(batch.map(d =>
            db.prepare("INSERT INTO tracker_sale_days (date) VALUES (?)").bind(d)
          ));
        }
      }

      // Insert clickup tasks
      if (clickupMap && Object.keys(clickupMap).length) {
        const cuRows = [];
        for (const [date, tasks] of Object.entries(clickupMap)) {
          for (const t of tasks) {
            cuRows.push({ date, task_id: t.taskId, task_name: t.taskName, content_type: t.contentType });
          }
        }
        for (let i = 0; i < cuRows.length; i += 50) {
          const batch = cuRows.slice(i, i + 50);
          await db.batch(batch.map(r =>
            db.prepare("INSERT OR IGNORE INTO tracker_clickup_tasks (date, task_id, task_name, content_type) VALUES (?, ?, ?, ?)")
              .bind(r.date, r.task_id, r.task_name, r.content_type)
          ));
        }
      }

      // Upsert settings
      const settingsStmts = [];
      if (caps) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('caps', JSON.stringify(caps)));
      if (weights) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('weights', JSON.stringify(weights)));
      if (ltMax !== undefined && ltMax !== null) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('ltMax', JSON.stringify(ltMax)));
      if (ltMid !== undefined && ltMid !== null) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('ltMid', JSON.stringify(ltMid)));
      if (goals) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('goals', JSON.stringify(goals)));
      if (sheetData) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('sheetData', JSON.stringify(sheetData)));
      if (sheetName !== undefined) settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('sheetName', JSON.stringify(sheetName)));
      settingsStmts.push(db.prepare("INSERT OR REPLACE INTO tracker_settings (key, value) VALUES (?, ?)").bind('percentileMode', JSON.stringify(!!percentileMode)));
      if (settingsStmts.length) await db.batch(settingsStmts);

      // Insert custom presets
      if (presets && presets.length) {
        const customPresets = presets.filter(p => !p.builtIn);
        if (customPresets.length) {
          await db.batch(customPresets.map(p =>
            db.prepare("INSERT INTO tracker_presets (name, weights, caps) VALUES (?, ?, ?)")
              .bind(p.name, JSON.stringify(p.weights), JSON.stringify(p.caps))
          ));
        }
      }

      return new Response(JSON.stringify({ ok: true }), { headers: cors });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: cors });
    }
  }

  return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers: cors });
}

// ===== MAIN WORKER =====
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // Helper: add noindex header to any response
    function addNoIndex(response) {
      const newResp = new Response(response.body, response);
      newResp.headers.set("X-Robots-Tag", "noindex, nofollow");
      return newResp;
    }

    if (request.method === "OPTIONS") { return new Response(null, { headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS", "Access-Control-Allow-Headers": "Content-Type" } }); }
    // ===== CONTENT TRACKER API (D1-backed) =====
    if (path === "/tracker/api/data") { return handleTrackerAPI(request, env); }
    // ===== 3PL API (D1-backed) =====
    if (path.startsWith("/3pl/api/")) { return handle3plAPI(request, env, path); }
    // ===== INVENTORY =====
    if (path === "/inventory/api/shipfusion") { return handleShipFusionAPI(request, env); }
    // ===== CALENDAR =====
    if (path === "/calendar/api/config") { try { const { results } = await env.DB.prepare("SELECT key, value FROM config_cache").all(); const config = {}; results.forEach(r => { try { config[r.key] = JSON.parse(r.value); } catch { config[r.key] = r.value; } }); return new Response(JSON.stringify(config), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }); } catch (err) { return new Response(JSON.stringify({ error: err.message }), { status: 500 }); } }
    if (path === "/calendar/api/tasks") { try { const { results } = await env.DB.prepare("SELECT * FROM tasks WHERE post_date IS NOT NULL ORDER BY post_date DESC").all(); const tasks = results.map(t => ({ ...t, customFields: JSON.parse(t.custom_fields || "[]") })); return new Response(JSON.stringify({ tasks, synced_at: new Date().toISOString() }), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }); } catch (err) { return new Response(JSON.stringify({ error: err.message }), { status: 500 }); } }
    if (path === "/calendar/api/sync") { try { const count = await fullSync(env); return new Response(JSON.stringify({ ok: true, tasks_synced: count }), { headers: { "Content-Type": "application/json" } }); } catch (err) { return new Response(JSON.stringify({ error: err.message }), { status: 500 }); } }
    if (path.startsWith("/calendar/api/")) { const clickupPath = path.replace("/calendar/api/", ""); const clickupUrl = `${CLICKUP_API}/${clickupPath}${url.search}`; const headers = new Headers(request.headers); headers.set("Authorization", env.CLICKUP_TOKEN); headers.set("Content-Type", "application/json"); try { const resp = await fetch(clickupUrl, { method: request.method, headers, body: request.method !== "GET" ? await request.text() : undefined }); const respHeaders = new Headers(resp.headers); respHeaders.set("Access-Control-Allow-Origin", "*"); return new Response(resp.body, { status: resp.status, headers: respHeaders }); } catch (err) { return new Response(JSON.stringify({ error: err.message }), { status: 502 }); } }
    // ===== REDIRECTS =====
    if (path === "/calendar") return Response.redirect(url.origin + "/calendar/", 301);
    if (path === "/3pl") return Response.redirect(url.origin + "/3pl/", 301);
    if (path === "/inventory") return Response.redirect(url.origin + "/inventory/", 301);
    if (path === "/ambassadors") return Response.redirect(url.origin + "/ambassadors/", 301);
    if (path === "/tracker") return Response.redirect(url.origin + "/tracker/", 301);
    // ===== LANDING PAGE =====
    if (path === "/" || path === "") { return new Response(landingPageHTML(), { headers: { "Content-Type": "text/html;charset=UTF-8", "X-Robots-Tag": "noindex, nofollow" } }); }
    // ===== AMBASSADOR API =====
    if (path === "/ambassadors/api" || path === "/ambassadors/api/") {
      const db = env.DB;
      await db.prepare(`CREATE TABLE IF NOT EXISTS ambassador_data (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT DEFAULT (datetime('now')))`).run();
      if (request.method === "GET") { const rows = await db.prepare("SELECT key, value FROM ambassador_data").all(); const result = {}; const orders = {}; for (const row of rows.results) { if (row.key === "ambassadors") { result.ambassadors = JSON.parse(row.value); } else if (row.key.startsWith("orders_")) { orders[row.key.replace("orders_", "")] = JSON.parse(row.value); } } result.orders = orders; return new Response(JSON.stringify(result), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }); }
      if (request.method === "POST") { const body = await request.json(); const { key, value } = body; if (value === null) { await db.prepare("DELETE FROM ambassador_data WHERE key = ?").bind(key).run(); } else { await db.prepare("INSERT OR REPLACE INTO ambassador_data (key, value, updated_at) VALUES (?, ?, datetime('now'))").bind(key, JSON.stringify(value)).run(); } return new Response(JSON.stringify({ ok: true }), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }); }
      if (request.method === "DELETE") { await db.prepare("DELETE FROM ambassador_data").run(); return new Response(JSON.stringify({ ok: true }), { headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" } }); }
      if (request.method === "OPTIONS") { return new Response(null, { headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS", "Access-Control-Allow-Headers": "Content-Type" } }); }
    }
    // ===== STATIC ASSETS (with noindex) =====
    const assetResp = await env.ASSETS.fetch(request);
    return addNoIndex(assetResp);
  },
  async scheduled(event, env, ctx) { ctx.waitUntil(fullSync(env)); },
};
function landingPageHTML() {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NITM Operations Hub</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0a0f1a;color:#f1f5f9;font-family:'DM Sans',sans-serif;min-height:100vh}
.top-nav{background:linear-gradient(180deg,#1a2332 0%,#141c28 100%);padding:12px 24px;display:flex;align-items:center;gap:24px;border-bottom:1px solid #1e293b}
.top-nav .brand{color:#f1f5f9;font-weight:700;font-size:14px;text-decoration:none;letter-spacing:0.5px}
.top-nav a{color:#64748b;text-decoration:none;font-size:13px;font-weight:500;transition:color 0.15s}
.top-nav a:hover,.top-nav a.active{color:#f1f5f9}
.hub-wrap{display:flex;align-items:center;justify-content:center;min-height:calc(100vh - 49px);padding:40px 20px}
.hub{text-align:center;max-width:760px;width:100%}
.hub-title{font-size:36px;font-weight:700;letter-spacing:-0.5px;margin-bottom:44px}
.app-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:16px;max-width:680px;margin:0 auto}
.app-card{background:#111827;border:1px solid #1f2937;border-radius:16px;padding:36px 24px;text-decoration:none;color:#f1f5f9;transition:all 0.2s;display:flex;flex-direction:column;align-items:center;gap:10px}
.app-card:hover{background:#1e293b;border-color:#334155;transform:translateY(-2px)}
.app-icon{font-size:40px}
.app-name{font-size:17px;font-weight:700}
.app-desc{font-size:13px;color:#64748b;text-align:center}
@media(max-width:600px){.app-grid{grid-template-columns:1fr}.hub-title{font-size:28px}}
</style></head><body>
<nav class="top-nav">
  <span class="brand">NITM Ops</span>
  <a href="/" class="active">Hub</a>
  <a href="/calendar/">Calendar</a>
  <a href="/inventory/">Inventory</a>
  <a href="/3pl/">3PL</a>
  <a href="/tracker/">Tracker</a>
  <a href="/ambassadors/">Ambassadors</a>
</nav>
<div class="hub-wrap">
<div class="hub">
  <h1 class="hub-title">Operations Hub</h1>
  <div class="app-grid">
    <a href="/calendar/" class="app-card"><div class="app-icon">\u{1F4C5}</div><div class="app-name">Content Calendar</div><div class="app-desc">Social media scheduling & tracking</div></a>
    <a href="/inventory/" class="app-card"><div class="app-icon">\u{1F4CA}</div><div class="app-name">Inventory Dashboard</div><div class="app-desc">Stock levels, orders & forecasting</div></a>
    <a href="/3pl/" class="app-card"><div class="app-icon">\u{1F4E6}</div><div class="app-name">3PL Dashboard</div><div class="app-desc">Warehouse & fulfillment</div></a>
    <a href="/tracker/" class="app-card"><div class="app-icon">\u{1F3AF}</div><div class="app-name">Content Tracker</div><div class="app-desc">IG performance scoring & attribution</div></a>
    <a href="/ambassadors/" class="app-card"><div class="app-icon">\u{1F91D}</div><div class="app-name">Ambassadors</div><div class="app-desc">Ambassador sales & commission tracking</div></a>
  </div>
</div>
</div></body></html>`;
}
