> Status: LIVE · Owns: the per-order profit feed (Shopify order + Finale SKU cost + ShipMonk fulfillment → profit before ads) and its break-even ad spend view · Updated: 2026-10-31

# Order profit feed

**Goal:** profit before ads for every Shopify website order, so Ben can set paid-ads targets:
the most we can spend to get an order before it loses money (break-even CPA, and break-even ROAS).

## Decisions (Ben, 2026-10-30)

- **Scope:** Shopify website orders fulfilled by ShipMonk. Not Amazon FBA/FBM, university/manual, or Etsy.
- **Revenue:** what the customer paid net of discount codes and refunds, plus shipping charged, **excluding sales tax**.
- **Product cost:** Finale average cost per SKU = Finale `cost / units` for that SKU in the order's ship month
  (from `nitm-PL-sheet/finale/YYYY-MM.csv`, all sources combined). If the SKU has no Finale row that month, use
  the nearest earlier month; if none, flag the SKU as "no cost".
- **Fulfillment:** ShipMonk per-order charges (shipping + packaging + pick & pack), **plus** an even share of
  that month's ShipMonk fees that aren't per order (storage, receiving, returns, etc.).
- **Payment processing fees:** subtract them (Shopify Payments / PayPal), actual where Shopify reports them.
- **Ads view:** first orders only (the customer's first Shopify order). Break-even CPA = average profit
  before ads per first order; break-even ROAS = average first-order revenue ÷ that profit. Show by week and month.

`profit_before_ads = revenue − product_cost − shipmonk_per_order − shipmonk_overhead_share − payment_fees`

## How each number is computed (built 2026-10-30)

- **Net revenue** = Shopify `total − tax − refunded` (total already includes shipping charged and is after discounts).
  Test and cancelled orders are excluded.
- **Matching:** Shopify order "#167623" = ShipMonk `order_number` "2-167623" (also "Merged: 2-…, 2-…").
  ShipMonk "E-…" orders are Etsy and "112-…"/"111-…" are Amazon, so both drop out. *Assumed; check the match rate after the backfill.*
- **Product cost** uses the SKUs ShipMonk actually shipped, freebies included; Shopify line items are the fallback when there's
  no ShipMonk match. Each SKU goes through `sku_map` (aliases, educator variants, later bundle recipes), then
  `sku_costs` for the ship month. Before Finale history starts, the earliest known cost is used.
- **ShipMonk per order** = estimated shipping + packaging + pick & pack (`tpl_orders`).
- **ShipMonk overhead share** = `shipmonk_monthly_overhead.per_order`, written by `nitm-PL-sheet/feeds.py shipmonk`
  (Monday refresh). For each month: (QBO paid − per-order estimates) for the trailing 3 closed months, divided by those
  months' orders, floored at $0. A single month swings with prepaid top-ups, which is why it's averaged.
  Oct 2026 value: $3.87/order. ShipMonk's API billing endpoints weren't checked yet (needs the worker's API key).
- **Payment fees** = Shopify-reported transaction fees when present; otherwise estimated at
  PayPal 3.49% + $0.49 or Shopify Payments 2.9% + $0.30 (`fees_estimated = yes`).
- **Break-even** counts only ShipMonk-matched orders with a cost for every SKU.
  `breakeven_cpa` = average profit before ads; `breakeven_roas` = revenue ÷ profit.

## Tables and endpoints

- D1: `shopify_orders`, `shopify_order_items`, `shopify_sync_state` (cron, ~every 4 min, from 2026-06-01);
  `sku_costs` (from `feeds.py finale-ingest`); `sku_map`; `shipmonk_monthly_overhead`.
- `GET /feeds/order-profit.csv?key=…&days=90` and `/feeds/order-profit-summary.csv?key=…`. They need the
  `ORDER_FEED_TOKEN` worker secret (sent as `X-Feed-Key`), and they sit behind a Cloudflare Access application
  "ops-hub feeds" (path `feeds/`) with a **Service Auth** policy. Only requests carrying the service token
  `nitm-pl-sheet` (`CF-Access-Client-Id` / `CF-Access-Client-Secret`) get through. Decision 2026-10-31: service
  token instead of a Bypass policy, so the feed is never reachable without Cloudflare's check.
- Manual sync: `/3pl/api/shopify-orders-sync?pages=15` (behind Access).
- Sheet: `nitm-PL-sheet/apps-script/order_profit_feed.gs` writes the "Order Profit" and "Ad Break-even" tabs daily at 6am.

## Where it runs

All joining happens in **ops-hub (D1 + the 2-minute cron)**, which runs around the clock. The Google Sheet
([NITM P&L](https://docs.google.com/spreadsheets/d/1Ky5Xy3c_wqHfa6DOdF2yduiBQyKy0TD9cajvfuHOkCI/edit)) pulls
the results. The site sits behind Cloudflare Access, so the sheet's Apps Script presents a Cloudflare service token
plus the worker key to read a CSV endpoint, with a
Google Apps Script on a daily trigger (same pattern as `nitm-PL-sheet/apps-script/save_finale_cogs.gs`).

## Data that already exists (2026-10-30)

- `tpl_orders` + `shipmonk_order_items` (D1 `content-calendar`): live ShipMonk sync via `syncShipmonkOrders` in
  `src/index.js`. Per order: order_number, ship/order date, estimated shipping, packaging and pick-pack charges;
  SKU × qty. **Order totals aren't saved.**
- `tpl_order_phases`: a one-off import that stopped 7/21/2026 (has order_total and customer_shipping_paid). Not maintained.
- Finale per-SKU monthly cost: `nitm-PL-sheet/finale/YYYY-MM.csv` (sku, source, units, revenue, cost),
  Feb 2025–Feb 2026 + Aug–Sep 2026; more arrive monthly through the Finale Apps Script.
- Shopify Admin API token: `SHOPIFY_ACCESS_TOKEN` worker secret (already used by the CX agent).
- ShipMonk paid per month: QBO account "ShipFusion" on the P&L tab (cash basis), next to the per-order estimates on the 3PL tab.

## Workstreams

1. **Worker data (ops-hub `src/index.js`)**
   - A Shopify order sync into D1 (backfill from 2026-06-01). It saves order id/name, created_at, customer id,
     whether it's the customer's first order, revenue fields, line items (sku, qty, price) and payment fees.
   - The key that matches Shopify orders to ShipMonk `order_number`.
   - ShipMonk's actual billed charges (invoice/billing endpoints if the API has them), so non-per-order fees
     are measured rather than taken from QBO cash top-ups.
2. **SKU cost table:** load Finale per-SKU monthly average cost into D1 from the CSVs, hooked into
   `feeds.py finale-ingest` so new months load automatically. Report SKU coverage: Shopify/ShipMonk SKUs with no
   Finale cost, bundles/kits that need mapping to their component SKUs.
3. **Join + sheet** (after 1 and 2):
   - an `order_profit` computation in the worker, with a CSV endpoint behind a token,
   - an Apps Script that pulls it daily into an "Order Profit" tab (last 90 days, one row per order),
   - an "Ad Break-even" summary tab (weekly/monthly first-order AOV, profit before ads, break-even CPA/ROAS).

## SKU coverage (2026-10-30)

Finale costs 22 core SKUs. Of ~10,450 customer-size ShipMonk shipments June–October, 7,705 (74%) are fully costed
before bundle mapping. The gaps:
- `S3-FREE`: a freebie in ~10,400 orders, with no Finale cost. **Ben: what does it cost?**
- Bundles/variants needing a recipe (shipped SKU → Finale SKUs × qty): `ALW-Bundle`, `NSCP-Bundle`,
  `FCB-EVERYTHING`, `FCB-kit`, `APN-KIT`, `SB-NSCB-PHARM`, `SB-NSCB-NCLEX`, `SB-NSCB-GME`, `SB-NSCB-NH`,
  `D-NSCB-*`, `PPG-FC-P`, `PPG-BCP`, `PPG-FCB`.
- Already mapped in `sku_map`: `LST`, `NSCP`, `PPG`, `ALW-FBM` (aliases), and the `-EDU` educator variants. The
  educator variants are *assumed* to cost the same as the regular SKU.
- Few-order, high-unit SKUs (e.g. `S3-SEIJ-10LV`, `NSCB-2027-FBA-2`) are FBA/bulk transfers, not Shopify orders.

## Open questions (fill in as they're answered)

- Do Shopify order names match ShipMonk `order_number` (e.g. ShipMonk "2-167623")?
- Bundles: does ShipMonk ship bundle SKUs or components, and which does Finale cost?
- Does Finale's average cost include freight/duty? (Ben chose Finale over Taner's landed-cost sheet regardless.)
