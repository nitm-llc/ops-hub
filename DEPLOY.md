# Ops Hub v4 — With CX Agent

## What's New in v4
- **🤖 CX Agent** at `/cx-agent/` — AI-powered Zendesk ticket processing
- New D1 database (`cx-agent`) for agent data, separate from main Ops Hub DB
- Zendesk webhook signature verification
- Admin UI for activity feed, decision traces, config, and response templates

Everything else from v3 (Calendar, Inventory, 3PL, Tracker, Ambassadors) is unchanged.

---

## Deploy

```bash
cd ops-hub-v4
npx wrangler deploy
```

This deploys just the Worker. If this is your first v4 deploy, also do the one-time CX Agent setup below.

---

## One-Time CX Agent Setup

Do this once. Skip if already set up.

### Step 1 — Create the CX Agent D1 database

```bash
npx wrangler d1 create cx-agent
```

Output will include a `database_id`. Copy it and paste into `wrangler.jsonc`, replacing `PUT_CX_AGENT_DATABASE_ID_HERE`.

### Step 2 — Deploy

```bash
npx wrangler deploy
```

The CX Agent tables create automatically on first use (no separate schema file needed).

### Step 3 — Set the CX Agent secrets

```bash
npx wrangler secret put ANTHROPIC_API_KEY
# Paste your Anthropic API key (starts with sk-ant-...)

npx wrangler secret put ZENDESK_API_TOKEN
# Paste your Zendesk API token

npx wrangler secret put ZENDESK_EMAIL
# Paste your Zendesk account email (e.g., mark@anurseinthemaking.com)

npx wrangler secret put SHOPIFY_ACCESS_TOKEN
# Paste your Shopify Admin API token (needs: read_orders, read_customers, write_order_edits)
```

**Optional but recommended:**
```bash
npx wrangler secret put ZENDESK_WEBHOOK_SECRET
# If set, the Worker verifies signatures on every Zendesk webhook
# and rejects anything not signed with this secret. See step 5.
```

#### How to get a Shopify access token
1. Shopify Admin → Settings → Apps and sales channels → Develop apps
2. Create an app called "NITM CX Agent"
3. Configure Admin API scopes: `read_orders`, `read_customers`, `write_order_edits`, `read_products`
4. Install app, copy the Admin API access token

### Step 4 — Test the UI

Open the CX Agent in your browser:
```
https://ops.anurseinthemaking.com/cx-agent/
```

You should see the admin UI with "No tickets yet".

### Step 5 — Wire up the Zendesk webhook

In Zendesk Admin Center → Apps and Integrations → Webhooks:

1. Create a new webhook (or update your existing CX Agent webhook)
2. Endpoint URL: `https://ops.anurseinthemaking.com/cx-agent/webhook/zendesk`
3. Request method: POST
4. Request format: JSON
5. Request body:
   ```json
   {
     "ticket_id": "{{ticket.id}}"
   }
   ```
6. Set authentication to "Signed" and copy the **signing secret** Zendesk gives you
7. Run `npx wrangler secret put ZENDESK_WEBHOOK_SECRET` and paste the secret

Then create a trigger that fires this webhook on new tickets (or whatever condition you want).

**Test it:** Create a test ticket in Zendesk. Within a few seconds, you should see it show up in the CX Agent activity feed.

---

## Troubleshooting

### Tickets aren't showing up in the feed
- `npx wrangler tail` to see live logs as webhooks come in
- Verify the webhook URL matches exactly (trailing slashes matter)
- Check that `ZENDESK_WEBHOOK_SECRET` matches what Zendesk has (or unset it temporarily to test)

### Agent classifies but doesn't draft responses
- Click any ticket row to open the decision trace
- Look at the `scope_check` step — is it saying "not in scope" or "below confidence threshold"?
- Adjust `min_confidence_to_respond` or `scoped_intents` in the Configuration tab

### Drafts aren't posting to Zendesk as internal notes
- Check the `post_internal_note` step in the decision trace
- Verify `ZENDESK_API_TOKEN` and `ZENDESK_EMAIL` are set correctly
- The API token needs ticket write permissions

### Agent can't find customer orders in Shopify
- Check `shopify_order_lookup` step in the trace
- Make sure `SHOPIFY_ACCESS_TOKEN` has `read_orders` scope
- The lookup tries: (1) order number in subject/body, (2) customer email — if neither works, agent escalates

### Disable the agent temporarily
- Admin UI → Configuration tab → set `agent_enabled` to `false`
- All incoming webhooks will be skipped with no processing

### Roll back to the n8n flow
- The old n8n `cx-agent` flow can run in parallel — this Worker skips tickets already processed
- Set `agent_enabled = false` in the admin UI to disable this Worker

---

## Structure (changes from v3)

```
ops-hub-v4/
├── src/
│   └── index.js          # Main Worker — CX Agent code added
├── public/
│   ├── calendar/
│   ├── inventory/
│   ├── 3pl/
│   ├── tracker/
│   ├── ambassadors/
│   ├── social/
│   └── cx-agent/         # NEW: Admin UI
│       └── index.html
└── wrangler.jsonc        # Added CX_AGENT_DB binding
```

## New Routes
- `/cx-agent/` — Admin UI
- `/cx-agent/webhook/zendesk` — Zendesk webhook endpoint (POST)
- `/cx-agent/api/tickets` — List processed tickets
- `/cx-agent/api/tickets/:id` — Single ticket + full decision trace
- `/cx-agent/api/stats` — Dashboard stats
- `/cx-agent/api/config` — GET/POST agent configuration
- `/cx-agent/api/templates` — GET/POST response templates

## New Secrets (CX Agent)
- `ANTHROPIC_API_KEY` — Claude API key
- `ZENDESK_API_TOKEN` — Zendesk API token
- `ZENDESK_EMAIL` — Zendesk account email
- `SHOPIFY_ACCESS_TOKEN` — Shopify Admin API token
- `ZENDESK_WEBHOOK_SECRET` — Optional, for webhook signature verification

---

## Old v3 Setup (unchanged)

### ShipFusion Credentials
```bash
npx wrangler secret put SHIPFUSION_USERNAME
npx wrangler secret put SHIPFUSION_PASSWORD
```

### ClickUp Token (for Content Calendar)
```bash
npx wrangler secret put CLICKUP_TOKEN
```
