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

---

## ClickUp Automation — one-time setup

Replaces the Zapier Zap. New task in a configured ClickUp list → sequential code,
task renamed `2026.NNNN - Task Name`, Drive folder of that name inside a per-list
parent, subfolders, links written back to ClickUp custom fields. Everything is
configured from `/clickup-automation/` — no code edits to add a list.

### Step 1 — Apply the schema

```bash
npx wrangler d1 migrations apply content-calendar --remote
```

### Step 2 — Set the secrets

```bash
# The Google service account that owns the Drive folders.
npx wrangler secret put GOOGLE_CLIENT_EMAIL
npx wrangler secret put GOOGLE_PRIVATE_KEY

# Gates the dangerous, once-ever endpoints (webhook register/delete, hard delete).
# Generate something long and random; it is never shown in the UI.
npx wrangler secret put CLICKUP_AUTOMATION_ADMIN_SECRET
```

`CLICKUP_TOKEN` is already set (shared with the Content Calendar and Stage
Timing). `CLICKUP_TEAM_ID` and `APP_HOSTNAME` live in `wrangler.jsonc` — a
workspace id appears in every ClickUp URL, so it is config, not a secret.

There is deliberately **no** `CLICKUP_WEBHOOK_SECRET` to set by hand: ClickUp
returns the signing secret once, when the webhook is created, and step 4 writes
it straight into D1 so it never passes through a person or a config file.

`GOOGLE_PRIVATE_KEY` can be pasted either with real newlines or with the
literal `\n` escapes that appear in the service-account JSON — the PEM parser
handles both.

### Step 3 — ⚠️ Cloudflare Access: exclude exactly ONE path

ClickUp cannot log in, so the webhook must bypass the Access gate. In the
Cloudflare Zero Trust dashboard, add a **Bypass** policy for exactly:

```
ops.anurseinthemaking.com/clickup-automation/webhook
```

**Not** `/clickup-automation/*`. That wildcard would put the entire admin API —
create, edit and delete automations, browse your Drive, register webhooks — on
the open internet. The HMAC signature is the only thing guarding the webhook
path, which is why it is mandatory and fails closed.

Verify after deploying, from a browser with no Access session (or `curl`):

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST https://ops.anurseinthemaking.com/clickup-automation/webhook
```

That must return `401` (our signature check ran). Then:

```bash
curl -s https://ops.anurseinthemaking.com/clickup-automation/api/automations
```

That must return an Access login redirect, **not** JSON. If it returns JSON, the
bypass rule is too broad — fix it before going live.

The `*.workers.dev` hostname skips Access entirely, so the admin API refuses any
request that did not arrive on `APP_HOSTNAME`. Setting `"workers_dev": false` in
`wrangler.jsonc` closes that door for every module and is worth doing.

Optional defence in depth — if you set both of these, the Worker verifies the
Access JWT itself rather than trusting the hostname:

```bash
# e.g. nitm.cloudflareaccess.com, and the application's AUD tag from the
# Zero Trust dashboard (Access > Applications > your app > Overview).
npx wrangler secret put ACCESS_TEAM_DOMAIN
npx wrangler secret put ACCESS_AUD
```

### Step 4 — Register the ClickUp webhook

One workspace-level webhook covers every automation; filtering happens in code
by list id. This stores the signing secret in D1 for you.

Open `/clickup-automation/`, click **Connection details**, and press
**Connect ClickUp**. That is the whole step.

> The curl that used to be documented here **could never have worked.**
> `/clickup-automation/api/*` is behind Cloudflare Access, so an unauthenticated
> request is redirected to the login page and the Worker never sees the
> `X-Ops-Admin-Secret` header at all. It returns `302`, not `401`. Found the hard
> way on 2026-09-21.
>
> Registering is now allowed for any human Cloudflare Access has already
> authenticated, which is what the button relies on. Destructive operations —
> force-replacing a webhook, deleting one — still require the admin secret, and
> for those you need a `curl` carrying an Access **service token**, not a bare
> header.

If a webhook already points at that endpoint it returns `409` rather than
creating a second one — two webhooks would both fire on every task. To inspect
or remove:

```bash
curl https://ops.anurseinthemaking.com/clickup-automation/api/webhook/list \
  -H "X-Ops-Admin-Secret: <secret>"

curl -X POST https://ops.anurseinthemaking.com/clickup-automation/api/webhook/delete \
  -H "X-Ops-Admin-Secret: <secret>" \
  -H "Content-Type: application/json" -d '{"webhook_id":"<id>"}'
```

### Step 5 — Give the service account access to the Drive folders

This cannot be done from code. A member of the shared drive must share it with
the service-account address (shown under "Connection details" on the page) as
**Content Manager**. Until then, the preflight check on each automation says so
in plain language and refuses to let it go live.

Keep this narrow: grant the account membership of the specific shared drive it
needs, and leave **domain-wide delegation off** — that would let the key
impersonate any user in the Google workspace.

### Step 6 — Set up a list

Go to `/clickup-automation/` and use "Set up a list". Nothing fires until an
automation is switched from **draft** to **live**, and it cannot go live while
any preflight check is red.

### Troubleshooting

**Nothing happens when a task is created.** Check the Activity tab. "No
automation set up for that list" means the list id doesn't match any row.
"That list's automation is still a draft" means it was never turned on. No row
at all means ClickUp isn't delivering — check the connection banner, then that
the Access bypass from step 3 still exists.

**"The automation's Google account doesn't have permission."** Step 5. The fix
button on the failing check copies the exact address to share with.

**A failure that should have worked.** Every error row has a Retry button. Rate
limits and transient Google/ClickUp errors are also retried automatically by the
2-minute cron, up to 5 attempts. Because the webhook always answers `200` —
repeated `5xx` would make ClickUp disable it and silently kill every automation
at once — ClickUp never retries, so these are the only retries there are.

**Codes have gaps.** Codes are allocated when a run starts, so a failed run
consumes one. Previews and preflight never do.
