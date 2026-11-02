# TikTok router

Assigns each stage's owner when a card changes status in **Organic Social > TikTok
Production**. This replaces the list's ClickUp automations, so **turn those off** once
the router is live. Otherwise both will reassign the same cards.

| Card moves to | Assigned to |
| --- | --- |
| Package | Tia |
| Script Review, Accuracy Check, Final Review | Karli if Diana is the Writer, otherwise Diana |
| Prep | Amber (skipped until her ClickUp id is added) |
| Voiceover | Kristine |
| Edit, Titles | Tia if Format is Funny or Trend, otherwise Gaby (including no Format) |
| Schedule | Cass |

The new owner replaces everyone else. Draft, Film, Posted and Cancelled are left
alone. Adding the `roadblock` tag sets priority to High and posts a comment assigned to
Avery.

## Change a rule

Open **https://ops.anurseinthemaking.com/tiktok-router/** (the Ops Hub nav has a "TikTok Router" link). For each stage you can pick:

- Leave alone
- Always assign one person
- The review rule
- The edit rule

You can also edit the review rule, the edit rule (including which Formats count), and the roadblock tag settings. Click **Save changes**. Saves take effect on the next card move, with no deploy. Every save records who made it and when, and the page shows recent router activity.

The page stores rules in D1 (`tiktok_router_rules`). Until someone saves, the router uses `DEFAULT_RULES` in `routing.js`, and **Reset** goes back to those. Editing `DEFAULT_RULES` only changes the starting point and what Reset restores. It doesn't change the live rules once someone has saved from the page.

## Files

- `src/tiktok-router/routing.js`: the default rules, the decision logic (`ownerFor`) and validation (`normalizeRules`).
- `src/tiktok-router/index.js`: the webhook, signature check, ClickUp calls, run log and the page's API.
- `public/tiktok-router/index.html`: the page.
- `migrations/0007_tiktok_routing_log.sql` and `migrations/0008_tiktok_router_rules.sql`: the database tables.
- `tests/tiktok-router.test.js`: tests against a fake ClickUp and a fake D1.
- `tools/tiktok-router/register-webhook.mjs`: one-time webhook setup.

## Deploy and setup

1. `npx wrangler d1 migrations apply content-calendar --remote`. The table is also created automatically on first use.
2. Commit and push, then run `npx wrangler deploy`.
3. Register the webhook. This saves the signing secret as `TIKTOK_ROUTER_WEBHOOK_SECRET` for you:
   ```bash
   node tools/tiktok-router/register-webhook.mjs
   ```
   `CLICKUP_TOKEN` is already a Worker secret, shared with Calendar, Stage Timing and ClickUp Automation. The script asks for that same token. Copy it from ClickUp > Settings > Apps. Do **not** click Regenerate, which would break those modules until the Worker secret is updated too.
4. Move a test card through every stage and check the log:
   ```bash
   npx wrangler d1 execute content-calendar --remote --command "SELECT at, task_id, event, status, action FROM tiktok_routing_log ORDER BY id DESC LIMIT 30"
   ```

## Things that aren't obvious

- **Endpoint:** `POST /clickup-automation/webhook/tiktok`. It sits under the existing Cloudflare Access bypass for `/clickup-automation/webhook`, which covers subpaths, so it needs no Access change. The HMAC signature is its only guard, and a request with no secret or a bad signature gets a 401.
- **Why the secret isn't named `CLICKUP_WEBHOOK_SECRET`:** ClickUp Automation already accepts a secret with that name, so sharing it would let either webhook's signature open the other's endpoint.
- **Responses:** the router always replies 200 and does the work afterwards in `ctx.waitUntil`. Errors go to the run log instead of producing a 5xx, because repeated 5xx responses make ClickUp disable the webhook.
- **Fresh data:** it re-fetches the task before acting, so the webhook's snapshot is never trusted.
- **Event subscriptions:** the webhook subscribes only to status and tag events. If you add assignee or priority events, the router will trigger itself.

## Adding handlers later (AI PM checks, creative engine sync)

Write an `async ({ env, event, task, stage, rules, log }) => {}` function in `index.js` and add it to `HANDLERS` under the event it needs. For example, a "reel cover missing before leaving Titles" check would go under `taskStatusUpdated`. Put any rule data it needs in `routing.js`, or in its own config file next to it.
