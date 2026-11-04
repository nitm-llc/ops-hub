# Flo (Google Chat app), step 1: Q&A and approvals

*Flo* is the marketing team's project-management helper, named after Florence Nightingale and for keeping work flowing. It started out as "NITM PM", which is why the folder, the endpoint and the D1 tables still use `nitm-pm` / `pm_`. Don't rename those: the endpoint URL is saved in Google's Chat app configuration.

People DM **Flo** or @mention it in a space. TikTok Production is the first ClickUp list it's connected to. It answers from ClickUp (TikTok space › Production, list `901704217808`), proposes changes as numbered pending actions, and only changes a card after an approved person says yes.

Things it handles:

- "What's stuck today?" lists overdue cards (not done or paused) with the likely reason.
- "What's posting next week?" lists cards by Post Date, with Type and Stage.
- "Is the calendar balanced for the next 3 weeks?" checks the balance rules and suggests swaps for Cass.
- "Move the peds quiz due date to Friday" creates proposal #N. An approver replies "yes" to apply it.
- "Run the daily check" runs the daily routine in suggest mode and lists numbered proposals. "approve 1 and 3" applies exactly those two.

## Guardrails (enforced in code)

| Rule | Where |
| --- | --- |
| Nothing changes until an approver says yes | `propose_change` only writes a D1 row. `applyAction()` is reached only from `approvals.js`. |
| Approvers are `PM_APPROVERS`, and only numbers they typed in that message | `approvals.js` parses the message. The model's `apply_approved` call is filtered to those numbers. |
| Anyone `@anurseinthemaking.com` can ask; others are turned away | `index.js` (`PM_ALLOWED_DOMAIN`) |
| Never a Post Date or a Stage | `validateAction()` whitelists due date, priority, assignee and comment, and refuses any other key. It runs again at apply time, so a tampered row can't get through. |
| Only cards on the Production list | The task is re-fetched and its list is checked at propose and apply time. |
| Every change gets a comment on the card | `applyAction()`: "Due date moved to Fri Nov 6, approved by Avery in Chat." |
| Everything is logged | `pm_action_log`: proposed, applied, apply_failed, rejected, approval_refused, approval_expired, proposal_refused. |
| Proposals expire after 48 hours | `store.js` (`PENDING_TTL_MS`) |
| Errors are reported plainly | ClickUp and Claude failures are shown in Chat. A change is never reported as done unless ClickUp accepted it. |

## Files

- `index.js`: the Chat endpoint, event parsing (add-on and legacy formats), the 20-second race and "On it…".
- `google.js`: request verification, the service-account token (RS256 with WebCrypto), and posting through the Chat API.
- `brain.js`: the system prompt, tools and Claude tool loop.
- `clickup.js`: reads, `validateAction`, and `applyAction` (the only ClickUp writer).
- `approvals.js`: parses "yes" and "approve 1 and 3", and applies or rejects.
- `store.js`: D1 tables `pm_threads`, `pm_pending_actions` and `pm_action_log` (also `migrations/0010_nitm_pm.sql`).
- `config.js`: list and field ids, people, the stage → owner map, and New York date helpers.
- `ai-pm-instructions.md`: the *default* instructions, copied from the "AI PM Instructions" tab of the TikTok AI PM doc. Day to day they are edited on the **Flo page** (`/flo/`, approvers only) and stored in D1 (`pm_instructions`, with every save in `pm_instruction_changes`). This file is the fallback and what "Reset" goes back to. Editing the Claude Doc does **not** change Flo.
- `admin.js` + `public/flo/index.html`: the Flo page and its API (`/flo/api/*`, behind the Access login). It shows the instructions, the built-in chat rules (read-only), pending proposals, activity and setup.
- `tests/nitm-pm.test.js`: mocked Google, ClickUp and Claude, with SQLite standing in for D1.

## Config

| Name | Kind | What |
| --- | --- | --- |
| `PM_MODEL` | var (`wrangler.jsonc`) | Default `claude-sonnet-5-5` |
| `PM_TIMEZONE` | var | Default `America/New_York` |
| `PM_EFFORT` | optional var | Claude effort, default `medium` |
| `PM_FALLBACKS` | optional var | `off` disables Claude's server-side refusal fallback |
| `PM_APPROVERS` | secret | Comma-separated emails, e.g. `mark@…,avery@…` |
| `PM_CHAT_PROJECT_NUMBER` | secret | The Google Cloud project **number**. Without it every request is rejected. |
| `GOOGLE_CHAT_SA_KEY` | secret | The service-account JSON key, for posting answers that take longer than 20 seconds |
| `ANTHROPIC_API_KEY`, `CLICKUP_TOKEN` | secret | Shared with other modules. ClickUp writes and comments appear as the token's owner. |

To add Cass and Tia as approvers later:

```bash
npx wrangler secret put PM_APPROVERS
```

Then enter `mark@anurseinthemaking.com,avery@anurseinthemaking.com,cassandra@anurseinthemaking.com,tia@anurseinthemaking.com`.

## Things that aren't obvious

- **Endpoint:** `POST https://ops.anurseinthemaking.com/clickup-automation/webhook/nitm-pm`. It sits under the existing Cloudflare Access bypass for `/clickup-automation/webhook`, which covers subpaths, because Google can't log in. The Google token is its only guard.
- **Verification accepts two token shapes, both tied to our project:** add-on format, where Google signs an OIDC token with `aud` set to the endpoint URL and email `service-<project number>@gcp-sa-gsuiteaddons.iam.gserviceaccount.com`; or legacy format with the "Project Number" audience. The legacy "HTTP endpoint URL" audience is **refused**, because any Google project's Chat app could produce one of those tokens for our URL.
- **Timing:** Chat waits about 30 seconds. If the answer is ready in 20 seconds, it's sent as the reply. Otherwise the bot replies "On it…" and posts the answer in the same thread. Workers allow only about 30 seconds of work after the reply, so a turn stops at 45 seconds and says so. If long daily checks keep hitting that limit, move the work to a Cloudflare Queue.
- **DMs:** Chat gives every DM message its own thread, so a DM is stored as one conversation (thread `""`). That way history and proposal numbers carry over from one message to the next.
- **Instagram and Metricool aren't connected.** The bot says so when a balance check needs them.

## Check what it did

```bash
npx wrangler d1 execute content-calendar --remote --command "SELECT at, actor, event, number, result FROM pm_action_log ORDER BY id DESC LIMIT 30"
```

## Later (not built)

- Step 2: the daily check, weekly planning and Monday report on Cron Triggers, posting to a "TikTok PM" space.
- Metricool tools for performance questions.
- The new 12-stage list: swap `LIST_ID`, `FIELDS` and the stage map in `config.js`.
