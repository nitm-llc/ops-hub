# Ops Hub — canonical project

This folder (`~/claude-dev/ops-hub`, GitHub `nitm-llc/ops-hub`) is the **one true
source** for the Ops Hub app live at https://ops.anurseinthemaking.com.

## How to deploy (the whole flow)

The app is a Cloudflare **Worker** (not Pages). To push changes live:

```bash
npx wrangler deploy
```

That's it. It builds the Worker from `src/index.js`, uploads the `public/` assets,
and goes live in a few seconds. No GitHub push or build step is required — deploy
is direct to Cloudflare from this folder.

Working flow with Claude Code: tell Claude what to change, Claude edits the files
here and runs `npx wrangler deploy`. Nothing gets downloaded or copied.

## Project shape

- `src/index.js` — the Worker: all API routes + server logic
- `public/` — front-end pages, one folder per module
  (`growth/`, `icp/`, `stage/`, `cx-agent/`, `calendar/`, `inventory/`,
  `tracker/`, `social/`, `ambassadors/`, `3pl/`)
- `wrangler.jsonc` — Worker config. Bindings:
  - `DB` → D1 database `content-calendar`
  - `CX_AGENT_DB` → D1 database `cx-agent`
  - `ASSETS` → static files in `public/`
  - cron trigger every 2 minutes
- Access: the live site sits behind Cloudflare Access (login gate).

## D1 migrations

To change the database schema, apply migrations against the remote DB:

```bash
npx wrangler d1 migrations apply content-calendar --remote
npx wrangler d1 migrations apply cx-agent --remote
```

## ⚠️ Ignore the duplicates

There are stale copies that are NOT this app — do not edit or deploy from them:

- `~/ops-hub-v4` (GitHub `markpodlas/ops-hub-v4`) — old 497-line fork, no growth module
- `~/Downloads/ops-hub-v2*`, `ops-hub-v3*`, `ops-hub-v4*`, etc. — ~30 old download copies

All real work happens in this folder only.
