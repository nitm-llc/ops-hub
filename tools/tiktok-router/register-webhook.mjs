// One-time setup: tell ClickUp to send TikTok Production status + tag changes
// to Ops Hub, and save the signing secret ClickUp hands back (it only shows it
// once).
//
// Run from the repo root:
//   CLICKUP_TOKEN=pk_xxx node tools/tiktok-router/register-webhook.mjs
//
// It refuses to create a duplicate (duplicates double-fire every event). Pass
// --replace to delete an existing webhook on the same endpoint and start over.
import { spawnSync } from "node:child_process";

const TEAM_ID = "9017029478";
const LIST_ID = 1600440000001184;
const ENDPOINT = "https://ops.anurseinthemaking.com/clickup-automation/webhook/tiktok";
// Only these two. Assignee/priority events would make the router trigger itself.
const EVENTS = ["taskStatusUpdated", "taskTagUpdated"];
const SECRET_NAME = "TIKTOK_ROUTER_WEBHOOK_SECRET";

const token = process.env.CLICKUP_TOKEN;
if (!token) {
  console.error("Set CLICKUP_TOKEN first:  CLICKUP_TOKEN=pk_xxx node tools/tiktok-router/register-webhook.mjs");
  process.exit(1);
}
const replace = process.argv.includes("--replace");

async function cu(method, path, body) {
  const res = await fetch(`https://api.clickup.com/api/v2${path}`, {
    method,
    headers: { Authorization: token, "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    console.error(`ClickUp ${method} ${path} failed (${res.status}):`, data);
    process.exit(1);
  }
  return data;
}

const { webhooks = [] } = await cu("GET", `/team/${TEAM_ID}/webhook`);
const existing = webhooks.filter((w) => w.endpoint === ENDPOINT);
if (existing.length && !replace) {
  console.error(`A webhook already points at ${ENDPOINT} (id ${existing.map((w) => w.id).join(", ")}).`);
  console.error("Re-run with --replace to delete it and register a fresh one (you get a new secret).");
  process.exit(1);
}
for (const w of existing) {
  await cu("DELETE", `/webhook/${w.id}`);
  console.log("Deleted old webhook", w.id);
}

const created = await cu("POST", `/team/${TEAM_ID}/webhook`, { endpoint: ENDPOINT, events: EVENTS, list_id: LIST_ID });
// ClickUp nests the secret under `webhook`; older responses put it at the top.
const hook = created.webhook || created;
const secret = hook.secret || created.secret;
if (!hook.id || !secret) {
  console.error("ClickUp did not return an id and secret. Keys:", Object.keys(created));
  process.exit(1);
}
console.log("Webhook registered:", hook.id, "->", ENDPOINT);
console.log("Events:", EVENTS.join(", "), "| list", LIST_ID);
console.log("\nSigning secret (keep it private):\n" + secret + "\n");

// Save it straight into the Worker so it doesn't have to be copy-pasted.
console.log(`Saving it as the ${SECRET_NAME} Worker secret...`);
const r = spawnSync("npx", ["wrangler", "secret", "put", SECRET_NAME], {
  input: secret,
  stdio: ["pipe", "inherit", "inherit"],
});
if (r.status !== 0) {
  console.error(`\nCould not save it automatically. Run this and paste the secret above:\n  npx wrangler secret put ${SECRET_NAME}`);
  process.exit(1);
}
console.log("Done. Move a card in TikTok Production and check the run log.");
