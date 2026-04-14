# Ops Hub v3 — With Inventory Dashboard

## What's New
- **📊 Inventory Dashboard** at `/inventory/` - Full inventory tracking
- **Top navigation** on all pages for easy switching between apps
- **CSV uploads** for Finale and Amazon data (no API needed)
- **ShipFusion API** secured via Cloudflare secrets

## Deploy
```bash
cd ~/Downloads/ops-hub-v3
npx wrangler deploy
```

## First-time Setup: Add ShipFusion Credentials
After deploying, run these commands to securely store your ShipFusion credentials:

```bash
npx wrangler secret put SHIPFUSION_USERNAME
# Enter: nurseinthemaking

npx wrangler secret put SHIPFUSION_PASSWORD
# Enter: $2y$10$jGMlpsA3k5vML6BdlW35tuJrplF5Q2IJqqChp
```

## Structure
```
ops-hub-v3/
├── src/
│   └── index.js          # Main Worker with all API routes
├── public/
│   ├── calendar/
│   │   └── index.html    # Content Calendar
│   ├── inventory/
│   │   └── index.html    # Inventory Dashboard
│   └── 3pl/
│       └── index.html    # 3PL placeholder
└── wrangler.jsonc
```

## Routes
- `/` - Landing page with app grid
- `/calendar/` - Content Calendar
- `/inventory/` - Inventory Dashboard
- `/3pl/` - 3PL Dashboard placeholder
- `/inventory/api/shipfusion` - ShipFusion API (uses secrets)
- `/calendar/api/*` - Calendar API routes

## Data Sources
1. **ShipFusion** - Live API (credentials in Cloudflare secrets)
2. **Finale Inventory** - CSV upload in Settings tab
3. **Amazon FBA** - CSV upload in Settings tab

## After Deploying
1. Run the `wrangler secret put` commands above
2. Visit https://ops.anurseinthemaking.com/inventory/
3. ShipFusion data will load automatically
4. Upload Finale/Amazon CSVs in Settings tab as needed
