# 🃏 Pokr Planning

Real-time planning poker for teams, powered by Cloudflare Workers + Durable Objects.

## Features

- **Real-time sync** — WebSocket-based, everyone sees updates instantly
- **Hidden votes** — Votes stay secret until the facilitator reveals
- **Card flip animation** — Staggered reveal with 3D card flips
- **Stats on reveal** — Average, median, spread, and consensus detection
- **Zero infrastructure** — No database, no server. Durable Objects handle everything at the edge.
- **Ephemeral rooms** — Rooms exist while people are in them, then vanish
- **Mobile friendly** — Responsive design, works on any device

## Quick Start

```bash
npm install
npm run dev
# → http://localhost:8787
```

## Deploy

```bash
npx wrangler login
npx wrangler deploy
```

> **Note:** Durable Objects require the Cloudflare Workers Paid plan ($5/month).

## How It Works

Each room is a [Durable Object](https://developers.cloudflare.com/durable-objects/) instance that:
- Accepts WebSocket connections from participants
- Stores votes in WebSocket attachments (survives hibernation)
- Broadcasts state changes to all connected clients
- Automatically hibernates when idle

The entire app is a single Worker (~500 lines) that serves the frontend HTML and routes WebSocket connections to the appropriate Durable Object.

## Card Values

`0` `½` `1` `2` `3` `5` `8` `13` `21` `?` `☕`
