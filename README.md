# Poly Recorder

Standalone recorder for Polymarket 5 Min / 15 Min up/down markets. It writes ticks under `DATA_DIR` and the same Mongo `markets.recordingEnabled` flag Admin CRM uses.

Do **not** record the same series on dest and this app at the same time (they would write the same files).

## Setup

1. Copy `.env.example` to `.env`
2. Set `MONGODB_URI` (same cluster as dest / CRM) and `DATA_DIR` (where tick files go — dest Replay reads this folder)
3. `npm install`
4. `npm start` — http://localhost:3849

## UI

- **Poly Recorder** header (same height as dest)
- Market dropdown
- **On / Off** — writes Mongo `recordingEnabled` (CRM sees the same flag; this app polls Mongo every 30s)
- Full-width UTC week grid: red = missing windows, green bar = recorded count (12 per hour on 5 Min, 4 on 15 Min)

## Env

| Variable | Meaning |
|----------|---------|
| `MONGODB_URI` | Shared Mongo |
| `MONGODB_DB` | Default `poly_recorder` |
| `DATA_DIR` | Tick / window files |
| `PORT` | Default `3849` |
