# Discord Statistics Bot

Async Discord bot that collects realtime activity metrics and publishes them in a single embed message inside a dedicated channel. Uses MongoDB for per-day aggregates only (no message backfill).

## Features
- Tracks messages, reactions given/received, and per-day active users from bot launch time.
- Rolling windows: 24h, 7d, 30d (per-day granularity).
- Dedicated stats message that is edited in place and recreated if deleted.
- Slash commands: `/stat_refresh` and `/my_stats` (ephemeral).
- Automatic daily refresh of the public stats embed.

## Tech Stack
- Python 3.11+
- discord.py 2.x
- MongoDB (motor async driver)

## Setup
1. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Set environment variables:
   - `DISCORD_TOKEN` - bot token
   - `MONGO_URI` - Mongo connection string (e.g. `mongodb://localhost:27017`); if omitted the bot will build one from the credentials below
   - `MONGO_APP_DB` / `MONGO_DB` - optional; database name (defaults to `discord_stats`)
   - `MONGO_APP_USERNAME` / `MONGO_APP_PASSWORD` - optional; used to build `MONGO_URI` when it is not provided
   - `MONGO_HOST` / `MONGO_PORT` / `MONGO_AUTH_DB` / `MONGO_AUTH_SOURCE` - optional; tune the generated `MONGO_URI` (host/port default to `localhost:27017`)
   - `MONGO_ROOT_URI` or `MONGO_ROOT_USERNAME` / `MONGO_ROOT_PASSWORD` - optional; if set the bot will create the app user on startup (idempotent)
   - `MONGO_ROOT_AUTH_DB` - optional; defaults to `admin`, used when creating the app user
   - `MONGO_COLLECTION` - optional; default collection name to use for all stats collections
   - `MONGO_USERS_COLLECTION` / `MONGO_SERVERS_COLLECTION` / `MONGO_META_COLLECTION` - optional; override individual collection names
   - `STATS_CHANNEL_ID` - channel ID where the stats embed lives
   - `GUILD_ID` - optional; restricts the bot to a single guild and speeds up slash-command sync
   - `LOG_LEVEL` - optional; defaults to `INFO`
3. Run the bot:
   ```bash
   python -m bot.main
   ```

## Behavior Notes
- Stats start from the time the bot launches; no historical backfill is performed.
- “Last 24h”/“Last 7d” windows are derived from per-day aggregates, so 24h counts align to calendar days, not exact hours.
- The bot requires permissions to read messages, read message history, add embeds, and manage messages in the stats channel.

## Project Structure
```
bot/
  cogs/
    stats_collector.py   # Event listeners for messages/reactions
    stats_commands.py    # Slash commands + stats message refresh
  db/
    mongo.py             # Mongo client helper
    models.py            # Dataclasses and date helpers
  services/
    aggregation.py       # Data aggregation + queries
    renderer.py          # Embed rendering helpers
  config.py              # Environment configuration
  main.py                # Bot bootstrap
```

## Limitations
- Reaction removal events may be missed if the bot lacks permission to read the target message; counters will only adjust when the message author can be resolved.
- 24h window accuracy is bounded by day-level aggregation (no per-hour buckets).
