import asyncio
import contextlib
import logging
import signal

import discord
from discord.ext import commands

from bot.config import Config, load_config
from bot.db.mongo import Mongo, ensure_app_user
from bot.services.aggregation import AggregationService
from bot.services.renderer import StatsRenderer


def setup_logging(level: int) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def create_bot(config: Config) -> commands.Bot:
    intents = discord.Intents.default()
    intents.message_content = False
    intents.messages = True
    intents.guilds = True
    intents.reactions = True
    bot = commands.Bot(command_prefix="!", intents=intents)
    return bot


async def start_bot():
    config = load_config()
    setup_logging(config.log_level)

    bot = create_bot(config)

    # Optionally bootstrap the app user if root credentials are available
    if config.mongo_root_uri and config.mongo_app_username and config.mongo_app_password:
        await ensure_app_user(
            config.mongo_root_uri,
            config.mongo_app_username,
            config.mongo_app_password,
            config.mongo_db_name,
            config.mongo_root_auth_db,
        )

    mongo = Mongo(config.mongo_uri, config.mongo_db_name)
    aggregation = AggregationService(
        mongo.db(),
        users_collection=config.mongo_users_collection,
        servers_collection=config.mongo_servers_collection,
        meta_collection=config.mongo_meta_collection,
    )
    await aggregation.ensure_indexes()
    renderer = StatsRenderer(bot)

    # Attach dependencies to bot for cogs to consume
    bot.config = config  # type: ignore[attr-defined]
    bot.mongo = mongo  # type: ignore[attr-defined]
    bot.aggregation = aggregation  # type: ignore[attr-defined]
    bot.renderer = renderer  # type: ignore[attr-defined]

    await bot.load_extension("bot.cogs.stats_collector")
    await bot.load_extension("bot.cogs.stats_commands")

    loop = asyncio.get_running_loop()

    def handle_stop(*_: object):
        if bot.is_closed():
            return
        loop.create_task(bot.close())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_stop)
        except NotImplementedError:
            pass

    async with bot:
        try:
            await bot.start(config.discord_token)
        finally:
            await mongo.close()


def main():
    asyncio.run(start_bot())


if __name__ == "__main__":
    main()
