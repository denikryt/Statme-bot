from __future__ import annotations

import logging
from collections import Counter, OrderedDict
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands

from bot.config import Config
from bot.services.aggregation import AggregationService

logger = logging.getLogger(__name__)


class MessageAuthorCache:
    def __init__(self, max_size: int = 5000):
        self.max_size = max_size
        self._data: OrderedDict[int, int] = OrderedDict()

    def put(self, message_id: int, author_id: int) -> None:
        if message_id in self._data:
            self._data.move_to_end(message_id)
        self._data[message_id] = author_id
        if len(self._data) > self.max_size:
            self._data.popitem(last=False)

    def get(self, message_id: int) -> Optional[int]:
        author = self._data.get(message_id)
        if author is not None:
            self._data.move_to_end(message_id)
        return author


class StatsCollector(commands.Cog):
    def __init__(self, bot: commands.Bot, config: Config, aggregation: AggregationService):
        self.bot = bot
        self.config = config
        self.aggregation = aggregation
        self.cache = MessageAuthorCache()
        self.user_counts: Counter[int] = Counter()
        self._user_count_logger = logging.getLogger("user_counts")
        self._configure_user_logger()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        if self.config.guild_id and message.guild.id != self.config.guild_id:
            return
        try:
            await self.aggregation.record_message(message.guild.id, message.author.id, message.created_at)
            self.cache.put(message.id, message.author.id)
            total = self._increment_user_count(message.author.id)
            self._user_count_logger.info(
                "user_id=%s username=%s message_count=%s", message.author.id, message.author.name, total
            )
        except Exception:
            logger.exception("Failed to record message for guild %s", message.guild.id)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        await self._handle_reaction(payload, is_add=True)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        await self._handle_reaction(payload, is_add=False)

    async def _handle_reaction(self, payload: discord.RawReactionActionEvent, is_add: bool) -> None:
        if payload.guild_id is None:
            return
        if self.config.guild_id and payload.guild_id != self.config.guild_id:
            return
        if self.bot.user and payload.user_id == self.bot.user.id:
            return

        author_id = await self._get_message_author(payload)
        ts = datetime.utcnow()
        try:
            if is_add:
                await self.aggregation.record_reaction_add(payload.guild_id, payload.user_id, author_id, ts)
            else:
                await self.aggregation.record_reaction_remove(payload.guild_id, payload.user_id, author_id, ts)
        except Exception:
            logger.exception("Failed to record reaction event for guild %s", payload.guild_id)

    async def _get_message_author(self, payload: discord.RawReactionActionEvent) -> Optional[int]:
        cached = self.cache.get(payload.message_id)
        if cached is not None:
            return cached

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return None

        channel = guild.get_channel(payload.channel_id) if guild else None
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(payload.channel_id)
            except Exception:
                logger.debug("Cannot fetch channel %s for guild %s", payload.channel_id, payload.guild_id, exc_info=True)
                return None

        try:
            message = await channel.fetch_message(payload.message_id)
            self.cache.put(message.id, message.author.id)
            return message.author.id
        except discord.Forbidden:
            logger.warning("Missing permissions to read message %s in channel %s", payload.message_id, payload.channel_id)
        except discord.NotFound:
            logger.info("Message %s not found for reaction event", payload.message_id)
        except Exception:
            logger.exception("Failed to fetch message %s for reaction event", payload.message_id)
        return None

    def _configure_user_logger(self) -> None:
        if self._user_count_logger.handlers:
            return
        handler = logging.FileHandler("user_counts.log", mode="w")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self._user_count_logger.addHandler(handler)
        self._user_count_logger.setLevel(logging.INFO)
        self._user_count_logger.propagate = False

    def _increment_user_count(self, user_id: int) -> int:
        self.user_counts[user_id] += 1
        return self.user_counts[user_id]


async def setup(bot: commands.Bot):
    # Defer dependency wiring to caller through bot attributes
    config: Config = bot.config  # type: ignore[attr-defined]
    aggregation: AggregationService = bot.aggregation  # type: ignore[attr-defined]
    await bot.add_cog(StatsCollector(bot, config, aggregation))
