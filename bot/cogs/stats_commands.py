from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

from bot.config import Config
from bot.services.aggregation import AggregationService
from bot.services.renderer import StatsRenderer

logger = logging.getLogger(__name__)


class StatsCommands(commands.Cog):
    stats_group = app_commands.Group(name="stats", description="Server statistics commands")

    def __init__(self, bot: commands.Bot, config: Config, aggregation: AggregationService, renderer: StatsRenderer):
        self.bot = bot
        self.config = config
        self.aggregation = aggregation
        self.renderer = renderer
        self._synced = False
        self._kyiv_tz = ZoneInfo("Europe/Kyiv")
        self._weekly_task = self.bot.loop.create_task(self._weekly_refresh())
        self._monthly_task = self.bot.loop.create_task(self._monthly_refresh())
        self.daily_refresh.start()

    def cog_unload(self):
        self.daily_refresh.cancel()
        for task in (self._weekly_task, self._monthly_task):
            if task and not task.done():
                task.cancel()

    @commands.Cog.listener()
    async def on_ready(self):
        await self._sync_commands()
        await self.refresh_stats_message()

    @app_commands.command(name="my_stats", description="Show your recent Discord activity")
    async def my_stats(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Use this command inside a server.", ephemeral=True)
            return
        if self.config.guild_id and interaction.guild.id != self.config.guild_id:
            await interaction.response.send_message("This bot is scoped to a different server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        stats = await self.aggregation.get_user_summary(interaction.guild.id, interaction.user.id)
        embed = await self.renderer.user_embed(interaction.user, stats)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @stats_group.command(name="refresh", description="Refresh the public statistics message")
    @app_commands.checks.has_permissions(administrator=True)
    async def refresh(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Use this command inside a server.", ephemeral=True)
            return
        if self.config.guild_id and interaction.guild.id != self.config.guild_id:
            await interaction.response.send_message("This bot is scoped to a different server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.refresh_stats_message()
        await interaction.followup.send("Statistics refreshed.", ephemeral=True)

    @refresh.error
    async def refresh_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("Administrator permission is required.", ephemeral=True)
            return
        logger.exception("Error handling /refresh command")
        await interaction.response.send_message("Failed to refresh stats. Check logs.", ephemeral=True)

    @tasks.loop(hours=24)
    async def daily_refresh(self):
        try:
            await self.refresh_stats_message()
        except Exception:
            logger.exception("Daily stats refresh failed")

    @daily_refresh.before_loop
    async def before_daily_refresh(self):
        await self.bot.wait_until_ready()
        await discord.utils.sleep_until(self._next_kyiv_midnight())

    async def _weekly_refresh(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await discord.utils.sleep_until(self._next_monday_start())
                await self.refresh_stats_message()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Weekly stats refresh failed")

    async def _monthly_refresh(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await discord.utils.sleep_until(self._next_month_start())
                await self.refresh_stats_message()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Monthly stats refresh failed")

    async def refresh_stats_message(self):
        guild = self._target_guild()
        if not guild:
            logger.warning("No guild available to refresh stats")
            return

        stats_24h = await self.aggregation.get_server_windows(guild.id, 1)
        stats_7d = await self.aggregation.get_server_windows(guild.id, 7)
        stats_30d = await self.aggregation.get_server_windows(guild.id, 30)
        messages = {
            "messages_24h": stats_24h.get("messages", 0),
            "active_24h": stats_24h.get("active_users", 0),
            "messages_7d": stats_7d.get("messages", 0),
            "active_7d": stats_7d.get("active_users", 0),
            "messages_30d": stats_30d.get("messages", 0),
            "active_30d": stats_30d.get("active_users", 0),
        }
        reactions_7d = stats_7d.get("reactions", 0)
        reactions_30d = stats_30d.get("reactions", 0)
        top_users_7d = await self.aggregation.get_top_users_by_messages(guild.id, 7, limit=5)
        top_users_30d = await self.aggregation.get_top_users_by_messages(guild.id, 30, limit=5)

        embed = await self.renderer.server_embed(
            guild, messages, reactions_7d, reactions_30d, top_users_7d, top_users_30d
        )

        channel = await self._get_stats_channel(guild)
        if not channel:
            return

        await self.aggregation.set_stats_channel_id(guild.id, channel.id)

        message = await self._get_existing_message(channel)
        try:
            if message:
                await message.edit(embed=embed)
            else:
                sent = await channel.send(embed=embed)
                await self.aggregation.set_stats_message_id(guild.id, sent.id)
        except discord.Forbidden:
            logger.warning("Missing permissions to edit or send stats message in %s", channel.id)
        except Exception:
            logger.exception("Failed to update stats message")

    def _target_guild(self) -> Optional[discord.Guild]:
        if self.config.guild_id:
            return self.bot.get_guild(self.config.guild_id)
        # fallback to the first guild the bot is in
        if self.bot.guilds:
            return self.bot.guilds[0]
        return None

    async def _sync_commands(self):
        if self._synced:
            return
        try:
            if self.config.guild_id:
                guild_obj = discord.Object(id=self.config.guild_id)
                self.bot.tree.add_command(self.stats_group, guild=guild_obj)
                await self.bot.tree.sync(guild=guild_obj)
            else:
                self.bot.tree.add_command(self.stats_group)
                await self.bot.tree.sync()
            self._synced = True
            logger.info("Slash commands synced")
        except Exception:
            logger.exception("Failed to sync commands")

    async def _get_stats_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        channel = guild.get_channel(self.config.stats_channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            return channel
        try:
            fetched = await guild.fetch_channel(self.config.stats_channel_id)
            return fetched if isinstance(fetched, discord.TextChannel) else None
        except discord.Forbidden:
            logger.warning("Missing permissions to fetch stats channel %s", self.config.stats_channel_id)
        except discord.HTTPException:
            logger.exception("Failed to fetch stats channel %s", self.config.stats_channel_id)
        return None

    async def _get_existing_message(self, channel: discord.TextChannel) -> Optional[discord.Message]:
        message_id = await self.aggregation.get_stats_message_id(channel.guild.id)
        if not message_id:
            return None
        try:
            return await channel.fetch_message(message_id)
        except discord.NotFound:
            logger.info("Stored stats message not found. Recreating.")
            return None
        except discord.Forbidden:
            logger.warning("Missing permissions to fetch stats message in %s", channel.id)
        except Exception:
            logger.exception("Failed to fetch stats message %s", message_id)
        return None

    def _kyiv_now(self) -> datetime:
        return datetime.now(self._kyiv_tz)

    def _next_kyiv_midnight(self) -> datetime:
        now = self._kyiv_now()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        return tomorrow

    def _next_monday_start(self) -> datetime:
        now = self._kyiv_now()
        days_ahead = (0 - now.weekday()) % 7
        if days_ahead == 0 and now.time() >= time(0, 0, 10):
            days_ahead = 7
        target_date = (now + timedelta(days=days_ahead)).date()
        return datetime.combine(target_date, time(0, 0, 10), tzinfo=self._kyiv_tz)

    def _next_month_start(self) -> datetime:
        now = self._kyiv_now()
        first_this_month = datetime(now.year, now.month, 1, 0, 0, 15, tzinfo=self._kyiv_tz)
        if now < first_this_month:
            return first_this_month
        year = now.year + (1 if now.month == 12 else 0)
        month = 1 if now.month == 12 else now.month + 1
        return datetime(year, month, 1, 0, 0, 15, tzinfo=self._kyiv_tz)


async def setup(bot: commands.Bot):
    config: Config = bot.config  # type: ignore[attr-defined]
    aggregation: AggregationService = bot.aggregation  # type: ignore[attr-defined]
    renderer: StatsRenderer = bot.renderer  # type: ignore[attr-defined]
    await bot.add_cog(StatsCommands(bot, config, aggregation, renderer))
