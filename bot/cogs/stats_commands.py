from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

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
        self.daily_refresh.start()

    def cog_unload(self):
        self.daily_refresh.cancel()

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
        # Align to midnight UTC to keep daily buckets consistent
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        await discord.utils.sleep_until(tomorrow)

    async def refresh_stats_message(self):
        guild = self._target_guild()
        if not guild:
            logger.warning("No guild available to refresh stats")
            return

        stats_24h = await self.aggregation.get_server_windows(guild.id, 1)
        stats_7d = await self.aggregation.get_server_windows(guild.id, 7)
        messages = {
            "messages_24h": stats_24h.get("messages", 0),
            "active_24h": stats_24h.get("active_users", 0),
            "messages_7d": stats_7d.get("messages", 0),
            "active_7d": stats_7d.get("active_users", 0),
        }
        reactions_7d = stats_7d.get("reactions", 0)
        top_users = await self.aggregation.get_top_users_by_messages(guild.id, 7, limit=5)

        embed = await self.renderer.server_embed(guild, messages, reactions_7d, top_users)

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


async def setup(bot: commands.Bot):
    config: Config = bot.config  # type: ignore[attr-defined]
    aggregation: AggregationService = bot.aggregation  # type: ignore[attr-defined]
    renderer: StatsRenderer = bot.renderer  # type: ignore[attr-defined]
    await bot.add_cog(StatsCommands(bot, config, aggregation, renderer))
