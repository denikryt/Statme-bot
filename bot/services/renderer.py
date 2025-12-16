from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Tuple

import discord

logger = logging.getLogger(__name__)


class StatsRenderer:
    def __init__(self, bot: discord.Client):
        self.bot = bot

    async def server_embed(
        self,
        guild: discord.Guild,
        stats: Dict[str, int],
        reactions_7d: int,
        top_users: List[Tuple[int, int]],
    ) -> discord.Embed:
        embed = discord.Embed(title=f"{guild.name} • Activity", colour=discord.Colour.blurple())
        embed.set_thumbnail(url=guild.icon.url if guild.icon else discord.Embed.Empty)

        embed.add_field(
            name="Messages (24h)",
            value=f"{stats.get('messages_24h', 0):,} msgs\n{stats.get('active_24h', 0):,} active users",
            inline=True,
        )
        embed.add_field(
            name="Messages (7d)",
            value=f"{stats.get('messages_7d', 0):,} msgs\n{stats.get('active_7d', 0):,} active users",
            inline=True,
        )
        embed.add_field(name="Reactions (7d)", value=f"{reactions_7d:,}", inline=True)

        embed.add_field(name="Top Users (7d)", value=await self._format_top_users(guild, top_users), inline=False)
        embed.set_footer(text="Stats start from the last bot restart")
        return embed

    async def user_embed(self, member: discord.Member, stats: Dict[str, int]) -> discord.Embed:
        embed = discord.Embed(title=f"{member.display_name} • Your Stats", colour=discord.Colour.green())
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Total Messages", value=f"{stats.get('total_messages', 0):,}")
        embed.add_field(name="Messages (7d)", value=f"{stats.get('messages_7d', 0):,}")
        embed.add_field(name="Messages (30d)", value=f"{stats.get('messages_30d', 0):,}")
        embed.add_field(name="Reactions Given", value=f"{stats.get('reactions_given', 0):,}")
        embed.add_field(name="Reactions Given (7d)", value=f"{stats.get('reactions_given_7d', 0):,}")
        embed.add_field(name="Reactions Received", value=f"{stats.get('reactions_received', 0):,}")
        embed.set_footer(text="Stats start from the last bot restart")
        return embed

    async def _format_top_users(self, guild: discord.Guild, entries: List[Tuple[int, int]]) -> str:
        if not entries:
            return "No data yet."
        lines = []
        for idx, (user_id, count) in enumerate(entries, start=1):
            member = guild.get_member(user_id) or await self._safe_fetch_member(guild, user_id)
            name = member.display_name if member else f"User {user_id}"
            lines.append(f"{idx}. {name}: {count:,} msgs")
        return "\n".join(lines)

    async def _safe_fetch_member(self, guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
        try:
            return await guild.fetch_member(user_id)
        except Exception:
            logger.debug("Failed to fetch member %s for guild %s", user_id, guild.id, exc_info=True)
            return None
