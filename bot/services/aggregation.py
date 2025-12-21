from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase

from bot.db.models import DATE_FORMAT, date_key, user_key

logger = logging.getLogger(__name__)


class AggregationService:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        users_collection: str = "users",
        servers_collection: str = "servers",
        meta_collection: str = "meta",
    ):
        self.db = db
        self.users: AsyncIOMotorCollection = db[users_collection]
        self.servers: AsyncIOMotorCollection = db[servers_collection]
        self.meta: AsyncIOMotorCollection = db[meta_collection]

    async def ensure_indexes(self) -> None:
        await self._ensure_index(self.users, [("guild_id", 1)], name="users_guild_id_idx")
        await self._ensure_index(self.users, [("user_id", 1)], name="users_user_id_idx")
        await self._ensure_index(self.servers, [("guild_id", 1)], name="servers_guild_id_unique", unique=True)
        await self._ensure_index(self.meta, [("guild_id", 1)], name="meta_guild_id_idx")

    async def _ensure_index(self, collection: AsyncIOMotorCollection, keys, name: str, **kwargs) -> None:
        existing = await collection.index_information()
        for existing_name, info in existing.items():
            if info.get("key") == keys:
                # If uniqueness expectations differ, log and keep existing index
                expected_unique = kwargs.get("unique", False)
                if expected_unique and not info.get("unique", False):
                    logger.warning(
                        "Index %s on %s already exists but is not unique as expected",
                        existing_name,
                        collection.name,
                    )
                return
        await collection.create_index(keys, name=name, **kwargs)

    # Write operations
    async def record_message(self, guild_id: int, user_id: int, ts: Optional[datetime] = None) -> None:
        ts = ts or datetime.utcnow()
        day = date_key(ts)
        now = datetime.utcnow()
        user_filter = {"_id": user_key(guild_id, user_id)}
        user_update = {
            "$inc": {"total_messages": 1, f"daily_stats.{day}.messages": 1},
            "$set": {"guild_id": guild_id, "user_id": user_id, "updated_at": now},
            "$setOnInsert": {
                "reactions_given": 0,
                "reactions_received": 0,
                "created_at": now,
            },
        }
        await self.users.update_one(user_filter, user_update, upsert=True)

        server_filter = {"_id": str(guild_id)}
        server_update = {
            "$inc": {f"daily_stats.{day}.messages": 1},
            "$addToSet": {f"daily_stats.{day}.active_users": user_id},
            "$set": {"guild_id": guild_id, "updated_at": now},
            "$setOnInsert": {"created_at": now, "stats_channel_id": None, "stats_message_id": None},
        }
        await self.servers.update_one(server_filter, server_update, upsert=True)

    async def record_reaction_add(
        self, guild_id: int, reactor_id: int, message_author_id: Optional[int], ts: Optional[datetime] = None
    ) -> None:
        await self._record_reaction_change(guild_id, reactor_id, message_author_id, delta=1, ts=ts)

    async def record_reaction_remove(
        self, guild_id: int, reactor_id: int, message_author_id: Optional[int], ts: Optional[datetime] = None
    ) -> None:
        await self._record_reaction_change(guild_id, reactor_id, message_author_id, delta=-1, ts=ts)

    async def _record_reaction_change(
        self, guild_id: int, reactor_id: int, message_author_id: Optional[int], delta: int, ts: Optional[datetime]
    ) -> None:
        ts = ts or datetime.utcnow()
        day = date_key(ts)
        now = datetime.utcnow()

        # Update reactions given
        await self._update_user_counter(
            guild_id,
            reactor_id,
            total_field="reactions_given",
            daily_field=f"daily_stats.{day}.reactions_given",
            delta=delta,
            timestamp=now,
        )

        # Update reactions received
        if message_author_id is not None:
            await self._update_user_counter(
                guild_id,
                message_author_id,
                total_field="reactions_received",
                daily_field=f"daily_stats.{day}.reactions_received",
                delta=delta,
                timestamp=now,
            )

        # Update server reactions
        await self._update_server_reactions(guild_id, day, delta, now)

    async def _update_user_counter(
        self,
        guild_id: int,
        user_id: int,
        total_field: str,
        daily_field: str,
        delta: int,
        timestamp: datetime,
    ) -> None:
        filter_doc = {"_id": user_key(guild_id, user_id)}
        day_key, daily_field_name = self._parse_daily_field(daily_field)
        projection = {total_field: 1, "daily_stats": 1}
        existing = await self.users.find_one(filter_doc, projection)
        daily_value = 0
        if day_key and daily_field_name and existing:
            daily_value = existing.get("daily_stats", {}).get(day_key, {}).get(daily_field_name, 0)

        update_doc = {
            "$inc": {total_field: delta, daily_field: delta},
            "$set": {"guild_id": guild_id, "user_id": user_id, "updated_at": timestamp},
            "$setOnInsert": {
                "created_at": timestamp,
            },
        }

        if delta > 0:
            await self.users.update_one(filter_doc, update_doc, upsert=True)
            return

        if not existing:
            return
        current_total = existing.get(total_field, 0)
        if current_total <= 0 and daily_value <= 0:
            return
        safe_total_delta = delta if current_total + delta >= 0 else -current_total
        safe_daily_delta = delta if daily_value + delta >= 0 else -daily_value
        if safe_total_delta == 0 and safe_daily_delta == 0:
            return
        update_doc["$inc"] = {total_field: safe_total_delta, daily_field: safe_daily_delta}
        await self.users.update_one(filter_doc, update_doc, upsert=False)

    async def _update_server_reactions(self, guild_id: int, day: str, delta: int, timestamp: datetime) -> None:
        filter_doc = {"_id": str(guild_id)}
        update_doc = {
            "$inc": {f"daily_stats.{day}.reactions": delta},
            "$set": {"guild_id": guild_id, "updated_at": timestamp},
            "$setOnInsert": {"created_at": timestamp, "stats_channel_id": None, "stats_message_id": None},
        }

        if delta > 0:
            await self.servers.update_one(filter_doc, update_doc, upsert=True)
            return

        existing = await self.servers.find_one(filter_doc, {"daily_stats": 1})
        if not existing:
            return
        current = existing.get("daily_stats", {}).get(day, {}).get("reactions", 0)
        if current <= 0:
            return
        safe_delta = delta if current + delta >= 0 else -current
        if safe_delta == 0:
            return
        update_doc["$inc"] = {f"daily_stats.{day}.reactions": safe_delta}
        await self.servers.update_one(filter_doc, update_doc, upsert=False)

    # Read operations
    async def get_server_windows(self, guild_id: int, days: int, now: Optional[datetime] = None) -> Dict[str, int]:
        now = now or datetime.utcnow()
        window_days = max(days, 1)
        cutoff_date = (now - timedelta(days=window_days - 1)).date()
        server_doc = await self.servers.find_one({"_id": str(guild_id)})
        messages = 0
        reactions = 0
        active_users: Set[int] = set()
        if server_doc and "daily_stats" in server_doc:
            for day_key, day_stats in server_doc["daily_stats"].items():
                if not self._day_within_window(day_key, cutoff_date):
                    continue
                messages += day_stats.get("messages", 0)
                reactions += day_stats.get("reactions", 0)
                active_users.update(day_stats.get("active_users", []))
        return {"messages": messages, "reactions": reactions, "active_users": len(active_users)}

    async def get_top_users_by_messages(
        self, guild_id: int, days: int, limit: int = 5, now: Optional[datetime] = None
    ) -> List[Tuple[int, int]]:
        now = now or datetime.utcnow()
        window_days = max(days, 1)
        cutoff_date = (now - timedelta(days=window_days - 1)).date()
        cursor = self.users.find({"guild_id": guild_id})
        top: List[Tuple[int, int]] = []
        async for doc in cursor:
            daily_stats = doc.get("daily_stats", {})
            total = 0
            for day_key, value in daily_stats.items():
                if not self._day_within_window(day_key, cutoff_date):
                    continue
                total += value.get("messages", 0)
            if total > 0:
                top.append((doc.get("user_id"), total))
        top.sort(key=lambda item: item[1], reverse=True)
        return top[:limit]

    async def get_user_summary(self, guild_id: int, user_id: int, now: Optional[datetime] = None) -> Dict[str, int]:
        now = now or datetime.utcnow()
        doc = await self.users.find_one({"_id": user_key(guild_id, user_id)})
        if not doc:
            return {
                "total_messages": 0,
                "reactions_given": 0,
                "reactions_received": 0,
                "messages_7d": 0,
                "messages_30d": 0,
                "reactions_given_7d": 0,
            }
        daily_stats = doc.get("daily_stats", {})
        stats_7d = self._sum_daily(daily_stats, 7, now)
        stats_30d = self._sum_daily(daily_stats, 30, now)
        return {
            "total_messages": doc.get("total_messages", 0),
            "reactions_given": doc.get("reactions_given", 0),
            "reactions_received": doc.get("reactions_received", 0),
            "messages_7d": stats_7d.get("messages", 0),
            "messages_30d": stats_30d.get("messages", 0),
            "reactions_given_7d": stats_7d.get("reactions_given", 0),
        }

    def _sum_daily(self, daily_stats: Dict[str, Dict], days: int, now: datetime) -> Dict[str, int]:
        totals = {"messages": 0, "reactions_given": 0, "reactions_received": 0}
        window_days = max(days, 1)
        cutoff_date = (now - timedelta(days=window_days - 1)).date()
        for day_key, values in daily_stats.items():
            if not self._day_within_window(day_key, cutoff_date):
                continue
            totals["messages"] += values.get("messages", 0)
            totals["reactions_given"] += values.get("reactions_given", 0)
            totals["reactions_received"] += values.get("reactions_received", 0)
        return totals

    def _parse_daily_field(self, daily_field: str) -> Tuple[Optional[str], Optional[str]]:
        parts = daily_field.split(".")
        if len(parts) >= 3:
            return parts[1], parts[2]
        return None, None

    def _day_within_window(self, day_key: str, cutoff_date: date) -> bool:
        try:
            day_date = datetime.strptime(day_key, DATE_FORMAT).date()
        except ValueError:
            logger.warning("Skipping malformed day key %s", day_key)
            return False
        return day_date >= cutoff_date

    async def get_stats_message_id(self, guild_id: int) -> Optional[int]:
        meta = await self.servers.find_one({"_id": str(guild_id)}, {"stats_message_id": 1})
        if meta:
            return meta.get("stats_message_id")
        return None

    async def set_stats_message_id(self, guild_id: int, message_id: int) -> None:
        now = datetime.utcnow()
        await self.servers.update_one(
            {"_id": str(guild_id)},
            {
                "$set": {
                    "guild_id": guild_id,
                    "stats_message_id": message_id,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    async def set_stats_channel_id(self, guild_id: int, channel_id: int) -> None:
        now = datetime.utcnow()
        await self.servers.update_one(
            {"_id": str(guild_id)},
            {
                "$set": {"stats_channel_id": channel_id, "updated_at": now, "guild_id": guild_id},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
