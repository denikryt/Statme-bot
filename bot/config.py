import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

@dataclass
class Config:
    discord_token: str
    mongo_uri: str
    mongo_db_name: str
    mongo_users_collection: str
    mongo_servers_collection: str
    mongo_meta_collection: str
    stats_channel_id: int
    guild_id: Optional[int]
    log_level: int = logging.INFO


def load_config() -> Config:
    # Load variables from a local .env file if present
    load_dotenv()

    token = os.getenv("DISCORD_TOKEN")
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db_name = os.getenv("MONGO_DB") or os.getenv("MONGO_DB_NAME", "discord_stats")
    default_collection = os.getenv("MONGO_COLLECTION")
    mongo_users_collection = os.getenv("MONGO_USERS_COLLECTION", default_collection or "users")
    mongo_servers_collection = os.getenv("MONGO_SERVERS_COLLECTION", default_collection or "servers")
    mongo_meta_collection = os.getenv("MONGO_META_COLLECTION", default_collection or "meta")
    stats_channel = os.getenv("STATS_CHANNEL_ID")
    guild_id = os.getenv("GUILD_ID")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if not token:
        raise RuntimeError("DISCORD_TOKEN is required")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is required")
    if not stats_channel:
        raise RuntimeError("STATS_CHANNEL_ID is required")

    guild_value: Optional[int] = int(guild_id) if guild_id else None

    return Config(
        discord_token=token,
        mongo_uri=mongo_uri,
        mongo_db_name=mongo_db_name,
        mongo_users_collection=mongo_users_collection,
        mongo_servers_collection=mongo_servers_collection,
        mongo_meta_collection=mongo_meta_collection,
        stats_channel_id=int(stats_channel),
        guild_id=guild_value,
        log_level=getattr(logging, log_level, logging.INFO),
    )
