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
    mongo_app_username: Optional[str] = None
    mongo_app_password: Optional[str] = None
    mongo_root_uri: Optional[str] = None
    mongo_root_auth_db: str = "admin"
    log_level: int = logging.INFO


def load_config() -> Config:
    # Load variables from a local .env file if present
    load_dotenv()

    token = os.getenv("DISCORD_TOKEN")
    mongo_user = os.getenv("MONGO_APP_USERNAME")
    mongo_password = os.getenv("MONGO_APP_PASSWORD")
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db_name = (
        os.getenv("MONGO_APP_DB")
        or os.getenv("MONGO_DB")
        or os.getenv("MONGO_DB_NAME", "discord_stats")
    )
    default_collection = os.getenv("MONGO_COLLECTION")
    mongo_users_collection = os.getenv("MONGO_USERS_COLLECTION", default_collection or "users")
    mongo_servers_collection = os.getenv("MONGO_SERVERS_COLLECTION", default_collection or "servers")
    mongo_meta_collection = os.getenv("MONGO_META_COLLECTION", default_collection or "meta")
    stats_channel = os.getenv("STATS_CHANNEL_ID")
    guild_id = os.getenv("GUILD_ID")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    mongo_host = os.getenv("MONGO_HOST", "localhost")
    mongo_port = os.getenv("MONGO_PORT", "27017")
    if not mongo_uri:
        # Build a connection string from app creds if a full URI is not provided
        auth_db = os.getenv("MONGO_AUTH_DB") or mongo_db_name or os.getenv("MONGO_INITDB_DATABASE")

        if mongo_user and mongo_password:
            auth_source = os.getenv("MONGO_AUTH_SOURCE", auth_db or "admin")
            target_db = mongo_db_name or auth_db or "admin"
            mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{target_db}?authSource={auth_source}"
        else:
            mongo_uri = f"mongodb://{mongo_host}:{mongo_port}"

    # Optional root URI for creating the app user on startup
    mongo_root_auth_db = os.getenv("MONGO_ROOT_AUTH_DB", "admin")
    mongo_root_uri = os.getenv("MONGO_ROOT_URI")
    if not mongo_root_uri:
        root_user = os.getenv("MONGO_ROOT_USERNAME")
        root_password = os.getenv("MONGO_ROOT_PASSWORD")
        if root_user and root_password:
            mongo_root_uri = f"mongodb://{root_user}:{root_password}@{mongo_host}:{mongo_port}/?authSource={mongo_root_auth_db}"

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
        mongo_app_username=mongo_user,
        mongo_app_password=mongo_password,
        mongo_root_uri=mongo_root_uri,
        mongo_root_auth_db=mongo_root_auth_db,
        log_level=getattr(logging, log_level, logging.INFO),
    )
