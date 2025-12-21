import logging
from typing import Optional

import motor.motor_asyncio
from pymongo.errors import OperationFailure

logger = logging.getLogger(__name__)


class Mongo:
    def __init__(self, uri: str, db_name: str = "discord_stats"):
        self._uri = uri
        self._db_name = db_name
        self._client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None

    def client(self) -> motor.motor_asyncio.AsyncIOMotorClient:
        if self._client is None:
            logger.info("Connecting to MongoDB at %s", self._uri)
            self._client = motor.motor_asyncio.AsyncIOMotorClient(self._uri, uuidRepresentation="standard")
        return self._client

    def db(self):
        return self.client()[self._db_name]

    async def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


async def ensure_app_user(
    root_uri: str,
    username: str,
    password: str,
    db_name: str,
    root_auth_db: str = "admin",
) -> None:
    """Create the application user if it does not exist (idempotent)."""
    client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(root_uri, uuidRepresentation="standard")
        admin_db = client.get_database(root_auth_db)
        # Check if user already exists
        info = await admin_db.command({"usersInfo": {"user": username, "db": root_auth_db}})
        if info.get("users"):
            logger.info("Mongo app user %s already exists in %s", username, root_auth_db)
            return

        logger.info("Creating Mongo app user %s for db %s", username, db_name)
        await admin_db.command(
            {
                "createUser": username,
                "pwd": password,
                "roles": [
                    {"role": "readWrite", "db": db_name},
                    {"role": "dbAdmin", "db": db_name},
                ],
            }
        )
    except OperationFailure as exc:
        logger.error("Failed to ensure Mongo app user %s: %s", username, exc)
        raise
    finally:
        if client is not None:
            client.close()
