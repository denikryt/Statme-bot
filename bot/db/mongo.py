import logging
from typing import Optional

import motor.motor_asyncio

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
