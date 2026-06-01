from __future__ import annotations

import redis
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class RedisSignalStore:
    def __init__(self, client: redis.Redis) -> None:
        self.client = client
        self._executor = ThreadPoolExecutor(max_workers=4)

    def get(self, key: str) -> int | None:
        value = self.client.get(key)
        if value is not None:
            logger.debug(f"Got signal {int(value)} for key {key} from Redis")
        return int(value) if value is not None else None

    async def get_async(self, key: str) -> int | None:
        return await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.get,
            key
        )

    def set(self, key: str, value: int) -> None:
        self.client.set(key, value)
        logger.debug(f"Set signal {value} for key {key} to Redis")

    async def set_async(self, key: str, value: int) -> None:
        await asyncio.get_event_loop().run_in_executor(
            self._executor,
            self.set,
            key,
            value
        )
