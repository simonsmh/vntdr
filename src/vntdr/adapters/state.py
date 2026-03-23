from __future__ import annotations

import redis


class RedisSignalStore:
    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    def get(self, key: str) -> int | None:
        value = self.client.get(key)
        return int(value) if value is not None else None

    def set(self, key: str, value: int) -> None:
        self.client.set(key, value)
