from __future__ import annotations

from dataclasses import dataclass

from vntdr.config import Settings


@dataclass
class NeverExecutor:
    calls: int = 0

    def execute(self, instructions):
        self.calls += 1
        raise AssertionError("notify_only must not submit orders")


def test_notify_only_is_the_safe_default(env_map: dict[str, str]) -> None:
    settings = Settings.from_mapping(env_map)
    assert settings.research.execution_mode == "notify_only"
