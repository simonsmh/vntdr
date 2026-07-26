from __future__ import annotations

from typing import Protocol

from vntdr.models import BarRecord


class FactorPlugin(Protocol):
    name: str

    def compute(self, bars: list[BarRecord], index: int) -> float | None: ...
