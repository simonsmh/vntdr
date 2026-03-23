from __future__ import annotations

from typing import Any

from vntdr.models import BarRecord

try:
    from vnpy_ctastrategy import CtaTemplate
except ImportError:  # pragma: no cover
    class CtaTemplate:  # type: ignore[override]
        pass


class ReviewedStrategyBase(CtaTemplate):
    """Compatibility base that can plug into VeighNa while staying testable."""

    @classmethod
    def signal_for_index(
        cls,
        bars: list[BarRecord],
        index: int,
        parameters: dict[str, Any],
    ) -> int:
        raise NotImplementedError
