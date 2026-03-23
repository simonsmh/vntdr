from __future__ import annotations

from vntdr.models import OrderInstruction


class SimulatedOrderExecutor:
    def execute(self, instructions: list[OrderInstruction]) -> list[OrderInstruction]:
        return instructions
