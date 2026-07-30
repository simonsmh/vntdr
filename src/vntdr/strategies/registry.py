"""Built-in strategy discovery and UI metadata.

The research engine only needs a module name. This registry adds the metadata
needed by the Gradio UI without making the engine depend on Gradio.
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Any

import vntdr.strategies as strategies_package

_DISPLAY_ORDER = ("cm_macd_ult_mtf", "kdj", "rsi", "volume", "multi_factor", "demo_momentum")
_DISPLAY_LABELS = {
    "cm_macd_ult_mtf": "CM MACD Ult MTF",
    "multi_factor": "多因子策略",
    "demo_momentum": "示例动量策略",
}


def available_strategy_names() -> list[str]:
    names: list[str] = []
    for module_info in pkgutil.iter_modules(strategies_package.__path__):
        name = module_info.name
        if name.startswith("_") or name in {"base", "indicators", "registry"}:
            continue
        try:
            module = importlib.import_module(f"vntdr.strategies.{name}")
        except ImportError:
            continue
        if getattr(module, "Strategy", None) is not None:
            names.append(name)
    return sorted(names, key=lambda name: (_DISPLAY_ORDER.index(name) if name in _DISPLAY_ORDER else 999, name))


def strategy_configs() -> dict[str, dict[str, Any]]:
    configs: dict[str, dict[str, Any]] = {}
    for name in available_strategy_names():
        module = importlib.import_module(f"vntdr.strategies.{name}")
        configs[name] = {
            "label": getattr(module, "STRATEGY_LABEL", _DISPLAY_LABELS.get(name, name)),
            "description": getattr(module, "STRATEGY_DESCRIPTION", ""),
            "defaults": dict(getattr(module, "DEFAULT_PARAMETERS", {})),
            "space": dict(getattr(module, "DEFAULT_PARAMETER_SPACE", {})),
            "bounds": dict(getattr(module, "DEFAULT_PARAMETER_BOUNDS", getattr(module, "DEFAULT_PARAMETER_SPACE", {}))),
        }
    return configs
