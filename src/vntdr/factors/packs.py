"""Curated, versionable factor sets for the initial supported asset families."""
from __future__ import annotations

from dataclasses import dataclass, field

from vntdr.factors.base import FactorPlugin
from vntdr.factors.ohlcv import AtrRatioFactor, BreakoutFactor, TrendFactor


@dataclass(frozen=True)
class AssetPack:
    name: str
    asset_class: str
    factors: list[FactorPlugin] = field(default_factory=list)


def gold_pack() -> AssetPack:
    return AssetPack("gold", "commodity", [TrendFactor(20), BreakoutFactor(20), AtrRatioFactor(14)])


def equity_index_proxy_pack() -> AssetPack:
    return AssetPack("equity_index_proxy", "equity", [TrendFactor(50), BreakoutFactor(20), AtrRatioFactor(20)])


def crypto_pack() -> AssetPack:
    return AssetPack("crypto", "crypto", [TrendFactor(20), BreakoutFactor(20), AtrRatioFactor(14)])
