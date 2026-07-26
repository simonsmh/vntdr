from __future__ import annotations

from uuid import uuid4

from vntdr.models import Instrument, StrategyDecision
from vntdr.services.portfolio import PortfolioAllocator


def _decision(symbol: str, asset_class: str, signal: float = 1.0) -> StrategyDecision:
    return StrategyDecision(strategy_instance_id=uuid4(), instrument=Instrument(symbol=symbol, exchange="OKX", asset_class=asset_class), signal=signal)


def test_allocator_nets_symbols_and_applies_asset_and_gross_caps() -> None:
    result = PortfolioAllocator(max_strategy_weight=0.30, max_symbol_weight=0.30, max_asset_class_weight=0.50, max_gross_exposure=0.40).allocate([
        _decision("BTC-USDT-SWAP", "crypto"), _decision("ETH-USDT-SWAP", "crypto"), _decision("XAU-USDT-SWAP", "commodity"),
    ])
    assert result.gross_exposure == 0.4
    assert result.target_weights["BTC-USDT-SWAP"] < 0.3
    assert "asset_class_cap:crypto" in result.scaling_reasons
    assert "gross_exposure_cap" in result.scaling_reasons


def test_allocator_nets_opposing_same_symbol_signals() -> None:
    result = PortfolioAllocator().allocate([_decision("BTC-USDT-SWAP", "crypto", 1), _decision("BTC-USDT-SWAP", "crypto", -1)])
    assert result.target_weights["BTC-USDT-SWAP"] == 0
    assert result.gross_exposure == 0


def test_allocator_scales_high_volatility_and_correlated_cluster() -> None:
    result = PortfolioAllocator(
        max_strategy_weight=0.30,
        max_symbol_weight=0.30,
        max_asset_class_weight=1.0,
        max_gross_exposure=1.0,
        target_annual_volatility=0.10,
        max_correlation_cluster_weight=0.20,
    ).allocate(
        [_decision("BTC-USDT-SWAP", "crypto"), _decision("ETH-USDT-SWAP", "crypto")],
        annualized_volatility_by_symbol={"BTC-USDT-SWAP": 0.20},
        correlations={("BTC-USDT-SWAP", "ETH-USDT-SWAP"): 0.95},
    )

    assert "volatility_target:BTC-USDT-SWAP" in result.scaling_reasons
    assert "correlation_cluster_cap:BTC-USDT-SWAP|ETH-USDT-SWAP" in result.scaling_reasons
    assert result.gross_exposure == 0.20
