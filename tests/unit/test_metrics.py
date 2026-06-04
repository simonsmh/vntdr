from __future__ import annotations

import math
import pytest
from vntdr.services.metrics import calculate_metrics

def test_calculate_metrics_empty():
    res = calculate_metrics(returns=[], equity_curve=[], trade_count=10)
    assert res == {
        "total_return": 0.0,
        "sharpe_ratio": 0.0,
        "max_drawdown": 0.0,
        "trade_count": 10.0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
    }

def test_calculate_metrics_typical():
    # Setup some test returns
    # Average return = 0.01
    # Sample standard deviation = sqrt(0.00055) = 0.023452
    returns = [0.03, -0.01, 0.02, -0.02, 0.03]
    equity_curve = [1.0, 1.03, 1.0197, 1.040094, 1.019292, 1.049871]
    
    # Let's test with interval = '1d' (periods_per_year = 365)
    res = calculate_metrics(returns, equity_curve, trade_count=5, interval="1d")
    
    assert res["trade_count"] == 5.0
    assert res["total_return"] == round(equity_curve[-1] - 1, 6)
    
    # Calculate expected Sharpe manually:
    # mean = 0.01, stdev = 0.02345207879911715
    # (0.01 / 0.02345207879911715) * sqrt(365) = 0.4264014294465492 * 19.1049731745428 = 8.1464
    assert abs(res["sharpe_ratio"] - 8.1464) < 0.0001
    
    # Max drawdown check
    # peak of [1.0, 1.03, 1.0197, 1.040094, 1.019292, 1.049871]
    # At 1.0197, dd = (1.0197 / 1.03) - 1 = -0.01
    # At 1.019292, dd = (1.019292 / 1.040094) - 1 = -0.02
    # So max drawdown = -0.02
    assert res["max_drawdown"] == -0.02
    
    # Win rate: 3 positive, 2 negative = 3/5 = 0.6
    assert res["win_rate"] == 0.6
    
    # Profit factor: sum_pos = 0.03 + 0.02 + 0.03 = 0.08
    # sum_neg = 0.01 + 0.02 = 0.03
    # profit_factor = 0.08 / 0.03 = 2.6667
    assert res["profit_factor"] == 2.6667

def test_calculate_metrics_no_volatility():
    # All returns are identical, standard deviation is zero
    returns = [0.01, 0.01, 0.01]
    equity_curve = [1.0, 1.01, 1.0201, 1.030301]
    
    res = calculate_metrics(returns, equity_curve, trade_count=3, interval="1h")
    assert res["sharpe_ratio"] == 0.0
    assert res["max_drawdown"] == 0.0
    assert res["win_rate"] == 1.0

def test_calculate_metrics_profit_factor_limits():
    # Only positive returns (no loss)
    res_only_win = calculate_metrics([0.01, 0.02], [1.0, 1.01, 1.0302], 2)
    assert res_only_win["profit_factor"] == 99.9
    
    # Only negative returns (no win)
    res_only_loss = calculate_metrics([-0.01, -0.02], [1.0, 0.99, 0.9702], 2)
    assert res_only_loss["profit_factor"] == 0.0
