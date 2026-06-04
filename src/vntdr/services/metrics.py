from __future__ import annotations

import math
from statistics import mean, stdev

def calculate_metrics(
    returns: list[float],
    equity_curve: list[float],
    trade_count: int,
    interval: str = "1h",
) -> dict[str, float]:
    """
    Calculate performance metrics from step returns and equity curve.
    """
    if not returns:
        return {
            "total_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "trade_count": float(trade_count),
            "win_rate": 0.0,
            "profit_factor": 0.0,
        }
        
    avg_return = mean(returns)
    volatility = stdev(returns) if len(returns) > 1 else 0.0
    
    # Annualization factors based on interval
    periods_map = {
        "1m": 525600, "3m": 175200, "5m": 105120, "15m": 35040, 
        "30m": 17520, "1h": 8760, "2h": 4380, "4h": 2190, 
        "6h": 1460, "12h": 730, "1d": 365
    }
    periods_per_year = periods_map.get(interval.lower(), 8760)
    
    if volatility > 0:
        sharpe = (avg_return / volatility) * math.sqrt(periods_per_year)
    else:
        sharpe = 0.0
        
    pos_returns = [r for r in returns if r > 0]
    neg_returns = [r for r in returns if r < 0]
    win_rate = len(pos_returns) / (len(pos_returns) + len(neg_returns)) if (len(pos_returns) + len(neg_returns)) > 0 else 0.0
    
    sum_pos = sum(pos_returns)
    sum_neg = abs(sum(neg_returns))
    profit_factor = sum_pos / sum_neg if sum_neg > 0 else (99.9 if sum_pos > 0 else 0.0)

    peak = equity_curve[0]
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        dd = (value / peak) - 1
        if dd < max_drawdown:
            max_drawdown = dd
            
    return {
        "total_return": round(equity_curve[-1] - 1, 6),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown": round(max_drawdown, 4),
        "trade_count": float(trade_count),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4),
    }
