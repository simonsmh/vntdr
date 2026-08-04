"""Research-only multi-factor ranking model for the ETF observation pool.

The model is deliberately separated from the ingestion and order paths.  It
uses only fields known after a daily close, predicts whether a symbol will
outperform the cross-sectional median over the next holding horizon, and
evaluates signals in expanding walk-forward folds.  A positive model score is
not an order instruction.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from math import sqrt
from typing import Any

import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ETF_FACTOR_FEATURES: tuple[str, ...] = (
    "main_flow_ratio",
    "large_flow_ratio",
    "main_flow_100m",
    "extra_large_flow_100m",
    "large_flow_100m",
    "main_flow_3d_100m",
    "main_flow_5d_100m",
    "main_positive_days_5d",
    "return_1d",
    "return_3d",
    "return_5d",
    "close_ma5_gap",
    "close_ma20_gap",
    "volatility_10d",
    "volume_ratio_5d",
    "intraday_return",
    "range_pct",
    "flow_ratio_rank",
    "return_1d_rank",
)


@dataclass(frozen=True)
class EtfFactorModelConfig:
    """Controls the research window and cost assumptions."""

    horizon_days: int = 3
    min_train_days: int = 30
    test_days: int = 10
    step_days: int = 10
    top_k: int = 10
    cost_rate: float = 0.0015
    max_iter: int = 1000
    random_state: int = 42

    def __post_init__(self) -> None:
        if self.horizon_days < 1:
            raise ValueError("horizon_days must be >= 1")
        if self.min_train_days < 5:
            raise ValueError("min_train_days must be >= 5")
        if self.test_days < 1 or self.step_days < 1:
            raise ValueError("test_days and step_days must be >= 1")
        if self.top_k < 1:
            raise ValueError("top_k must be >= 1")
        if not 0 <= self.cost_rate < 1:
            raise ValueError("cost_rate must be in [0, 1)")


@dataclass
class EtfFactorModelResult:
    """Model outputs kept as tables so the CLI and Gradio can share them."""

    latest_scores: pd.DataFrame = field(default_factory=pd.DataFrame)
    fold_metrics: pd.DataFrame = field(default_factory=pd.DataFrame)
    event_returns: pd.DataFrame = field(default_factory=pd.DataFrame)
    feature_importance: pd.DataFrame = field(default_factory=pd.DataFrame)
    metrics: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    status: str = "insufficient_data"


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def build_etf_factor_frame(
    rows: Sequence[dict[str, Any]] | pd.DataFrame,
    *,
    horizon_days: int = 3,
) -> pd.DataFrame:
    """Create close-available factors and a next-open forward-return label."""

    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1")
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    required = {"symbol", "trade_date", "open_price", "close_price"}
    if frame.empty or not required.issubset(frame.columns):
        columns = ["symbol", "trade_date", *ETF_FACTOR_FEATURES, "forward_return", "target"]
        return pd.DataFrame(columns=columns)

    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.zfill(6)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "trade_date"]).copy()
    records: list[pd.DataFrame] = []
    for symbol, group in frame.groupby("symbol", sort=True):
        group = group.sort_values("trade_date").drop_duplicates("trade_date", keep="last").copy()
        close = _numeric(group, "close_price")
        open_price = _numeric(group, "open_price")
        high = _numeric(group, "high_price")
        low = _numeric(group, "low_price")
        volume = _numeric(group, "volume")
        main_flow = _numeric(group, "main_net_inflow")
        extra_large_flow = _numeric(group, "extra_large_net_inflow")
        large_flow = _numeric(group, "large_net_inflow")
        main_ratio = _numeric(group, "main_inflow_ratio")
        large_ratio = _numeric(group, "large_inflow_ratio")
        returns = close.pct_change()
        prior_volume = volume.rolling(5, min_periods=3).mean().shift(1)
        ma5 = close.rolling(5, min_periods=3).mean()
        ma20 = close.rolling(20, min_periods=10).mean()

        factors = pd.DataFrame(
            {
                "symbol": symbol,
                "trade_date": group["trade_date"].to_numpy(),
                "main_flow_ratio": main_ratio.to_numpy(),
                "large_flow_ratio": large_ratio.to_numpy(),
                "main_flow_100m": (main_flow / 100_000_000).to_numpy(),
                "extra_large_flow_100m": (extra_large_flow / 100_000_000).to_numpy(),
                "large_flow_100m": (large_flow / 100_000_000).to_numpy(),
                "main_flow_3d_100m": (
                    main_flow.rolling(3, min_periods=1).sum() / 100_000_000
                ).to_numpy(),
                "main_flow_5d_100m": (
                    main_flow.rolling(5, min_periods=3).sum() / 100_000_000
                ).to_numpy(),
                "main_positive_days_5d": main_flow.gt(0).rolling(5, min_periods=3).sum().to_numpy(),
                "return_1d": returns.to_numpy(),
                "return_3d": close.pct_change(3).to_numpy(),
                "return_5d": close.pct_change(5).to_numpy(),
                "close_ma5_gap": (close / ma5 - 1).to_numpy(),
                "close_ma20_gap": (close / ma20 - 1).to_numpy(),
                "volatility_10d": returns.rolling(10, min_periods=5).std().to_numpy(),
                "volume_ratio_5d": (volume / prior_volume).to_numpy(),
                "intraday_return": (close / open_price - 1).to_numpy(),
                "range_pct": ((high - low) / close).to_numpy(),
                "next_open": open_price.shift(-1).to_numpy(),
                "exit_close": close.shift(-horizon_days).to_numpy(),
            }
        )
        factors["forward_return"] = factors["exit_close"] / factors["next_open"] - 1
        records.append(factors)

    result = pd.concat(records, ignore_index=True)
    result["forward_median"] = result.groupby("trade_date")["forward_return"].transform("median")
    result["target"] = (result["forward_return"] > result["forward_median"]).astype("float64")
    result.loc[result["forward_return"].isna() | result["forward_median"].isna(), "target"] = pd.NA
    result["flow_ratio_rank"] = result.groupby("trade_date")["main_flow_ratio"].rank(pct=True)
    result["return_1d_rank"] = result.groupby("trade_date")["return_1d"].rank(pct=True)
    result["feature_ready"] = result[list(ETF_FACTOR_FEATURES)].notna().sum(axis=1) >= 8
    return result.sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def _model_pipeline(config: EtfFactorModelConfig, *, one_class: int | None = None) -> Pipeline:
    if one_class is None:
        estimator: Any = LogisticRegression(
            class_weight="balanced",
            max_iter=config.max_iter,
            random_state=config.random_state,
            C=0.5,
        )
    else:
        estimator = DummyClassifier(strategy="constant", constant=one_class)
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            ("scaler", StandardScaler()),
            ("model", estimator),
        ]
    )


def _fit_model(train: pd.DataFrame, config: EtfFactorModelConfig) -> Pipeline:
    labels = train["target"].astype(int)
    unique = labels.unique()
    model = _model_pipeline(config, one_class=int(unique[0]) if len(unique) < 2 else None)
    model.fit(train[list(ETF_FACTOR_FEATURES)], labels)
    return model


def _positive_probability(model: Pipeline, frame: pd.DataFrame) -> pd.Series:
    if hasattr(model, "predict_proba"):
        probabilities = model.predict_proba(frame[list(ETF_FACTOR_FEATURES)])
        classes = model.named_steps["model"].classes_
        if 1 in classes:
            return pd.Series(probabilities[:, list(classes).index(1)], index=frame.index)
        return pd.Series(0.0, index=frame.index)
    return pd.Series(
        model.predict(frame[list(ETF_FACTOR_FEATURES)]),
        index=frame.index,
        dtype="float64",
    )


def _max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1 + returns.fillna(0)).cumprod()
    return float((equity / equity.cummax() - 1).min())


def _sharpe(returns: pd.Series) -> float:
    if len(returns) < 2 or returns.std(ddof=1) == 0:
        return 0.0
    return float(returns.mean() / returns.std(ddof=1) * sqrt(len(returns)))


def _empty_result(frame: pd.DataFrame, warning: str) -> EtfFactorModelResult:
    return EtfFactorModelResult(
        latest_scores=pd.DataFrame(),
        fold_metrics=pd.DataFrame(),
        event_returns=pd.DataFrame(),
        feature_importance=pd.DataFrame(),
        metrics={"feature_rows": int(len(frame)), "fold_count": 0, "event_count": 0},
        warnings=[warning],
        status="insufficient_data",
    )


def run_etf_factor_model(
    rows: Sequence[dict[str, Any]] | pd.DataFrame,
    *,
    config: EtfFactorModelConfig | None = None,
) -> EtfFactorModelResult:
    """Fit an expanding walk-forward ETF factor ranker and score the latest day."""

    config = config or EtfFactorModelConfig()
    frame = build_etf_factor_frame(rows, horizon_days=config.horizon_days)
    if frame.empty:
        return _empty_result(
            frame,
            "没有包含 symbol/trade_date/open_price/close_price 的 ETF 日数据",
        )
    labeled = frame[frame["feature_ready"] & frame["target"].notna()].copy()
    dates = sorted(labeled["trade_date"].dt.date.unique())
    if len(dates) < config.min_train_days + config.test_days:
        return _empty_result(
            frame,
            f"可用交易日仅 {len(dates)} 个，至少需要 {config.min_train_days + config.test_days} 个",
        )

    fold_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    latest_model: Pipeline | None = None
    for train_end in range(
        config.min_train_days,
        len(dates) - config.test_days + 1,
        config.step_days,
    ):
        train_dates = dates[:train_end]
        test_dates = dates[train_end : train_end + config.test_days]
        train = labeled[labeled["trade_date"].dt.date.isin(train_dates)]
        test = frame[frame["trade_date"].dt.date.isin(test_dates) & frame["feature_ready"]].copy()
        if train.empty or test.empty:
            continue
        model = _fit_model(train, config)
        test["model_probability"] = _positive_probability(model, test)
        # Use non-overlapping signal events within each test fold so a 3-day
        # holding horizon is not counted as three independent trades.
        event_dates = test_dates[:: config.horizon_days]
        fold_event_returns: list[float] = []
        fold_benchmark_returns: list[float] = []
        for event_date in event_dates:
            day = test[test["trade_date"].dt.date == event_date].dropna(subset=["forward_return"])
            if day.empty:
                continue
            top = day.nlargest(min(config.top_k, len(day)), "model_probability")
            model_gross = float(top["forward_return"].mean())
            benchmark_gross = float(day["forward_return"].mean())
            model_net = model_gross - config.cost_rate
            benchmark_net = benchmark_gross - config.cost_rate
            fold_event_returns.append(model_net)
            fold_benchmark_returns.append(benchmark_net)
            event_rows.append(
                {
                    "fold": len(fold_rows) + 1,
                    "signal_date": event_date,
                    "selected_count": len(top),
                    "model_gross_return": model_gross,
                    "model_net_return": model_net,
                    "benchmark_net_return": benchmark_net,
                    "excess_net_return": model_net - benchmark_net,
                    "hit": int(model_net > 0),
                    "mean_probability": float(top["model_probability"].mean()),
                }
            )
        fold_rows.append(
            {
                "fold": len(fold_rows) + 1,
                "train_start": train_dates[0],
                "train_end": train_dates[-1],
                "test_start": test_dates[0],
                "test_end": test_dates[-1],
                "train_rows": len(train),
                "test_rows": len(test),
                "event_count": len(fold_event_returns),
                "model_mean_net_return": sum(fold_event_returns) / len(fold_event_returns)
                if fold_event_returns
                else 0.0,
                "benchmark_mean_net_return": (
                    sum(fold_benchmark_returns) / len(fold_benchmark_returns)
                )
                if fold_benchmark_returns
                else 0.0,
            }
        )

    all_train = labeled[labeled["trade_date"].dt.date < dates[-1]]
    if all_train.empty:
        return _empty_result(frame, "没有可用于拟合的历史标签")
    latest_model = _fit_model(all_train, config)
    latest_date = frame.loc[frame["feature_ready"], "trade_date"].max()
    latest = frame[frame["trade_date"] == latest_date].copy()
    latest["model_probability"] = _positive_probability(latest_model, latest)
    latest = latest.sort_values("model_probability", ascending=False).reset_index(drop=True)
    latest["rank"] = latest.index + 1
    latest["model_score"] = (latest["model_probability"] * 100).round(2)
    latest["trend"] = latest["model_probability"].map(
        lambda value: "偏强" if value >= 0.6 else "偏弱" if value <= 0.4 else "中性"
    )
    latest_scores = latest[
        [
            "trade_date", "symbol", "rank", "model_score", "model_probability", "trend",
            "main_flow_ratio", "main_flow_3d_100m", "return_1d", "return_5d",
            "close_ma5_gap", "volume_ratio_5d",
        ]
    ].copy()

    model_step = latest_model.named_steps["model"]
    if hasattr(model_step, "coef_"):
        coefficients = model_step.coef_[0]
        imputer = latest_model.named_steps["imputer"]
        missing_indexes = getattr(getattr(imputer, "indicator_", None), "features_", [])
        feature_names = list(ETF_FACTOR_FEATURES) + [
            f"missing_{ETF_FACTOR_FEATURES[index]}" for index in missing_indexes
        ]
        # SimpleImputer(add_indicator=True) appends indicators only for columns
        # that were missing in the training fold.
        feature_importance = pd.DataFrame(
            {"feature": feature_names[: len(coefficients)], "coefficient": coefficients}
        )
        feature_importance["abs_coefficient"] = feature_importance["coefficient"].abs()
        feature_importance = feature_importance.sort_values(
            "abs_coefficient", ascending=False
        ).reset_index(drop=True)
    else:
        feature_importance = pd.DataFrame(columns=["feature", "coefficient", "abs_coefficient"])

    events = pd.DataFrame(event_rows)
    folds = pd.DataFrame(fold_rows)
    warnings: list[str] = []
    if len(dates) < 80:
        warnings.append(f"样本只有 {len(dates)} 个可标记交易日，参数稳定性仍需更长历史验证")
    if len(events) < 20:
        warnings.append(f"非重叠样本外事件只有 {len(events)} 笔，不足以证明可盈利")
    model_returns = events["model_net_return"] if not events.empty else pd.Series(dtype=float)
    benchmark_returns = (
        events["benchmark_net_return"] if not events.empty else pd.Series(dtype=float)
    )
    metrics = {
        "feature_rows": int(len(frame)),
        "labeled_rows": int(len(labeled)),
        "trade_days": int(len(dates)),
        "fold_count": int(len(folds)),
        "event_count": int(len(events)),
        "horizon_days": config.horizon_days,
        "top_k": config.top_k,
        "cost_rate": config.cost_rate,
        "model_mean_net_return": float(model_returns.mean()) if not model_returns.empty else 0.0,
        "benchmark_mean_net_return": (
            float(benchmark_returns.mean()) if not benchmark_returns.empty else 0.0
        ),
        "excess_mean_net_return": float((model_returns - benchmark_returns).mean())
        if not model_returns.empty
        else 0.0,
        "model_compounded_return": float((1 + model_returns).prod() - 1)
        if not model_returns.empty
        else 0.0,
        "benchmark_compounded_return": float((1 + benchmark_returns).prod() - 1)
        if not benchmark_returns.empty
        else 0.0,
        "hit_rate": float((model_returns > 0).mean()) if not model_returns.empty else 0.0,
        "max_drawdown": _max_drawdown(model_returns),
        "sharpe_ratio": _sharpe(model_returns),
    }
    return EtfFactorModelResult(
        latest_scores=latest_scores,
        fold_metrics=folds,
        event_returns=events,
        feature_importance=feature_importance,
        metrics=metrics,
        warnings=warnings,
        status="ok" if len(events) > 0 else "insufficient_data",
    )


__all__ = [
    "ETF_FACTOR_FEATURES",
    "EtfFactorModelConfig",
    "EtfFactorModelResult",
    "build_etf_factor_frame",
    "run_etf_factor_model",
]
