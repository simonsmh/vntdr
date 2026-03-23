from __future__ import annotations

import json
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class BarRecord(BaseModel):
    symbol: str
    exchange: str = "OKX"
    interval: str
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    is_synthetic: bool = False

    @field_validator("datetime")
    @classmethod
    def normalize_datetime(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @property
    def key(self) -> tuple[str, str, str, datetime]:
        return (self.symbol, self.exchange, self.interval, self.datetime)


class CleanBarsResult(BaseModel):
    bars: list[BarRecord]
    duplicates_removed: int = 0
    gaps_detected: int = 0
    gaps_filled: int = 0


class SyncResult(BaseModel):
    job_id: int
    inserted_count: int
    cleaned_count: int
    duplicates_removed: int
    gaps_detected: int = 0
    gaps_filled: int = 0


class OrderInstruction(BaseModel):
    symbol: str
    action: str
    volume: float
    reason: str


class MonitorResult(BaseModel):
    symbol: str
    interval: str
    strategy_name: str
    signal: int
    previous_signal: int | None = None
    best_parameters: dict[str, Any] = Field(default_factory=dict)
    actions: list[str] = Field(default_factory=list)
    notification_sent: bool = False


class HealthCheckResult(BaseModel):
    ok: bool
    checks: dict[str, bool]
    details: dict[str, str] = Field(default_factory=dict)

    def lines(self) -> list[str]:
        lines = []
        for name, status in self.checks.items():
            suffix = self.details.get(name)
            line = f"{name}: {'ok' if status else 'failed'}"
            if suffix:
                line = f"{line} ({suffix})"
            lines.append(line)
        return lines


class FoldResult(BaseModel):
    fold_index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    metrics: dict[str, float]
    parameters: dict[str, Any]

    @field_validator("train_start", "train_end", "test_start", "test_end")
    @classmethod
    def normalize_dates(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class ResearchJobConfig(BaseModel):
    strategy_name: str
    symbol: str
    interval: str
    start: datetime
    end: datetime
    mode: Literal["backtest", "optimize", "walk-forward"] = "backtest"
    parameters: dict[str, Any] = Field(default_factory=dict)
    parameter_space: dict[str, list[Any]] = Field(default_factory=dict)
    train_window: int | None = None
    test_window: int | None = None

    @field_validator("start", "end")
    @classmethod
    def normalize_dates(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode="after")
    def validate_ranges(self) -> "ResearchJobConfig":
        if self.start >= self.end:
            raise ValueError("start must be earlier than end")
        if self.mode in {"optimize", "walk-forward"} and not self.parameter_space:
            raise ValueError("parameter_space is required for optimization modes")
        if self.mode == "walk-forward":
            if not self.train_window or not self.test_window:
                raise ValueError("train_window and test_window are required for walk-forward")
            if self.train_window <= 0 or self.test_window <= 0:
                raise ValueError("train_window and test_window must be positive")
        return self

    @property
    def report_slug(self) -> str:
        return "walk_forward" if self.mode == "walk-forward" else self.mode


class ResearchReport(BaseModel):
    strategy_name: str
    symbol: str
    interval: str
    mode: Literal["backtest", "optimize", "walk-forward"]
    metrics: dict[str, float]
    best_parameters: dict[str, Any] = Field(default_factory=dict)
    fold_results: list[FoldResult] = Field(default_factory=list)
    top_results: list[dict[str, Any]] = Field(default_factory=list)

    def to_markdown(self) -> str:
        lines = [
            f"# Research Report: {self.strategy_name}",
            "",
            f"- Symbol: {self.symbol}",
            f"- Interval: {self.interval}",
            f"- Mode: {self.mode}",
            "",
            "## Metrics",
        ]
        for key, value in self.metrics.items():
            pretty = key.replace("_", " ").title()
            lines.append(f"- {pretty}: {value}")
        if self.best_parameters:
            lines.extend(["", "## Best Parameters"])
            for key, value in self.best_parameters.items():
                lines.append(f"- {key}: {value}")
        if self.top_results:
            lines.extend(["", "## Top Results"])
            for entry in self.top_results:
                lines.append(f"- {entry}")
        if self.fold_results:
            lines.extend(["", "## Walk-Forward Folds"])
            for fold in self.fold_results:
                lines.append(
                    f"- Fold {fold.fold_index}: return={fold.metrics.get('total_return', 0.0)} "
                    f"sharpe={fold.metrics.get('sharpe_ratio', 0.0)} params={fold.parameters}"
                )
        return "\n".join(lines)

    def to_json(self) -> str:
        payload = self.model_dump(mode="json")
        return json.dumps(payload, indent=2, ensure_ascii=True)


def aggregate_metrics(metric_rows: list[dict[str, float]]) -> dict[str, float]:
    if not metric_rows:
        return {"total_return": 0.0, "sharpe_ratio": 0.0, "max_drawdown": 0.0, "trade_count": 0.0}
    keys = metric_rows[0].keys()
    return {key: mean(float(row.get(key, 0.0)) for row in metric_rows) for key in keys}
