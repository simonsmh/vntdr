from __future__ import annotations

import json
from datetime import datetime, timezone
from statistics import mean
from typing import Any, Literal
from uuid import UUID, uuid4

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


class Instrument(BaseModel):
    """A tradable or research-only instrument, independent of a venue's symbol syntax."""

    symbol: str
    exchange: str
    asset_class: Literal["crypto", "commodity", "equity", "fx", "index"] = "crypto"
    calendar: Literal["continuous", "weekday"] = "continuous"
    quote_currency: str | None = None

    @field_validator("symbol", "exchange")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        return value.strip().upper()


class Interval(BaseModel):
    """Normalised bar interval with a single canonical spelling."""

    value: str

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str) -> str:
        normalized = value.strip().lower()
        aliases = {"d": "1d", "day": "1d", "h": "1h", "hour": "1h", "min": "1m"}
        normalized = aliases.get(normalized, normalized)
        import re
        if not re.fullmatch(r"[1-9]\d*[mhdw]", normalized):
            raise ValueError("interval must look like 15m, 4h, 1d, or 1w")
        return normalized

    @property
    def seconds(self) -> int:
        unit_seconds = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
        return int(self.value[:-1]) * unit_seconds[self.value[-1]]


class StrategyVersion(BaseModel):
    """Immutable snapshot of executable strategy code/configuration."""

    id: UUID = Field(default_factory=uuid4)
    strategy_name: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    factor_config: dict[str, Any] = Field(default_factory=dict)
    code_version: str = "local"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    parent_id: UUID | None = None

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    def clone(self, *, parameters: dict[str, Any] | None = None, factor_config: dict[str, Any] | None = None) -> "StrategyVersion":
        return StrategyVersion(
            strategy_name=self.strategy_name,
            parameters=parameters if parameters is not None else self.parameters,
            factor_config=factor_config if factor_config is not None else self.factor_config,
            code_version=self.code_version,
            parent_id=self.id,
        )


class StrategyInstance(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    instrument: Instrument
    primary_interval: Interval
    auxiliary_intervals: list[Interval] = Field(default_factory=list)
    execution_mode: Literal["notify_only", "paper", "live"] = "notify_only"
    enabled: bool = True


class StrategyActivation(BaseModel):
    instance_id: UUID
    strategy_version_id: UUID
    effective_at: datetime
    approved_by: str = "system"
    rollback_of: UUID | None = None

    @field_validator("effective_at")
    @classmethod
    def normalize_effective_at(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class ValidationGate(BaseModel):
    backtest_passed: bool = False
    walk_forward_passed: bool = False
    shadow_passed: bool = False
    max_drawdown: float | None = None

    @property
    def approved(self) -> bool:
        return self.backtest_passed and self.walk_forward_passed and self.shadow_passed


class ShadowRun(BaseModel):
    """Auditable notification-only observation period for one strategy version."""

    id: UUID = Field(default_factory=uuid4)
    instance_id: UUID
    strategy_version_id: UUID
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_observed_at: datetime | None = None
    initial_equity: float = 1.0
    current_equity: float = 1.0
    peak_equity: float = 1.0
    max_drawdown: float = 0.0
    observation_count: int = 0
    status: Literal["active", "passed", "failed"] = "active"

    @field_validator("started_at", "last_observed_at")
    @classmethod
    def normalize_times(cls, value: datetime | None) -> datetime | None:
        return _ensure_utc(value) if value is not None else value


class FactorObservation(BaseModel):
    instrument: Instrument
    factor_name: str
    value: float
    observed_at: datetime
    available_at: datetime
    interval: Interval | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("observed_at", "available_at")
    @classmethod
    def normalize_factor_dates(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode="after")
    def validate_availability(self) -> "FactorObservation":
        if self.available_at < self.observed_at:
            raise ValueError("available_at cannot be earlier than observed_at")
        return self


class StrategyDecision(BaseModel):
    strategy_instance_id: UUID
    instrument: Instrument
    signal: float = Field(ge=-1.0, le=1.0)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reason: str = ""

    @field_validator("created_at")
    @classmethod
    def normalize_decision_time(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class PortfolioDecision(BaseModel):
    target_weights: dict[str, float] = Field(default_factory=dict)
    gross_exposure: float = 0.0
    net_exposure: float = 0.0
    scaling_reasons: list[str] = Field(default_factory=list)


class PortfolioRunResult(BaseModel):
    decisions: list[StrategyDecision] = Field(default_factory=list)
    portfolio: PortfolioDecision = Field(default_factory=PortfolioDecision)
    errors: dict[str, str] = Field(default_factory=dict)


class ResearchValidationResult(BaseModel):
    """Objective evidence used before a strategy version is approved."""

    backtest: "ResearchReport"
    walk_forward: "ResearchReport"
    passed: bool
    reasons: list[str] = Field(default_factory=list)
    max_drawdown_limit: float
    minimum_fold_count: int


class PositionSizingDecision(BaseModel):
    units: float
    notional: float
    risk_budget: float
    stop_distance: float
    capped: bool = False


class DataQualityReport(BaseModel):
    interval: Interval
    checked_at: datetime
    bar_count: int
    gaps_detected: int = 0
    stale: bool = False
    usable: bool = False
    reason: str | None = None

    @field_validator("checked_at")
    @classmethod
    def normalize_checked_at(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


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


class TradeRecord(BaseModel):
    """A completed, cost-inclusive trade produced by the event-driven backtest."""

    direction: Literal["long", "short"]
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    gross_return: float
    net_return: float
    bars_held: int
    transaction_cost: float = 0.0
    funding_cost: float = 0.0

    @field_validator("entry_time", "exit_time")
    @classmethod
    def normalize_trade_dates(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class MonitorResult(BaseModel):
    symbol: str
    interval: str
    strategy_name: str
    signal: int
    previous_signal: int | None = None
    best_parameters: dict[str, Any] = Field(default_factory=dict)
    actions: list[str] = Field(default_factory=list)
    notification_sent: bool = False
    error: str | None = None
    strategy_version_id: UUID | None = None


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
    exchange: str | None = None
    interval: str
    auxiliary_intervals: list[Interval] = Field(default_factory=list)
    start: datetime
    end: datetime
    mode: Literal["backtest", "optimize", "walk-forward"] = "backtest"
    method: str = "ga"
    parameters: dict[str, Any] = Field(default_factory=dict)
    parameter_space: dict[str, list[Any]] = Field(default_factory=dict)
    train_window: int | None = None
    test_window: int | None = None
    optimize_target: str = "sharpe"

    @field_validator("auxiliary_intervals", mode="before")
    @classmethod
    def normalize_auxiliary_intervals(cls, value: Any) -> list[Interval]:
        if value is None:
            return []
        return [item if isinstance(item, Interval) else Interval(value=str(item)) for item in value]

    @field_validator("start", "end")
    @classmethod
    def normalize_dates(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str | None) -> str | None:
        return value.strip().upper() if value else None

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


class AblationResult(BaseModel):
    strategy_name: str
    symbol: str
    interval: str
    variants: list[dict[str, Any]] = Field(default_factory=list)


def aggregate_metrics(metric_rows: list[dict[str, float]]) -> dict[str, float]:
    if not metric_rows:
        return {"total_return": 0.0, "sharpe_ratio": 0.0, "max_drawdown": 0.0, "trade_count": 0.0}
    keys = metric_rows[0].keys()
    return {key: mean(float(row.get(key, 0.0)) for row in metric_rows) for key in keys}
