from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class BarORM(Base):
    __tablename__ = "bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    interval: Mapped[str] = mapped_column(String(16), index=True)
    datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    is_synthetic: Mapped[bool] = mapped_column(Boolean, default=False)


class SyncJobORM(Base):
    __tablename__ = "sync_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    interval: Mapped[str] = mapped_column(String(16))
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), default="started")
    inserted_count: Mapped[int] = mapped_column(Integer, default=0)
    cleaned_count: Mapped[int] = mapped_column(Integer, default=0)
    duplicates_removed: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(String(255), nullable=True)


class ResearchRunORM(Base):
    __tablename__ = "research_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    mode: Mapped[str] = mapped_column(String(32), index=True)
    strategy_name: Mapped[str] = mapped_column(String(128))
    symbol: Mapped[str] = mapped_column(String(64))
    interval: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(32), default="started")
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    best_parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    top_results: Mapped[list] = mapped_column(JSON, default=list)
    report_path: Mapped[str | None] = mapped_column(String(255), nullable=True)


class WalkForwardFoldORM(Base):
    __tablename__ = "walk_forward_folds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    research_run_id: Mapped[int] = mapped_column(Integer, index=True)
    fold_index: Mapped[int] = mapped_column(Integer)
    train_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    train_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    test_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    test_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)


class StrategyVersionORM(Base):
    __tablename__ = "strategy_versions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(128), index=True)
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    factor_config: Mapped[dict] = mapped_column(JSON, default=dict)
    code_version: Mapped[str] = mapped_column(String(128), default="local")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    parent_id: Mapped[str | None] = mapped_column(String(36), nullable=True)


class StrategyInstanceORM(Base):
    __tablename__ = "strategy_instances"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    asset_class: Mapped[str] = mapped_column(String(32))
    calendar: Mapped[str] = mapped_column(String(32), default="continuous")
    quote_currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    primary_interval: Mapped[str] = mapped_column(String(16))
    auxiliary_intervals: Mapped[list] = mapped_column(JSON, default=list)
    execution_mode: Mapped[str] = mapped_column(String(32), default="notify_only")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class StrategyActivationORM(Base):
    __tablename__ = "strategy_activations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(36), index=True)
    strategy_version_id: Mapped[str] = mapped_column(String(36), index=True)
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    approved_by: Mapped[str] = mapped_column(String(128), default="system")
    rollback_of: Mapped[str | None] = mapped_column(String(36), nullable=True)


class ShadowRunORM(Base):
    __tablename__ = "shadow_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(36), index=True)
    strategy_version_id: Mapped[str] = mapped_column(String(36), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    last_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    initial_equity: Mapped[float] = mapped_column(Float, default=1.0)
    current_equity: Mapped[float] = mapped_column(Float, default=1.0)
    peak_equity: Mapped[float] = mapped_column(Float, default=1.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    observation_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default="active")


class FactorObservationORM(Base):
    __tablename__ = "factor_observations"
    __table_args__ = (
        UniqueConstraint("symbol", "exchange", "factor_name", "observed_at", "interval", name="uq_factor_observation"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    factor_name: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[float] = mapped_column(Float)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    interval: Mapped[str | None] = mapped_column(String(16), nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class EtfMoneyFlowDailyORM(Base):
    """Normalized daily ETF money-flow observations from the research source."""

    __tablename__ = "etf_money_flow_daily"
    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_etf_money_flow_daily"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(16), index=True)
    market: Mapped[str] = mapped_column(String(8))
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    main_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True)
    main_inflow_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra_large_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True)
    large_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True)
    large_inflow_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    calculated_main_net_inflow: Mapped[float | None] = mapped_column(Float, nullable=True)
    main_component_gap: Mapped[float | None] = mapped_column(Float, nullable=True)
    close_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pct_change: Mapped[float | None] = mapped_column(Float, nullable=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    source: Mapped[str] = mapped_column(String(32), default="akshare")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)


class EtfFlowIngestionRunORM(Base):
    """Audit record for one scheduled ETF-flow ingestion run."""

    __tablename__ = "etf_flow_ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_key: Mapped[str] = mapped_column(String(96), unique=True, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="started")
    requested_count: Mapped[int] = mapped_column(Integer, default=0)
    successful_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    details: Mapped[dict] = mapped_column(JSON, default=dict)


class Database:
    def __init__(self, url: str) -> None:
        self.url = url
        connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
        self.engine = create_engine(url, future=True, connect_args=connect_args)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False, class_=Session)

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def ping(self) -> bool:
        with self.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
