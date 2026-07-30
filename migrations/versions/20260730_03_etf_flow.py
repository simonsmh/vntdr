"""add ETF money-flow storage and ingestion audit tables

Revision ID: 20260730_03
Revises: 20260725_02
Create Date: 2026-07-30
"""
import sqlalchemy as sa
from alembic import op

revision = "20260730_03"
down_revision = "20260725_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "etf_money_flow_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=16), nullable=False),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("main_net_inflow", sa.Float(), nullable=True),
        sa.Column("main_inflow_ratio", sa.Float(), nullable=True),
        sa.Column("extra_large_net_inflow", sa.Float(), nullable=True),
        sa.Column("large_net_inflow", sa.Float(), nullable=True),
        sa.Column("large_inflow_ratio", sa.Float(), nullable=True),
        sa.Column("calculated_main_net_inflow", sa.Float(), nullable=True),
        sa.Column("main_component_gap", sa.Float(), nullable=True),
        sa.Column("close_price", sa.Float(), nullable=True),
        sa.Column("pct_change", sa.Float(), nullable=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.UniqueConstraint("symbol", "trade_date", name="uq_etf_money_flow_daily"),
    )
    op.create_index("ix_etf_money_flow_daily_symbol", "etf_money_flow_daily", ["symbol"])
    op.create_index("ix_etf_money_flow_daily_trade_date", "etf_money_flow_daily", ["trade_date"])
    op.create_index(
        "ix_etf_money_flow_daily_available_at", "etf_money_flow_daily", ["available_at"]
    )
    op.create_index(
        "ix_etf_money_flow_daily_fetched_at", "etf_money_flow_daily", ["fetched_at"]
    )

    op.create_table(
        "etf_flow_ingestion_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_key", sa.String(length=96), nullable=False, unique=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("requested_count", sa.Integer(), nullable=False),
        sa.Column("successful_count", sa.Integer(), nullable=False),
        sa.Column("failed_count", sa.Integer(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
    )
    op.create_index("ix_etf_flow_ingestion_runs_run_key", "etf_flow_ingestion_runs", ["run_key"])
    op.create_index(
        "ix_etf_flow_ingestion_runs_started_at", "etf_flow_ingestion_runs", ["started_at"]
    )


def downgrade() -> None:
    op.drop_table("etf_flow_ingestion_runs")
    op.drop_table("etf_money_flow_daily")
