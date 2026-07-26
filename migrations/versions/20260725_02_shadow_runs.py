"""add auditable shadow runs

Revision ID: 20260725_02
Revises: 20260725_01
Create Date: 2026-07-25
"""
from alembic import op
import sqlalchemy as sa


revision = "20260725_02"
down_revision = "20260725_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "shadow_runs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("instance_id", sa.String(length=36), nullable=False),
        sa.Column("strategy_version_id", sa.String(length=36), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(timezone=True)),
        sa.Column("initial_equity", sa.Float(), nullable=False),
        sa.Column("current_equity", sa.Float(), nullable=False),
        sa.Column("peak_equity", sa.Float(), nullable=False),
        sa.Column("max_drawdown", sa.Float(), nullable=False),
        sa.Column("observation_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
    )
    op.create_index("ix_shadow_runs_instance_id", "shadow_runs", ["instance_id"])
    op.create_index("ix_shadow_runs_strategy_version_id", "shadow_runs", ["strategy_version_id"])
    op.create_index("ix_shadow_runs_started_at", "shadow_runs", ["started_at"])


def downgrade() -> None:
    op.drop_table("shadow_runs")
