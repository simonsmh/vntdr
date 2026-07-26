"""strategy platform tables

Revision ID: 20260725_01
Revises:
Create Date: 2026-07-25
"""
from alembic import op
import sqlalchemy as sa

revision = "20260725_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_versions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=False),
        sa.Column("factor_config", sa.JSON(), nullable=False),
        sa.Column("code_version", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("parent_id", sa.String(length=36), nullable=True),
    )
    op.create_index("ix_strategy_versions_strategy_name", "strategy_versions", ["strategy_name"])
    op.create_index("ix_strategy_versions_created_at", "strategy_versions", ["created_at"])
    op.create_table(
        "strategy_instances",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False, unique=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("asset_class", sa.String(length=32), nullable=False),
        sa.Column("calendar", sa.String(length=32), nullable=False),
        sa.Column("quote_currency", sa.String(length=16)),
        sa.Column("primary_interval", sa.String(length=16), nullable=False),
        sa.Column("auxiliary_intervals", sa.JSON(), nullable=False),
        sa.Column("execution_mode", sa.String(length=32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
    )
    op.create_index("ix_strategy_instances_name", "strategy_instances", ["name"])
    op.create_table(
        "strategy_activations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instance_id", sa.String(length=36), nullable=False),
        sa.Column("strategy_version_id", sa.String(length=36), nullable=False),
        sa.Column("effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_by", sa.String(length=128), nullable=False),
        sa.Column("rollback_of", sa.String(length=36)),
    )
    op.create_index("ix_strategy_activations_instance_id", "strategy_activations", ["instance_id"])
    op.create_index("ix_strategy_activations_strategy_version_id", "strategy_activations", ["strategy_version_id"])
    op.create_index("ix_strategy_activations_effective_at", "strategy_activations", ["effective_at"])
    op.create_table(
        "factor_observations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("factor_name", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interval", sa.String(length=16)),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.UniqueConstraint("symbol", "exchange", "factor_name", "observed_at", "interval", name="uq_factor_observation"),
    )
    op.create_index("ix_factor_observations_symbol", "factor_observations", ["symbol"])
    op.create_index("ix_factor_observations_factor_name", "factor_observations", ["factor_name"])
    op.create_index("ix_factor_observations_observed_at", "factor_observations", ["observed_at"])
    op.create_index("ix_factor_observations_available_at", "factor_observations", ["available_at"])


def downgrade() -> None:
    op.drop_table("factor_observations")
    op.drop_table("strategy_activations")
    op.drop_table("strategy_instances")
    op.drop_table("strategy_versions")
