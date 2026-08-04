"""store ETF daily OHLCV alongside money flow observations

Revision ID: 20260803_04
Revises: 20260730_03
Create Date: 2026-08-03
"""

from alembic import op
import sqlalchemy as sa


revision = "20260803_04"
down_revision = "20260730_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    existing = {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns("etf_money_flow_daily")
    }
    for name in (
        "open_price",
        "high_price",
        "low_price",
        "volume",
        "turnover",
        "turnover_rate",
    ):
        if name not in existing:
            op.add_column("etf_money_flow_daily", sa.Column(name, sa.Float(), nullable=True))


def downgrade() -> None:
    for name in (
        "turnover_rate",
        "turnover",
        "volume",
        "low_price",
        "high_price",
        "open_price",
    ):
        op.drop_column("etf_money_flow_daily", name)
