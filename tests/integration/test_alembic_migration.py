from __future__ import annotations

import os
import subprocess
import sys

from sqlalchemy import create_engine, inspect


def test_alembic_upgrade_creates_strategy_platform_tables(tmp_path) -> None:
    db_url = f"sqlite+pysqlite:///{tmp_path / 'migrated.sqlite'}"
    env = {**os.environ, "VNTDR_DATABASE_URL": db_url}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd="/home/ubuntu/vntdr", env=env, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    tables = set(inspect(create_engine(db_url)).get_table_names())
    assert {
        "strategy_versions",
        "strategy_instances",
        "strategy_activations",
        "factor_observations",
        "etf_money_flow_daily",
        "etf_flow_ingestion_runs",
    } <= tables
