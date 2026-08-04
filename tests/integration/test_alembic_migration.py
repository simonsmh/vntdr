from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from vntdr.config import Settings
from vntdr.storage.database import Database


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
    columns = {column["name"] for column in inspect(create_engine(db_url)).get_columns("etf_money_flow_daily")}
    assert {
        "open_price",
        "high_price",
        "low_price",
        "volume",
        "turnover",
        "turnover_rate",
    } <= columns


def test_entrypoint_stamps_complete_legacy_schema_before_upgrade(tmp_path) -> None:
    db_url = f"sqlite+pysqlite:///{tmp_path / 'legacy.sqlite'}"
    Database(db_url).create_schema()
    env = {**os.environ, "VNTDR_DATABASE_URL": db_url, "VNTDR_RUN_MIGRATIONS": "true"}
    python_dir = str(Path(sys.executable).parent)
    env["PATH"] = f"{python_dir}{os.pathsep}{env.get('PATH', '')}"
    result = subprocess.run(
        ["sh", "docker-entrypoint.sh", "true"],
        cwd=Path(__file__).parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    with create_engine(db_url).connect() as connection:
        version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert version == "20260803_04"
