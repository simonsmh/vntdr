#!/bin/sh
set -eu

if [ "${VNTDR_RUN_MIGRATIONS:-false}" = "true" ]; then
  export VNTDR_DATABASE_URL="$(python -c 'from vntdr.config import Settings; print(Settings.from_env().database.dsn)')"
  # Older images called Database.create_schema() before Alembic was enabled.
  # Such databases contain the complete initial schema but no alembic_version
  # row. Stamp only that verified legacy baseline; never mask a partial schema
  # or a database already managed by Alembic.
  if python - <<'PY'
from sqlalchemy import create_engine, inspect, text

from vntdr.config import Settings

required = {
    "strategy_versions",
    "strategy_instances",
    "strategy_activations",
    "factor_observations",
    "shadow_runs",
    "etf_money_flow_daily",
    "etf_flow_ingestion_runs",
}
engine = create_engine(Settings.from_env().database.dsn)
tables = set(inspect(engine).get_table_names())
if "alembic_version" in tables:
    with engine.connect() as connection:
        if connection.execute(text("SELECT COUNT(*) FROM alembic_version")).scalar_one():
            raise SystemExit(1)
if not required.issubset(tables):
    raise SystemExit(1)
PY
  then
    echo "Detected complete legacy schema without Alembic state; stamping baseline 20260730_03."
    alembic stamp 20260730_03
  fi
  alembic upgrade head
fi

exec "$@"
