#!/bin/sh
set -eu

if [ "${VNTDR_RUN_MIGRATIONS:-false}" = "true" ]; then
  export VNTDR_DATABASE_URL="$(python -c 'from vntdr.config import Settings; print(Settings.from_env().database.dsn)')"
  alembic upgrade head
fi

exec "$@"
