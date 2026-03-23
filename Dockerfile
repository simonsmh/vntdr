FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md .python-version ./
RUN uv sync --frozen --extra veighna --group dev || uv sync --extra veighna --group dev

COPY src ./src
COPY docker ./docker
COPY .env.example ./

RUN mkdir -p /app/reports /app/.vntrader

CMD ["vntdr", "live"]
