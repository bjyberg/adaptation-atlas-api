FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (tiny) for uvicorn[standard] (uvloop/httptools wheels are manylinux; keep minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
# Use uv for faster, deterministic installs; fall back on requirements.txt until a lockfile is added.
RUN pip install --no-cache-dir uv \
    && uv pip install --system --no-cache-dir -r /app/requirements.txt

# Pre-install DuckDB httpfs extension during build (best-effort).
# NOTE: avoid try/except in a python -c one-liner (requires newlines/indentation).
RUN python -c "import duckdb; con=duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('LOAD httpfs'); con.close()" || true

COPY app /app/app

EXPOSE 8000

# Uvicorn workers: set UVICORN_WORKERS env in docker-compose if desired
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
