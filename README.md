# Atlas Hazard Exposure Query API (FastAPI + Redis + DuckDB materialize)

This is a small server-side query service designed to make **Quarto OJS notebooks fast** when the raw Parquet datasets are very large.

**Key idea**

- The notebook sends filters (scenario/timeframe/geo/hazard_vars/commodities)
- The API runs the heavy query server-side (DuckDB)
- The API returns **small chart-ready JSON**
- Results are cached:
  - **Redis** = hot cache (fastest, TTL)
  - **DuckDB file** = _materialized cache_ that persists across restarts

---

## Quick start (Docker)

1. Open a terminal in this folder
2. Run:

```bash
docker compose up --build
```

API should be available at:

- `http://localhost:8000/health`
- Swagger docs: `http://localhost:8000/docs`

The materialized DuckDB cache is persisted in `./data/materialized_cache.duckdb`.

---
