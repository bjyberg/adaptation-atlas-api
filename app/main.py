from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.cache import init_cache, close_cache
from app.climate_routes import router as climate_router
from app.exposure_routes import router as exposure_router
from app.haz_exposure_routes import router as haz_exposure_router
from app.settings import S
from app.utils import duckdb_connect


print("CORS config:", {"cors_origins": S.cors_origins, "cors_origin_regex": S.cors_origin_regex})



# ----------------------------
# App
# ----------------------------

app = FastAPI(title="Atlas Hazard Exposure Query API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    # If you set CORS_ORIGIN_REGEX, it takes precedence and allows matching origins (e.g., any localhost port).
    # NOTE: allow_origin_regex (if set) will also be honored by Starlette.
    allow_origins=(["*"] if ("*" in S.cors_origins) else S.cors_origins),
    allow_origin_regex=S.cors_origin_regex,
    # We don't use cookies/auth from the browser; credentials can stay off (also avoids '*' + credentials issues).
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(haz_exposure_router)
app.include_router(exposure_router)
app.include_router(climate_router)


@app.on_event("startup")
async def startup() -> None:
    await init_cache()

    # Warm up DuckDB + httpfs
    con = duckdb_connect(for_http_parquet=True)
    con.close()


@app.on_event("shutdown")
async def shutdown() -> None:
    await close_cache()


@app.get("/health")
async def health() -> Dict[str, Any]:
    r_ok = False
    try:
        from app.cache import redis_client

        if redis_client is not None:
            await redis_client.ping()
            r_ok = True
    except Exception:
        r_ok = False

    return {
        "ok": r_ok,
        "redis": r_ok,
        "duckdb_path": S.duckdb_path,
        "cache_materialize": S.cache_materialize,
        "allowed_hosts": ["*"] if S.allow_any_url else S.allowed_parquet_hosts,
        "allow_broad_geo": S.allow_broad_geo,
    }
