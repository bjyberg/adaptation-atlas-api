import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Settings:
    redis_url: str
    duckdb_path: str
    duckdb_threads: int
    cache_ttl_seconds: int
    cache_clear_token: str
    cache_clear_local_only: bool
    cache_materialize: bool
    materialize_keep_days: int

    allow_any_url: bool
    allowed_parquet_hosts: List[str]

    cors_origins: List[str]
    cors_origin_regex: Optional[str]
    allow_broad_geo: bool

    parquet_magic_check: bool
    export_max_rows: int
    admin_lookup_path: str
    admin_lookup_enable: bool

    @classmethod
    def from_env(cls) -> "Settings":
        def _bool(name: str, default: str) -> bool:
            return os.getenv(name, default).strip().lower() == "true"

        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        duckdb_path = os.getenv("DUCKDB_DB_PATH", "/data/materialized_cache.duckdb")
        duckdb_threads = int(os.getenv("DUCKDB_THREADS", "8"))
        cache_ttl_seconds = int(os.getenv("CACHE_TTL_SECONDS", "86400"))
        cache_clear_token = os.getenv("CACHE_CLEAR_TOKEN", "").strip()
        cache_clear_local_only = _bool("CACHE_CLEAR_LOCAL_ONLY", "true")
        cache_materialize = _bool("CACHE_MATERIALIZE", "true")
        materialize_keep_days = int(os.getenv("MATERIALIZE_KEEP_DAYS", "30"))

        allow_any_url = _bool("ALLOW_ANY_URL", "false")
        allowed_parquet_hosts = [
            h.strip().lower()
            for h in os.getenv("ALLOWED_PARQUET_HOSTS", "digital-atlas.s3.amazonaws.com").split(",")
            if h.strip()
        ]

        cors_origins = [
            o.strip()
            for o in os.getenv(
                "CORS_ORIGINS",
                "http://localhost:4774,http://127.0.0.1:4774,http://localhost:8000,http://127.0.0.1:8000",
            ).split(",")
            if o.strip()
        ]

        # Optional: allow origin regex (useful for Quarto preview random ports in dev)
        cors_origin_regex = os.getenv("CORS_ORIGIN_REGEX", "").strip() or r"^https?://(localhost|127\.0\.0\.1)(:\\d+)?$"

        allow_broad_geo = _bool("ALLOW_BROAD_GEO", "false")

        parquet_magic_check = _bool("PARQUET_MAGIC_CHECK", "true")
        export_max_rows = int(os.getenv("EXPORT_MAX_ROWS", "200000"))
        admin_lookup_path = os.getenv("ADMIN_LOOKUP_PATH", "/data/admin_lookup.parquet")
        admin_lookup_enable = _bool("ADMIN_LOOKUP_ENABLE", "true")

        return cls(
            redis_url=redis_url,
            duckdb_path=duckdb_path,
            duckdb_threads=duckdb_threads,
            cache_ttl_seconds=cache_ttl_seconds,
            cache_clear_token=cache_clear_token,
            cache_clear_local_only=cache_clear_local_only,
            cache_materialize=cache_materialize,
            materialize_keep_days=materialize_keep_days,
            allow_any_url=allow_any_url,
            allowed_parquet_hosts=allowed_parquet_hosts,
            cors_origins=cors_origins,
            cors_origin_regex=cors_origin_regex,
            allow_broad_geo=allow_broad_geo,
            parquet_magic_check=parquet_magic_check,
            export_max_rows=export_max_rows,
            admin_lookup_path=admin_lookup_path,
            admin_lookup_enable=admin_lookup_enable,
        )


S = Settings.from_env()
