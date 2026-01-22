import json
from typing import Any, Dict, List, Optional, Tuple

from redis.asyncio import Redis

from app.settings import S
from app.utils import duckdb_connect


class CacheStore:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    def init_materialized_cache(self) -> None:
        if not S.cache_materialize:
            return
        con = duckdb_connect(for_http_parquet=False)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS response_cache (
                  cache_key VARCHAR PRIMARY KEY,
                  response_json VARCHAR,
                  created_at TIMESTAMP
                );
                """
            )
            # cleanup old entries
            if S.materialize_keep_days > 0:
                # DuckDB does not support parameter placeholders in INTERVAL.
                # This value comes from server config (not user input), so safe to inline.
                keep_days = int(S.materialize_keep_days)
                con.execute(
                    f"DELETE FROM response_cache WHERE created_at < (now() - INTERVAL '{keep_days} days')"
                )
        finally:
            con.close()

    async def get_json(self, key: str, ttl_seconds: Optional[int]) -> Tuple[Optional[Any], str]:
        # ttl_seconds == -1 means cache disabled for this request
        if ttl_seconds == -1:
            return None, "disabled"

        # 1) Redis hit
        cached = await self.redis.get(key)
        if cached:
            try:
                return json.loads(cached), "redis"
            except Exception:
                # fall through
                pass

        # 2) DuckDB materialized cache hit
        if not S.cache_materialize:
            return None, "miss"

        con = duckdb_connect(for_http_parquet=False)
        try:
            row = con.execute(
                "SELECT response_json FROM response_cache WHERE cache_key = ?",
                [key],
            ).fetchone()
        finally:
            con.close()

        if not row:
            return None, "miss"

        raw = row[0]
        # refresh Redis (so next request can hit Redis quickly)
        if ttl_seconds is None or (isinstance(ttl_seconds, int) and ttl_seconds <= 0):
            await self.redis.set(key, raw)
        else:
            await self.redis.set(key, raw, ex=int(ttl_seconds))

        try:
            return json.loads(raw), "duckdb"
        except Exception:
            return None, "miss"

    async def set_json(self, key: str, value: Any, ttl_seconds: Optional[int]) -> None:
        raw = json.dumps(value, separators=(",", ":"))

        # ttl_seconds semantics:
        #  -1   => do not write cache
        #  None => persist without expiry
        #  >0   => expire in seconds
        if ttl_seconds == -1:
            return
        if ttl_seconds is None:
            await self.redis.set(key, raw)
        else:
            # Redis rejects EX=0 / negative; guard here just in case a caller bypassed _ttl()
            if int(ttl_seconds) <= 0:
                await self.redis.set(key, raw)
            else:
                await self.redis.set(key, raw, ex=int(ttl_seconds))

        # materialize to DuckDB
        if not S.cache_materialize:
            return

        con = duckdb_connect(for_http_parquet=False)
        try:
            con.execute("DELETE FROM response_cache WHERE cache_key = ?", [key])
            con.execute(
                "INSERT INTO response_cache VALUES (?, ?, now())",
                [key, raw],
            )
        finally:
            con.close()

    async def clear_prefixes(self, prefixes: List[str], *, dry_run: bool = False, batch_size: int = 1000) -> Dict[str, Any]:
        """Delete cached responses for one or more key prefixes (e.g., 'by_admin', 'q1')."""
        deleted_total = 0
        patterns = [f"{p}:*" for p in prefixes if p]
        details: Dict[str, int] = {p: 0 for p in prefixes if p}

        for p, pattern in zip([p for p in prefixes if p], patterns):
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(cursor=cursor, match=pattern, count=batch_size)
                if keys:
                    if not dry_run:
                        # UNLINK is preferred (non-blocking). Fallback to DEL if needed.
                        try:
                            n = await self.redis.unlink(*keys)
                        except Exception:
                            n = await self.redis.delete(*keys)
                        details[p] += int(n or 0)
                        deleted_total += int(n or 0)
                    else:
                        details[p] += len(keys)
                        deleted_total += len(keys)

                if cursor == 0:
                    break

        return {"deleted": deleted_total, "by_prefix": details, "dry_run": dry_run}


HZ_CACHE_PREFIXES: List[str] = [
    "totals_by_hazard",
    "totals_by_crop",
    "hazard_by_crop",
    "by_admin",
    "q1",
    "records",
    "denom_total",
]


redis_client: Optional[Redis] = None
cache_store: Optional[CacheStore] = None


async def init_cache() -> None:
    global redis_client, cache_store

    redis_client = Redis.from_url(S.redis_url, decode_responses=True)
    try:
        await redis_client.ping()
    except Exception as e:
        raise RuntimeError(f"Redis not reachable at {S.redis_url}: {e}")

    cache_store = CacheStore(redis_client)
    cache_store.init_materialized_cache()


async def close_cache() -> None:
    global redis_client
    if redis_client is not None:
        await redis_client.close()


def get_cache_store() -> CacheStore:
    if cache_store is None:
        raise RuntimeError("Cache store not initialized")
    return cache_store
