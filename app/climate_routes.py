import time
from typing import Any, Dict

from fastapi import APIRouter

from app.cache import get_cache_store
from app.climate_queries import query_climate
from app.climate_models import ClimateQueryRequest
from app.utils import cache_key, ttl


router = APIRouter()


@router.post("/api/v1/climate/query")
async def climate_query(req: ClimateQueryRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("climate_query", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, **cached}

    t0 = time.time()
    out = query_climate(req)
    out["t_ms"] = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, out, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, **out}
