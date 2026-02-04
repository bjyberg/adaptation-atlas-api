import time
from typing import Any, Dict

from fastapi import APIRouter

from app.cache import get_cache_store
from app.caching.keys import cache_key, ttl
from app.hazard_exposure.models import HazardExposureQueryRequest
from app.hazard_exposure.queries import query_hazard_exposure

router = APIRouter()


@router.post("/api/v1/hazard-exposure/query")
async def hazard_exposure_query(req: HazardExposureQueryRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("hazard_exposure_query", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, **cached}

    t0 = time.time()
    out = query_hazard_exposure(req)
    out["t_ms"] = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, out, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, **out}
