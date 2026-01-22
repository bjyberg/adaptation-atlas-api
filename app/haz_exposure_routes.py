import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request
from fastapi.responses import FileResponse

from app.cache import HZ_CACHE_PREFIXES, get_cache_store
from app.haz_exposure_models import (
    ByAdminRequest,
    CacheClearRequest,
    DenomTotalRequest,
    HazardByCropRequest,
    Q1Request,
    RecordsRequest,
    TotalsByCropRequest,
    TotalsByHazardRequest,
)
from app.haz_exposure_queries import (
    export_records_csv,
    query_by_admin,
    query_denom_total,
    query_hazard_by_crop,
    query_records_page,
    query_totals_by_crop,
    query_totals_by_hazard,
)
from app.settings import S
from app.utils import cache_key, cleanup_file, ttl


router = APIRouter()


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


async def _require_cache_admin(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_admin_token: Optional[str] = Header(default=None),
) -> None:
    """Admin guard for cache-clear endpoints."""
    token = (S.cache_clear_token or "").strip()
    if not token:
        raise HTTPException(status_code=503, detail="Cache clear is disabled (CACHE_CLEAR_TOKEN not set).")

    if S.cache_clear_local_only:
        host = (request.client.host if request.client else "")
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise HTTPException(status_code=403, detail="Cache clear is restricted to localhost.")

    provided = _extract_bearer(authorization) or (x_admin_token or "").strip()
    if provided != token:
        raise HTTPException(status_code=401, detail="Unauthorized.")


@router.get("/api/v1/hz/cache/prefixes")
async def cache_prefixes(_: Any = Depends(_require_cache_admin)) -> Dict[str, Any]:
    return {"ok": True, "prefixes": HZ_CACHE_PREFIXES}


@router.post("/api/v1/hz/cache/clear")
async def cache_clear(req: CacheClearRequest, _: Any = Depends(_require_cache_admin)) -> Dict[str, Any]:
    cache_store = get_cache_store()
    prefixes = []
    if req.all:
        prefixes = list(HZ_CACHE_PREFIXES)
    elif req.prefixes:
        prefixes = [p for p in req.prefixes if p]

    if not prefixes:
        raise HTTPException(status_code=400, detail="Provide prefixes or set all=true.")

    # Safety: only allow known prefixes
    unknown = [p for p in prefixes if p not in HZ_CACHE_PREFIXES]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown prefixes: {unknown}. Allowed: {HZ_CACHE_PREFIXES}")

    info = await cache_store.clear_prefixes(prefixes, dry_run=req.dry_run)
    return {"ok": True, "prefixes": prefixes, **info}


@router.post("/api/v1/hz/totals-by-hazard")
@router.post("/api/v1/hz/totals_by_hazard")
async def totals_by_hazard(req: TotalsByHazardRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("totals_by_hazard", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, "data": cached}

    t0 = time.time()
    data = query_totals_by_hazard(req)
    dt_ms = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, data, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, "t_ms": dt_ms, "data": data}


@router.post("/api/v1/hz/totals-by-crop")
@router.post("/api/v1/hz/totals_by_crop")
async def totals_by_crop(req: TotalsByCropRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("totals_by_crop", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, "data": cached}

    t0 = time.time()
    data = query_totals_by_crop(req)
    dt_ms = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, data, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, "t_ms": dt_ms, "data": data}


@router.post("/api/v1/hz/hazard-by-crop")
@router.post("/api/v1/hz/hazard_by_crop")
async def hazard_by_crop(req: HazardByCropRequest) -> Dict[str, Any]:
    """Return exposure aggregated by hazard × crop (single side)."""
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("hazard_by_crop", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, "data": cached}

    t0 = time.time()
    data = query_hazard_by_crop(req)
    dt_ms = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, data, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, "t_ms": dt_ms, "data": data}


@router.post("/api/v1/hz/by-admin")
@router.post("/api/v1/hz/by_admin")
async def by_admin(req: ByAdminRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("by_admin", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, "data": cached}

    t0 = time.time()
    data = query_by_admin(req)
    dt_ms = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, data, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, "t_ms": dt_ms, "data": data}


@router.post("/api/v1/exposure/denom-total")
@router.post("/api/v1/exposure/denom_total")
async def denom_total(req: DenomTotalRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("denom_total", payload)
    ttl_seconds = ttl(req.cache_ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"cached": True, "cache_source": source, **cached}

    t0 = time.time()
    data = query_denom_total(req)
    dt_ms = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, data, ttl_seconds=ttl_seconds)
    return {"cached": False, "t_ms": dt_ms, **data}


@router.post("/api/v1/hz/q1")
async def q1(req: Q1Request) -> Dict[str, Any]:
    """Convenience endpoint for the Q1 chart."""
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("q1", payload)
    # Use standard TTL semantics (0 => no expiry)
    ttl_seconds = ttl(None)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, **cached}

    t0 = time.time()

    left_rows = query_totals_by_hazard(req.left)
    right_rows = query_totals_by_hazard(req.right)

    denom_meta = {"ok": False, "denom": None, "error": "No denom"}
    denom_value = None
    if req.denom is not None:
        denom_meta = query_denom_total(req.denom)
        if denom_meta.get("ok"):
            denom_value = denom_meta.get("denom")

    by1 = {r.get("hazard"): float(r.get("total") or 0) for r in left_rows}
    by2 = {r.get("hazard"): float(r.get("total") or 0) for r in right_rows}

    hazards = sorted(set(list(by1.keys()) + list(by2.keys())))
    sum1 = sum(by1.values())
    sum2 = sum(by2.values())

    def pct(val: float, s: float) -> float:
        if denom_value is not None and denom_value and denom_value > 0:
            return (val / denom_value) * 100.0
        if s and s > 0:
            return (val / s) * 100.0
        return 0.0

    merged: List[Dict[str, Any]] = []
    for h in hazards:
        t1 = by1.get(h, 0.0)
        t2 = by2.get(h, 0.0)
        merged.append(
            {
                "hazard": h,
                "total1": t1,
                "total2": t2,
                "total_diff": t2 - t1,
                "perc1": pct(t1, sum1),
                "perc2": pct(t2, sum2),
                "pct_diff": pct(t2, sum2) - pct(t1, sum1),
            }
        )

    merged.sort(key=lambda r: abs(r.get("total_diff", 0.0)), reverse=True)

    dt_ms = int((time.time() - t0) * 1000)

    out = {
        "left": left_rows,
        "right": right_rows,
        "merged": merged,
        "denom": denom_meta,
        "relative_label": "% of total exposure" if denom_meta.get("ok") else "% of hazard sum (fallback)",
        "t_ms": dt_ms,
    }

    await cache_store.set_json(key, out, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, **out}


@router.post("/api/v1/hz/records")
async def records(req: RecordsRequest) -> Dict[str, Any]:
    cache_store = get_cache_store()

    payload = req.model_dump()
    key = cache_key("records", payload)

    # Records pages are less reusable; cache briefly to make UI paging snappy.
    ttl_seconds = ttl(req.cache_ttl_seconds)
    if isinstance(ttl_seconds, int) and ttl_seconds > 0:
        ttl_seconds = min(120, ttl_seconds)

    cached, source = await cache_store.get_json(key, ttl_seconds=ttl_seconds)
    if cached is not None:
        return {"ok": True, "cached": True, "cache_source": source, **cached}

    t0 = time.time()
    out = query_records_page(req)
    out["t_ms"] = int((time.time() - t0) * 1000)

    await cache_store.set_json(key, out, ttl_seconds=ttl_seconds)
    return {"ok": True, "cached": False, **out}


@router.post("/api/v1/hz/records.csv")
@router.post("/api/v1/hz/records_csv")
async def records_csv(req: RecordsRequest, bg: BackgroundTasks) -> FileResponse:
    """Generate a CSV export server-side."""
    path = export_records_csv(req)
    bg.add_task(cleanup_file, path)

    filename = f"hazard_exposure_records_{int(time.time())}.csv"
    return FileResponse(path, filename=filename, media_type="text/csv")
