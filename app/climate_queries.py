from typing import Any, Dict

from fastapi import HTTPException

from app.climate_models import ClimateQueryRequest
from app.utils import normalize_geo, parquet_magic_check, validate_url


def query_climate(req: ClimateQueryRequest) -> Dict[str, Any]:
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)
    normalize_geo(req.geo)

    raise HTTPException(
        status_code=501,
        detail="Climate query not implemented yet. Define dataset schema and filters, then implement query_climate().",
    )
