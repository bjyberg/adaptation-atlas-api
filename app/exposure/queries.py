from typing import Any, Dict

from fastapi import HTTPException

from app.exposure.models import ExposureQueryRequest
from app.db.registry import resolve_dataset
from app.common.geo import validate_geo


def query_exposure(req: ExposureQueryRequest) -> Dict[str, Any]:
    dataset = resolve_dataset("exposure", req.selector)
    validate_geo(req.geo)

    raise HTTPException(
        status_code=501,
        detail="Exposure query not implemented yet. Define dataset schema and filters, then implement query_exposure().",
    )
