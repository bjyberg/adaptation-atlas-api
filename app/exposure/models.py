from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from app.common.geo import GeoFilter


class ExposureQueryRequest(BaseModel):
    selector: Dict[str, Any] = Field(default_factory=dict, description="Dataset selection fields.")
    geo: GeoFilter
    filters: dict = Field(default_factory=dict, description="Dataset-specific filters (expand as schemas are defined).")
    cache_ttl_seconds: Optional[int] = None
