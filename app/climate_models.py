from typing import Optional

from pydantic import BaseModel, Field

from app.common_models import GeoFilter


class ClimateQueryRequest(BaseModel):
    dataset_url: str = Field(..., description="HTTPS URL to climate parquet dataset")
    geo: GeoFilter
    filters: dict = Field(default_factory=dict, description="Dataset-specific filters (expand as schemas are defined).")
    cache_ttl_seconds: Optional[int] = None
