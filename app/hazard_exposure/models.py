from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.common.geo import GeoFilter
from app.common.hazards import HazardEnum
from app.common.scenario import ScenarioEnum, TimeframeEnum


class HazardExposureQueryRequest(BaseModel):
    selector: Dict[str, Any] = Field(
        default_factory=dict, description="Dataset selection fields."
    )
    geo: GeoFilter
    scenarios: Optional[List[ScenarioEnum]] = Field(
        default=None, description="Scenario values (e.g., ssp245)."
    )
    timeframes: Optional[List[TimeframeEnum]] = Field(
        default=None,
        description="Timeframe values (e.g., 1995-2014, 2021-2040).",
    )
    hazards: Optional[List[HazardEnum]] = Field(
        default=None, description="Hazard values (e.g., TAVG, TMAX)."
    )
    hazard_vars: Optional[List[str]] = Field(
        default=None, description="hazard_vars values"
    )
    commodities: Optional[List[str]] = Field(
        default=None, description="crop codes; use ['all'] for all"
    )
    method: str = Field(default="generic", description="generic | crop_specific")
    commodity_group: str = Field(default="all")

    limit: Optional[int] = Field(
        default=None, description="Max rows to return (server capped)."
    )
    offset: Optional[int] = Field(default=0, description="Offset for pagination.")
    verbose: bool = Field(
        default=False, description="If true, log the SQL query for debugging."
    )
    cache_ttl_seconds: Optional[int] = None
