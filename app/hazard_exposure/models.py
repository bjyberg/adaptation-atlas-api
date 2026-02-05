from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.common.commodities import CommodityEnum
from app.common.geo import GeoFilter
from app.common.scenario import ScenarioEnum, TimeframeEnum


class variableEnum(str, Enum):
    vop_intld15 = "vop_intld15"
    vop_usd15 = "vop_usd15"  # Default


class periodEnum(str, Enum):
    annual = "annual"  # Default
    jagermeyr = "jagermeyr"


class severityEnum(str, Enum):
    moderate = "moderate"
    severe = "severe"  # Default
    extreme = "extreme"


class modelEnum(str, Enum):
    historic = "historic"
    ENSEMBLE = "ENSEMBLE"


class HazardVarMethodEnum(str, Enum):
    generic = "generic"
    crop_specific = "crop_specific"


class HazardInteractionEnum(str, Enum):
    any = "any"
    heat = "heat"
    dry_heat_wet = "dry+heat+wet"
    heat_wet = "heat+wet"
    dry = "dry"
    dry_heat = "dry+heat"
    wet = "wet"
    dry_wet = "dry+wet"


class HazardExposureQueryRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

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
    hazards: Optional[List[HazardInteractionEnum]] = Field(
        default=None, description="Hazard values (e.g., TAVG, TMAX)."
    )
    variable: Optional[List[variableEnum]] = Field(
        default=None, description="Exposure variable (e.g., vop_intld15, vop_usd15)."
    )
    period: Optional[List[periodEnum]] = Field(
        default=[periodEnum.annual],
        description="Period values (e.g., annual, jagermeyr).",
    )
    severity: Optional[List[severityEnum]] = Field(
        default=[severityEnum.severe], description="Severity class values."
    )
    commodities: Optional[List[CommodityEnum]] = Field(
        default=None, description="crop codes; use ['all'] for all"
    )
    method: HazardVarMethodEnum = Field(
        default=HazardVarMethodEnum.generic, description="generic | crop_specific"
    )

    limit: Optional[int] = Field(
        default=None, description="Max rows to return (server capped)."
    )
    offset: Optional[int] = Field(default=0, description="Offset for pagination.")
    verbose: bool = Field(
        default=False, description="If true, log the SQL query for debugging."
    )
    cache_ttl_seconds: Optional[int] = None

    @field_validator("commodities")
    @classmethod
    def _validate_commodities(
        cls, v: Optional[List[CommodityEnum]]
    ) -> Optional[List[CommodityEnum]]:
        if not v:
            return v
        vals = [str(x.value).strip().lower() for x in v]
        if "all" in vals and len(vals) > 1:
            raise ValueError(
                "commodities must be either ['all'] or a list of values (no mixing)."
            )
        return v
