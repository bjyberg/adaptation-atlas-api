from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from app.common.geo import GeoFilter
from app.common.hazards import HazardEnum
from app.common.scenario import ScenarioEnum, TimeframeEnum, validate_historic_combo

_TIMEFRAME_RANGES = {
    TimeframeEnum.y1995_2014: (1995, 2014),
    TimeframeEnum.y2021_2040: (2021, 2040),
    TimeframeEnum.y2041_2060: (2041, 2060),
    TimeframeEnum.y2061_2081: (2061, 2081),
    TimeframeEnum.historic: (1995, 2014),
    TimeframeEnum.historical: (1995, 2014),
}


class SeasonEnum(str, Enum):
    annual = "annual"
    jfm = "jfm"
    fma = "fma"
    mam = "mam"
    amj = "amj"
    mjj = "mjj"
    jja = "jja"
    jas = "jas"
    aso = "aso"
    son = "son"
    djf = "djf"


class ClimateQueryRequest(BaseModel):
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
    year: Optional[int] = Field(default=None, description="Exact year filter.")
    year_start: Optional[int] = Field(
        default=None, description="Start year (inclusive)."
    )
    year_end: Optional[int] = Field(default=None, description="End year (inclusive).")
    seasons: Optional[List[SeasonEnum]] = Field(
        default=None,
        description="Season values (annual, jfm, fma, mam, amj, mjj, jja).",
    )
    hazards: Optional[List[HazardEnum]] = Field(
        default=None, description="Hazard values (e.g., TAVG, TMAX)."
    )
    limit: Optional[int] = Field(
        default=None, description="Max rows to return (server capped)."
    )
    offset: Optional[int] = Field(default=0, description="Offset for pagination.")
    verbose: bool = Field(
        default=False, description="If true, log the SQL query for debugging."
    )
    cache_ttl_seconds: Optional[int] = None

    @model_validator(mode="after")
    def _validate_historic_combo(self) -> "ClimateQueryRequest":
        validate_historic_combo(self.scenarios, self.timeframes)

        if self.timeframes:
            ranges = [_TIMEFRAME_RANGES[t] for t in self.timeframes]
            if self.year is not None:
                if not any(start <= self.year <= end for start, end in ranges):
                    raise ValueError("year is outside the requested timeframe range.")
            else:
                if self.year_start is not None and self.year_end is not None:
                    if self.year_start > self.year_end:
                        raise ValueError("year_start must be <= year_end.")
                if self.year_start is not None or self.year_end is not None:
                    y0 = self.year_start if self.year_start is not None else -(10**9)
                    y1 = self.year_end if self.year_end is not None else 10**9
                    if not any((start <= y1) and (end >= y0) for start, end in ranges):
                        raise ValueError(
                            "year range is outside the requested timeframe range."
                        )

        return self
