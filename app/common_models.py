from enum import Enum
from typing import List

from pydantic import BaseModel, Field


class ScenarioEnum(str, Enum):
    ssp126 = "ssp126"
    ssp245 = "ssp245"
    ssp375 = "ssp375"
    ssp585 = "ssp585"


class GeoFilter(BaseModel):
    admin0: List[str] = Field(
        default_factory=list, description="admin0_name values; use ['all'] for broad"
    )
    admin1: List[str] = Field(default_factory=list, description="admin1_name values")
    admin2: List[str] = Field(default_factory=list, description="admin2_name values")


class ScenarioPick(BaseModel):
    scenario: ScenarioEnum
    timeframe: str
