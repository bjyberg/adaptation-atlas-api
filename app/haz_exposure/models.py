from typing import List, Optional

from pydantic import BaseModel, Field

from app.common.geo import GeoFilter
from app.common.scenario import ScenarioPick


class BaseQuery(BaseModel):
    dataset_url: str = Field(..., description="HTTPS URL to hazard-exposure parquet (interaction.parquet)")
    scen: ScenarioPick
    geo: GeoFilter

    commodities: List[str] = Field(default_factory=list, description="crop codes; use ['all'] for all")
    hazard_vars: Optional[List[str]] = Field(default=None, description="hazard_vars values")

    # Used only if hazard_vars is not provided.
    method: str = Field(default="generic", description="generic | crop_specific")
    commodity_group: str = Field(default="all")

    cache_ttl_seconds: Optional[int] = None


class TotalsByHazardRequest(BaseQuery):
    hazards: Optional[List[str]] = None


class TotalsByCropRequest(BaseQuery):
    hazards: Optional[List[str]] = None


class HazardByCropRequest(BaseQuery):
    # Return a hazard×crop matrix (already aggregated), suitable for stacked bars or heatmaps.
    # Optional limits keep payloads small & UI snappy.
    hazards: Optional[List[str]] = None
    top_hazards: Optional[int] = None
    top_crops: Optional[int] = None


class ByAdminRequest(BaseQuery):
    group_child: bool = True
    hazards: Optional[List[str]] = None


class DenomTotalRequest(BaseModel):
    denom_url: str = Field(..., description="HTTPS URL to total exposure parquet")
    geo: GeoFilter
    commodities: List[str] = Field(default_factory=list)
    exposure_unit: Optional[str] = None
    cache_ttl_seconds: Optional[int] = None


class Q1Request(BaseModel):
    left: TotalsByHazardRequest
    right: TotalsByHazardRequest
    denom: Optional[DenomTotalRequest] = None


class RecordsRequest(BaseQuery):
    page: int = 1
    page_size: int = 100
    sort: str = Field(default="value_desc", description="value_desc | value_asc")


class CacheClearRequest(BaseModel):
    prefixes: Optional[List[str]] = Field(default=None, description="Key prefixes to clear (e.g., ['by_admin','q1']).")
    all: bool = Field(default=False, description="If true, clear all known hazard-exposure cache prefixes.")
    dry_run: bool = Field(default=False, description="If true, only count keys that would be deleted.")
