from typing import List, Union

from pydantic import BaseModel, Field, field_validator


class GeoFilter(BaseModel):
    admin0: Union[str, List[str]] = Field(
        default_factory=list, description="admin0_name values; use ['all'] for broad"
    )
    admin1: Union[str, List[str]] = Field(
        default_factory=list, description="admin1_name values; use ['all'] for broad"
    )
    admin2: Union[str, List[str]] = Field(
        default_factory=list, description="admin2_name values; use ['all'] for broad"
    )
    iso3: Union[str, List[str]] = Field(
        default_factory=list, description="ISO3 country codes (e.g., KEN)"
    )

    @field_validator("admin0", "admin1", "admin2", "iso3")
    @classmethod
    def _validate_all_marker(cls, v: Union[str, List[str]]) -> List[str]:
        raw = v if isinstance(v, list) else [v]
        vals = [str(x).strip() for x in raw or [] if x is not None]
        if any(x.lower() == "all" for x in vals) and len(vals) > 1:
            raise ValueError(
                "admin levels must be either ['all'] or a list of values (no mixing)."
            )
        return vals


def validate_geo(geo: GeoFilter) -> None:
    return None
