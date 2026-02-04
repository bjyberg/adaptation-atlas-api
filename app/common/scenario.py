from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, model_validator


class ScenarioEnum(str, Enum):
    historic = "historic"
    historical = "historical"
    ssp126 = "ssp126"
    ssp245 = "ssp245"
    ssp375 = "ssp375"
    ssp585 = "ssp585"


class TimeframeEnum(str, Enum):
    historic = "historic"
    historical = "historical"
    y1995_2014 = "1995-2014"
    y2021_2040 = "2021-2040"
    y2041_2060 = "2041-2060"
    y2061_2081 = "2061-2081"

def normalize_timeframe_value(value: str) -> str:
    v = str(value or "").strip().lower()
    if v in ("historic", "historical"):
        return "1995-2014"
    return str(value).strip()


def validate_historic_combo(
    scenarios: Optional[List[ScenarioEnum]],
    timeframes: Optional[List[TimeframeEnum]],
) -> None:
    scen_vals = [str(s.value).lower() for s in (scenarios or [])]
    tf_vals = [normalize_timeframe_value(t.value) for t in (timeframes or [])]

    scen_vals = ["historical" if s == "historic" else s for s in scen_vals]
    has_historic_scenario = "historical" in scen_vals
    has_historic_timeframe = "1995-2014" in tf_vals

    if has_historic_scenario and not has_historic_timeframe:
        raise ValueError("Historical scenario requires historical timeframe (1995-2014).")
    if has_historic_timeframe and not scen_vals:
        raise ValueError("Historical timeframe (1995-2014) requires scenario=historical.")
    if has_historic_timeframe and any(s != "historical" for s in scen_vals):
        raise ValueError("Historical timeframe (1995-2014) only supports historical scenario.")


class ScenarioPick(BaseModel):
    scenario: ScenarioEnum
    timeframe: TimeframeEnum

    @model_validator(mode="after")
    def _validate_historic(self) -> "ScenarioPick":
        validate_historic_combo([self.scenario], [self.timeframe])
        return self
