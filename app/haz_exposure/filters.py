from typing import List, Optional

from app.common.scenario import ScenarioPick, normalize_timeframe_value
from app.sql.clauses import normalize_list, where_in_lower
from app.sql.escape import quote_literal


def scen_where(scen: ScenarioPick) -> str:
    sc = str(scen.scenario.value).strip() if hasattr(scen.scenario, "value") else str(scen.scenario).strip()
    tf = normalize_timeframe_value(str(getattr(scen.timeframe, "value", scen.timeframe))).strip()
    if not sc or not tf:
        return "FALSE"
    return f"LOWER(scenario) = {quote_literal(str(sc).strip().lower())} AND LOWER(timeframe) = {quote_literal(str(tf).strip().lower())}"


def crop_where(commodities: List[str]) -> str:
    vals = normalize_list(commodities)
    if len(vals) == 0:
        return "TRUE"
    if any(v.lower() == "all" for v in vals):
        vals = [v for v in vals if v.lower() != "all"]
        if len(vals) == 0:
            return "TRUE"
    return where_in_lower("crop", vals)


def haz_where(hazards: Optional[List[str]]) -> str:
    vals = normalize_list(hazards or [])
    if len(vals) == 0:
        return "TRUE"
    return where_in_lower("hazard", vals)


def hazard_vars_where(hazard_vars: Optional[List[str]], method: str, commodity_group: str) -> str:
    # If user provides hazard_vars, use them.
    if hazard_vars is not None:
        vals = normalize_list(hazard_vars)
        if len(vals) == 0:
            return "TRUE"
        return where_in_lower("hazard_vars", vals)

    # Defaults (matching what you used in the notebook)
    generic = ["NDWS+NTx35+NDWL0", "NDWS+THI-max+NDWL0"]
    crop_specific = ["PTOT-L+NTxS+PTOT-G", "PTOT-L+THI-max+PTOT-G"]

    m = (method or "").lower().strip()
    vals = crop_specific if m in ("crop", "crop_specific", "crop-specific") else generic
    return where_in_lower("hazard_vars", vals)
