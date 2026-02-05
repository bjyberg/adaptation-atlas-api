from typing import Any, Dict, List

from fastapi import HTTPException

from app.common.geo import validate_geo
from app.common.scenario import normalize_timeframe_value
from app.db.duckdb import duckdb_connect, rows
from app.db.registry import resolve_dataset
from app.geo.filters import geo_where
from app.settings import S
from app.sql.clauses import coerce_values, normalize_list, where_in_exact
from app.sql.escape import quote_ident


_HZ_EXPOSURE_COLUMNS = [
    "iso3",
    "admin0_name",
    "admin1_name",
    "admin2_name",
    "value",
    "scenario",
    "model",
    "timeframe",
    "hazard",
    "hazard_vars",
    "crop",
    "severity",
    "exposure_var",
    "exposure_unit",
]


_HAZARD_VARS_BY_METHOD = {
    "generic": ["NDWS+NTx35+NDWL0", "NDWS+THI-max+NDWL0"],
    "crop_specific": ["PTOT-L+NTxS+PTOT-G", "PTOT-L+THI-max+PTOT-G"],
}


def query_hazard_exposure(req) -> Dict[str, Any]:
    dataset = resolve_dataset("hazExposure", req.selector)
    validate_geo(req.geo)
    geo_where_expr = geo_where(req.geo)

    wheres: List[str] = [geo_where_expr]

    if req.scenarios:
        expr = where_in_exact("scenario", req.scenarios)
        if expr:
            wheres.append(expr)
    if req.timeframes:
        timeframes = coerce_values(req.timeframes)
        expr = where_in_exact("timeframe", timeframes)
        if expr:
            wheres.append(expr)
    if req.hazards:
        expr = where_in_exact("hazard", req.hazards)
        if expr:
            wheres.append(expr)
    if req.variable:
        expr = where_in_exact("exposure_var", req.variable)
        if expr:
            wheres.append(expr)
    m = (req.method.value if hasattr(req.method, "value") else str(req.method)).lower().strip()
    vals = _HAZARD_VARS_BY_METHOD.get(m, _HAZARD_VARS_BY_METHOD["generic"])
    expr = where_in_exact("hazard_vars", vals)
    if expr:
        wheres.append(expr)
    if req.period:
        expr = where_in_exact("period", req.period)
        if expr:
            wheres.append(expr)
    if req.severity:
        expr = where_in_exact("severity", req.severity)
        if expr:
            wheres.append(expr)
    if req.commodities:
        vals = normalize_list(coerce_values(req.commodities))
        if any(v.lower() == "all" for v in vals):
            # all => no crop filter
            vals = []
        if vals:
            expr = where_in_exact("crop", vals)
            if expr:
                wheres.append(expr)

    select_fields = ", ".join(_HZ_EXPOSURE_COLUMNS)
    limit = int(req.limit) if req.limit is not None else int(S.max_rows)
    offset = int(req.offset) if req.offset is not None else 0
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be greater than 0.")
    if limit > int(S.max_rows):
        raise HTTPException(
            status_code=400,
            detail=f"limit exceeds maximum allowed ({S.max_rows}). Narrow your query.",
        )
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0.")

    limit_plus = limit + 1
    q = f"""
      SELECT {select_fields}
      FROM {quote_ident(dataset.key)}
      WHERE {' AND '.join(wheres)}
      LIMIT {limit_plus} OFFSET {offset}
    """
    if req.verbose:
        print("HAZARD_EXPOSURE SQL:", q)

    con = duckdb_connect(for_http_parquet=True)
    try:
        data = rows(con, q)
    finally:
        con.close()

    has_more = len(data) > limit
    return {"rows": data[:limit], "limit": limit, "offset": offset, "has_more": has_more}
