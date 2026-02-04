from typing import Any, Dict, List

from fastapi import HTTPException

from app.common.geo import validate_geo
from app.common.scenario import normalize_timeframe_value
from app.db.duckdb import duckdb_connect, rows
from app.db.registry import resolve_dataset
from app.geo.filters import geo_where
from app.settings import S
from app.sql.clauses import coerce_values, normalize_list, where_in_lower
from app.sql.escape import quote_ident


def query_hazard_exposure(req) -> Dict[str, Any]:
    dataset = resolve_dataset("hazExposure", req.selector)
    validate_geo(req.geo)
    geo_where_expr = geo_where(req.geo)

    wheres: List[str] = [geo_where_expr]

    if req.scenarios:
        expr = where_in_lower("scenario", req.scenarios)
        if expr:
            wheres.append(expr)
    if req.timeframes:
        timeframes = [normalize_timeframe_value(v) for v in coerce_values(req.timeframes)]
        expr = where_in_lower("timeframe", timeframes)
        if expr:
            wheres.append(expr)
    if req.hazards:
        expr = where_in_lower("hazard", req.hazards)
        if expr:
            wheres.append(expr)
    if req.hazard_vars:
        expr = where_in_lower("hazard_vars", req.hazard_vars)
        if expr:
            wheres.append(expr)
    if req.commodities:
        vals = normalize_list(req.commodities)
        if any(v.lower() == "all" for v in vals):
            vals = [v for v in vals if v.lower() != "all"]
        if vals:
            expr = where_in_lower("crop", vals)
            if expr:
                wheres.append(expr)

    select_fields = "*"
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

    if len(data) > limit:
        raise HTTPException(
            status_code=400,
            detail="Query result exceeds limit. Please narrow your filters or paginate with a smaller limit.",
        )

    return {"rows": data, "limit": limit, "offset": offset}
