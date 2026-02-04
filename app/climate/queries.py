from typing import Any, Dict, List

from fastapi import HTTPException

from app.climate.models import ClimateQueryRequest
from app.common.scenario import normalize_timeframe_value
from app.db.duckdb import duckdb_connect, rows
from app.db.registry import resolve_dataset
from app.common.geo import validate_geo
from app.settings import S
from app.sql.clauses import coerce_values, normalize_list, where_in_lower
from app.sql.escape import quote_ident

_ALLOWED_FIELDS: List[str] = [
    "iso3",
    "admin0_name",
    "admin1_name",
    "gaul0_code",
    "gaul1_code",
    "scenario",
    "timeframe",
    "year",
    "hazard",
    "season",
    "baseline_name",
    "mean",
    "max",
    "min",
    "sd",
    "mean_anomaly",
    "max_anomaly",
    "min_anomaly",
    "sd_anomaly",
]


def query_climate(req: ClimateQueryRequest) -> Dict[str, Any]:
    dataset = resolve_dataset("climate", req.selector)
    validate_geo(req.geo)
    geo_norm = req.geo

    con = duckdb_connect(for_http_parquet=True)
    try:
        cols = {
            row[1]
            for row in con.execute(
                f"PRAGMA table_info({quote_ident(dataset.key)})"
            ).fetchall()
        }
    finally:
        con.close()

    wheres: List[str] = []
    wheres.append(_geo_where_with_columns(geo_norm, cols))

    if req.year is not None and (
        req.year_start is not None or req.year_end is not None
    ):
        raise HTTPException(
            status_code=400,
            detail="Provide either year or year_start/year_end, not both.",
        )

    if req.scenarios:
        expr = where_in_lower("scenario", req.scenarios)
        if expr:
            wheres.append(expr)
    if req.timeframes:
        timeframes = [
            normalize_timeframe_value(v) for v in coerce_values(req.timeframes)
        ]
        expr = where_in_lower("period", timeframes)
        if expr:
            wheres.append(expr)
    if req.seasons:
        expr = where_in_lower("season", req.seasons)
        if expr:
            wheres.append(expr)
    if req.hazards:
        expr = where_in_lower("hazard", req.hazards)
        if expr:
            wheres.append(expr)

    if req.year is not None:
        wheres.append(f"year = {int(req.year)}")
    else:
        if req.year_start is not None:
            wheres.append(f"year >= {int(req.year_start)}")
        if req.year_end is not None:
            wheres.append(f"year <= {int(req.year_end)}")

    select_fields = ", ".join(_ALLOWED_FIELDS)
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
      WHERE {" AND ".join(wheres)}
      LIMIT {limit_plus} OFFSET {offset}
    """
    if req.verbose:
        print("CLIMATE SQL:", q)

    con = duckdb_connect(for_http_parquet=True)
    try:
        data = rows(con, q)
    finally:
        con.close()

    if len(data) > limit:
        raise HTTPException(
            status_code=400,
            detail=(
                "Query result exceeds limit. Please narrow your filters or paginate with a smaller limit."
            ),
        )

    return {"rows": data, "limit": limit, "offset": offset}


def _geo_where_with_columns(geo, cols: set[str]) -> str:
    admin0 = normalize_list(geo.admin0)
    admin1 = normalize_list(geo.admin1)
    admin2 = normalize_list(geo.admin2)

    if admin2 and "admin2_name" not in cols:
        raise HTTPException(
            status_code=400,
            detail="admin2 filtering is not available for this dataset.",
        )
    if admin1 and "admin1_name" not in cols:
        raise HTTPException(
            status_code=400,
            detail="admin1 filtering is not available for this dataset.",
        )
    if admin0 and "admin0_name" not in cols:
        raise HTTPException(
            status_code=400,
            detail="admin0 filtering is not available for this dataset.",
        )

    has_all = (len(admin0) == 0) or any(x.lower() == "all" for x in admin0)
    a0 = [] if has_all else [x for x in admin0 if x.lower() != "all"]
    a1 = [] if has_all else admin1
    a2 = [] if has_all else admin2

    wh: List[str] = []
    if len(a0) > 0 and "admin0_name" in cols:
        wh.append(where_in_lower("admin0_name", a0))

    if len(a2) > 0 and "admin2_name" in cols:
        if len(a1) > 0 and "admin1_name" in cols:
            wh.append(where_in_lower("admin1_name", a1))
        wh.append(where_in_lower("admin2_name", a2))
    elif len(a1) > 0 and "admin1_name" in cols:
        wh.append(where_in_lower("admin1_name", a1))
        if "admin2_name" in cols:
            wh.append("admin2_name IS NULL")
    else:
        if "admin1_name" in cols:
            wh.append("admin1_name IS NULL")
        if "admin2_name" in cols:
            wh.append("admin2_name IS NULL")

    return " AND ".join(wh) if wh else "TRUE"
