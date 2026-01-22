import tempfile
from typing import Any, Dict, List, Tuple

from fastapi import HTTPException

from app.haz_exposure_models import (
    ByAdminRequest,
    DenomTotalRequest,
    HazardByCropRequest,
    RecordsRequest,
    TotalsByCropRequest,
    TotalsByHazardRequest,
)
from app.settings import S
from app.utils import (
    crop_where,
    duckdb_connect,
    geo_where,
    geo_where_parent,
    haz_where,
    hazard_vars_where,
    is_broad_geo,
    norm_list,
    parquet_magic_check,
    rows,
    scen_where,
    sql_q,
    normalize_geo,
    validate_url,
)


def _resolve_admin_group_fields(geo) -> Tuple[str, str]:
    a1 = norm_list(geo.admin1)
    a2 = norm_list(geo.admin2)

    if len(a2) > 0:
        return ("admin2_name", "TRUE")
    if len(a1) > 0:
        return ("admin2_name", "admin2_name IS NOT NULL")
    return ("admin1_name", "admin1_name IS NOT NULL")


def _resolve_admin_group_fields_current(geo) -> Tuple[str, str]:
    """Return grouping field for *current* selected level (not children)."""
    a1 = norm_list(geo.admin1)
    a2 = norm_list(geo.admin2)

    if len(a2) > 0:
        return ("admin2_name", "admin2_name IS NOT NULL")
    if len(a1) > 0:
        return ("admin1_name", "admin1_name IS NOT NULL")
    return ("admin0_name", "admin0_name IS NOT NULL")


def query_totals_by_hazard(req: TotalsByHazardRequest) -> List[Dict[str, Any]]:
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection (admin0=all with no admin1/admin2) is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )

    geo_norm = normalize_geo(req.geo)
    geo_where_expr = geo_where(geo_norm)

    q = f"""
      SELECT hazard, COALESCE(SUM(CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END), 0.0) AS total
      FROM read_parquet({sql_q(req.dataset_url)})
      WHERE {scen_where(req.scen)}
        AND {geo_where_expr}
        AND {crop_where(req.commodities)}
        AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
        AND {haz_where(req.hazards)}
      GROUP BY hazard
      ORDER BY total DESC
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        return rows(con, q)
    finally:
        con.close()


def query_totals_by_crop(req: TotalsByCropRequest) -> List[Dict[str, Any]]:
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )

    geo_norm = normalize_geo(req.geo)
    geo_where_expr = geo_where(geo_norm)

    q = f"""
      SELECT crop, COALESCE(SUM(CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END), 0.0) AS total
      FROM read_parquet({sql_q(req.dataset_url)})
      WHERE {scen_where(req.scen)}
        AND {geo_where_expr}
        AND {crop_where(req.commodities)}
        AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
        AND {haz_where(req.hazards)}
      GROUP BY crop
      ORDER BY total DESC
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        return rows(con, q)
    finally:
        con.close()


def query_hazard_by_crop(req: HazardByCropRequest) -> List[Dict[str, Any]]:
    """Aggregate exposure by hazard × crop for a single side."""
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection (admin0=all with no admin1/admin2) is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )

    geo_norm = normalize_geo(req.geo)
    geo_where_expr = geo_where(geo_norm)

    q = f"""
      SELECT hazard, crop, COALESCE(SUM(CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END), 0.0) AS total
      FROM read_parquet({sql_q(req.dataset_url)})
      WHERE {scen_where(req.scen)}
        AND {geo_where_expr}
        AND {crop_where(req.commodities)}
        AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
        AND {haz_where(req.hazards)}
        AND hazard IS NOT NULL
        AND crop IS NOT NULL
      GROUP BY hazard, crop
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        result_rows = rows(con, q)
    finally:
        con.close()

    # Normalize totals to float (JSON-safe)
    for r in result_rows:
        try:
            t = float(r.get("total") or 0.0)
        except Exception:
            t = 0.0
        if not (t == t) or t in (float("inf"), float("-inf")):
            t = 0.0
        r["total"] = t

    # Top hazards (filter)
    if req.top_hazards and req.top_hazards > 0:
        haz_tot: Dict[str, float] = {}
        for r in result_rows:
            h = str(r.get("hazard") or "")
            haz_tot[h] = haz_tot.get(h, 0.0) + float(r["total"])
        keep = [h for h, _ in sorted(haz_tot.items(), key=lambda kv: kv[1], reverse=True)[: int(req.top_hazards)]]
        keep_set = set(keep)
        result_rows = [r for r in result_rows if str(r.get("hazard") or "") in keep_set]

    # Top crops (bucket non-top into "Other")
    if req.top_crops and req.top_crops > 0:
        crop_tot: Dict[str, float] = {}
        for r in result_rows:
            c = str(r.get("crop") or "")
            crop_tot[c] = crop_tot.get(c, 0.0) + float(r["total"])
        keep = [c for c, _ in sorted(crop_tot.items(), key=lambda kv: kv[1], reverse=True)[: int(req.top_crops)]]
        keep_set = set(keep)

        agg: Dict[Tuple[str, str], float] = {}
        for r in result_rows:
            h = str(r.get("hazard") or "")
            c0 = str(r.get("crop") or "")
            c = c0 if c0 in keep_set else "Other"
            agg[(h, c)] = agg.get((h, c), 0.0) + float(r["total"])

        result_rows = [{"hazard": h, "crop": c, "total": t} for (h, c), t in agg.items()]

    # Sort hazards by their total, then crops within hazard by total
    haz_tot2: Dict[str, float] = {}
    for r in result_rows:
        h = str(r.get("hazard") or "")
        haz_tot2[h] = haz_tot2.get(h, 0.0) + float(r["total"])
    haz_order = {h: i for i, (h, _) in enumerate(sorted(haz_tot2.items(), key=lambda kv: kv[1], reverse=True))}

    result_rows.sort(
        key=lambda r: (
            haz_order.get(str(r.get("hazard") or ""), 10**9),
            -float(r.get("total") or 0.0),
            str(r.get("crop") or ""),
        )
    )

    return result_rows


def query_by_admin(req: ByAdminRequest) -> List[Dict[str, Any]]:
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )
    geo_norm = normalize_geo(req.geo)
    group_field, non_null = (
        _resolve_admin_group_fields(geo_norm) if req.group_child else _resolve_admin_group_fields_current(geo_norm)
    )
    geo_where_expr = geo_where_parent(geo_norm) if req.group_child else geo_where(geo_norm)

    q = f"""
  SELECT
    {group_field} AS admin,
    COALESCE(
      SUM(
        CASE
          WHEN CAST(value AS DOUBLE) = CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE)
          ELSE NULL
        END
      ),
      0.0
    ) AS total
  FROM read_parquet({sql_q(req.dataset_url)})
  WHERE {scen_where(req.scen)}
    AND {geo_where_expr}
    AND {crop_where(req.commodities)}
    AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
    AND {haz_where(req.hazards)}
    AND {non_null}
  GROUP BY admin
  ORDER BY total DESC
"""

    con = duckdb_connect(for_http_parquet=True)
    try:
        return rows(con, q)
    finally:
        con.close()


def query_denom_total(req: DenomTotalRequest) -> Dict[str, Any]:
    validate_url(req.denom_url)
    parquet_magic_check(req.denom_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )

    geo_norm = normalize_geo(req.geo)

    wheres = [
        geo_where(geo_norm),
        crop_where(req.commodities),
    ]
    if req.exposure_unit:
        wheres.append(f"exposure_unit = {sql_q(req.exposure_unit)}")

    q = f"""
      SELECT COALESCE(SUM(CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END), 0.0) AS denom
      FROM read_parquet({sql_q(req.denom_url)})
      WHERE {' AND '.join(wheres)}
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        result_rows = rows(con, q)
        denom = result_rows[0].get("denom") if result_rows else None
        try:
            n = float(denom) if denom is not None else None
        except Exception:
            n = None

        ok = n is not None and n == n
        return {"ok": ok, "denom": n, "error": None if ok else "Denominator is missing/NaN"}
    finally:
        con.close()


def query_records_page(req: RecordsRequest) -> Dict[str, Any]:
    validate_url(req.dataset_url)
    parquet_magic_check(req.dataset_url)

    if is_broad_geo(req.geo) and not S.allow_broad_geo:
        raise HTTPException(
            status_code=400,
            detail="Broad geo selection is disabled. Select a specific admin0/admin1/admin2 or set ALLOW_BROAD_GEO=true.",
        )

    geo_norm = normalize_geo(req.geo)

    page = max(1, int(req.page))
    page_size = min(500, max(1, int(req.page_size)))
    offset = (page - 1) * page_size

    order = "value DESC" if req.sort == "value_desc" else "value ASC"
    limit = page_size + 1

    geo_where_expr = geo_where(geo_norm)

    q = f"""
      SELECT admin0_name, admin1_name, admin2_name,
             scenario, timeframe, hazard, hazard_vars, crop,
             CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END AS value
      FROM read_parquet({sql_q(req.dataset_url)})
      WHERE {scen_where(req.scen)}
        AND {geo_where_expr}
        AND {crop_where(req.commodities)}
        AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
      ORDER BY {order}
      LIMIT {limit} OFFSET {offset}
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        result_rows = rows(con, q)
    finally:
        con.close()

    has_more = len(result_rows) > page_size
    result_rows = result_rows[:page_size]

    return {"page": page, "page_size": page_size, "has_more": has_more, "rows": result_rows}


def export_records_csv(req: RecordsRequest) -> str:
    # Guardrail for CSV exports
    limit_rows = min(S.export_max_rows, max(1, int(req.page_size)))
    order = "value DESC" if req.sort == "value_desc" else "value ASC"

    geo_norm = normalize_geo(req.geo)
    geo_where_expr = geo_where(geo_norm)

    q = f"""
      SELECT admin0_name, admin1_name, admin2_name,
             scenario, timeframe, hazard, hazard_vars, crop,
             CASE WHEN CAST(value AS DOUBLE)=CAST(value AS DOUBLE) THEN CAST(value AS DOUBLE) ELSE NULL END AS value
      FROM read_parquet({sql_q(req.dataset_url)})
      WHERE {scen_where(req.scen)}
        AND {geo_where_expr}
        AND {crop_where(req.commodities)}
        AND {hazard_vars_where(req.hazard_vars, req.method, req.commodity_group)}
      ORDER BY {order}
      LIMIT {limit_rows}
    """

    con = duckdb_connect(for_http_parquet=True)
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp_path = tmp.name
        tmp.close()
        # Use a double-quote character as the CSV quote char.
        con.execute(f"COPY ({q}) TO {sql_q(tmp_path)} (HEADER TRUE, DELIMITER ',', QUOTE '\"')")
        return tmp_path
    finally:
        con.close()
