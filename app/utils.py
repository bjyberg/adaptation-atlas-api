import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import duckdb
import httpx
from fastapi import HTTPException

from app.common_models import GeoFilter, ScenarioPick
from app.settings import S


def ttl(req_ttl: Optional[int]) -> Optional[int]:
    """Return cache TTL semantics.

    - None  => use server default (S.cache_ttl_seconds)
    - <0    => disable cache for this request (no read/write)
    - 0     => no expiry (persistent until manually cleared)
    - >0    => expiry in seconds
    """
    v: int
    if req_ttl is None:
        v = int(S.cache_ttl_seconds)
    else:
        try:
            v = int(req_ttl)
        except Exception:
            v = int(S.cache_ttl_seconds)

    if v < 0:
        return -1
    if v == 0:
        return None
    return max(1, v)


def sha1_json(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def cache_key(prefix: str, payload: Any) -> str:
    return f"{prefix}:{sha1_json(payload)}"


def sql_q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _lower_q(s: str) -> str:
    return sql_q(str(s).strip().lower())


def norm_list(values: List[str]) -> List[str]:
    out: List[str] = []
    for v in values or []:
        if v is None:
            continue
        vv = str(v).strip()
        if vv:
            out.append(vv)
    return out


def is_broad_geo(geo: GeoFilter) -> bool:
    a0 = norm_list(geo.admin0)
    a1 = norm_list(geo.admin1)
    a2 = norm_list(geo.admin2)
    all0 = (len(a0) == 0) or ("all" in [x.lower() for x in a0])
    return all0 and len(a1) == 0 and len(a2) == 0


def geo_where(geo: GeoFilter) -> str:
    admin0 = norm_list(geo.admin0)
    admin1 = norm_list(geo.admin1)
    admin2 = norm_list(geo.admin2)

    # "all" in admin0 means no admin0 filter.
    has_all = (len(admin0) == 0) or any(x.lower() == "all" for x in admin0)
    a0 = [] if has_all else [x for x in admin0 if x.lower() != "all"]
    a1 = [] if has_all else admin1
    a2 = [] if has_all else admin2

    wh: List[str] = []
    if len(a0) > 0:
        wh.append(f"LOWER(admin0_name) IN ({', '.join(_lower_q(v) for v in a0)})")

    if len(a2) > 0:
        if len(a1) > 0:
            wh.append(f"LOWER(admin1_name) IN ({', '.join(_lower_q(v) for v in a1)})")
        wh.append(f"LOWER(admin2_name) IN ({', '.join(_lower_q(v) for v in a2)})")
    elif len(a1) > 0:
        wh.append(f"LOWER(admin1_name) IN ({', '.join(_lower_q(v) for v in a1)})")
        wh.append("admin2_name IS NULL")
    else:
        wh.append("admin1_name IS NULL")
        wh.append("admin2_name IS NULL")

    return " AND ".join(wh) if wh else "TRUE"


def geo_where_parent(geo: GeoFilter) -> str:
    """Looser geo filter used when we want *child breakdown* (by-admin)."""
    admin0 = norm_list(geo.admin0)
    admin1 = norm_list(geo.admin1)
    admin2 = norm_list(geo.admin2)

    # "all" in admin0 means no admin0 filter (and treat as broad).
    has_all = (len(admin0) == 0) or any(x.lower() == "all" for x in admin0)
    a0 = [] if has_all else [x for x in admin0 if x.lower() != "all"]
    a1 = [] if has_all else admin1
    a2 = [] if has_all else admin2

    wh: List[str] = []
    if len(a0) > 0:
        wh.append(f"LOWER(admin0_name) IN ({', '.join(_lower_q(v) for v in a0)})")
    if len(a1) > 0:
        wh.append(f"LOWER(admin1_name) IN ({', '.join(_lower_q(v) for v in a1)})")
    if len(a2) > 0:
        wh.append(f"LOWER(admin2_name) IN ({', '.join(_lower_q(v) for v in a2)})")

    return " AND ".join(wh) if wh else "TRUE"


def scen_where(scen: ScenarioPick) -> str:
    sc = str(scen.scenario.value).strip() if hasattr(scen.scenario, "value") else str(scen.scenario).strip()
    tf = str(scen.timeframe).strip()
    if not sc or not tf:
        return "FALSE"
    return f"LOWER(scenario) = {_lower_q(sc)} AND LOWER(timeframe) = {_lower_q(tf)}"


def crop_where(commodities: List[str]) -> str:
    vals = norm_list(commodities)
    if len(vals) == 0:
        return "TRUE"
    if any(v.lower() == "all" for v in vals):
        vals = [v for v in vals if v.lower() != "all"]
        if len(vals) == 0:
            return "TRUE"
    return f"LOWER(crop) IN ({', '.join(_lower_q(v) for v in vals)})"


def haz_where(hazards: Optional[List[str]]) -> str:
    vals = norm_list(hazards or [])
    if len(vals) == 0:
        return "TRUE"
    return f"LOWER(hazard) IN ({', '.join(_lower_q(v) for v in vals)})"


def hazard_vars_where(hazard_vars: Optional[List[str]], method: str, commodity_group: str) -> str:
    # If user provides hazard_vars, use them.
    if hazard_vars is not None:
        vals = norm_list(hazard_vars)
        if len(vals) == 0:
            return "TRUE"
        return f"LOWER(hazard_vars) IN ({', '.join(_lower_q(v) for v in vals)})"

    # Defaults (matching what you used in the notebook)
    generic = ["NDWS+NTx35+NDWL0", "NDWS+THI-max+NDWL0"]
    crop_specific = ["PTOT-L+NTxS+PTOT-G", "PTOT-L+THI-max+PTOT-G"]

    m = (method or "").lower().strip()
    vals = crop_specific if m in ("crop", "crop_specific", "crop-specific") else generic
    return f"LOWER(hazard_vars) IN ({', '.join(_lower_q(v) for v in vals)})"


def validate_url(url: str) -> None:
    if S.allow_any_url:
        return
    u = urlparse(url)
    if u.scheme != "https":
        raise HTTPException(status_code=400, detail="Only https:// URLs are allowed")
    host = (u.hostname or "").lower()
    if host not in S.allowed_parquet_hosts:
        raise HTTPException(
            status_code=400,
            detail=f"Host '{host}' not allowlisted. Allowed: {', '.join(S.allowed_parquet_hosts)}",
        )


def parquet_magic_check(url: str) -> None:
    if not S.parquet_magic_check:
        return
    try:
        with httpx.Client(timeout=10.0, follow_redirects=True) as client:
            r = client.get(url, headers={"Range": "bytes=0-3"})
            if r.status_code >= 400:
                raise HTTPException(status_code=400, detail=f"Parquet URL returned {r.status_code}")
            if r.content != b"PAR1":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "URL did not look like a parquet file (missing PAR1 header). "
                        "This often happens when the URL is wrong or access is denied."
                    ),
                )
    except HTTPException:
        raise
    except Exception:
        # Best-effort only
        return


def duckdb_connect(for_http_parquet: bool = False) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(S.duckdb_path), exist_ok=True)
    con = duckdb.connect(S.duckdb_path)
    con.execute(f"PRAGMA threads={S.duckdb_threads}")
    con.execute("PRAGMA enable_object_cache=true")
    con.execute("SET preserve_insertion_order=false")

    if for_http_parquet:
        try:
            con.execute("LOAD httpfs")
        except Exception:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")

    return con


def rows(con: duckdb.DuckDBPyConnection, query: str) -> List[Dict[str, Any]]:
    rel = con.execute(query)
    cols = [d[0] for d in rel.description]
    out: List[Dict[str, Any]] = []
    for r in rel.fetchall():
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return out


def normalize_geo(geo: GeoFilter) -> GeoFilter:
    if not S.admin_lookup_enable:
        return geo

    lookup_path = S.admin_lookup_path
    if not os.path.exists(lookup_path):
        raise HTTPException(status_code=500, detail=f"Admin lookup parquet not found at {lookup_path}")

    a0 = norm_list(geo.admin0)
    a1 = norm_list(geo.admin1)
    a2 = norm_list(geo.admin2)

    has_all = (len(a0) == 0) or any(x.lower() == "all" for x in a0)
    if has_all:
        return geo

    con = duckdb_connect(for_http_parquet=False)
    try:
        try:
            rows0 = con.execute(
                "SELECT DISTINCT admin0_name, iso3 FROM read_parquet(?)",
                [lookup_path],
            ).fetchall()
        except Exception:
            rows0 = con.execute(
                "SELECT DISTINCT admin0_name FROM read_parquet(?)",
                [lookup_path],
            ).fetchall()
            rows0 = [(r[0], None) for r in rows0]
    finally:
        con.close()

    admin0_map: Dict[str, str] = {}
    for admin0_name, iso3 in rows0:
        if admin0_name:
            admin0_map[str(admin0_name).strip().lower()] = str(admin0_name)
        if iso3:
            admin0_map[str(iso3).strip().lower()] = str(admin0_name)

    bad0 = [v for v in a0 if str(v).strip().lower() not in admin0_map]
    if bad0:
        raise HTTPException(status_code=400, detail=f"Unknown admin0 values: {bad0}")

    admin0_names = sorted({admin0_map[str(v).strip().lower()] for v in a0})

    if not a1 and not a2:
        return geo.model_copy(update={"admin0": admin0_names})

    con = duckdb_connect(for_http_parquet=False)
    try:
        if a1:
            placeholders = ", ".join(["?"] * len(admin0_names))
            q1 = f"SELECT DISTINCT admin1_name FROM read_parquet(?) WHERE admin0_name IN ({placeholders})"
            rows1 = con.execute(q1, [lookup_path, *admin0_names]).fetchall()
            valid_a1 = {str(r[0]).strip().lower() for r in rows1 if r[0] is not None}
            bad1 = [v for v in a1 if str(v).strip().lower() not in valid_a1]
            if bad1:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown admin1 values for admin0 {admin0_names}: {bad1}",
                )

        if a2:
            filters = [f"admin0_name IN ({', '.join(['?'] * len(admin0_names))})"]
            params: List[str] = [lookup_path, *admin0_names]
            if a1:
                filters.append(f"admin1_name IN ({', '.join(['?'] * len(a1))})")
                params.extend(a1)
            q2 = f"SELECT DISTINCT admin2_name FROM read_parquet(?) WHERE {' AND '.join(filters)}"
            rows2 = con.execute(q2, params).fetchall()
            valid_a2 = {str(r[0]).strip().lower() for r in rows2 if r[0] is not None}
            bad2 = [v for v in a2 if str(v).strip().lower() not in valid_a2]
            if bad2:
                scope = f"admin0 {admin0_names}"
                if a1:
                    scope += f" admin1 {a1}"
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown admin2 values for {scope}: {bad2}",
                )
    finally:
        con.close()

    return geo.model_copy(update={"admin0": admin0_names})


def validate_geo(geo: GeoFilter) -> None:
    normalize_geo(geo)


def cleanup_file(path: str) -> None:
    try:
        os.remove(path)
    except Exception:
        pass
