from typing import List

from fastapi import HTTPException

from app.common.geo import GeoFilter
from app.sql.clauses import normalize_list, where_in_lower


def geo_where(geo: GeoFilter) -> str:
    admin0 = normalize_list(geo.admin0)
    admin1 = normalize_list(geo.admin1)
    admin2 = normalize_list(geo.admin2)
    iso3 = normalize_list(geo.iso3)

    admin0_all = (len(admin0) == 0) or any(x.lower() == "all" for x in admin0)
    a0 = [] if admin0_all else [x for x in admin0 if x.lower() != "all"]
    admin1_all = any(x.lower() == "all" for x in admin1)
    admin2_all = any(x.lower() == "all" for x in admin2)
    a1 = [] if (admin0_all or admin1_all) else [x for x in admin1 if x.lower() != "all"]
    a2 = [] if (admin0_all or admin2_all) else [x for x in admin2 if x.lower() != "all"]

    if admin0_all and (admin1_all or admin2_all):
        raise HTTPException(
            status_code=400,
            detail="admin0=all cannot be combined with admin1=all or admin2=all.",
        )

    filters: List[str] = []
    if len(a0) > 0 and len(iso3) > 0:
        filters.append(f"({where_in_lower('admin0_name', a0)} OR {where_in_lower('iso3', iso3)})")
    elif len(a0) > 0:
        filters.append(where_in_lower("admin0_name", a0))
    elif len(iso3) > 0:
        filters.append(where_in_lower("iso3", iso3))

    if len(a2) > 0:
        if len(a1) > 0:
            filters.append(where_in_lower("admin1_name", a1))
        filters.append(where_in_lower("admin2_name", a2))
    elif admin2_all:
        if len(a1) > 0:
            filters.append(where_in_lower("admin1_name", a1))
        filters.append("admin2_name IS NOT NULL")
    elif len(a1) > 0:
        filters.append(where_in_lower("admin1_name", a1))
        filters.append("admin2_name IS NULL")
    elif admin1_all:
        filters.append("admin1_name IS NOT NULL")
        filters.append("admin2_name IS NULL")
    else:
        filters.append("admin1_name IS NULL")
        filters.append("admin2_name IS NULL")

    return " AND ".join(filters) if filters else "TRUE"


def geo_where_parent(geo: GeoFilter) -> str:
    # TODO: This does not seem to be used anywhere. Remove
    """Looser geo filter used when we want *child breakdown* (by-admin)."""
    admin0 = normalize_list(geo.admin0)
    admin1 = normalize_list(geo.admin1)
    admin2 = normalize_list(geo.admin2)
    iso3 = normalize_list(geo.iso3)

    # "all" in admin0 means no admin0 filter (and treat as broad).
    admin0_all = (len(admin0) == 0) or any(x.lower() == "all" for x in admin0)
    a0 = [] if admin0_all else [x for x in admin0 if x.lower() != "all"]
    admin1_all = any(x.lower() == "all" for x in admin1)
    admin2_all = any(x.lower() == "all" for x in admin2)
    a1 = [] if (admin0_all or admin1_all) else [x for x in admin1 if x.lower() != "all"]
    a2 = [] if (admin0_all or admin2_all) else [x for x in admin2 if x.lower() != "all"]

    if admin0_all and (admin1_all or admin2_all):
        raise HTTPException(
            status_code=400,
            detail="admin0=all cannot be combined with admin1=all or admin2=all.",
        )

    filters: List[str] = []
    if len(a0) > 0 and len(iso3) > 0:
        filters.append(f"({where_in_lower('admin0_name', a0)} OR {where_in_lower('iso3', iso3)})")
    elif len(a0) > 0:
        filters.append(where_in_lower("admin0_name", a0))
    elif len(iso3) > 0:
        filters.append(where_in_lower("iso3", iso3))
    if len(a1) > 0:
        filters.append(where_in_lower("admin1_name", a1))
    elif admin1_all:
        filters.append("admin1_name IS NOT NULL")
    if len(a2) > 0:
        filters.append(where_in_lower("admin2_name", a2))
    elif admin2_all:
        filters.append("admin2_name IS NOT NULL")
    if not filters:
        filters.append("admin1_name IS NULL")
        filters.append("admin2_name IS NULL")

    return " AND ".join(filters) if filters else "TRUE"
