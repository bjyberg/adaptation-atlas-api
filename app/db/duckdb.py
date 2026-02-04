import os
from typing import Any, Dict, List

import duckdb

from app.settings import S


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
