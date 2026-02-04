from urllib.parse import urlparse

import httpx
from fastapi import HTTPException

from app.settings import S


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
