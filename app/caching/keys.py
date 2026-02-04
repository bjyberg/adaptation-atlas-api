import hashlib
import json
from typing import Any, Optional

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
