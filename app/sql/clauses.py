"""SQL clause helpers."""

from typing import Any, List


def normalize_list(values: str | List[Any]) -> List[str]:
    """Normalize list-like input to a list of non-empty strings."""
    if isinstance(values, str):
        values = [values]
    return [s for s in (str(v).strip() for v in values or [] if v is not None) if s]


def coerce_values(values: List[Any]) -> List[str]:
    """Coerce enum-like values to their string values."""
    out: List[str] = []
    for v in values or []:
        if hasattr(v, "value"):
            out.append(str(v.value))
        else:
            out.append(str(v))
    return out


def where_in_lower(column: str, values: List[Any]) -> str:
    """Build a case-insensitive IN clause for string values."""
    vals = normalize_list(coerce_values(values))
    if not vals:
        return ""
    clean_vals = [v.strip().lower() for v in vals]
    quoted_vals = [f"'{v}'" for v in clean_vals]
    return f"LOWER({column}) IN ({', '.join(quoted_vals)})"
