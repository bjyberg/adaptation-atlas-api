import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from fastapi import HTTPException

from app.db.duckdb import duckdb_connect
from app.settings import S
from app.sql.escape import quote_ident, quote_literal


@dataclass(frozen=True)
class DatasetEntry:
    key: str
    description: str
    paths: List[str]
    hive_partitioning: bool
    selector: Dict[str, Any]


@dataclass(frozen=True)
class DomainRegistry:
    name: str
    description: str
    datasets: List[DatasetEntry]


_REGISTRY: Optional[Dict[str, DomainRegistry]] = None


def _normalize_selector_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip().lower()
    return value


def _normalize_selector(selector: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): _normalize_selector_value(v) for k, v in (selector or {}).items()}


def _resolve_registry_path() -> str:
    registry_path = S.dataset_registry_path
    if os.path.isabs(registry_path):
        return registry_path
    return os.path.join(os.getcwd(), registry_path)


def _resolve_dataset_path(raw_path: str, registry_path: str) -> str:
    parsed = urlparse(raw_path)
    if parsed.scheme:
        return raw_path
    if os.path.isabs(raw_path):
        return raw_path
    if S.dataset_base_dir:
        return os.path.join(S.dataset_base_dir, raw_path)
    return os.path.join(os.path.dirname(registry_path), raw_path)


def load_registry() -> Dict[str, DomainRegistry]:
    global _REGISTRY
    if _REGISTRY is not None:
        return _REGISTRY

    registry_path = _resolve_registry_path()
    if not os.path.exists(registry_path):
        raise RuntimeError(f"Dataset registry not found at {registry_path}")

    with open(registry_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    out: Dict[str, DomainRegistry] = {}
    seen_keys: Dict[str, str] = {}
    for domain, info in data.items():
        if domain == "$schema":
            continue
        if not isinstance(info, dict):
            raise RuntimeError(f"Domain '{domain}' must be an object in registry.")
        desc = str((info or {}).get("description") or "")
        dataset_list = (info or {}).get("datasets", [])
        if not dataset_list:
            raise RuntimeError(f"Domain '{domain}' must define a non-empty datasets list")

        datasets = []
        for d in dataset_list:
            key = str(d.get("key") or "").strip()
            if len(key) < 2:
                raise RuntimeError(f"Dataset entry missing/short key in domain '{domain}'")
            raw_path = d.get("path")
            paths: List[str] = []
            if isinstance(raw_path, list):
                for p in raw_path:
                    p_str = str(p or "").strip()
                    if len(p_str) < 2:
                        raise RuntimeError(f"Dataset '{key}' has invalid path in domain '{domain}'")
                    paths.append(_resolve_dataset_path(p_str, registry_path))
            else:
                p_str = str(raw_path or "").strip()
                if len(p_str) < 2:
                    raise RuntimeError(f"Dataset '{key}' missing/short path in domain '{domain}'")
                paths = [_resolve_dataset_path(p_str, registry_path)]

            hive = bool(d.get("hive") or False)
            if len(paths) > 1 and not hive:
                raise RuntimeError(
                    f"Dataset '{key}' in domain '{domain}' uses a path array; set hive=true."
                )
            if key in seen_keys:
                raise RuntimeError(
                    f"Dataset key '{key}' is duplicated in domains '{seen_keys[key]}' and '{domain}'"
                )
            seen_keys[key] = str(domain)
            datasets.append(
                DatasetEntry(
                    key=key,
                    description=str(d.get("description") or ""),
                    paths=paths,
                    hive_partitioning=hive,
                    selector=_normalize_selector(d.get("selector") or {}),
                )
            )

        out[str(domain)] = DomainRegistry(name=str(domain), description=desc, datasets=datasets)

    _REGISTRY = out
    return out


def resolve_dataset(domain: str, selector: Dict[str, Any]) -> DatasetEntry:
    registry = load_registry()
    if domain not in registry:
        raise HTTPException(status_code=500, detail=f"Dataset domain '{domain}' not found in registry.")

    domain_registry = registry[domain]
    normalized_selector = _normalize_selector(selector)

    matches = []
    for d in domain_registry.datasets:
        if all(normalized_selector.get(k) == v for k, v in d.selector.items()):
            matches.append(d)

    if not matches:
        raise HTTPException(
            status_code=400,
            detail=f"No dataset matched selector for domain '{domain}'.",
        )
    if len(matches) > 1:
        keys = [m.key for m in matches]
        raise HTTPException(
            status_code=400,
            detail=f"Selector is ambiguous for domain '{domain}': {keys}",
        )
    return matches[0]


def iter_domain_datasets(domains: Iterable[str], *, allow_missing: bool = False) -> Iterable[DatasetEntry]:
    registry = load_registry()
    for domain in domains:
        if domain not in registry:
            if allow_missing:
                continue
            raise RuntimeError(f"Dataset domain '{domain}' not found in registry.")
        for d in registry[domain].datasets:
            yield d


def register_dataset_views(domains: Iterable[str], *, allow_missing: bool = False) -> None:
    con = duckdb_connect(for_http_parquet=True)
    try:
        for d in iter_domain_datasets(domains, allow_missing=allow_missing):
            if d.hive_partitioning:
                if len(d.paths) == 1:
                    source = f"read_parquet({quote_literal(d.paths[0])}, hive_partitioning=1, union_by_name=1)"
                else:
                    path_list = ", ".join(quote_literal(p) for p in d.paths)
                    source = f"read_parquet([{path_list}], hive_partitioning=1, union_by_name=1)"
            else:
                source = f"read_parquet({quote_literal(d.paths[0])})"
            con.execute(f"CREATE OR REPLACE VIEW {quote_ident(d.key)} AS SELECT * FROM {source}")
    finally:
        con.close()
