from __future__ import annotations

import urllib.error
import urllib.request

from duckdb_iceberg.config import Settings


def _rest_delete(settings: Settings, path: str) -> None:
    base = (settings.iceberg.rest_uri or "").rstrip("/")
    if not base:
        return
    url = f"{base}{path}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in {404, 409}:
            return
        raise


def purge_rest_namespace(settings: Settings) -> None:
    """Best-effort REST cleanup when DuckDB cannot drop a broken table."""
    ns = settings.table.namespace
    table = settings.table.name
    _rest_delete(settings, f"/v1/namespaces/{ns}/tables/{table}")
    _rest_delete(settings, f"/v1/namespaces/{ns}")
