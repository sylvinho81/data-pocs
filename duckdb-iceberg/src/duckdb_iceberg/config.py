from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class IcebergSettings:
    attach_path: str
    catalog_alias: str
    rest_uri: str | None
    endpoint_type: str | None
    authorization_type: str | None
    access_delegation_mode: str | None
    oauth2_server_uri: str | None
    client_id: str | None
    client_secret: str | None
    stage_create_tables: bool | None
    disable_multi_table_commit: bool | None


@dataclass(frozen=True)
class StorageSettings:
    region: str
    provider: str
    aws_profile: str | None
    access_key_id: str | None
    secret_access_key: str | None
    endpoint: str | None
    url_style: str | None
    use_ssl: bool | None


@dataclass(frozen=True)
class TableSettings:
    namespace: str
    name: str

    @property
    def qualified(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True)
class Settings:
    name: str
    iceberg: IcebergSettings
    storage: StorageSettings
    table: TableSettings
    repo_root: Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_config_path() -> Path:
    return _repo_root() / "config" / "local.yaml"


def _coalesce_env(env_key: str, value: str | None) -> str | None:
    override = os.getenv(env_key)
    if override is not None and override != "":
        return override
    return value


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def load_settings(config_path: Path | None = None) -> Settings:
    path = config_path or Path(os.getenv("CONFIG_PATH", str(_default_config_path())))
    if not path.is_absolute():
        path = _repo_root() / path

    with path.open(encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}

    iceberg_raw = raw.get("iceberg") or {}
    storage_raw = raw.get("storage") or {}
    table_raw = raw.get("table") or {}

    iceberg = IcebergSettings(
        attach_path=_coalesce_env("ICEBERG_WAREHOUSE", iceberg_raw.get("attach_path")) or "",
        catalog_alias=iceberg_raw.get("catalog_alias") or "lake",
        rest_uri=_coalesce_env("ICEBERG_REST_URI", iceberg_raw.get("rest_uri")),
        endpoint_type=iceberg_raw.get("endpoint_type"),
        authorization_type=iceberg_raw.get("authorization_type"),
        access_delegation_mode=iceberg_raw.get("access_delegation_mode"),
        oauth2_server_uri=iceberg_raw.get("oauth2_server_uri"),
        client_id=_coalesce_env("ICEBERG_CLIENT_ID", iceberg_raw.get("client_id")),
        client_secret=_coalesce_env("ICEBERG_CLIENT_SECRET", iceberg_raw.get("client_secret")),
        stage_create_tables=_bool_or_none(iceberg_raw.get("stage_create_tables")),
        disable_multi_table_commit=_bool_or_none(iceberg_raw.get("disable_multi_table_commit")),
    )

    storage = StorageSettings(
        region=_coalesce_env("AWS_REGION", storage_raw.get("region")) or "us-east-1",
        provider=storage_raw.get("provider") or "static",
        aws_profile=_coalesce_env("AWS_PROFILE", storage_raw.get("aws_profile")),
        access_key_id=_coalesce_env("AWS_ACCESS_KEY_ID", storage_raw.get("access_key_id")),
        secret_access_key=_coalesce_env("AWS_SECRET_ACCESS_KEY", storage_raw.get("secret_access_key")),
        endpoint=_coalesce_env("S3_ENDPOINT", storage_raw.get("endpoint")),
        url_style=storage_raw.get("url_style"),
        use_ssl=_bool_or_none(storage_raw.get("use_ssl")),
    )

    table = TableSettings(
        namespace=_coalesce_env("ICEBERG_NAMESPACE", table_raw.get("namespace")) or "poc",
        name=_coalesce_env("ICEBERG_TABLE", table_raw.get("name")) or "daily_metrics",
    )

    return Settings(
        # Optional label for logs (local / docker / aws_s3_tables). Not the AWS CLI profile.
        name=raw.get("name") or raw.get("profile") or "unknown",
        iceberg=iceberg,
        storage=storage,
        table=table,
        repo_root=_repo_root(),
    )
