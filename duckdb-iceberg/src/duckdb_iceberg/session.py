from __future__ import annotations

import os

import duckdb

from duckdb_iceberg.config import Settings


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _attach_options(settings: Settings) -> str:
    iceberg = settings.iceberg
    # s3:// attach paths default to read-only; writes need an explicit read-write attach.
    parts: list[str] = ["TYPE iceberg", "READ_ONLY false"]

    if iceberg.endpoint_type:
        parts.append(f"ENDPOINT_TYPE '{_sql_string(iceberg.endpoint_type.upper())}'")
        return ",\n        ".join(parts)

    if iceberg.rest_uri:
        parts.append(f"ENDPOINT '{_sql_string(iceberg.rest_uri)}'")

    auth = (iceberg.authorization_type or "oauth2").lower()
    if auth in {"none", "no"}:
        parts.append("AUTHORIZATION_TYPE 'none'")
    elif auth == "sigv4":
        parts.append("AUTHORIZATION_TYPE 'SigV4'")

    if iceberg.access_delegation_mode:
        parts.append(f"ACCESS_DELEGATION_MODE '{_sql_string(iceberg.access_delegation_mode)}'")

    if iceberg.stage_create_tables is False:
        parts.append("STAGE_CREATE_TABLES false")
    if iceberg.disable_multi_table_commit is True:
        parts.append("DISABLE_MULTI_TABLE_COMMIT true")

    return ",\n        ".join(parts)


def _create_storage_secret(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    storage = settings.storage

    # AWS path: resolve named profile with boto3 (supports role_arn + source_profile),
    # then hand temporary keys to DuckDB. DuckDB's own credential_chain cannot assume
    # nested role profiles (Secret Validation Failure on PROFILE + CHAIN config).
    if storage.provider == "credential_chain":
        profile = storage.aws_profile or os.getenv("AWS_PROFILE")
        if not profile:
            raise ValueError(
                "storage.provider=credential_chain requires an AWS profile. "
                "Set storage.aws_profile in config/aws.yaml or export AWS_PROFILE=your-profile"
            )
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required to resolve AWS profiles for DuckDB. "
                "pip install boto3"
            ) from exc

        session = boto3.Session(profile_name=profile, region_name=storage.region)
        creds = session.get_credentials()
        if creds is None:
            raise RuntimeError(f"boto3 could not load credentials for profile {profile!r}")
        frozen = creds.get_frozen_credentials()
        params = {
            "key_id": frozen.access_key,
            "secret": frozen.secret_key,
            "region": storage.region,
        }
        if frozen.token:
            con.execute(
                """
                CREATE OR REPLACE SECRET s3_storage (
                    TYPE s3,
                    KEY_ID $key_id,
                    SECRET $secret,
                    SESSION_TOKEN $token,
                    REGION $region
                );
                """,
                {**params, "token": frozen.token},
            )
        else:
            con.execute(
                """
                CREATE OR REPLACE SECRET s3_storage (
                    TYPE s3,
                    KEY_ID $key_id,
                    SECRET $secret,
                    REGION $region
                );
                """,
                params,
            )
        return

    if not storage.access_key_id or not storage.secret_access_key:
        raise ValueError("storage.access_key_id and storage.secret_access_key are required for static S3 auth")

    endpoint = storage.endpoint or ""
    host = endpoint.removeprefix("https://").removeprefix("http://")
    use_ssl = storage.use_ssl if storage.use_ssl is not None else endpoint.startswith("https://")
    url_style = storage.url_style or "path"

    con.execute(
        """
        CREATE OR REPLACE SECRET s3_storage (
            TYPE s3,
            KEY_ID $key_id,
            SECRET $secret,
            REGION $region,
            ENDPOINT $endpoint,
            URL_STYLE $url_style,
            USE_SSL $use_ssl
        );
        """,
        {
            "key_id": storage.access_key_id,
            "secret": storage.secret_access_key,
            "region": storage.region,
            "endpoint": host,
            "url_style": url_style,
            "use_ssl": use_ssl,
        },
    )


def _create_iceberg_secret(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    iceberg = settings.iceberg
    if iceberg.endpoint_type:
        return

    auth = (iceberg.authorization_type or "oauth2").lower()
    if auth in {"none", "no"}:
        return

    if iceberg.client_id and iceberg.client_secret:
        if iceberg.oauth2_server_uri:
            con.execute(
                """
                CREATE OR REPLACE SECRET iceberg_rest (
                    TYPE iceberg,
                    CLIENT_ID $client_id,
                    CLIENT_SECRET $client_secret,
                    OAUTH2_SERVER_URI $oauth2_server_uri
                );
                """,
                {
                    "client_id": iceberg.client_id,
                    "client_secret": iceberg.client_secret,
                    "oauth2_server_uri": iceberg.oauth2_server_uri,
                },
            )
        else:
            con.execute(
                """
                CREATE OR REPLACE SECRET iceberg_rest (
                    TYPE iceberg,
                    CLIENT_ID $client_id,
                    CLIENT_SECRET $client_secret
                );
                """,
                {"client_id": iceberg.client_id, "client_secret": iceberg.client_secret},
            )
        return

    raise ValueError(
        "Iceberg REST auth requires authorization_type none, or client_id/client_secret in config"
    )


def connect_catalog(settings: Settings, *, memory: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:" if memory else str(settings.repo_root / "duckdb_local.db"))
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")

    _create_storage_secret(con, settings)
    _create_iceberg_secret(con, settings)

    alias = settings.iceberg.catalog_alias
    attach_path = settings.iceberg.attach_path
    options = _attach_options(settings)

    secret_clause = ""
    auth = (settings.iceberg.authorization_type or "oauth2").lower()
    if not settings.iceberg.endpoint_type and auth not in {"none", "no"}:
        secret_clause = ",\n        SECRET iceberg_rest"

    con.execute(
        f"""
        ATTACH '{_sql_string(attach_path)}' AS {alias} (
            {options}{secret_clause}
        );
        """
    )
    return con
