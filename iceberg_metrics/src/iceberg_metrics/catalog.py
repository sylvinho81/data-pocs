"""Hive catalog connection shared by the seed writer and the metrics puller."""

from __future__ import annotations

import os
import time

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table


def load_hive_catalog(timeout_seconds: int = 120):
    """Open the Hive catalog, retrying while the metastore is still starting.

    Hive stores the table pointer (``metadata_location``). PyIceberg reads the
    metadata JSON and the manifests from MinIO.
    """
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    properties = {
        "type": "hive",
        "uri": os.getenv("HIVE_METASTORE_URI", "thrift://localhost:19083"),
        "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
        "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:19100"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": "true",
        "s3.region": os.getenv("AWS_REGION", "us-east-1"),
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }
    while time.monotonic() < deadline:
        try:
            catalog = load_catalog("hive", **properties)
            catalog.list_namespaces()
            return catalog
        except Exception as exc:  # noqa: BLE001 - thrift refuses connections until the metastore listens
            last_error = exc
            print(f"waiting for the Hive metastore ({exc})")
            time.sleep(2)
    raise TimeoutError(f"Hive metastore did not become ready: {last_error}")


def ensure_namespace(catalog, namespace: str) -> None:
    try:
        catalog.create_namespace(namespace)
        print(f"created namespace {namespace}")
    except NamespaceAlreadyExistsError:
        return


def drop_table_if_exists(catalog, identifier: str) -> None:
    try:
        # HiveCatalog.purge_table is not implemented. drop_table removes the
        # metastore entry and leaves the files in MinIO.
        catalog.drop_table(identifier)
        print(f"dropped {identifier}")
    except NoSuchTableError:
        return


def recreate_table(catalog, identifier: str, schema: Schema, partition_spec=None) -> Table:
    """Replace a table so each run starts from an empty snapshot log."""
    last_error: Exception | None = None
    namespace = identifier.split(".", 1)[0]
    for attempt in range(1, 16):
        try:
            ensure_namespace(catalog, namespace)
            drop_table_if_exists(catalog, identifier)
            kwargs = {
                "schema": schema,
                "properties": {"format-version": "2"},
            }
            if partition_spec is not None:
                kwargs["partition_spec"] = partition_spec
            table = catalog.create_table(identifier, **kwargs)
            print(f"created {identifier} at {table.location()}")
            return table
        except Exception as exc:  # noqa: BLE001 - catalog and bucket come up together
            last_error = exc
            print(f"could not create {identifier} ({exc}); retry {attempt}/15")
            time.sleep(2)
    raise RuntimeError(f"could not create {identifier}: {last_error}")


def warehouse_uri(*parts: str) -> str:
    base = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/").rstrip("/")
    return "/".join([base, *parts])


def write_bytes(io, uri: str, payload: bytes) -> None:
    """Write one object through the same S3 FileIO the catalog uses."""
    output = io.new_output(uri)
    try:
        stream = output.create(overwrite=True)
    except TypeError:
        stream = output.create()
    try:
        stream.write(payload)
    finally:
        stream.close()
