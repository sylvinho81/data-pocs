"""Poll USGS FDSN Event API and publish earthquake events to Kafka."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("usgs_producer")

USGS_API_URL = os.getenv(
    "USGS_API_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query"
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes")
POLL_SECONDS = int(os.getenv("USGS_POLL_SECONDS", "60"))
MIN_MAGNITUDE = float(os.getenv("USGS_MIN_MAGNITUDE", "2.5"))
LOOKBACK_HOURS = int(os.getenv("USGS_LOOKBACK_HOURS", "24"))


def ms_to_iso(epoch_ms: int | None) -> str | None:
    """Flink JSON ISO-8601 expects millisecond precision with a Z suffix."""
    if epoch_ms is None:
        return None
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


def now_iso() -> str:
    dt = datetime.now(tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


def flatten_feature(feature: dict[str, Any]) -> dict[str, Any]:
    props = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or [None, None, None]

    longitude = coords[0] if len(coords) > 0 else None
    latitude = coords[1] if len(coords) > 1 else None
    depth_km = coords[2] if len(coords) > 2 else None

    event_time_ms = props.get("time")
    updated_ms = props.get("updated")

    return {
        "event_id": feature.get("id"),
        "magnitude": props.get("mag"),
        "place": props.get("place"),
        "event_time": ms_to_iso(event_time_ms),
        "event_time_ms": event_time_ms,
        "updated_at": ms_to_iso(updated_ms),
        "updated_ms": updated_ms,
        "url": props.get("url"),
        "status": props.get("status"),
        "tsunami": props.get("tsunami"),
        "significance": props.get("sig"),
        "network": props.get("net"),
        "mag_type": props.get("magType"),
        "event_type": props.get("type"),
        "title": props.get("title"),
        "longitude": longitude,
        "latitude": latitude,
        "depth_km": depth_km,
        "alert": props.get("alert"),
        "felt": props.get("felt"),
        "ingested_at": now_iso(),
    }


def fetch_earthquakes(start_time: datetime, end_time: datetime | None = None) -> list[dict[str, Any]]:
    params = {
        "format": "geojson",
        "orderby": "time-asc",
        "minmagnitude": MIN_MAGNITUDE,
        "starttime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "eventtype": "earthquake",
    }
    if end_time is not None:
        params["endtime"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info("Fetching USGS events start=%s minmag=%s", params["starttime"], MIN_MAGNITUDE)
    response = requests.get(USGS_API_URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    features = payload.get("features") or []
    return [flatten_feature(f) for f in features]


def create_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready at %s — retrying in 5s", KAFKA_BOOTSTRAP_SERVERS)
            time.sleep(5)


def publish_events(producer: KafkaProducer, events: list[dict[str, Any]]) -> int:
    published = 0
    for event in events:
        event_id = event.get("event_id")
        producer.send(KAFKA_TOPIC, key=event_id, value=event)
        published += 1
    producer.flush()
    return published


def main() -> None:
    producer = create_producer()
    seen_ids: set[str] = set()

    # Bootstrap: last N hours, then incremental via updated watermark
    cursor = datetime.now(tz=timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    logger.info(
        "Starting USGS → Kafka producer topic=%s poll=%ss lookback=%sh",
        KAFKA_TOPIC,
        POLL_SECONDS,
        LOOKBACK_HOURS,
    )

    while True:
        try:
            events = fetch_earthquakes(start_time=cursor)
            new_events = []
            max_updated: datetime | None = None

            for event in events:
                event_id = event.get("event_id")
                if not event_id or event_id in seen_ids:
                    continue
                seen_ids.add(event_id)
                new_events.append(event)

                updated_ms = event.get("updated_ms")
                if updated_ms is not None:
                    updated_dt = datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc)
                    if max_updated is None or updated_dt > max_updated:
                        max_updated = updated_dt

            if new_events:
                count = publish_events(producer, new_events)
                logger.info("Published %s new earthquakes (seen=%s)", count, len(seen_ids))
            else:
                logger.info("No new earthquakes since cursor=%s", cursor.isoformat())

            # Advance cursor slightly before last update to avoid gaps; dedupe via seen_ids
            if max_updated is not None:
                cursor = max_updated - timedelta(seconds=1)
            else:
                cursor = datetime.now(tz=timezone.utc) - timedelta(minutes=5)

            # Bound memory for long-running producer
            if len(seen_ids) > 50_000:
                seen_ids = set(list(seen_ids)[-25_000:])

        except Exception:
            logger.exception("Poll cycle failed; will retry")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
