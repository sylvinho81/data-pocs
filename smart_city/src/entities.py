"""
Create and manage AirQualityObserved entities in Orion Context Broker.
Uses the FIWARE data model: https://fiware-datamodels.readthedocs.io/
"""

import requests
from datetime import datetime, timezone
from .config import (
    ORION_ENTITIES_URL,
    ORION_BASE_URL,
    DEFAULT_ENTITY_TYPE,
    DEFAULT_ENTITY_ID,
    FIWARE_SERVICE,
    FIWARE_SERVICEPATH,
)


def _headers_json():
    """Use for POST/PATCH (requests with body). Orion rejects Content-Type on GET/DELETE."""
    return {
        "Content-Type": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
    }


def _headers_accept_json():
    """Use for GET (no body). Orion forbids Content-Type on GET requests."""
    return {
        "Accept": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
    }


def create_air_quality_entity(
    entity_id: str = DEFAULT_ENTITY_ID,
    date_observed: str | None = None,
    address_locality: str = "Ciudad de México",
    street_address: str = "Centro",
    address_country: str = "MX",
    longitude: float = -99.133167,
    latitude: float = 19.434072,
    temperature: float = 0.0,
    relative_humidity: float = 0.0,
    co: float = 0.0,
    o3: float = 0.0,
    no2: float = 0.0,
    so2: float = 0.0,
    pm10: float = 0.0,
    source: str = "http://www.aire.cdmx.gob.mx/",
) -> requests.Response:
    """
    Create an AirQualityObserved entity in Orion (NGSI v2).
    Returns the response object; 201 Created on success.
    """
    if date_observed is None:
        date_observed = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

    payload = {
        "id": entity_id,
        "type": DEFAULT_ENTITY_TYPE,
        "dateObserved": {"type": "DateTime", "value": date_observed},
        "address": {
            "type": "StructuredValue",
            "value": {
                "addressCountry": address_country,
                "addressLocality": address_locality,
                "streetAddress": street_address,
            },
        },
        "location": {
            "value": {"type": "Point", "coordinates": [longitude, latitude]},
            "type": "geo:json",
        },
        "source": {"type": "Text", "value": source},
        "temperature": {"type": "Number", "value": temperature},
        "relativeHumidity": {"type": "Number", "value": relative_humidity},
        "CO": {"type": "Number", "value": co},
        "O3": {"type": "Number", "value": o3},
        "NO2": {"type": "Number", "value": no2},
        "SO2": {"type": "Number", "value": so2},
        "PM10": {"type": "Number", "value": pm10},
    }

    return requests.post(ORION_ENTITIES_URL, json=payload, headers=_headers_json(), timeout=10)


def get_entity(entity_id: str, entity_type: str = DEFAULT_ENTITY_TYPE) -> requests.Response:
    """GET a single entity by id. Optionally filter by type."""
    url = f"{ORION_BASE_URL}/v2/entities/{entity_id}"
    if entity_type:
        url += f"?type={entity_type}"
    return requests.get(url, headers=_headers_accept_json(), timeout=10)


def update_entity_attrs(
    entity_id: str,
    attrs: dict,
) -> requests.Response:
    """
    PATCH entity attributes. attrs: e.g. {"CO": {"type": "Number", "value": 25}}
    """
    url = f"{ORION_BASE_URL}/v2/entities/{entity_id}/attrs"
    return requests.patch(url, json=attrs, headers=_headers_json(), timeout=10)


def delete_entity(entity_id: str) -> requests.Response:
    """DELETE an entity from Orion (uses Fiware-Service tenant)."""
    url = f"{ORION_BASE_URL}/v2/entities/{entity_id}"
    return requests.delete(url, headers=_headers_accept_json(), timeout=10)


def list_entities(entity_type: str | None = None, limit: int = 100) -> requests.Response:
    """List entities. Optionally filter by type."""
    url = f"{ORION_BASE_URL}/v2/entities?limit={limit}"
    if entity_type:
        url += f"&type={entity_type}"
    return requests.get(url, headers=_headers_accept_json(), timeout=10)
