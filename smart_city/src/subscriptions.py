"""
Create Orion subscriptions so that context changes are notified to QuantumLeap.
QuantumLeap stores history in CrateDB for time-series and Grafana.
"""

import requests
from .config import (
    ORION_SUBSCRIPTIONS_URL,
    QUANTUMLEAP_BASE_URL,
    FIWARE_SERVICE,
    FIWARE_SERVICEPATH,
)


def _headers_json():
    return {
        "Content-Type": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
    }


def _headers_accept_json():
    """Orion forbids Content-Type on GET requests."""
    return {
        "Accept": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
    }


# From Orion's perspective, QuantumLeap runs in the same Docker network as "quantumleap".
# So the notify URL must be http://quantumleap:8668/v2/notify (not localhost).
# We allow override for when running scripts from host (e.g. in CI) where a tunnel might be used.
def _quantumleap_notify_url() -> str:
    import os
    base = os.environ.get("QUANTUMLEAP_INTERNAL_URL", "http://quantumleap:8668")
    return f"{base.rstrip('/')}/v2/notify"


def create_quantumleap_subscription(
    description: str = "Suscripcion QuantumLeap AirQuality",
    entity_id_pattern: str = ".*",
    entity_type: str = "AirQualityObserved",
    condition_attrs: list[str] | None = None,
    notify_attrs: list[str] | None = None,
    notify_url: str | None = None,
) -> requests.Response:
    """
    Create a subscription in Orion so that every change to AirQualityObserved
    (matching condition attributes) is sent to QuantumLeap.
    """
    if condition_attrs is None:
        condition_attrs = [
            "CO", "O3", "PM10", "SO2", "NO2",
            "temperature", "relativeHumidity", "dateObserved",
        ]
    if notify_attrs is None:
        notify_attrs = [
            "id", "CO", "O3", "PM10", "SO2", "NO2",
            "temperature", "relativeHumidity", "dateObserved",
            "address", "location",
        ]
    if notify_url is None:
        notify_url = _quantumleap_notify_url()

    payload = {
        "description": description,
        "subject": {
            "entities": [{"idPattern": entity_id_pattern, "type": entity_type}],
            "condition": {"attrs": condition_attrs},
        },
        "notification": {
            "attrs": notify_attrs,
            "http": {"url": notify_url},
            "metadata": ["dateCreated", "dateModified"],
        },
    }

    return requests.post(
        ORION_SUBSCRIPTIONS_URL,
        json=payload,
        headers=_headers_json(),
        timeout=10,
    )


def list_subscriptions() -> requests.Response:
    """List all subscriptions in Orion."""
    return requests.get(ORION_SUBSCRIPTIONS_URL, headers=_headers_accept_json(), timeout=10)


def delete_subscription(subscription_id: str) -> requests.Response:
    """Delete a subscription by id."""
    base = ORION_SUBSCRIPTIONS_URL.rsplit("/v2/subscriptions", 1)[0]
    url = f"{base}/v2/subscriptions/{subscription_id}"
    return requests.delete(url, timeout=10)
