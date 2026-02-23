"""Query historical data from QuantumLeap (time-series stored in CrateDB)."""

import requests
from .config import (
    QUANTUMLEAP_BASE_URL,
    DEFAULT_ENTITY_TYPE,
    FIWARE_SERVICE,
    FIWARE_SERVICEPATH,
)

# History queries can be slow (CrateDB); use a longer timeout.
HISTORY_TIMEOUT = 60


def _headers():
    return {
        "Accept": "application/json",
        "Fiware-Service": FIWARE_SERVICE,
        "Fiware-ServicePath": FIWARE_SERVICEPATH,
    }


def get_entity_history(
    entity_id: str,
    entity_type: str = DEFAULT_ENTITY_TYPE,
    from_date: str | None = None,
    to_date: str | None = None,
    last_n: int | None = None,
    limit: int | None = None,
) -> requests.Response:
    """
    Get time-series history for an entity from QuantumLeap.
    Optional: from_date, to_date (ISO8601), last_n records, or limit (max rows).
    """
    url = f"{QUANTUMLEAP_BASE_URL}/v2/entities/{entity_id}"
    params = {"type": entity_type}
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    if last_n is not None:
        params["lastN"] = last_n
    if limit is not None:
        params["limit"] = limit
    elif last_n is not None:
        # Bound the query when using lastN so the server doesn't scan too much
        params["limit"] = max(last_n, 1000)

    return requests.get(url, params=params, headers=_headers(), timeout=HISTORY_TIMEOUT)


def get_version() -> requests.Response:
    """Check QuantumLeap service version."""
    return requests.get(f"{QUANTUMLEAP_BASE_URL}/v2/version", headers=_headers(), timeout=10)
