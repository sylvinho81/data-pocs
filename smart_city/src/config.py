"""Configuration for Orion Context Broker and related services."""

import os

# Orion Context Broker (NGSI v2)
ORION_BASE_URL = os.environ.get("ORION_BASE_URL", "http://localhost:1026")
ORION_ENTITIES_URL = f"{ORION_BASE_URL}/v2/entities"
ORION_SUBSCRIPTIONS_URL = f"{ORION_BASE_URL}/v2/subscriptions"

# QuantumLeap (time-series from Orion notifications)
QUANTUMLEAP_BASE_URL = os.environ.get("QUANTUMLEAP_BASE_URL", "http://localhost:8668")

# Default entity type and ID for air quality
DEFAULT_ENTITY_TYPE = "AirQualityObserved"
DEFAULT_ENTITY_ID = "AirQualityUnit01"

# FIWARE multi-tenancy: use one service so Orion and QuantumLeap store/query the same tenant
FIWARE_SERVICE = os.environ.get("FIWARE_SERVICE", "airquality")
FIWARE_SERVICEPATH = os.environ.get("FIWARE_SERVICEPATH", "/")
