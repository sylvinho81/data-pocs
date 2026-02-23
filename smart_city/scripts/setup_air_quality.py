#!/usr/bin/env python3
"""
Setup the FIWARE air quality demo:
  1. Create an AirQualityObserved entity in Orion.
  2. Create a subscription so Orion notifies QuantumLeap on updates.

Run after the stack is up: docker-compose -f docker-compose-demo.yml up -d

Usage:
  python scripts/setup_air_quality.py
  ORION_BASE_URL=http://localhost:1026 python scripts/setup_air_quality.py
"""

import sys

# Allow running from project root
sys.path.insert(0, ".")

from src.entities import create_air_quality_entity, get_entity
from src.subscriptions import create_quantumleap_subscription
from src.config import DEFAULT_ENTITY_ID, DEFAULT_ENTITY_TYPE


def main():
    print("Creating AirQualityObserved entity in Orion...")
    r = create_air_quality_entity()
    if r.status_code in (201, 204):
        print("  Entity created (or already exists).")
    elif r.status_code == 422:
        print("  Entity already exists (422 Unprocessable). Checking...")
        r2 = get_entity(DEFAULT_ENTITY_ID)
        if r2.status_code == 200:
            print("  Entity exists. OK.")
        else:
            print(f"  Unexpected: {r2.status_code} {r2.text}")
            sys.exit(1)
    else:
        print(f"  Failed: {r.status_code} {r.text}")
        sys.exit(1)

    print("Creating QuantumLeap subscription in Orion...")
    r = create_quantumleap_subscription()
    if r.status_code in (201, 204):
        print("  Subscription created.")
    else:
        print(f"  Response: {r.status_code} {r.text}")
        # Subscription might already exist
        if r.status_code in (400, 409):
            print("  You can list subscriptions and delete duplicates if needed.")
    print("Done. You can now update the entity (e.g. scripts/update_sensor_values.py) and check history in QuantumLeap/Grafana.")


if __name__ == "__main__":
    main()
