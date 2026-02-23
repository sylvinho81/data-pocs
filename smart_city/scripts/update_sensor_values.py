#!/usr/bin/env python3
"""
Simulate sensor updates: PATCH an AirQualityObserved entity with new values.
Orion will notify QuantumLeap, which stores the time-series in CrateDB.

Usage:
  python scripts/update_sensor_values.py
  python scripts/update_sensor_values.py --co 25 --temperature 22.5
"""

import argparse
import sys

sys.path.insert(0, ".")

from src.entities import update_entity_attrs, get_entity
from src.config import DEFAULT_ENTITY_ID


def main():
    ap = argparse.ArgumentParser(description="Update air quality entity attributes")
    ap.add_argument("--co", type=float, help="CO value")
    ap.add_argument("--o3", type=float, help="O3 value")
    ap.add_argument("--no2", type=float, help="NO2 value")
    ap.add_argument("--so2", type=float, help="SO2 value")
    ap.add_argument("--pm10", type=float, help="PM10 value")
    ap.add_argument("--temperature", type=float, help="Temperature")
    ap.add_argument("--relative-humidity", type=float, dest="relative_humidity", help="Relative humidity")
    args = ap.parse_args()

    attrs = {}
    if args.co is not None:
        attrs["CO"] = {"type": "Number", "value": args.co}
    if args.o3 is not None:
        attrs["O3"] = {"type": "Number", "value": args.o3}
    if args.no2 is not None:
        attrs["NO2"] = {"type": "Number", "value": args.no2}
    if args.so2 is not None:
        attrs["SO2"] = {"type": "Number", "value": args.so2}
    if args.pm10 is not None:
        attrs["PM10"] = {"type": "Number", "value": args.pm10}
    if args.temperature is not None:
        attrs["temperature"] = {"type": "Number", "value": args.temperature}
    if args.relative_humidity is not None:
        attrs["relativeHumidity"] = {"type": "Number", "value": args.relative_humidity}

    if not attrs:
        # Default: one sample update so user can see something in QuantumLeap/Grafana
        attrs = {"CO": {"type": "Number", "value": 18.0}, "temperature": {"type": "Number", "value": 21.5}}

    r = update_entity_attrs(DEFAULT_ENTITY_ID, attrs)
    if r.status_code in (200, 204):
        print("Entity updated. Orion will notify QuantumLeap.")
        print("Check history: GET http://localhost:8668/v2/entities/AirQualityUnit01?type=AirQualityObserved")
    else:
        print(f"Failed: {r.status_code} {r.text}")
        # Check if entity exists
        r2 = get_entity(DEFAULT_ENTITY_ID)
        if r2.status_code != 200:
            print("Tip: Run scripts/setup_air_quality.py first to create the entity.")
        sys.exit(1)


if __name__ == "__main__":
    main()
