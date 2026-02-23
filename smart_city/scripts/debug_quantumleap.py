#!/usr/bin/env python3
"""Check Orion subscriptions and QuantumLeap connectivity. Run after setup + update if history returns 404."""

import json
import sys

sys.path.insert(0, ".")

from src.subscriptions import list_subscriptions
from src.quantumleap_client import get_version, get_entity_history
from src.config import DEFAULT_ENTITY_ID, DEFAULT_ENTITY_TYPE, FIWARE_SERVICE


def main():
    print("1. QuantumLeap version:")
    r = get_version()
    if r.status_code != 200:
        print(f"   Failed: {r.status_code} {r.text}")
    else:
        print(f"   {r.json()}")

    print("\n2. Orion subscriptions (notification URL and lastSuccess):")
    r = list_subscriptions()
    if r.status_code != 200:
        print(f"   Failed: {r.status_code} {r.text}")
        sys.exit(1)
    subs = r.json()
    if not subs:
        print("   No subscriptions. Run: python scripts/setup_air_quality.py")
    for s in subs:
        url = s.get("notification", {}).get("http", {}).get("url", "?")
        last = s.get("notification", {}).get("lastSuccess") or s.get("notification", {}).get("lastNotification")
        print(f"   URL: {url}")
        print(f"   lastSuccess/lastNotification: {last}")

    print(f"\n3. QuantumLeap history (Fiware-Service={FIWARE_SERVICE}, last 5):")
    r = get_entity_history(DEFAULT_ENTITY_ID, entity_type=DEFAULT_ENTITY_TYPE, last_n=5)
    if r.status_code == 404:
        print("   404 No records. Ensure you ran setup_air_quality.py and update_sensor_values.py with the same tenant.")
        print("   If you just added Fiware-Service: re-run setup, then update_sensor_values.py again.")
    elif r.status_code != 200:
        print(f"   {r.status_code}: {r.text[:200]}")
    else:
        print("   OK:", json.dumps(r.json(), indent=4)[:500])

    print("\nIf history is 404: re-run 'python scripts/setup_air_quality.py' then 'python scripts/update_sensor_values.py' (tenant is now Fiware-Service: airquality).")
    print("Check QuantumLeap logs: docker compose -f docker-compose-demo.yml logs quantumleap --tail 50")


if __name__ == "__main__":
    main()
