#!/usr/bin/env python3
"""Query current entity from Orion or history from QuantumLeap."""

import argparse
import json
import sys

sys.path.insert(0, ".")

from src.entities import get_entity
from src.quantumleap_client import get_entity_history
from src.config import DEFAULT_ENTITY_ID, DEFAULT_ENTITY_TYPE


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity-id", default=DEFAULT_ENTITY_ID, help="Entity ID")
    ap.add_argument("--history", action="store_true", help="Query QuantumLeap history instead of Orion")
    ap.add_argument("--last-n", type=int, default=10, help="Last N records (history only)")
    args = ap.parse_args()

    if args.history:
        r = get_entity_history(args.entity_id, entity_type=DEFAULT_ENTITY_TYPE, last_n=args.last_n)
        print(f"QuantumLeap history (last {args.last_n}):")
    else:
        r = get_entity(args.entity_id)
        print("Orion current entity:")
    if r.status_code != 200:
        print(f"Error {r.status_code}: {r.text}")
        sys.exit(1)
    print(json.dumps(r.json(), indent=2))


if __name__ == "__main__":
    main()
