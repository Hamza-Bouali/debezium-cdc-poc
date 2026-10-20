#!/usr/bin/env python3
"""Create or update the project's Kafka Connect connectors."""

import argparse
import json
from pathlib import Path
import time

import requests


ROOT = Path(__file__).resolve().parents[1]
CONNECTORS = ("source-connector.json", "sink-connector.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connect-url", default="http://localhost:8083")
    args = parser.parse_args()
    session = requests.Session()

    for filename in CONNECTORS:
        payload = json.loads((ROOT / "connect" / filename).read_text())
        name = payload["name"]
        response = session.post(
            f"{args.connect_url}/connectors", json=payload, timeout=30
        )
        if response.status_code == 409:
            response = session.put(
                f"{args.connect_url}/connectors/{name}/config",
                json=payload["config"],
                timeout=30,
            )
        response.raise_for_status()
        print(f"{name}: {'created' if response.status_code == 201 else 'updated'}")

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        statuses = [
            session.get(
                f"{args.connect_url}/connectors/{name}/status", timeout=10
            ).json()
            for name in ("postgres-source", "sink-connector")
        ]
        if all(
            status.get("connector", {}).get("state") == "RUNNING"
            and status.get("tasks")
            and all(task.get("state") == "RUNNING" for task in status["tasks"])
            for status in statuses
        ):
            print("Both connector tasks are RUNNING")
            return
        time.sleep(2)

    raise TimeoutError("Connectors did not reach RUNNING state within 120 seconds")


if __name__ == "__main__":
    main()