#!/usr/bin/env python3
"""Expose Kafka Connect health and Kafka lag as Prometheus metrics."""

import argparse
import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def sanitize_label(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def format_metric(name, value, labels=None):
    label_string = ""
    if labels:
        entries = [f'{key}="{sanitize_label(value)}"' for key, value in sorted(labels.items())]
        label_string = "{" + ",".join(entries) + "}"
    return f"{name}{label_string} {value}\n"


def fetch_json(url):
    with urlopen(url, timeout=10) as response:
        payload = response.read().decode("utf-8")
        return json.loads(payload)


def collect_kafka_lag():
    metrics = []
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "kafka",
                "/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--describe",
                "--all-groups",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if result.returncode != 0:
            metrics.append(format_metric("kafka_consumer_group_lag_total", 0, {"group": "unknown"}))
            metrics.append(format_metric("kafka_consumer_group_lag_fetch_errors_total", 1, {"source": "kafka"}))
            return "".join(metrics)

        seen = False
        for raw_line in result.stdout.splitlines():
            parts = raw_line.split()
            if len(parts) < 6 or parts[0] == "GROUP" or parts[0] == "Consumer":
                continue
            if parts[0].startswith("__"):
                continue
            group, topic, partition, current_offset, log_end_offset, lag = parts[:6]
            try:
                lag_value = int(lag)
            except ValueError:
                continue
            seen = True
            metrics.append(
                format_metric(
                    "kafka_consumer_group_lag",
                    lag_value,
                    {"group": group, "topic": topic, "partition": partition},
                )
            )
        if not seen:
            metrics.append(format_metric("kafka_consumer_group_lag_total", 0, {"group": "none"}))
        return "\n".join(metrics)
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        metrics.append(format_metric("kafka_consumer_group_lag_fetch_errors_total", 1, {"source": "kafka"}))
        metrics.append(format_metric("kafka_consumer_group_lag_total", 0, {"group": "unknown"}))
        return "\n".join(metrics)


def collect_metrics(connect_url):
    metrics = []
    metrics.append(format_metric("kafka_connect_scrape_timestamp_seconds", int(time.time())))

    try:
        connectors = fetch_json(f"{connect_url}/connectors")
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        metrics.append(format_metric("kafka_connect_scrape_errors_total", 1, {"kind": "connectors"}))
        metrics.append(format_metric("kafka_connect_connectors_total", 0))
        return "".join(metrics) + f"# scrape_error {exc}\n"

    if not connectors:
        metrics.append(format_metric("kafka_connect_connectors_total", 0))
        lag_metrics = collect_kafka_lag()
        if lag_metrics:
            metrics.append(lag_metrics)
        return "\n".join(metric for metric in metrics if metric)

    metrics.append(format_metric("kafka_connect_connectors_total", len(connectors)))

    for connector in connectors:
        status_url = f"{connect_url}/connectors/{connector}/status"
        try:
            status = fetch_json(status_url)
        except (HTTPError, URLError, TimeoutError, ValueError):
            metrics.append(format_metric("kafka_connect_connector_up", 0, {"connector": connector}))
            metrics.append(format_metric("kafka_connect_connector_state", 1, {"connector": connector, "state": "UNKNOWN"}))
            metrics.append(format_metric("kafka_connect_scrape_errors_total", 1, {"kind": "connector_status", "connector": connector}))
            continue

        connector_state = str(status.get("connector", {}).get("state", "UNKNOWN")).upper()
        tasks = status.get("tasks", []) or []
        metrics.append(format_metric("kafka_connect_connector_up", 1 if connector_state == "RUNNING" else 0, {"connector": connector}))
        metrics.append(format_metric("kafka_connect_connector_state", 1, {"connector": connector, "state": connector_state}))
        metrics.append(format_metric("kafka_connect_connector_task_count", len(tasks), {"connector": connector}))

        for task in tasks:
            task_id = task.get("id", "unknown")
            task_state = str(task.get("state", "UNKNOWN")).upper()
            worker_id = task.get("worker_id", "unknown")
            metrics.append(format_metric("kafka_connect_task_state", 1, {"connector": connector, "task_id": str(task_id), "state": task_state}))
            metrics.append(format_metric("kafka_connect_task_worker_info", 1, {"connector": connector, "task_id": str(task_id), "worker_id": str(worker_id)}))

    lag_metrics = collect_kafka_lag()
    if lag_metrics:
        metrics.append(lag_metrics)
    return "\n".join(metric for metric in metrics if metric)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in {"/", "/metrics"}:
            payload = collect_metrics(self.server.connect_url)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(payload.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(payload.encode("utf-8"))
            return

        self.send_error(404, "Not found")

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connect-url", default=os.getenv("CONNECT_URL", "http://connect:8083"))
    parser.add_argument("--listen-host", default=os.getenv("LISTEN_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    args = parser.parse_args()

    httpd = HTTPServer((args.listen_host, args.port), MetricsHandler)
    httpd.connect_url = args.connect_url
    print(f"Starting Kafka Connect metrics exporter on {args.listen_host}:{args.port}")
    print(f"Scraping Kafka Connect at {args.connect_url}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
