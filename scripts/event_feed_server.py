#!/usr/bin/env python3
"""Serve a simple visual event feed from Kafka topics."""

import argparse
import json
import os
import re
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = ROOT / "web"
TABLE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_event_line(line, table, topic):
    event = {"table": table, "topic": topic, "raw": line}
    metadata = re.search(r"CreateTime:(\d+)\s+Partition:(\d+)\s+Offset:(\d+)", line)
    if metadata:
        event["timestamp"], event["partition"], event["offset"] = metadata.groups()

    decoder = json.JSONDecoder()
    records = []
    search_from = 0
    while len(records) < 2:
        start = line.find("{", search_from)
        if start < 0:
            break
        try:
            record, end = decoder.raw_decode(line, start)
        except json.JSONDecodeError:
            search_from = start + 1
            continue
        if isinstance(record, dict) and "payload" in record:
            records.append(record)
        search_from = end

    if records:
        key_payload = records[0].get("payload")
        event["key"] = key_payload
    if len(records) > 1:
        payload = records[1].get("payload") or {}
        event["operation"] = payload.get("op")
        event["before"] = payload.get("before")
        event["after"] = payload.get("after")
        event["source_timestamp"] = payload.get("ts_ms")
    elif records and line.rstrip().endswith("null"):
        event["operation"] = "tombstone"
        event["before"] = None
        event["after"] = None

    return event


def fetch_events(table, limit=20):
    if not TABLE_PATTERN.fullmatch(table):
        raise ValueError("Table name must be a simple SQL identifier")
    limit = max(1, min(limit, 100))
    topic = f"cdc.public.{table}"
    offsets = subprocess.run(
        [
            "docker",
            "exec",
            "kafka",
            "/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
            "--time",
            "-1",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    partitions = []
    for line in offsets.stdout.splitlines():
        parts = line.rsplit(":", 2)
        if len(parts) == 3:
            partitions.append((int(parts[1]), int(parts[2])))

    events = []
    for partition, end_offset in partitions:
        start_offset = max(0, end_offset - limit)
        cmd = [
            "docker",
            "exec",
            "kafka",
            "/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
            "--partition",
            str(partition),
            "--offset",
            str(start_offset),
            "--max-messages",
            str(limit),
            "--timeout-ms",
            "1000",
            "--formatter-property",
            "print.timestamp=true",
            "--formatter-property",
            "print.key=true",
            "--formatter-property",
            "print.headers=true",
            "--formatter-property",
            "print.offset=true",
            "--formatter-property",
            "print.partition=true",
            "--formatter-property",
            "print.value=true",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        events.extend(parse_event_line(line, table, topic) for line in lines)

    events.sort(
        key=lambda event: (
            int(event.get("timestamp", 0)),
            int(event.get("partition", 0)),
            int(event.get("offset", 0)),
        ),
        reverse=True,
    )
    return events[:limit]


class EventFeedHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/events":
            query = parse_qs(parsed.query)
            table = query.get("table", ["customers"])[0]
            try:
                limit = int(query.get("limit", ["20"])[0])
                response = fetch_events(table, limit)
                status = 200
            except (ValueError, subprocess.SubprocessError) as error:
                response = {"error": str(error)}
                status = 400 if isinstance(error, ValueError) else 502
            payload = json.dumps(response).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        html = (WEB_ROOT / "event-feed.html").read_text(encoding="utf-8")
        payload = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-host", default=os.getenv("LISTEN_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8001")))
    parser.add_argument("--event-table", default=os.getenv("EVENT_TABLE", "customers"))
    parser.add_argument("--event-limit", type=int, default=int(os.getenv("EVENT_LIMIT", "20")))
    args = parser.parse_args()

    class Handler(EventFeedHandler):
        def __init__(self, *p, **kw):
            super().__init__(*p, **kw)

    httpd = ThreadingHTTPServer((args.listen_host, args.port), Handler)
    print(f"Serving event feed on http://{args.listen_host}:{args.port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
