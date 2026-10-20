#!/usr/bin/env python3
"""Measure source-to-target latency for a single generated insert batch."""

import argparse
import os
import secrets
import subprocess
import time


SOURCE = os.getenv("CDC_SOURCE_CONTAINER", "postgres-source")
TARGET = os.getenv("CDC_TARGET_CONTAINER", "postgres-target")
DB = os.getenv("CDC_DATABASE", "appdb")
DB_USER = os.getenv("CDC_DB_USER", "postgres")
SOURCE_PASSWORD = os.getenv("CDC_SOURCE_PASSWORD", "postgres")
TARGET_PASSWORD = os.getenv("CDC_TARGET_PASSWORD", "postgres")


def psql(container, password, sql):
    result = subprocess.run(
        [
            "docker", "exec", "-e", f"PGPASSWORD={password}", container,
            "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
            "-U", DB_USER, "-d", DB, "-c", sql,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if not 1 <= args.rows <= 100_000:
        parser.error("--rows must be between 1 and 100000")

    base_id = 1_500_000_000 + secrets.randbelow(500_000_000 - args.rows)
    upper_id = base_id + args.rows
    inserted = False
    started = time.perf_counter()

    try:
        psql(
            SOURCE,
            SOURCE_PASSWORD,
            f"""
            INSERT INTO public.customers (id, name, email)
            SELECT generated_id, 'CDC benchmark',
                   'cdc-bench-' || generated_id || '@example.invalid'
            FROM generate_series({base_id}, {upper_id - 1}) AS generated_id;
            """,
        )
        inserted = True

        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            count = int(
                psql(
                    TARGET,
                    TARGET_PASSWORD,
                    f"SELECT count(*) FROM public.customers "
                    f"WHERE id >= {base_id} AND id < {upper_id};",
                )
            )
            if count == args.rows:
                elapsed = time.perf_counter() - started
                print(f"Rows: {args.rows}")
                print(f"Source-to-target batch latency: {elapsed:.3f} s")
                print(f"Effective throughput: {args.rows / elapsed:.1f} rows/s")
                print("Scope: one local batch; includes source insert and target convergence")
                return
            time.sleep(0.2)

        raise TimeoutError(
            f"Target received {count} of {args.rows} rows within {args.timeout}s"
        )
    finally:
        if inserted:
            psql(
                SOURCE,
                SOURCE_PASSWORD,
                f"DELETE FROM public.customers "
                f"WHERE id >= {base_id} AND id < {upper_id};",
            )
            cleanup_deadline = time.monotonic() + args.timeout
            while time.monotonic() < cleanup_deadline:
                remaining = int(
                    psql(
                        TARGET,
                        TARGET_PASSWORD,
                        f"SELECT count(*) FROM public.customers "
                        f"WHERE id >= {base_id} AND id < {upper_id};",
                    )
                )
                if remaining == 0:
                    break
                time.sleep(0.2)


if __name__ == "__main__":
    main()