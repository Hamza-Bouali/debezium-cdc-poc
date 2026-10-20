
import os
import time
import uuid
import subprocess

import pytest
import requests


SOURCE = os.getenv("CDC_SOURCE_CONTAINER", "postgres-source")
TARGET = os.getenv("CDC_TARGET_CONTAINER", "postgres-target")
SOURCE_PASSWORD = os.getenv("CDC_SOURCE_PASSWORD", "postgres")
TARGET_PASSWORD = os.getenv("CDC_TARGET_PASSWORD", "postgres")
DB = os.getenv("CDC_DATABASE", "appdb")
DB_USER = os.getenv("CDC_DB_USER", "postgres")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
SOURCE_CONNECTOR = os.getenv(
    "CDC_SOURCE_CONNECTOR", "postgres-source"
)
SINK_CONNECTOR = os.getenv(
    "CDC_SINK_CONNECTOR", "sink-connector"
)
TIMEOUT = int(os.getenv("CDC_TIMEOUT", "90"))
POLL_INTERVAL = 2

def psql(container, sql):
    """Execute SQL inside a PostgreSQL Docker container."""
    password = (
        SOURCE_PASSWORD if container == SOURCE
        else TARGET_PASSWORD
    )

    result = subprocess.run(
        [
            "docker", "exec",
            "-e", f"PGPASSWORD={password}",
            container,
            "psql", "-X", "-q", "-A", "-t", "-F", "|",
            "-v", "ON_ERROR_STOP=1",
            "-U", DB_USER, "-d", DB, "-c", sql,
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )

    if result.returncode:
        pytest.fail(
            f"psql failed in {container}:\n"
            f"{result.stderr}\nSQL:\n{sql}"
        )

    return [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    ]

def connector_status(name):
    response = requests.get(
        f"{CONNECT_URL}/connectors/{name}/status", timeout=5
    )
    response.raise_for_status()
    return response.json()


def assert_connectors_running():
    for name in (SOURCE_CONNECTOR, SINK_CONNECTOR):
        status = connector_status(name)
        assert status["connector"]["state"] == "RUNNING", status
        assert status["tasks"], f"{name} has no tasks"
        assert all(
            task["state"] == "RUNNING" for task in status["tasks"]
        ), status


def wait_until(description, check, timeout=TIMEOUT):
    """Wait until the source and target reach the expected state."""
    deadline = time.monotonic() + timeout
    last_error = None

    while time.monotonic() < deadline:
        try:
            assert_connectors_running()
            if check():
                return
        except AssertionError as exc:
            last_error = str(exc)

            if "FAILED" in last_error:
                pytest.fail(
                    f"Connector task failed while waiting for: {description}\n"
                    f"{last_error}"
                )
        except requests.RequestException as exc:
            last_error = str(exc)

        time.sleep(POLL_INTERVAL)

    pytest.fail(
        f"Timed out waiting for: {description}\n"
        f"Last observed error/state: {last_error}"
    )


def customer_rows(container, ids):
    id_list = ", ".join(str(i) for i in ids)
    return psql(
        container,
        f"""
        SELECT id, name, email
        FROM public.customers
        WHERE id IN ({id_list})
        ORDER BY id;
        """,
    )


@pytest.fixture
def customer_test_ids():
    """Reserve three fresh IDs and remove only those IDs afterward."""
    base = 700_000_000 + (uuid.uuid4().int % 100_000_000)
    ids = [base, base + 1, base + 2]

    for container in (SOURCE, TARGET):
        existing = customer_rows(container, ids)
        assert not existing, (
            f"Refusing to overwrite existing test data in {container}: "
            f"{existing}"
        )

    try:
        yield ids
    finally:
        # These IDs were checked before the test and are unique to this run.
        psql(
            SOURCE,
            f"DELETE FROM public.customers "
            f"WHERE id IN ({', '.join(map(str, ids))});",
        )

        try:
            wait_until(
                "test customer rows are removed from target",
                lambda: (
                    customer_rows(SOURCE, ids) == []
                    and customer_rows(TARGET, ids) == []
                ),
            )
        except Exception as exc:
            print(f"WARNING: CDC cleanup did not finish: {exc}")


@pytest.fixture(scope="session", autouse=True)
def infrastructure_preflight():
    """Fail early if Docker, PostgreSQL, or Kafka Connect is unavailable."""
    for container in (SOURCE, TARGET):
        psql(container, "SELECT 1;")
    assert_connectors_running()


def test_insert_update_delete_replicate_to_target(customer_test_ids):
    """Verify INSERT, UPDATE, and DELETE propagate end to end."""
    id1, id2, id3 = customer_test_ids

    # INSERT: write to source; wait until both databases match.
    psql(
        SOURCE,
        f"""
        INSERT INTO public.customers (id, name, email)
        VALUES
          ({id1}, 'Pytest Alpha', 'alpha@example.com'),
          ({id2}, 'Pytest Beta', 'beta@example.com'),
          ({id3}, 'Pytest Gamma', 'gamma@example.com');
        """,
    )

    inserted = [
        f"{id1}|Pytest Alpha|alpha@example.com",
        f"{id2}|Pytest Beta|beta@example.com",
        f"{id3}|Pytest Gamma|gamma@example.com",
    ]

    wait_until(
        "INSERT replicated",
        lambda: customer_rows(SOURCE, customer_test_ids) == inserted
        and customer_rows(TARGET, customer_test_ids) == inserted,
    )

    # UPDATE: change one record in the source.
    psql(
        SOURCE,
        f"""
        UPDATE public.customers
        SET name = 'Pytest Beta Updated',
            email = 'beta-updated@example.com'
        WHERE id = {id2};
        """,
    )

    updated = [
        f"{id1}|Pytest Alpha|alpha@example.com",
        f"{id2}|Pytest Beta Updated|beta-updated@example.com",
        f"{id3}|Pytest Gamma|gamma@example.com",
    ]

    wait_until(
        "UPDATE replicated",
        lambda: customer_rows(SOURCE, customer_test_ids) == updated
        and customer_rows(TARGET, customer_test_ids) == updated,
    )

    # DELETE: delete one record and check that the sink removes it.
    psql(SOURCE, f"DELETE FROM public.customers WHERE id = {id1};")

    after_delete = [
        f"{id2}|Pytest Beta Updated|beta-updated@example.com",
        f"{id3}|Pytest Gamma|gamma@example.com",
    ]

    wait_until(
        "DELETE replicated",
        lambda: customer_rows(SOURCE, customer_test_ids) == after_delete
        and customer_rows(TARGET, customer_test_ids) == after_delete,
    )


def test_connect_restart_recovery(customer_test_ids):
    """Verify changes made while Connect is stopped arrive after restart."""
    customer_id = customer_test_ids[0]
    expected = [f"{customer_id}|Recovery Check|recovery-{customer_id}@example.com"]

    subprocess.run(["docker", "stop", "kafka-connect"], check=True, timeout=30)
    try:
        psql(
            SOURCE,
            f"INSERT INTO public.customers (id, name, email) "
            f"VALUES ({customer_id}, 'Recovery Check', "
            f"'recovery-{customer_id}@example.com');",
        )
    finally:
        subprocess.run(["docker", "start", "kafka-connect"], check=True, timeout=30)

    wait_until(
        "CDC resumes after Kafka Connect restart",
        lambda: customer_rows(TARGET, [customer_id]) == expected,
        timeout=180,
    )

    assert customer_rows(SOURCE, [customer_id]) == expected
    assert customer_rows(TARGET, [customer_id]) == expected


@pytest.mark.dynamic_table
def test_dynamic_table_auto_creation_and_cdc():
    """
    Verify automatic table creation plus INSERT/UPDATE/DELETE.

    Requires source capture of public tables and a sink configured with
    topics.regex, RegexRouter, and schema.evolution=basic.
    """
    suffix = uuid.uuid4().hex[:8]
    table = f"pytest_cdc_{suffix}"
    created_source = False
    created_target = False

    # table is generated from hexadecimal characters, so it is a safe identifier.
    try:
        psql(
            SOURCE,
            f"""
            CREATE TABLE public.{table} (
                id INTEGER PRIMARY KEY,
                product TEXT,
                quantity INTEGER
            );
            """,
        )
        created_source = True

        time.sleep(3)

        psql(
            SOURCE,
            f"""
            INSERT INTO public.{table} (id, product, quantity)
            VALUES (1, 'Keyboard', 2);
            """,
        )

        def rows(container):
            exists = psql(
                container,
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = '{table}'
                );
                """,
            )

            if exists != ["t"]:
                return None

            return psql(
                container,
                f"SELECT id, product, quantity "
                f"FROM public.{table} ORDER BY id;",
            )

        expected = ["1|Keyboard|2"]
        wait_until(
            "dynamic table auto-created and INSERT replicated",
            lambda: rows(SOURCE) == expected and rows(TARGET) == expected,
        )
        created_target = True

        psql(
            SOURCE,
            f"""
            UPDATE public.{table}
            SET product = 'Updated Keyboard', quantity = 5
            WHERE id = 1;
            """,
        )

        expected = ["1|Updated Keyboard|5"]
        wait_until(
            "dynamic table UPDATE replicated",
            lambda: rows(SOURCE) == expected and rows(TARGET) == expected,
        )

        psql(SOURCE, f"DELETE FROM public.{table} WHERE id = 1;")
        wait_until(
            "dynamic table DELETE replicated",
            lambda: rows(SOURCE) == [] and rows(TARGET) == [],
        )

    finally:
        # DDL drops are not normally replicated by the JDBC sink.
        if created_source:
            psql(SOURCE, f"DROP TABLE IF EXISTS public.{table};")
        # The sink may have created the table even if a later assertion failed.
        psql(TARGET, f"DROP TABLE IF EXISTS public.{table};")
