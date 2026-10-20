#!/usr/bin/env python3
"""Generate committed CDC events for a selected public PostgreSQL table."""

import argparse
import json
import re
import secrets
import subprocess
import time
import uuid


IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def quote_identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
    return f'"{value}"'


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_psql(sql):
    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres-source",
                "sh",
                "-c",
                'psql -X -q -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" '
                '-d "$POSTGRES_DB"',
            ],
            input=sql,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(error.stderr.strip() or str(error)) from error
    return result.stdout.strip()


def inspect_columns(table):
    table_id = quote_identifier(table)
    query = f"""
        SELECT coalesce(
            json_agg(row_to_json(column_info) ORDER BY ordinal_position),
            '[]'::json
        )::text
        FROM (
            SELECT ordinal_position, column_name, data_type, udt_name, is_nullable,
                   column_default, is_identity, is_generated,
                   character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = {sql_literal(table)}
        ) AS column_info;
    """
    columns = json.loads(run_psql(query))
    if not columns:
        raise ValueError(f"Table public.{table} does not exist or has no columns")

    relation = f"public.{table_id}"
    primary_key_query = f"""
        SELECT coalesce(json_agg(attribute.attname ORDER BY key_column.ordinality),
                        '[]'::json)::text
        FROM pg_index AS index_info
        CROSS JOIN LATERAL unnest(index_info.indkey)
            WITH ORDINALITY AS key_column(attnum, ordinality)
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = index_info.indrelid
         AND attribute.attnum = key_column.attnum
        WHERE index_info.indrelid = {sql_literal(relation)}::regclass
          AND index_info.indisprimary
          AND key_column.ordinality <= index_info.indnkeyatts;
    """
    primary_key = json.loads(run_psql(primary_key_query))

    for column in columns:
        enum_query = f"""
            SELECT coalesce(json_agg(enum.enumlabel ORDER BY enum.enumsortorder),
                            '[]'::json)::text
            FROM pg_attribute AS attribute
            JOIN pg_type AS column_type ON column_type.oid = attribute.atttypid
            JOIN pg_enum AS enum ON enum.enumtypid = column_type.oid
            WHERE attribute.attrelid = {sql_literal(relation)}::regclass
              AND attribute.attname = {sql_literal(column['column_name'])};
        """
        column["enum_values"] = json.loads(run_psql(enum_query))

    return columns, primary_key


def generated_value(column, run_id, index, phase):
    name = column["column_name"].lower()
    data_type = column["data_type"].lower()
    udt_name = column["udt_name"].lower()
    changed = phase == "updated"
    suffix = f"{run_id}-{index}-{phase}"

    if column["enum_values"]:
        choices = column["enum_values"]
        initial_index = (index - 1) % len(choices)
        choice_index = (initial_index + (1 if changed and len(choices) > 1 else 0)) % len(choices)
        return sql_literal(choices[choice_index])
    if data_type in {"text", "character varying", "character", "name"}:
        if "email" in name:
            value = f"cdc-{suffix}@example.invalid"
        elif name in {"name", "title", "description"}:
            value = f"CDC demo {run_id} {index} {phase}"
        else:
            value = f"cdc-{suffix}"
        max_length = column["character_maximum_length"]
        return sql_literal(value[:max_length] if max_length else value)
    if data_type in {"smallint", "integer", "bigint"}:
        return str(index + (101 if changed else 1))
    if data_type in {"numeric", "decimal", "real", "double precision"}:
        return str(index + (101 if changed else 1))
    if data_type == "boolean":
        return "TRUE" if changed else "FALSE"
    if data_type == "uuid" or udt_name == "uuid":
        return sql_literal(str(uuid.uuid4()))
    if data_type == "json":
        return f"{sql_literal(json.dumps({'run': run_id, 'row': index, 'phase': phase}))}::json"
    if data_type == "jsonb":
        return f"{sql_literal(json.dumps({'run': run_id, 'row': index, 'phase': phase}))}::jsonb"
    if data_type == "date":
        return f"CURRENT_DATE + {index + (1 if changed else 0)}"
    if data_type.startswith("timestamp"):
        delta = "CURRENT_TIMESTAMP" if changed else "CURRENT_TIMESTAMP - INTERVAL '1 minute'"
        return delta
    if data_type.startswith("time"):
        return sql_literal("11:00:00" if changed else "10:00:00")
    if data_type == "interval":
        return f"INTERVAL '{2 if changed else 1} minutes'"
    if data_type == "bytea":
        return f"decode({sql_literal(suffix.encode().hex())}, 'hex')"
    if data_type == "ARRAY":
        element_type = udt_name.removeprefix("_")
        if IDENTIFIER.fullmatch(element_type):
            return f"ARRAY[]::{element_type}[]"
    if data_type == "inet":
        return sql_literal("192.0.2.2" if changed else "192.0.2.1")
    if data_type == "cidr":
        return sql_literal("198.51.100.0/24")
    if data_type == "macaddr":
        return sql_literal("02:00:00:00:00:02" if changed else "02:00:00:00:00:01")
    if data_type == "xml":
        return f"XML {sql_literal(f'<event phase=\"{phase}\">{run_id}</event>')}"
    if column["is_nullable"] == "YES":
        return "NULL"
    raise ValueError(
        f"Cannot generate a value for required column {name!r} "
        f"of type {column['data_type']} ({column['udt_name']})"
    )


def insert_event(table, columns, primary_key, run_id, index):
    writable = [
        column
        for column in columns
        if column["is_identity"] != "YES"
        and column["is_generated"] == "NEVER"
        and column["column_default"] is None
    ]
    for column in writable:
        if column["is_nullable"] == "NO":
            generated_value(column, run_id, index, "created")

    table_id = quote_identifier(table)
    if writable:
        column_sql = ", ".join(quote_identifier(column["column_name"]) for column in writable)
        value_sql = ", ".join(
            generated_value(column, run_id, index, "created") for column in writable
        )
        insert_sql = f"INSERT INTO public.{table_id} ({column_sql}) VALUES ({value_sql})"
    else:
        insert_sql = f"INSERT INTO public.{table_id} DEFAULT VALUES"
    inserted = json.loads(run_psql(f"{insert_sql} RETURNING to_jsonb({table_id});"))
    key = {column: inserted[column] for column in primary_key}
    return key, writable


def change_event(table, columns, primary_key, run_id, key, writable, index, keep):
    table_id = quote_identifier(table)
    predicate_json = sql_literal(json.dumps(key, separators=(",", ":")))
    predicate = f"to_jsonb(target) @> {predicate_json}::jsonb"
    mutable = [column for column in writable if column["column_name"] not in primary_key]
    if not mutable:
        raise ValueError(f"Table public.{table} has no writable non-primary-key column to update")

    update_column = next(
        (column for column in mutable if column["column_name"] in {"name", "updated_at", "status", "email"}),
        mutable[0],
    )
    update_value = generated_value(update_column, run_id, index, "updated")
    update_sql = (
        f"UPDATE public.{table_id} AS target "
        f"SET {quote_identifier(update_column['column_name'])} = {update_value} "
        f"WHERE {predicate};"
    )
    run_psql(update_sql)
    print(f"UPDATE public.{table} key={json.dumps(key)} column={update_column['column_name']}", flush=True)

    if not keep:
        run_psql(f"DELETE FROM public.{table_id} AS target WHERE {predicate};")
        print(f"DELETE public.{table} key={json.dumps(key)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="customers", help="public source table name (default: customers)")
    parser.add_argument("--count", type=int, default=3, help="rows to generate (default: 3)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between changes (default: 1)")
    parser.add_argument("--keep", action="store_true", help="leave generated rows in the source and target databases")
    args = parser.parse_args()

    try:
        quote_identifier(args.table)
    except ValueError as error:
        parser.error(str(error))
    if not 1 <= args.count <= 100:
        parser.error("--count must be between 1 and 100")
    if args.interval < 0:
        parser.error("--interval must be zero or greater")

    try:
        columns, primary_key = inspect_columns(args.table)
        if not primary_key:
            raise ValueError(f"Table public.{args.table} needs a primary key for update/delete events")
        run_id = secrets.token_hex(4)
        print(f"Generating events for public.{args.table} ({args.count} row(s), run {run_id})", flush=True)

        for index in range(1, args.count + 1):
            key, writable = insert_event(args.table, columns, primary_key, run_id, index)
            print(f"INSERT public.{args.table} key={json.dumps(key)}", flush=True)
            if args.interval:
                time.sleep(args.interval)
            change_event(args.table, columns, primary_key, run_id, key, writable, index, args.keep)
            if not args.keep and args.interval:
                time.sleep(args.interval)

        operation_names = "insert/update" if args.keep else "insert/update/delete"
        print(f"Done: {args.count} row(s), {operation_names} events requested.")
    except (ValueError, RuntimeError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()