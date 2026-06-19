#!/usr/bin/env python
"""Read-only overview of the analytics Postgres: databases, schemas, key tables.

Reads ANALYTICS_DB_URL (user:pass@host:port, no scheme/db) from .env.
"""

from __future__ import annotations

import os
import sys

import psycopg
from dotenv import load_dotenv

# Orderbook tables we care about for the penalties dataset.
KEY_TABLES = [
    "orders", "order_quotes", "order_execution", "order_events",
    "proposed_solutions", "proposed_trade_executions",
    "reference_scores", "settlements", "settlement_executions",
    "competition_auctions", "auctions", "trades", "jit_orders",
]


def parse_endpoint(raw: str) -> dict:
    userinfo, hostinfo = raw.rsplit("@", 1)
    user, password = userinfo.split(":", 1)
    host, _, port = hostinfo.partition(":")
    return {"host": host, "port": int(port or 5432), "user": user, "password": password}


def connect(conn_kwargs: dict, dbname: str):
    return psycopg.connect(
        dbname=dbname, connect_timeout=15, autocommit=True,
        options="-c default_transaction_read_only=on -c statement_timeout=30000",
        **conn_kwargs,
    )


def main() -> None:
    load_dotenv()
    raw = os.environ["ANALYTICS_DB_URL"]
    base = parse_endpoint(raw)
    print(f"server: {base['host']}:{base['port']} as {base['user']}\n")

    bootstrap = None
    for cand in ("postgres", base["user"], "defaultdb", "template1"):
        try:
            conn = connect(base, cand)
            bootstrap = cand
            break
        except Exception as e:  # noqa: BLE001
            print(f"  (cannot bootstrap via '{cand}': {str(e).splitlines()[0]})", file=sys.stderr)
    if bootstrap is None:
        sys.exit("could not connect to any bootstrap database")
    print(f"connected via '{bootstrap}'")

    with conn.cursor() as cur:
        cur.execute("select pg_is_in_recovery()")
        print(f"pg_is_in_recovery (read replica?): {cur.fetchone()[0]}")
        cur.execute("select version()")
        print(f"version: {cur.fetchone()[0].split(',')[0]}")
        cur.execute(
            "select datname from pg_database "
            "where datistemplate = false and datallowconn order by 1"
        )
        databases = [r[0] for r in cur.fetchall()]
    conn.close()
    print(f"\ndatabases ({len(databases)}): {', '.join(databases)}\n")

    for db in databases:
        print(f"=== {db} " + "=" * (60 - len(db)))
        try:
            c = connect(base, db)
        except Exception as e:  # noqa: BLE001
            print(f"  (cannot connect: {str(e).splitlines()[0]})\n")
            continue
        with c.cursor() as cur:
            cur.execute(
                "select table_schema, count(*) from information_schema.tables "
                "where table_type='BASE TABLE' "
                "and table_schema not in ('pg_catalog','information_schema') "
                "group by 1 order by 2 desc"
            )
            schemas = cur.fetchall()
            print("  schemas (tables): " + ", ".join(f"{s}={n}" for s, n in schemas))

            # Which key orderbook tables exist, and in which schema.
            cur.execute(
                "select table_name, table_schema from information_schema.tables "
                "where table_name = any(%s) "
                "and table_schema not in ('pg_catalog','information_schema') "
                "order by table_name",
                (KEY_TABLES,),
            )
            found = cur.fetchall()
            if found:
                print("  key tables: " + ", ".join(f"{t}({s})" for t, s in found))
            else:
                print("  key tables: none")
        c.close()
        print()


if __name__ == "__main__":
    main()
