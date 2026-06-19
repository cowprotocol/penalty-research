#!/usr/bin/env python
"""Detail the target prod_<network> DBs: schemas + table listings for the
schemas we might query (raw replication vs dbt vs public)."""

from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

from db_overview import connect, parse_endpoint  # type: ignore

TARGETS = ["prod_polygon", "prod_bnb"]


def main() -> None:
    load_dotenv()
    base = parse_endpoint(os.environ["ANALYTICS_DB_URL"])
    for db in TARGETS:
        print(f"\n################ {db} ################")
        c = connect(base, db)
        with c.cursor() as cur:
            cur.execute(
                "select table_schema, count(*) from information_schema.tables "
                "where table_type in ('BASE TABLE','VIEW') "
                "and table_schema not in ('pg_catalog','information_schema') "
                "group by 1 order by 2 desc"
            )
            print("schemas (tables+views):", dict(cur.fetchall()))
            for schema in (f"raw_{db}", "dbt", "public"):
                cur.execute(
                    "select table_name from information_schema.tables "
                    "where table_schema = %s order by table_name",
                    (schema,),
                )
                names = [r[0] for r in cur.fetchall()]
                print(f"\n  [{schema}] ({len(names)}): {', '.join(names)}")
        c.close()


if __name__ == "__main__":
    main()
