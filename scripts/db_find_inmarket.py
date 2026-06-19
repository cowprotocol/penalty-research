#!/usr/bin/env python
"""Locate the in-market / quote-reward-eligibility flag in the dbt schema, and
capture view definitions for the eligibility-related models."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from db_overview import connect, parse_endpoint  # type: ignore

DB = "prod_polygon"


def main() -> None:
    load_dotenv()
    base = parse_endpoint(os.environ["ANALYTICS_DB_URL"])
    c = connect(base, DB)
    with c.cursor() as cur:
        print("### dbt columns matching market/eligible/quote_reward/verified")
        cur.execute(
            "select table_name, column_name, data_type from information_schema.columns "
            "where table_schema='dbt' and ("
            " column_name ilike '%market%' or column_name ilike '%eligib%'"
            " or column_name ilike '%quote_reward%' or column_name ilike '%verified%') "
            "order by table_name, column_name"
        )
        for t, col, dt in cur.fetchall():
            print(f"  {t}.{col}:{dt}")

        print("\n### which dbt objects are views vs tables (eligibility-related)")
        cur.execute(
            "select table_name, table_type from information_schema.tables "
            "where table_schema='dbt' and ("
            " table_name ilike '%quote%' or table_name ilike '%eligib%'"
            " or table_name ilike '%market%' or table_name ilike '%order%') "
            "order by table_name"
        )
        objs = cur.fetchall()
        for t, tt in objs:
            print(f"  {t}: {tt}")

        print("\n### view definitions (if any) for eligibility models")
        for t, tt in objs:
            if tt == "VIEW":
                cur.execute("select pg_get_viewdef(%s::regclass, true)", (f"dbt.{t}",))
                print(f"\n--- dbt.{t} ---\n{cur.fetchone()[0]}")

        print("\n### columns: int_backend_data__excluded_quotes")
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema='dbt' and table_name='int_backend_data__excluded_quotes' "
            "order by ordinal_position"
        )
        print("  " + ", ".join(f"{n}:{t}" for n, t in cur.fetchall()))
    c.close()


if __name__ == "__main__":
    main()
