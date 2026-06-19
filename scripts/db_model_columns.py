#!/usr/bin/env python
"""Inspect columns of the high-value dbt models + dump small config tables,
to decide how much the script can lean on the dbt layer vs raw tables."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from db_overview import connect, parse_endpoint  # type: ignore

DB = "prod_polygon"

MODELS = [
    ("dbt", "fct_solver_rewards_per_auction"),
    ("dbt", "int_backend_data__winning_solutions_with_onchain_status"),
    ("dbt", "int_backend_data__winning_solutions"),
    ("dbt", "fct_data_per_trade"),
    ("dbt", "int_backend_data__orders_with_winning_quotes"),
    ("dbt", "fct_time_to_happy_moo__sli"),
    ("dbt", "int_backend_data__trade_data_processed_with_native_prices"),
]

DUMP = ["reward_config", "protocol_fees_scaling_cap_config"]


def main() -> None:
    load_dotenv()
    base = parse_endpoint(os.environ["ANALYTICS_DB_URL"])
    c = connect(base, DB)
    with c.cursor() as cur:
        for schema, table in MODELS:
            cur.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_schema=%s and table_name=%s order by ordinal_position",
                (schema, table),
            )
            cols = cur.fetchall()
            print(f"\n### {schema}.{table} ({len(cols)} cols)")
            print("  " + ", ".join(f"{n}:{t}" for n, t in cols))

        for table in DUMP:
            print(f"\n### dbt.{table} contents")
            try:
                cur.execute(f"select * from dbt.{table} limit 50")
                headers = [d.name for d in cur.description]
                print("  cols:", headers)
                for row in cur.fetchall():
                    print("  ", row)
            except Exception as e:  # noqa: BLE001
                print("  (error:", str(e).splitlines()[0], ")")
    c.close()


if __name__ == "__main__":
    main()
