#!/usr/bin/env python
"""Confirm grains + columns needed to write the assembly query, and sanity-check
counts for a recent window on prod_polygon."""

from __future__ import annotations

import os

from dotenv import load_dotenv

from db_overview import connect, parse_endpoint  # type: ignore

DB = "prod_polygon"


def cols(cur, schema, table):
    cur.execute(
        "select column_name, data_type from information_schema.columns "
        "where table_schema=%s and table_name=%s order by ordinal_position",
        (schema, table),
    )
    return cur.fetchall()


def main() -> None:
    load_dotenv()
    base = parse_endpoint(os.environ["ANALYTICS_DB_URL"])
    c = connect(base, DB)
    with c.cursor() as cur:
        for schema, table in [
            ("public", "block_to_timestamp"),
            ("raw_prod_polygon", "proposed_trade_executions"),
            ("dbt", "stg_backend_data__orders"),
        ]:
            print(f"### {schema}.{table}")
            print("  " + ", ".join(f"{n}:{t}" for n, t in cols(cur, schema, table)))

        # Grain checks (rows vs distinct keys) on a recent window.
        print("\n### grain checks")
        checks = [
            ("dbt.fct_data_per_trade", "order_uid", "auction_id"),
            ("dbt.fct_time_to_happy_moo__sli", "uid", None),
            ("dbt.int_backend_data__orders_with_winning_quotes", "order_uid", None),
            ("dbt.int_backend_data__winning_solutions_with_onchain_status", "auction_id", "solution_uid"),
            ("dbt.fct_solver_rewards_per_auction", "auction_id", "solver"),
        ]
        for tbl, k1, k2 in checks:
            keys = k1 if k2 is None else f"{k1}, {k2}"
            cur.execute(f"select count(*) total, count(distinct ({keys})) distinct_keys from {tbl}")
            total, distinct = cur.fetchone()
            print(f"  {tbl}: rows={total} distinct({keys})={distinct}")

        # Eligibility / FOK / settled distribution on a recent 3-day window.
        print("\n### in-market FOK funnel (winning orders, last ~3d)")
        cur.execute("""
            with wins as (
                select pte.auction_id, pte.order_uid, ws.solver, ws.is_settled_in_time
                from dbt.int_backend_data__winning_solutions_with_onchain_status ws
                join raw_prod_polygon.proposed_trade_executions pte
                  on pte.auction_id = ws.auction_id and pte.solution_uid = ws.solution_uid
                where ws.block_deadline >= (select max(block_deadline) - 120000
                                            from dbt.int_backend_data__winning_solutions_with_onchain_status)
            )
            select
                count(*) as winning_orders,
                count(*) filter (where oq.is_eligible_for_quote_reward) as in_market,
                count(*) filter (where oq.is_eligible_for_quote_reward and not o.partially_fillable) as in_market_fok,
                count(*) filter (where oq.is_eligible_for_quote_reward and not o.partially_fillable
                                       and not wins.is_settled_in_time) as in_market_fok_reverted
            from wins
            left join dbt.int_backend_data__orders_with_winning_quotes oq on oq.order_uid = wins.order_uid
            left join dbt.stg_backend_data__orders o on o.uid = wins.order_uid
        """)
        print("  ", cur.fetchone())
    c.close()


if __name__ == "__main__":
    main()
