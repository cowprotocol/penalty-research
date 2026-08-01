#!/usr/bin/env python
"""Fetch the analytics-DB inputs needed by the analysis.

The script writes two CSV files:

1. Order-level dataset:
   One row per auction x winning-solution order, using the existing
   sql/orderbook_dataset.sql query. The exact production accounting-period
   identifiers and boundaries are attached from
   dbt.int_accounting_period_data__conversion_rates.

2. Consistency-share dataset:
   One row per accounting_period x solver from
   dbt.fct_consistency_rewards_per_solver_and_accounting_period.

Sources:
  * cow-analytics-db Postgres only (ANALYTICS_DB_URL).
  * One database per network/environment: <env>_<network>.

Usage:
    python scripts/fetch_penalties_data_cons.py \
        --chain ethereum \
        --start 2026-05-26 \
        --end 2026-06-30
Note: The script should be run from a Tuesday to Tuesday as the incomplete accounting period leads to wrong consistency reward allocation.

Default outputs:
    data/ethereum_2026-05-05_2026-07-21.csv
    data/ethereum_2026-05-05_2026-07-21_consistency_shares.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
ORDERBOOK_SQL = (REPO / "sql" / "orderbook_dataset.sql").read_text()

# Canonical CLI chain -> analytics DB network and reward-config network.
CHAINS: dict[str, dict[str, str]] = {
    "ethereum": {"db_network": "mainnet", "reward_network": "mainnet"},
    "gnosis": {"db_network": "xdai", "reward_network": "gnosis"},
    "arbitrum": {"db_network": "arbitrum-one", "reward_network": "arbitrum"},
    "base": {"db_network": "base", "reward_network": "base"},
    "avalanche_c": {"db_network": "avalanche", "reward_network": "avalanche"},
    "polygon": {"db_network": "polygon", "reward_network": "polygon"},
    "bnb": {"db_network": "bnb", "reward_network": "bnb"},
}

ACCOUNTING_PERIOD_SQL = """
select
    block_number as block_deadline,
    accounting_period
from dbt.int_accounting_period_data__conversion_rates
where block_number between %(block_lo)s and %(block_hi)s
"""

CONSISTENCY_SHARES_SQL = """
with selected_periods as (
    select distinct accounting_period
    from dbt.int_accounting_period_data__conversion_rates
    where block_number between %(block_lo)s and %(block_hi)s
      and accounting_period is not null
)
select
    c.accounting_period,
    '0x' || encode(c.solver, 'hex') as solver,
    c.consistency_reward_share,
    c.total_consistency_budget,
    c.consistency_reward_native
from dbt.fct_consistency_rewards_per_solver_and_accounting_period as c
inner join selected_periods as p
    on c.accounting_period = p.accounting_period
order by c.accounting_period, c.solver
"""

ORDER_OUTPUT_COLUMNS = [
    "auction_id",
    "order_uid",
    "solver",
    "accounting_period",
    "sell_token",
    "buy_token",
    "settled",
    "is_excluded_from_penalties",
    "volume_native",
    "reward_penalty_native",
    "reward_penalty_uncapped_native",
    "reward_cap_upper_native",
]


def parse_endpoint(raw: str) -> dict[str, object]:
    try:
        userinfo, hostinfo = raw.rsplit("@", 1)
        user, password = userinfo.split(":", 1)
        host, _, port = hostinfo.partition(":")
    except ValueError as exc:
        raise ValueError(
            "ANALYTICS_DB_URL must have the form user:password@host:port"
        ) from exc

    return {
        "host": host,
        "port": int(port or 5432),
        "user": user,
        "password": password,
    }


def dataframe_from_cursor(cur: psycopg.Cursor) -> pd.DataFrame:
    columns = [description.name for description in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=columns)


def fetch_inputs(
    chain: str,
    start: datetime,
    end: datetime,
    timeout_s: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_url = os.environ.get("ANALYTICS_DB_URL")
    if not raw_url:
        sys.exit("ANALYTICS_DB_URL is not set.")

    chain_config = CHAINS[chain]
    database = f"prod_{chain_config['db_network']}"

    params: dict[str, object] = {
        "start": start,
        "end": end,
        "network": chain_config["reward_network"],
        "solver_env": "prod",
    }

    try:
        with (
            psycopg.connect(
                dbname=database,
                connect_timeout=20,
                autocommit=True,
                options=(
                    "-c default_transaction_read_only=on "
                    f"-c statement_timeout={timeout_s * 1000} "
                    "-c timezone=UTC"
                ),
                **parse_endpoint(raw_url),
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                """
                select min(block_number), max(block_number)
                from dbt.stg_rpc_data__block_timestamp
                where time >= %(start)s
                  and time < %(end)s
                """,
                params,
            )
            params["block_lo"], params["block_hi"] = cur.fetchone()

            if params["block_lo"] is None or params["block_hi"] is None:
                sys.exit(
                    f"[db] no blocks found for {start.isoformat()}..{end.isoformat()} "
                    f"in {database}"
                )

            cur.execute(ORDERBOOK_SQL, params)
            orders = dataframe_from_cursor(cur)

            cur.execute(ACCOUNTING_PERIOD_SQL, params)
            period_map = dataframe_from_cursor(cur)

            cur.execute(CONSISTENCY_SHARES_SQL, params)
            shares = dataframe_from_cursor(cur)

    except psycopg.errors.QueryCanceled:
        sys.exit(
            f"[db] query exceeded {timeout_s}s. Narrow the date range or "
            "increase --db-timeout."
        )
    except psycopg.Error as exc:
        sys.exit(f"[db] PostgreSQL error while reading {database}: {exc}")

    if orders.empty:
        return orders, shares

    if "block_deadline" not in orders.columns:
        raise RuntimeError("sql/orderbook_dataset.sql must return block_deadline.")

    orders = orders.merge(
        period_map,
        on="block_deadline",
        how="left",
        validate="many_to_one",
    )

    missing_columns = sorted(set(ORDER_OUTPUT_COLUMNS) - set(orders.columns))
    if missing_columns:
        raise KeyError(
            f"orderbook_dataset.sql is missing required columns: {missing_columns}"
        )

    orders = orders[ORDER_OUTPUT_COLUMNS].copy()

    raw_key = ["auction_id", "solver", "order_uid"]
    duplicates = orders.duplicated(raw_key, keep=False)
    if duplicates.any():
        raise ValueError(
            "Duplicate auction-solver-order rows found:\n"
            + orders.loc[duplicates, raw_key].head(20).to_string(index=False)
        )

    missing_periods = int(orders["accounting_period"].isna().sum())
    if missing_periods:
        raise ValueError(
            f"{missing_periods}/{len(orders)} order rows have no accounting period."
        )

    return orders, shares


def parse_day(value: str) -> datetime:
    return datetime.combine(
        date.fromisoformat(value),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )


def complete_accounting_window(
    start: datetime,
    end: datetime,
) -> tuple[datetime, datetime]:
    days_to_next_tuesday = (1 - start.weekday()) % 7
    complete_start = start + pd.Timedelta(days=days_to_next_tuesday)

    days_since_tuesday = (end.weekday() - 1) % 7
    complete_end = end - pd.Timedelta(days=days_since_tuesday)

    if complete_end <= complete_start:
        raise ValueError(
            "The requested range does not contain a complete "
            "Tuesday-to-Tuesday accounting period."
        )

    return complete_start, complete_end


def consistency_path(order_path: Path) -> Path:
    return order_path.with_name(
        f"{order_path.stem}_consistency_shares{order_path.suffix}"
    )


def main() -> None:
    load_dotenv(REPO / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", required=True, choices=sorted(CHAINS))
    parser.add_argument("--start", required=True, type=parse_day)
    parser.add_argument("--end", required=True, type=parse_day)
    parser.add_argument("--out", default=None)
    parser.add_argument("--db-timeout", type=int, default=900)
    args = parser.parse_args()

    if args.end <= args.start:
        sys.exit("--end must be after --start")

    requested_start = args.start
    requested_end = args.end
    args.start, args.end = complete_accounting_window(args.start, args.end)
    if args.start != requested_start or args.end != requested_end:
        print(
            "[dates] adjusted to complete accounting periods: "
            f"{requested_start:%Y-%m-%d}..{requested_end:%Y-%m-%d} "
            f"-> {args.start:%Y-%m-%d}..{args.end:%Y-%m-%d}",
            file=sys.stderr,
        )
    order_out = (
        Path(args.out)
        if args.out
        else REPO
        / "data"
        / f"{args.chain}_{args.start:%Y-%m-%d}_{args.end:%Y-%m-%d}.csv"
    )
    shares_out = consistency_path(order_out)

    if order_out.exists() and shares_out.exists():
        print(
            f"[skip] cached files already exist for {args.chain}: "
            f"{order_out.name}, {shares_out.name}",
            file=sys.stderr,
        )
        return

    if order_out.exists() or shares_out.exists():
        print(
            "[cache] only one output exists; refetching and writing both files",
            file=sys.stderr,
        )

    database = f"prod_{CHAINS[args.chain]['db_network']}"
    print(
        f"[db] {database} {args.start:%Y-%m-%d}..{args.end:%Y-%m-%d}",
        file=sys.stderr,
    )

    orders, shares = fetch_inputs(
        chain=args.chain,
        start=args.start,
        end=args.end,
        timeout_s=args.db_timeout,
    )

    orders.insert(0, "blockchain", args.chain)
    shares.insert(0, "blockchain", args.chain)

    order_out.parent.mkdir(parents=True, exist_ok=True)
    orders.to_csv(order_out, index=False)
    shares.to_csv(shares_out, index=False)

    print(f"[out] wrote {len(orders)} rows -> {order_out}", file=sys.stderr)
    print(f"[out] wrote {len(shares)} rows -> {shares_out}", file=sys.stderr)


if __name__ == "__main__":
    main()
