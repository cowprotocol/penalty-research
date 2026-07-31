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

# chain -> names in each namespace. db_network forms the database
# (`<env>_<network>`); reward_network keys dbt.reward_config / solver registry.
CHAINS: dict[str, dict[str, str]] = {
    "polygon": {
        "dune": "polygon",
        "db_network": "polygon",
        "reward_network": "polygon",
    },
    "bnb": {"dune": "bnb", "db_network": "bnb", "reward_network": "bnb"},
    # ready to extend:
    "ethereum": {
        "dune": "ethereum",
        "db_network": "mainnet",
        "reward_network": "mainnet",
    },
    "gnosis": {"dune": "gnosis", "db_network": "xdai", "reward_network": "gnosis"},
    "arbitrum": {
        "dune": "arbitrum",
        "db_network": "arbitrum-one",
        "reward_network": "arbitrum",
    },
    "base": {"dune": "base", "db_network": "base", "reward_network": "base"},
    "avalanche_c": {
        "dune": "avalanche_c",
        "db_network": "avalanche",
        "reward_network": "avalanche",
    },
}

ACCOUNTING_PERIOD_SQL = """
select
    block_number as block_deadline,
    accounting_period,
    accounting_period_start_time,
    accounting_period_end_time
from dbt.int_accounting_period_data__conversion_rates
where block_number between %(block_lo)s and %(block_hi)s
"""

CONSISTENCY_SHARES_SQL = """
with selected_periods as (
    select distinct
        accounting_period,
        accounting_period_start_time,
        accounting_period_end_time
    from dbt.int_accounting_period_data__conversion_rates
    where block_number between %(block_lo)s and %(block_hi)s
      and accounting_period is not null
)
select
    c.accounting_period,
    p.accounting_period_start_time,
    p.accounting_period_end_time,
    '0x' || encode(c.solver, 'hex') as solver,
    c.metric_name,
    c.total_consistency_budget,
    c.success_rate,
    c.total_relative_surplus,
    c.probabilistic_total_relative_surplus,
    c.consistency_reward_share,
    c.num_executed_orders_bid_on,
    c.total_bids_on_executed,
    c.consistency_reward_native
from dbt.fct_consistency_rewards_per_solver_and_accounting_period as c
inner join selected_periods as p
    on c.accounting_period = p.accounting_period
order by
    p.accounting_period_start_time,
    c.solver
"""


# --- connection -------------------------------------------------------------


def parse_endpoint(raw: str) -> dict[str, object]:
    """Parse ANALYTICS_DB_URL in `user:pass@host:port` form."""
    try:
        userinfo, hostinfo = raw.rsplit("@", 1)
        user, password = userinfo.split(":", 1)
        host, _, port = hostinfo.partition(":")
    except ValueError as exc:
        raise ValueError(
            "ANALYTICS_DB_URL must have the form user:pass@host:port"
        ) from exc

    return {
        "host": host,
        "port": int(port or 5432),
        "user": user,
        "password": password,
    }


def db_name(chain: str, environment: str) -> str:
    """Return the Postgres database name (`<env>_<network>`)."""
    return f"{environment}_{CHAINS[chain]['db_network']}"


def dataframe_from_cursor(cur: psycopg.Cursor) -> pd.DataFrame:
    """Materialize the current cursor result as a DataFrame."""
    columns = [description.name for description in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=columns)


# --- analytics DB sources ---------------------------------------------------


def fetch_analytics_data(
    chain: str,
    start: datetime,
    end: datetime,
    environment: str,
    timeout_s: int = 900,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch order-level rows and accounting-period consistency shares.

    Returns:
        orderbook:
            Existing winning-solution-order dataset with exact accounting
            period columns attached by block_deadline.

        consistency_shares:
            One row per accounting_period x solver for every accounting period
            intersecting the requested block range.
    """
    raw_url = os.environ.get("ANALYTICS_DB_URL")
    if not raw_url:
        sys.exit("ANALYTICS_DB_URL is not set (see .env.example).")

    database = db_name(chain, environment)
    params: dict[str, object] = {
        "start": start,
        "end": end,
        "network": CHAINS[chain]["reward_network"],
        "solver_env": "prod" if environment == "prod" else "barn",
    }

    conn_kwargs = parse_endpoint(raw_url)

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
                **conn_kwargs,
            ) as conn,
            conn.cursor() as cur,
        ):
            # Resolve the requested timestamp window to block bounds first.
            cur.execute(
                """
                select
                    min(block_number + 0),
                    max(block_number + 0)
                from dbt.stg_rpc_data__block_timestamp
                where time >= %(start)s
                  and time < %(end)s
                """,
                params,
            )
            params["block_lo"], params["block_hi"] = cur.fetchone()

            if params["block_lo"] is None or params["block_hi"] is None:
                sys.exit(
                    "[db] no block timestamps found for "
                    f"{start.isoformat()}..{end.isoformat()} in {database}"
                )

            # Existing order-level dataset query.
            cur.execute(ORDERBOOK_SQL, params)
            orderbook = dataframe_from_cursor(cur)

            # Change 4: attach the production accounting period and boundaries.
            cur.execute(ACCOUNTING_PERIOD_SQL, params)
            period_map = dataframe_from_cursor(cur)

            # Change 3: fetch materialized consistency shares by week and solver.
            cur.execute(CONSISTENCY_SHARES_SQL, params)
            consistency_shares = dataframe_from_cursor(cur)

    except psycopg.errors.QueryCanceled:
        sys.exit(
            f"[db] query exceeded --db-timeout ({timeout_s}s). "
            "Narrow --start/--end or raise --db-timeout."
        )
    except psycopg.Error as exc:
        sys.exit(f"[db] PostgreSQL error while reading {database}: {exc}")

    if not orderbook.empty:
        if "block_deadline" not in orderbook.columns:
            raise RuntimeError(
                "sql/orderbook_dataset.sql must return block_deadline so the "
                "accounting period can be attached."
            )

        orderbook = orderbook.merge(
            period_map,
            on="block_deadline",
            how="left",
            validate="many_to_one",
        )

        missing_periods = int(orderbook["accounting_period"].isna().sum())
        if missing_periods:
            print(
                f"[db] warning: {missing_periods}/{len(orderbook)} order rows "
                "have no accounting-period mapping",
                file=sys.stderr,
            )

    return orderbook, consistency_shares


# --- CLI --------------------------------------------------------------------


def parse_day(value: str) -> datetime:
    """Parse a UTC YYYY-MM-DD boundary."""
    return datetime.combine(
        date.fromisoformat(value),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )


def default_consistency_path(orderbook_path: Path) -> Path:
    """Derive the consistency-share output path from the main output path."""
    return orderbook_path.with_name(
        f"{orderbook_path.stem}_consistency_shares{orderbook_path.suffix}"
    )


def main() -> None:
    load_dotenv(REPO / ".env")

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--chain", required=True, choices=sorted(CHAINS))
    parser.add_argument(
        "--start",
        required=True,
        type=parse_day,
        help="inclusive, YYYY-MM-DD (UTC)",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=parse_day,
        help="exclusive, YYYY-MM-DD (UTC)",
    )
    parser.add_argument(
        "--out",
        default=None,
        help=("order-level output CSV path (default: data/{chain}_{start}_{end}.csv)"),
    )
    parser.add_argument(
        "--consistency-out",
        default=None,
        help=(
            "consistency-share output CSV path "
            "(default: <out stem>_consistency_shares.csv)"
        ),
    )
    parser.add_argument(
        "--environment",
        default="prod",
        choices=("prod", "staging"),
        help="prod (default) or staging (= barn)",
    )
    parser.add_argument(
        "--db-timeout",
        type=int,
        default=900,
        metavar="SECONDS",
        help="Postgres statement_timeout in seconds (default: 900)",
    )
    args = parser.parse_args()

    if args.end <= args.start:
        sys.exit("--end must be after --start")

    orderbook_out = (
        Path(args.out)
        if args.out
        else REPO
        / "data"
        / f"{args.chain}_{args.start:%Y-%m-%d}_{args.end:%Y-%m-%d}.csv"
    )
    consistency_out = (
        Path(args.consistency_out)
        if args.consistency_out
        else default_consistency_path(orderbook_out)
    )

    existing = [path for path in (orderbook_out, consistency_out) if path.exists()]
    if existing:
        joined = ", ".join(str(path) for path in existing)
        print(
            f"[skip] output already exists: {joined}; delete it to re-fetch",
            file=sys.stderr,
        )
        return

    database = db_name(args.chain, args.environment)
    print(
        f"[db] {database} {args.start:%Y-%m-%d}..{args.end:%Y-%m-%d}",
        file=sys.stderr,
    )

    orderbook, consistency_shares = fetch_analytics_data(
        args.chain,
        args.start,
        args.end,
        args.environment,
        args.db_timeout,
    )

    print(
        f"[db] {len(orderbook)} winning-solution order rows",
        file=sys.stderr,
    )
    print(
        f"[db] {len(consistency_shares)} "
        "accounting-period x solver consistency-share rows",
        file=sys.stderr,
    )

    orderbook.insert(0, "blockchain", args.chain)
    orderbook.insert(1, "environment", args.environment)

    consistency_shares.insert(0, "blockchain", args.chain)
    consistency_shares.insert(1, "environment", args.environment)

    orderbook_out.parent.mkdir(parents=True, exist_ok=True)
    consistency_out.parent.mkdir(parents=True, exist_ok=True)

    orderbook.to_csv(orderbook_out, index=False)
    consistency_shares.to_csv(consistency_out, index=False)

    print(
        f"[out] wrote {len(orderbook)} rows -> {orderbook_out}",
        file=sys.stderr,
    )
    print(
        f"[out] wrote {len(consistency_shares)} rows -> {consistency_out}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
