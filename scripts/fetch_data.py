#!/usr/bin/env python
"""Fetch the analytics-DB inputs for the penalty-cap counterfactual notebook.

Writes three CSVs per chain and window, holding only what the counterfactual
consumes:

  <chain>_<start>_<end>.csv                     sql/counterfactual_rewards.sql
      one row per (auction, solver): capped and uncapped reward/penalty, the
      upper reward cap, the penalty-exclusion flag, and the accounting period.

  <chain>_<start>_<end>_failed_volumes.csv      sql/counterfactual_failed_volumes.sql
      one row per (auction, solver, sell_token, buy_token) for NOT-settled
      orders only -- the only ones a volume-based penalty cap applies to.

  <chain>_<start>_<end>_consistency_shares.csv
      one row per (accounting_period, solver).

Source: cow-analytics-db Postgres only (ANALYTICS_DB_URL), one database per
network: prod_<network>.

Usage:
    python scripts/fetch_data.py --chain ethereum --start 2026-06-30 --end 2026-07-28

--start and --end must both be Tuesdays: accounting periods run Tuesday to
Tuesday, and a partial period mis-attributes the consistency rewards.
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
REWARDS_SQL = (REPO / "sql" / "counterfactual_rewards.sql").read_text()
FAILED_VOLUMES_SQL = (REPO / "sql" / "counterfactual_failed_volumes.sql").read_text()

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

BLOCK_RANGE_SQL = """
select min(block_number), max(block_number)
from dbt.stg_rpc_data__block_timestamp
where time >= %(start)s and time < %(end)s
"""

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


def parse_endpoint(raw: str) -> dict[str, object]:
    try:
        userinfo, hostinfo = raw.rsplit("@", 1)
        user, password = userinfo.split(":", 1)
        host, _, port = hostinfo.partition(":")
    except ValueError as exc:
        raise SystemExit(
            "ANALYTICS_DB_URL must have the form user:password@host:port"
        ) from exc

    return {"host": host, "port": int(port or 5432), "user": user, "password": password}


def read_frame(cur: psycopg.Cursor, sql: str, params: dict) -> pd.DataFrame:
    """Run a query, building the frame in batches.

    fetchall() would hold the full list of row tuples and the DataFrame at the
    same time; that peak is enough to get the process OOM-killed on the busiest
    chain, so the rows are consumed in batches instead.
    """
    cur.execute(sql, params)
    columns = [description.name for description in cur.description]

    frames: list[pd.DataFrame] = []
    while rows := cur.fetchmany(10_000):
        frames.append(pd.DataFrame(rows, columns=columns))

    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def fetch(
    chain: str,
    start: datetime,
    end: datetime,
    timeout_s: int,
) -> dict[str, pd.DataFrame]:
    raw_url = os.environ.get("ANALYTICS_DB_URL")
    if not raw_url:
        sys.exit("ANALYTICS_DB_URL is not set.")

    config = CHAINS[chain]
    database = f"prod_{config['db_network']}"
    params: dict[str, object] = {
        "start": start,
        "end": end,
        "network": config["reward_network"],
        "solver_env": "prod",
    }
    print(f"[db] {database} {start:%Y-%m-%d}..{end:%Y-%m-%d}", file=sys.stderr)

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
            # The block bracket is passed into the big queries as a literal so the
            # planner can estimate its selectivity -- see sql/orderbook_dataset.sql
            # for why deriving it inline instead cripples the plan.
            cur.execute(BLOCK_RANGE_SQL, params)
            params["block_lo"], params["block_hi"] = cur.fetchone()
            if params["block_lo"] is None:
                sys.exit(f"[db] no blocks found for this window in {database}")

            rewards = read_frame(cur, REWARDS_SQL, params)
            volumes = read_frame(cur, FAILED_VOLUMES_SQL, params)
            periods = read_frame(cur, ACCOUNTING_PERIOD_SQL, params)
            shares = read_frame(cur, CONSISTENCY_SHARES_SQL, params)

    except psycopg.errors.QueryCanceled:
        sys.exit(
            f"[db] query exceeded {timeout_s}s. Narrow the date range or "
            "increase --db-timeout."
        )
    except psycopg.Error as exc:
        sys.exit(f"[db] PostgreSQL error while reading {database}: {exc}")

    rewards = rewards.merge(
        periods, on="block_deadline", how="left", validate="many_to_one"
    ).drop(columns="block_deadline")

    if rewards.duplicated(["auction_id", "solver"]).any():
        sys.exit("counterfactual_rewards.sql returned duplicate (auction, solver) rows")

    no_period = int(rewards["accounting_period"].isna().sum())
    if no_period:
        sys.exit(f"{no_period}/{len(rewards)} reward rows have no accounting period")

    for frame in (rewards, volumes, shares):
        frame.insert(0, "blockchain", chain)

    return {
        "": rewards,
        "_failed_volumes": volumes,
        "_consistency_shares": shares,
    }


def parse_day(value: str) -> datetime:
    return datetime.combine(
        date.fromisoformat(value), datetime.min.time(), tzinfo=timezone.utc
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

    # Accounting periods run Tuesday to Tuesday; an incomplete period silently
    # mis-attributes the consistency rewards, so reject it rather than adjust it.
    if args.start.weekday() != 1 or args.end.weekday() != 1:
        sys.exit("--start and --end must both be Tuesdays (accounting-period bounds)")
    if args.end <= args.start:
        sys.exit("--end must be after --start")

    base = (
        Path(args.out)
        if args.out
        else REPO / "data" / f"{args.chain}_{args.start:%Y-%m-%d}_{args.end:%Y-%m-%d}.csv"
    )
    paths = {
        suffix: base.with_name(f"{base.stem}{suffix}{base.suffix}")
        for suffix in ("", "_failed_volumes", "_consistency_shares")
    }

    if all(path.exists() for path in paths.values()):
        print(
            f"[skip] cached files already exist for {args.chain}: "
            + ", ".join(path.name for path in paths.values()),
            file=sys.stderr,
        )
        return

    frames = fetch(args.chain, args.start, args.end, args.db_timeout)

    base.parent.mkdir(parents=True, exist_ok=True)
    for suffix, frame in frames.items():
        path = paths[suffix]
        # Write via a scratch name so an interrupted run cannot leave a truncated
        # CSV that the next run would treat as a complete cache entry.
        scratch = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        frame.to_csv(scratch, index=False)
        scratch.replace(path)
        print(f"[out] wrote {len(frame)} rows -> {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
