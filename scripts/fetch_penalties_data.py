#!/usr/bin/env python
"""Build the penalties dataset CSV for a (chain, time range).

One row per auction x winning-solution order -- ALL such orders, with
partially_fillable / is_out_of_market carried as flags (see docs/dataset.md).
Reverted winners are kept and flagged via `settled`.

Sources:
  * cow-analytics-db Postgres (ANALYTICS_DB_URL) -- the spine: dbt analytics
    models + raw mirror. One database per network/environment (prod_<network>).
  * Dune (DUNE_API_KEY, saved query 7755542) -- USD order size + markout,
    joined on (order_uid, tx_hash).

Usage:
    python scripts/fetch_penalties_data.py --chain polygon --start 2026-05-01 --end 2026-06-01
    # writes data/polygon_2026-05-01_2026-06-01.csv by default; override with --out
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
DUNE_TRADE_MARKOUT_QUERY_ID = 7755542
ORDERBOOK_SQL = (REPO / "sql" / "orderbook_dataset.sql").read_text()

# chain -> names in each namespace. db_network forms the database (`<env>_<network>`)
# and raw schema (`raw_<env>_<network>`); dune is the cow_protocol_<x>.trades schema;
# reward_network keys dbt.reward_config / solver registry.
CHAINS: dict[str, dict[str, str]] = {
    "polygon":     {"dune": "polygon",     "db_network": "polygon",      "reward_network": "polygon"},
    "bnb":         {"dune": "bnb",         "db_network": "bnb",          "reward_network": "bnb"},
    # ready to extend:
    "ethereum":    {"dune": "ethereum",    "db_network": "mainnet",      "reward_network": "mainnet"},
    "gnosis":      {"dune": "gnosis",      "db_network": "xdai",         "reward_network": "gnosis"},
    "arbitrum":    {"dune": "arbitrum",    "db_network": "arbitrum-one", "reward_network": "arbitrum"},
    "base":        {"dune": "base",        "db_network": "base",         "reward_network": "base"},
    "avalanche_c": {"dune": "avalanche_c", "db_network": "avalanche",    "reward_network": "avalanche"},
}

# Columns Dune contributes (joined on order_uid + tx_hash). Everything else is from the DB.
DUNE_COLS = ["order_size_usd", "markout_usd", "markout_relative", "execution_cost_native"]

# The DB spine windows on auction (block_deadline) time; the Dune query windows on the
# settlement-tx block_time. Pad the Dune fetch so late / window-boundary settlements still
# match. Over-fetched Dune rows simply don't join (the merge is keyed on order_uid + tx_hash).
DUNE_WINDOW_BUFFER = timedelta(days=1)


# --- connection -------------------------------------------------------------

def parse_endpoint(raw: str) -> dict:
    """ANALYTICS_DB_URL is `user:pass@host:port` (no scheme, no db)."""
    userinfo, hostinfo = raw.rsplit("@", 1)
    user, password = userinfo.split(":", 1)
    host, _, port = hostinfo.partition(":")
    return {"host": host, "port": int(port or 5432), "user": user, "password": password}


def db_name(chain: str, environment: str) -> str:
    """Return the Postgres database name (`<env>_<network>`) for a chain + environment."""
    return f"{environment}_{CHAINS[chain]['db_network']}"


# --- sources ----------------------------------------------------------------

def fetch_orderbook(chain: str, start: datetime, end: datetime, environment: str,
                    timeout_s: int = 900) -> pd.DataFrame:
    """All winning-solution orders + reward/penalty + slippage + timing (one row/attempt)."""
    raw_url = os.environ.get("ANALYTICS_DB_URL")
    if not raw_url:
        sys.exit("ANALYTICS_DB_URL is not set (see .env.example).")
    database = db_name(chain, environment)
    params = {
        "start": start,
        "end": end,
        "network": CHAINS[chain]["reward_network"],
        "solver_env": "prod" if environment == "prod" else "barn",
    }
    conn_kwargs = parse_endpoint(raw_url)
    with psycopg.connect(
        dbname=database, connect_timeout=20, autocommit=True,
        options=f"-c default_transaction_read_only=on -c statement_timeout={timeout_s * 1000} -c timezone=UTC",
        **conn_kwargs,
    ) as conn, conn.cursor() as cur:
        try:
            cur.execute(ORDERBOOK_SQL, params)
        except psycopg.errors.QueryCanceled:
            sys.exit(f"[db]   query exceeded --db-timeout ({timeout_s}s). "
                     "Narrow the --start/--end window or raise --db-timeout.")
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def fetch_dune_trades(chain: str, start: datetime, end: datetime) -> pd.DataFrame:
    """USD order size + markout per settled trade, keyed by (order_uid, tx_hash)."""
    from dune_client.client import DuneClient
    from dune_client.query import QueryBase
    from dune_client.types import QueryParameter

    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        sys.exit("DUNE_API_KEY is not set (see .env.example).")
    query = QueryBase(
        query_id=DUNE_TRADE_MARKOUT_QUERY_ID,
        params=[
            QueryParameter.text_type("blockchain", CHAINS[chain]["dune"]),
            QueryParameter.text_type("start_time", start.strftime("%Y-%m-%d %H:%M:%S")),
            QueryParameter.text_type("end_time", end.strftime("%Y-%m-%d %H:%M:%S")),
        ],
    )
    df = DuneClient(api_key, request_timeout=600).run_query_dataframe(query, performance="medium")
    if not df.empty:
        for col in ("order_uid", "tx_hash"):
            df[col] = df[col].map(normalize_hex)
    return df


# --- assembly ---------------------------------------------------------------

def normalize_hex(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).lower()
    return s if s.startswith("0x") else "0x" + s


def assemble(orderbook: pd.DataFrame, dune: pd.DataFrame) -> pd.DataFrame:
    """Left-join Dune USD/markout onto the DB spine (keyed by (order_uid, tx_hash))."""
    orderbook["order_uid"] = orderbook["order_uid"].map(normalize_hex)
    orderbook["tx_hash"] = orderbook["tx_hash"].map(normalize_hex)
    if dune.empty:
        for col in DUNE_COLS:
            orderbook[col] = pd.NA
        return orderbook
    # Join on (order_uid, tx_hash): attaches markout only to the row that actually
    # settled, so a revert that later settles elsewhere doesn't inherit its markout.
    right = dune[["order_uid", "tx_hash", *DUNE_COLS]].drop_duplicates(["order_uid", "tx_hash"])
    return orderbook.merge(right, on=["order_uid", "tx_hash"], how="left")


# --- CLI --------------------------------------------------------------------

def parse_day(s: str) -> datetime:
    return datetime.combine(date.fromisoformat(s), datetime.min.time(), tzinfo=timezone.utc)


def main() -> None:
    load_dotenv(REPO / ".env")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--chain", required=True, choices=sorted(CHAINS))
    p.add_argument("--start", required=True, type=parse_day, help="inclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, type=parse_day, help="exclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--out", default=None,
                   help="output CSV path (default: data/{chain}_{start}_{end}.csv)")
    p.add_argument("--environment", default="prod", choices=("prod", "staging"),
                   help="prod (default) or staging (= barn)")
    p.add_argument("--db-timeout", type=int, default=900, metavar="SECONDS",
                   help="Postgres statement_timeout in seconds (default: 900)")
    args = p.parse_args()
    if args.end <= args.start:
        sys.exit("--end must be after --start")
    out = Path(args.out) if args.out else (
        REPO / "data" / f"{args.chain}_{args.start:%Y-%m-%d}_{args.end:%Y-%m-%d}.csv")

    print(f"[db]   {args.environment}_{CHAINS[args.chain]['db_network']} "
          f"{args.start:%Y-%m-%d}..{args.end:%Y-%m-%d}", file=sys.stderr)
    orderbook = fetch_orderbook(args.chain, args.start, args.end, args.environment, args.db_timeout)
    print(f"[db]   {len(orderbook)} winning-solution orders "
          f"({int(orderbook['settled'].eq(False).sum())} not settled)", file=sys.stderr)

    print(f"[dune] fetching {CHAINS[args.chain]['dune']} trades", file=sys.stderr)
    # widen the Dune window so late / boundary settlements still match (see DUNE_WINDOW_BUFFER)
    dune = fetch_dune_trades(args.chain, args.start - DUNE_WINDOW_BUFFER, args.end + DUNE_WINDOW_BUFFER)
    print(f"[dune] {len(dune)} FOK trades", file=sys.stderr)

    result = assemble(orderbook, dune)
    result.insert(0, "blockchain", args.chain)
    result.insert(1, "environment", args.environment)
    matched = int(result["markout_usd"].notna().sum()) if "markout_usd" in result else 0
    print(f"[join] {matched}/{len(result)} rows enriched with Dune markout", file=sys.stderr)

    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out, index=False)
    print(f"[out]  wrote {len(result)} rows -> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
