#!/usr/bin/env python
"""Build the penalties dataset CSV from the analytics DB only -- no Dune.

One row per auction x winning-solution order -- ALL such orders, with
partially_fillable / is_out_of_market carried as flags (see docs/dataset.md).
Reverted winners are kept and flagged via `settled`.

This is the self-contained DB fetcher; fetch_penalties_data.py builds on it and
additionally joins Dune markout columns. Use this one when USD markout is not
needed -- e.g. notebooks/fixed_caps_from_revert_target.ipynb, which derives USD
sizes from volume_native x Binance native-token closes instead.

Usage:
    python scripts/fetch_orderbook_data.py --chain polygon --start 2026-05-01 --end 2026-06-01
    # writes data/polygon_2026-05-01_2026-06-01_db.csv by default; override with --out
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

# chain -> names in each namespace. db_network forms the database (`<env>_<network>`);
# dune is the cow_protocol_<x>.trades schema (used by fetch_penalties_data.py);
# reward_network keys dbt.reward_config / the solver registry.
CHAINS: dict[str, dict[str, str]] = {
    "polygon":     {"dune": "polygon",     "db_network": "polygon",      "reward_network": "polygon"},
    "bnb":         {"dune": "bnb",         "db_network": "bnb",          "reward_network": "bnb"},
    "ethereum":    {"dune": "ethereum",    "db_network": "mainnet",      "reward_network": "mainnet"},
    "gnosis":      {"dune": "gnosis",      "db_network": "xdai",         "reward_network": "gnosis"},
    "arbitrum":    {"dune": "arbitrum",    "db_network": "arbitrum-one", "reward_network": "arbitrum"},
    "base":        {"dune": "base",        "db_network": "base",        "reward_network": "base"},
    "avalanche_c": {"dune": "avalanche_c", "db_network": "avalanche",    "reward_network": "avalanche"},
}


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


# --- fetch ------------------------------------------------------------------

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
        # Map the auction-time window to a block-number range and pass it to the main query as
        # literal bounds the planner can estimate from. Doing this inline (a CTE over the
        # block-timestamp table) would hide the bounds and freeze the plan into per-row probes
        # (see sql/orderbook_dataset.sql). The `+ 0` keeps this a single seq-scan aggregate
        # rather than an index walk from the chain tip back to the window.
        try:
            cur.execute(
                "select min(block_number + 0), max(block_number + 0) "
                "from dbt.stg_rpc_data__block_timestamp where time >= %(start)s and time < %(end)s",
                params,
            )
            params["block_lo"], params["block_hi"] = cur.fetchone()
            cur.execute(ORDERBOOK_SQL, params)
        except psycopg.errors.QueryCanceled:
            sys.exit(f"[db]   query exceeded --db-timeout ({timeout_s}s). "
                     "Narrow the --start/--end window or raise --db-timeout.")
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


# --- helpers shared with fetch_penalties_data.py ----------------------------

def normalize_hex(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).lower()
    return s if s.startswith("0x") else "0x" + s


def parse_day(s: str) -> datetime:
    return datetime.combine(date.fromisoformat(s), datetime.min.time(), tzinfo=timezone.utc)


# --- CLI --------------------------------------------------------------------

def main() -> None:
    load_dotenv(REPO / ".env")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--chain", required=True, choices=sorted(CHAINS))
    p.add_argument("--start", required=True, type=parse_day, help="inclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, type=parse_day, help="exclusive, YYYY-MM-DD (UTC)")
    p.add_argument("--out", default=None,
                   help="output CSV path (default: data/{chain}_{start}_{end}_db.csv)")
    p.add_argument("--environment", default="prod", choices=("prod", "staging"),
                   help="prod (default) or staging (= barn)")
    p.add_argument("--db-timeout", type=int, default=900, metavar="SECONDS",
                   help="Postgres statement_timeout in seconds (default: 900)")
    args = p.parse_args()
    if args.end <= args.start:
        sys.exit("--end must be after --start")
    out = Path(args.out) if args.out else (
        REPO / "data" / f"{args.chain}_{args.start:%Y-%m-%d}_{args.end:%Y-%m-%d}_db.csv")
    if out.exists():
        print(f"[skip] {out} already exists; delete it to re-fetch", file=sys.stderr)
        return

    print(f"[db]   {args.environment}_{CHAINS[args.chain]['db_network']} "
          f"{args.start:%Y-%m-%d}..{args.end:%Y-%m-%d}", file=sys.stderr)
    orderbook = fetch_orderbook(args.chain, args.start, args.end, args.environment, args.db_timeout)
    print(f"[db]   {len(orderbook)} winning-solution orders "
          f"({int(orderbook['settled'].eq(False).sum())} not settled)", file=sys.stderr)

    orderbook.insert(0, "blockchain", args.chain)
    orderbook.insert(1, "environment", args.environment)
    out.parent.mkdir(parents=True, exist_ok=True)
    orderbook.to_csv(out, index=False)
    print(f"[out]  wrote {len(orderbook)} rows -> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
