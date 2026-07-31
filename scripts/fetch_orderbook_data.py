#!/usr/bin/env python
"""Build the penalties dataset CSV from the analytics DB only -- no Dune.

Same rows as fetch_penalties_data.py (one row per auction x winning-solution
order) minus its four Dune-sourced columns (order_size_usd, markout_usd,
markout_relative, execution_cost_native). Use this when USD markout is not
needed -- e.g. notebooks/fixed_caps_from_revert_target.ipynb, which derives
USD sizes from volume_native x Binance native-token closes instead.

Usage:
    python scripts/fetch_orderbook_data.py --chain polygon --start 2026-05-01 --end 2026-06-01
    # writes data/polygon_2026-05-01_2026-06-01_db.csv by default; override with --out
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

from fetch_penalties_data import CHAINS, REPO, fetch_orderbook, parse_day


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
