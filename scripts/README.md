# scripts

`fetch_penalties_data.py` builds the penalties dataset CSV for one `(chain, time range)`
(one row per auction × winning-solution order — see [`docs/dataset.md`](../docs/dataset.md)).

First copy `.env.example` → `.env` and fill in `DUNE_API_KEY` + `ANALYTICS_DB_URL`, then `uv sync`.

`--start` is inclusive, `--end` exclusive (both UTC dates). Each run writes
`data/{chain}_{start}_{end}.csv`. Below: ~6 months from `2026-01-01`, one line per
chain so you can copy just the ones you need.

```bash
# chains with existing data
uv run python scripts/fetch_penalties_data.py --chain polygon     --start 2026-01-01 --end 2026-07-01
uv run python scripts/fetch_penalties_data.py --chain ethereum    --start 2026-01-01 --end 2026-07-01
uv run python scripts/fetch_penalties_data.py --chain bnb         --start 2026-01-01 --end 2026-07-01

# other supported chains
uv run python scripts/fetch_penalties_data.py --chain gnosis      --start 2026-01-01 --end 2026-07-01
uv run python scripts/fetch_penalties_data.py --chain arbitrum    --start 2026-01-01 --end 2026-07-01
uv run python scripts/fetch_penalties_data.py --chain base        --start 2026-01-01 --end 2026-07-01
uv run python scripts/fetch_penalties_data.py --chain avalanche_c --start 2026-01-01 --end 2026-07-01
```

Six months of a busy chain is a large query — if the DB times out, narrow the window
or raise `--db-timeout` (default 900s). Add `--environment staging` for barn.
