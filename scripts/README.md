# scripts

`fetch_penalties_data.py` builds the penalties dataset CSV for one `(chain, time range)`
(one row per auction × winning-solution order — see [`docs/dataset.md`](../docs/dataset.md)).

`fetch_orderbook_data.py` is the self-contained DB-only fetcher (the connection and query
logic live here; `fetch_penalties_data.py` imports it and adds the Dune join): same rows and
flags, but no Dune, so the four Dune-sourced columns (`order_size_usd`, `markout_usd`,
`markout_relative`, `execution_cost_native`) are absent. It writes
`data/{chain}_{start}_{end}_db.csv` and needs only `ANALYTICS_DB_URL`. Its extracts are what
`derive_pairs.py` reads.

`fetch_counterfactual_data.py` builds the three inputs of
`notebooks/penalties_analysis_counterfactual.ipynb` — `_rewards`, `_failed_orders` and
`_consistency_shares`, one CSV each per chain and window, from `sql/counterfactual_*.sql`.
The notebook invokes it itself for any chain whose files are missing, so running it by hand is
only for pre-populating the cache. `--start` and `--end` must both be Tuesdays (accounting-period
bounds); it needs only `ANALYTICS_DB_URL`.

```bash
uv run python scripts/fetch_counterfactual_data.py --chain ethereum --start 2026-06-30 --end 2026-07-28
```

`derive_pairs.py` derives the `PAIRS` table hardcoded in
`notebooks/fixed_caps_from_revert_target.ipynb` — the token pairs the caps are fitted on and
their CoW flow weights — and prints a ready-to-paste literal. The notebook does not call it:
the list only changes when CoW's traded pairs shift. Its default window matches `MONTHS` in
the notebook, and it warns if the extracts do not span it.

```bash
uv run python scripts/derive_pairs.py                                  # notebook's window
uv run python scripts/derive_pairs.py --start 2026-05-01 --end 2026-08-01 --top 10
```

First copy `.env.example` → `.env` and fill in `DUNE_API_KEY` + `ANALYTICS_DB_URL`
(`fetch_penalties_data.py` is the only script that needs the Dune key), then `uv sync`.

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
