# Penalties dataset — design

`fetch_penalties_data.py` builds a CSV for the *Rethinking Penalties* analysis:
**one row per (auction_id, order_uid) settlement attempt**, in-market fill-or-kill
orders only, for a `(chain, time range)`. Not-settled attempts are kept and flagged.

```bash
uv run python fetch_penalties_data.py --chain polygon --start 2026-05-01 --end 2026-06-01 \
    --out data/polygon_may.csv
# --environment prod (default) | staging   (staging = barn)
```

## Sources

- **cow-analytics-db** (`ANALYTICS_DB_URL`) — the spine. One database per network/env
  (`prod_<network>`), each with a `dbt` analytics layer, a `raw_<env>_<network>`
  replication mirror, and a `public` backend copy. We lean on dbt models so numbers match
  the official accounting; see `scripts/db_overview.py` for the full map.
- **Dune** (`DUNE_API_KEY`, query [7755542](https://dune.com/queries/7755542)) — USD order
  size + markout (`cow_protocol_<chain>.trades`) and settlement gas cost (`<chain>.transactions`).

Join key: **`(order_uid, tx_hash)`** — attaches Dune values only to the row that settled.

## Grain, outcomes & filters

- **Grain = attempt**: one row per `(auction_id, order_uid)` where the order was in that
  auction's **winning** solution. An order re-auctioned across attempts → multiple rows.
- **`settled`** (bool): the attempt produced a real settlement tx (`tx_hash is not null`).
  Two outcomes only — settled / not-settled. Revert vs fail-to-submit is **not** split, and
  late settlements are **not** special-cased (kept simple).
- **In-market filter**: all orders are stored `class='limit'`; the marketable subset is
  condition 1 of cow-dagster's quote-reward logic — the *effective* (gas + volume-fee
  adjusted) creation quote price meets the limit price. We replicate **only** that condition
  (not verified / not-excluded / quoter-bid / CIP-72), which would otherwise drop ~70–80% of
  in-market orders. The full flag is carried as informational `is_quote_reward_eligible`.
- **Fill-or-kill** (`not partially_fillable`); long-standing out-of-market limit orders are
  excluded by the in-market test.

## Units convention

All `*_native` columns are in the chain's **native-token wei** (1e18) — `volume_native`,
`reward_penalty_native`, `reward_native`, `penalty_native`, `penalty_uncapped_native`,
`penalty_cap_native`, `reward_cap_upper_native`, `slippage_native`, `execution_cost_native`.
This keeps ratios (e.g. penalty / volume for the variable-rate counterfactual) unitless.
Divide by 1e18 for whole tokens.

## Output columns (44)

| Column(s) | Source | Notes |
| --- | --- | --- |
| `blockchain`, `environment` | CLI | e.g. `polygon`/`bnb`, `prod` |
| `auction_id`, `order_uid` | DB | grain |
| `settled` | DB | real settlement tx exists |
| `is_excluded_from_penalties` | DB | auction excluded from penalties (e.g. protocol-side issue) |
| `is_quote_reward_eligible` | DB | informational: full quote-reward eligibility (not a filter) |
| `solver`, `solver_name` | DB | winning solver of the attempt |
| `tx_hash` | DB | settlement tx; null if not settled |
| `sell_token`, `buy_token`, `kind` | DB | token pair + sell/buy |
| `executed_sell/buy`, `limit_sell/buy_amount`, `quote_sell/buy_amount` | DB | amounts (atoms) |
| `volume_native` | DB | order size in native wei (all attempts) — `executed_sell × auction native price` |
| `order_size_usd` | Dune | USD order size; **settled only** |
| `slippage_tolerance_bps` | DB | signed (raw) limit-vs-quote tolerance, computed; all rows |
| `calculated_slippage_bps`, `smart_slippage` | DB | realized slippage + smart-slippage flag |
| `slippage_native`, `slippage_usd` | DB | solver execution slippage on the tx (`fct_slippage_per_transaction`); settled only |
| `execution_cost_native` | Dune | settlement gas cost (native wei), per tx; settled only |
| `markout_usd`, `markout_relative` | Dune | settled only; null for feed-less tokens |
| `reward_penalty_native` | DB | **profit/loss** = capped reward (`batch_reward_native`); signed, neg = penalty |
| `reward_penalty_uncapped_native` | DB | pre-cap (`uncapped_reward`), signed |
| `reward_native`, `penalty_native` | DB | the split (both ≥ 0): `reward = max(0,·)`, `penalty = max(0,−·)` |
| `penalty_uncapped_native` | DB | uncapped penalty magnitude — for "realized penalties if uncapped" |
| `penalty_cap_native`, `reward_cap_upper_native` | DB | `reward_config` caps (Polygon `c_l`=30 POL, BNB `c_l`=0.04 BNB) |
| `reference_score`, `observed_score` | DB | scoring inputs |
| `auction_timestamp`, `creation_timestamp`, `seconds_since_created` | DB | time since order created, per attempt |
| `seconds_to_settle` | DB | happy-moo `order_duration` (time to the landed settlement) |
| `block_deadline`, `settlement_block` | DB | |

Reward/penalty is `dbt.fct_solver_rewards_per_auction` per `(auction_id, solver)`, attached to
each of the solver's winning orders that auction (repeats for multi-order solutions).

## Caveats

- **P&L is not computed** — raw components (slippage, execution cost, fees via markout) are
  stored; combine in the analysis.
- **Reverts / fail-to-submit** have no Dune row → `order_size_usd`, `markout`,
  `execution_cost_native`, `slippage_*` are null; `volume_native` is still present (from
  `auction_prices`). `executed_sell/buy` reflect the solver's *proposed* execution.
- **Slippage coverage** ~90% of settled rows (`fct_slippage_per_transaction` doesn't cover
  every tx).
- **Reward grain:** a `(auction, solver)` value repeated across the solver's orders in that
  auction; don't sum it across rows of the same auction/solver.

## Extending

- **staging / barn:** `--environment staging` (database `staging_<network>`).
- **other chains:** add to `CHAINS` in `fetch_penalties_data.py` (dune name, db_network,
  reward_network — e.g. ethereum→mainnet, gnosis→xdai, arbitrum→arbitrum-one).

## Introspection helpers (`scripts/`)

`db_overview.py`, `db_target_detail.py`, `db_model_columns.py`, `db_find_inmarket.py`,
`db_grain_check.py`.
