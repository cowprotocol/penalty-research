# Penalties dataset — design

`scripts/fetch_penalties_data.py` builds a CSV for the *Rethinking Penalties* analysis:
**one row per (auction_id, order_uid) settlement attempt**, for a `(chain, time range)`.
All winning-solution orders are kept; fill-or-kill and in-market status are carried as
the `partially_fillable` / `is_out_of_market` flags. Not-settled attempts are kept and flagged.

```bash
uv run python scripts/fetch_penalties_data.py --chain polygon --start 2026-05-01 --end 2026-06-01
# writes data/polygon_2026-05-01_2026-06-01.csv by default; override with --out
# --environment prod (default) | staging   (staging = barn)
# --db-timeout 900   Postgres statement_timeout in seconds (raise for very large windows)
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
- **No order filtering**: every winning-solution order is kept. The two properties that
  used to be filters are now flags, so the analysis can slice on them:
  - **`is_out_of_market`** (bool): all orders are stored `class='limit'`; the marketable test
    is condition 1 of cow-dagster's quote-reward logic — the *effective* (gas + volume-fee
    adjusted) creation quote price meets the limit price. `is_out_of_market` is its negation
    (true = effective quote fails the limit). We compute **only** that condition (not verified /
    not-excluded / quoter-bid / CIP-72); the full quote-reward flag is carried separately as
    informational `is_quote_reward_eligible`. `is_out_of_market = (slippage_tolerance_bps < 0)`.
  - **`partially_fillable`** (bool): false = fill-or-kill. Partial-fill orders are included
    (Dune query 7755542 covers them too).
- **Volume-fee correction** (differs from dbt): the effective quote applies the compounded
  multiplier `∏(1 − volume_factor_i)` over **all** volume fees in the first auction the order
  carries them in (most orders have two). dbt instead uses `max(volume_factor)` of a single
  fee, which understates the take.

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
| `is_out_of_market` | DB | effective (corrected) quote fails the limit price; null if no quote |
| `solver`, `solver_name` | DB | winning solver of the attempt |
| `tx_hash` | DB | settlement tx; null if not settled |
| `sell_token`, `buy_token`, `kind` | DB | token pair + sell/buy |
| `partially_fillable` | DB | false = fill-or-kill |
| `executed_sell/buy`, `limit_sell/buy_amount` | DB | amounts (atoms); raw quote dropped (use `slippage_tolerance_bps`) |
| `volume_native` | DB | order size in native wei (all attempts) — `executed_sell × auction native price` |
| `order_size_usd` | Dune | USD order size; **settled only** |
| `slippage_tolerance_bps` | DB | signed limit-vs-**effective** (gas + volume-fee corrected) quote tolerance; all rows |
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
- **Partial fills**: for `partially_fillable` orders the Dune `order_size_usd` / `markout_*`
  reflect the *filled portion* in that settlement tx, not the full order amount.
- **Reward grain:** a `(auction, solver)` value repeated across the solver's orders in that
  auction; don't sum it across rows of the same auction/solver.

## Extending

- **staging / barn:** `--environment staging` (database `staging_<network>`).
- **other chains:** add to `CHAINS` in `scripts/fetch_penalties_data.py` (dune name, db_network,
  reward_network — e.g. ethereum→mainnet, gnosis→xdai, arbitrum→arbitrum-one).

## Introspection helpers (`scripts/`)

`db_overview.py`, `db_target_detail.py`, `db_model_columns.py`, `db_find_inmarket.py`,
`db_grain_check.py`.
