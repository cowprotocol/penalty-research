-- Failed-order volume per (auction, solver, token pair) for the penalty-cap counterfactual.
--
-- The proposed cap is bps x volume of the orders that did NOT settle, with the rate
-- depending on whether the pair is correlated. Settled orders can never contribute to
-- it, so only not-settled attempts are read here -- which also keeps the per-(auction,
-- token) probe into the large auction_prices table off the settled majority.
--
-- Grain: one row per (auction_id, solver, order_uid). The proposed cap is bounded per
-- ORDER, so orders must not be pre-aggregated -- summing two orders on the same token
-- pair and bounding the total would give a different (larger) cap than bounding each
-- and adding. The token pair rides along because the correlated/uncorrelated rate is
-- applied downstream from the CoW token lists.
--
-- Volume is valued on the SURPLUS side (buy amount for sell orders, sell amount for
-- buy orders) in native-token wei, with the same price priority as
-- sql/orderbook_dataset.sql:
--   1. stg_auction_prices_corrections  -- explicit manual correction, always wins
--   2. int_backend_data__price_data    -- per-trade corrected price
--   3. stg_backend_data__auction_prices -- raw auction price
-- All three are native atoms per surplus-token atom (auction price / 1e18), so they
-- multiply the executed amount directly.
--
-- Bind params: %(start)s, %(end)s, %(block_lo)s, %(block_hi)s (as in counterfactual_rewards.sql)
with windowed as (
    select
        ws.auction_id,
        ws.solver,
        pte.order_uid,
        pte.executed_sell,
        pte.executed_buy
    from dbt.int_backend_data__winning_solutions_with_onchain_status as ws
    join dbt.stg_rpc_data__block_timestamp as bt
        on bt.block_number = ws.block_deadline
       and bt.time >= %(start)s and bt.time < %(end)s
    join dbt.stg_backend_data__proposed_trade_executions as pte
        on pte.auction_id = ws.auction_id
       and pte.solution_uid = ws.solution_uid
    where ws.block_deadline between %(block_lo)s and %(block_hi)s
      -- Not settled BY THE DEADLINE, which is the condition the penalty tracks:
      -- either the solution never landed, or it landed late. A late settlement
      -- executed its orders, but not in time, so its volume is failed volume.
      -- Matching on `tx_hash is null` alone would record zero failed volume for
      -- every late settlement, waiving a penalty the protocol did charge -- that
      -- is roughly half of all penalised auctions on arbitrum and avalanche.
      and (ws.tx_hash is null or ws.block_number > ws.block_deadline)
)

select
    w.auction_id,
    '0x' || encode(w.solver, 'hex')     as solver,
    '0x' || encode(w.order_uid, 'hex')  as order_uid,
    '0x' || encode(o.sell_token, 'hex') as sell_token,
    '0x' || encode(o.buy_token, 'hex')  as buy_token,
    coalesce(
        sum(
            case
                when o.kind::text = 'sell'
                    then w.executed_buy * coalesce(
                        apc.price / 1e18,
                        pd.surplus_token_native_price,
                        ap.price / 1e18
                    )
                else w.executed_sell * coalesce(
                    apc.price / 1e18,
                    pd.surplus_token_native_price,
                    ap.price / 1e18
                )
            end
        ),
        0
    ) as failed_volume_native
from windowed as w
join dbt.stg_backend_data__orders as o
    on o.uid = w.order_uid
left join dbt.stg_auction_prices_corrections as apc
    on apc.auction_id = w.auction_id
   and apc.token = case when o.kind::text = 'sell' then o.buy_token else o.sell_token end
left join dbt.int_backend_data__price_data as pd
    on pd.auction_id = w.auction_id
   and pd.order_uid = w.order_uid
left join dbt.stg_backend_data__auction_prices as ap
    on ap.auction_id = w.auction_id
   and ap.token = case when o.kind::text = 'sell' then o.buy_token else o.sell_token end
-- group by the order too: a solution can list an order across several execution
-- rows, and those are one order for capping purposes.
group by w.auction_id, w.solver, w.order_uid, o.sell_token, o.buy_token
