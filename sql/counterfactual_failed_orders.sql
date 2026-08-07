-- Failed-order volume for the penalty-cap counterfactual.
--
-- Grain: one row per (auction_id, solver, order_uid). The proposed cap is bounded
-- per ORDER, so orders must not be pre-aggregated -- summing two orders on one
-- token pair and bounding the total gives a larger cap than bounding each and
-- adding. The token pair rides along because the correlated/uncorrelated rate is
-- applied downstream from the CoW token lists.
--
-- The windowed CTE selects over the same joins as sql/counterfactual_rewards.sql,
-- plus the not-settled-in-time condition, so the two cover the same auctions.
--
-- Volume is valued on the SURPLUS side (buy amount for sell orders, sell amount
-- for buy orders) in native-token wei, with the same price priority as
-- sql/orderbook_dataset.sql:
--   1. stg_auction_prices_corrections   -- explicit manual correction, always wins
--   2. int_backend_data__price_data     -- per-trade corrected price
--   3. stg_backend_data__auction_prices -- raw auction price
-- All three are native atoms per surplus-token atom (auction price / 1e18), so
-- they multiply the executed amount directly.
--
-- Bind params: %(start)s, %(end)s, %(block_lo)s, %(block_hi)s (as in counterfactual_rewards.sql)
with windowed as (
    select
        ws.auction_id,
        ws.solver,
        pte.order_uid,
        o.sell_token,
        o.buy_token,
        case when o.kind::text = 'sell' then pte.executed_buy else pte.executed_sell end
                                        as executed_amount_surplus_token,
        case when o.kind::text = 'sell' then o.buy_token else o.sell_token end
                                        as surplus_token
    from dbt.int_backend_data__winning_solutions_with_onchain_status as ws
    join dbt.stg_rpc_data__block_timestamp as bt
        on bt.block_number = ws.block_deadline
       and bt.time >= %(start)s and bt.time < %(end)s
    join dbt.stg_backend_data__proposed_trade_executions as pte
        on pte.auction_id = ws.auction_id
       and pte.solution_uid = ws.solution_uid
    join dbt.stg_backend_data__orders as o
        on o.uid = pte.order_uid
    where ws.block_deadline between %(block_lo)s and %(block_hi)s
      -- The condition the penalty tracks: never landed, or landed late. A late
      -- settlement executed its orders, but not in time, so its volume is failed
      -- volume. Matching on `tx_hash is null` alone would waive the penalty on
      -- every late settlement -- about half of them on arbitrum and avalanche.
      and not ws.is_settled_in_time
)

select
    w.auction_id,
    '0x' || encode(w.solver, 'hex')     as solver,
    '0x' || encode(w.order_uid, 'hex')  as order_uid,
    '0x' || encode(w.sell_token, 'hex') as sell_token,
    '0x' || encode(w.buy_token, 'hex')  as buy_token,
    coalesce(
        sum(
            w.executed_amount_surplus_token * coalesce(
                apc.price / 1e18,
                pd.surplus_token_native_price,
                ap.price / 1e18
            )
        ),
        0
    )                                   as failed_volume_native
from windowed as w
left join dbt.stg_auction_prices_corrections as apc
    on apc.auction_id = w.auction_id
   and apc.token = w.surplus_token
left join dbt.int_backend_data__price_data as pd
    on pd.auction_id = w.auction_id
   and pd.order_uid = w.order_uid
left join dbt.stg_backend_data__auction_prices as ap
    on ap.auction_id = w.auction_id
   and ap.token = w.surplus_token
-- an order can appear across several execution rows; those are one order here
group by w.auction_id, w.solver, w.order_uid, w.sell_token, w.buy_token
