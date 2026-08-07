-- Volume of the orders that did not settle by their deadline -- either the
-- solution never landed, or it landed late -- one row per (auction_id, solver,
-- order_uid). Orders are not pre-aggregated, because the proposed penalty cap is
-- bounded per order.
--
-- Volume is the executed amount on the surplus side (buy amount for sell orders,
-- sell amount for buy orders), in native-token wei. Price sources, highest
-- priority first: stg_auction_prices_corrections, int_backend_data__price_data,
-- stg_backend_data__auction_prices.
--
-- Bind params:
--   %(start)s, %(end)s  auction-time window [start, end)
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
    where not ws.is_settled_in_time
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
group by w.auction_id, w.solver, w.order_uid, w.sell_token, w.buy_token
