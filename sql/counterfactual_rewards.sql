-- Capped and uncapped reward per (auction_id, solver), for auctions in the window
-- whose winning solution has orders present in stg_backend_data__orders. Rewards
-- are signed: negative means the solver paid a penalty.
--
-- All winning solutions are returned, not only penalised ones: the consistency
-- budget is sum(upper_reward_cap - reward) over all of them.
--
-- Bind params:
--   %(start)s, %(end)s  auction-time window [start, end)
with windowed as (
    select distinct
        ws.auction_id,
        ws.solver,
        ws.block_deadline
    from dbt.int_backend_data__winning_solutions_with_onchain_status as ws
    join dbt.stg_rpc_data__block_timestamp as bt
        on bt.block_number = ws.block_deadline
       and bt.time >= %(start)s and bt.time < %(end)s
    join dbt.stg_backend_data__proposed_trade_executions as pte
        on pte.auction_id = ws.auction_id
       and pte.solution_uid = ws.solution_uid
    join dbt.stg_backend_data__orders as o
        on o.uid = pte.order_uid
),

periods as (
    select block_number, accounting_period
    from dbt.int_accounting_period_data__conversion_rates
    where block_time >= %(start)s and block_time < %(end)s
)

select
    w.auction_id,
    '0x' || encode(w.solver, 'hex')     as solver,
    (r.is_excluded
     or exists (
         select 1 from dbt.stg_no_penalties_auctions as npa
         where w.block_deadline between npa.block_deadline_start and npa.block_deadline_end
     ))                                 as is_excluded_from_penalties,
    r.batch_reward_native               as reward_penalty_native,
    r.uncapped_reward                   as reward_penalty_uncapped_native,
    r.upper_reward_cap                  as reward_cap_upper_native,
    cr.accounting_period
from windowed as w
left join dbt.fct_solver_rewards_per_auction as r
    on r.auction_id = w.auction_id
   and r.solver = w.solver
left join periods as cr
    on cr.block_number = w.block_deadline
order by w.auction_id, w.solver
