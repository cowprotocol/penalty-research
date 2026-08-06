-- Capped / uncapped rewards per (auction, solver) for the penalty-cap counterfactual.
--
-- Grain: one row per (auction_id, solver) whose winning solution had at least one
-- proposed trade execution in the window. That "at least one" condition is what
-- sql/orderbook_dataset.sql implies by joining proposed_trade_executions, and it is
-- kept here as a semi-join so the row set matches without fanning out per order.
--
-- Every winning solution belongs here, not only the penalised ones: the consistency
-- budget the counterfactual reallocates is sum(upper_reward_cap - net_batch) over ALL
-- of them.
--
-- Bind params:
--   %(start)s, %(end)s        auction-time window [start, end)
--   %(block_lo)s, %(block_hi)s block-number bracket for the same window (see
--                             sql/orderbook_dataset.sql for why these are passed in
--                             as literals rather than derived in a CTE)
with windowed as (
    select
        ws.auction_id,
        ws.solver,
        ws.block_deadline
    from dbt.int_backend_data__winning_solutions_with_onchain_status as ws
    join dbt.stg_rpc_data__block_timestamp as bt
        on bt.block_number = ws.block_deadline
       and bt.time >= %(start)s and bt.time < %(end)s
    where ws.block_deadline between %(block_lo)s and %(block_hi)s
      -- The order join is part of the row-set definition, not decoration:
      -- sql/orderbook_dataset.sql joins stg_backend_data__orders inline, so a
      -- winning solution whose orders are all absent from that table does not
      -- appear there either. Those rows carry upper_reward_cap = 0, which would
      -- make consistency_budget (= upper_reward_cap - net_batch) negative for any
      -- of them holding a positive reward -- a case the counterfactual rejects
      -- outright. Kept identical here rather than quietly widened.
      and exists (
          select 1
          from dbt.stg_backend_data__proposed_trade_executions as pte
          join dbt.stg_backend_data__orders as o
              on o.uid = pte.order_uid
          where pte.auction_id = ws.auction_id
            and pte.solution_uid = ws.solution_uid
      )
    group by ws.auction_id, ws.solver, ws.block_deadline
)

select
    w.auction_id,
    '0x' || encode(w.solver, 'hex')     as solver,
    w.block_deadline,
    -- excluded from penalties: an explicitly-excluded auction, OR a block_deadline
    -- inside a no-penalties block range (penalty floored to 0).
    (r.is_excluded
     or exists (
         select 1 from dbt.stg_no_penalties_auctions as npa
         where w.block_deadline between npa.block_deadline_start and npa.block_deadline_end
     ))                                 as is_excluded_from_penalties,
    -- signed: negative = penalty. reward_penalty_native is the capped value that was
    -- actually paid; reward_penalty_uncapped_native is what the score formula produced
    -- before the cap, i.e. the quantity the proposed cap is applied to instead.
    r.batch_reward_native               as reward_penalty_native,
    r.uncapped_reward                   as reward_penalty_uncapped_native,
    r.upper_reward_cap                  as reward_cap_upper_native
from windowed as w
left join dbt.fct_solver_rewards_per_auction as r
    on r.auction_id = w.auction_id
   and r.solver = w.solver
