-- Consistency-reward share per (accounting_period, solver), for every accounting
-- period the window touches.
--
-- Bind params:
--   %(start)s, %(end)s  auction-time window [start, end)
with selected_periods as (
    select distinct accounting_period
    from dbt.int_accounting_period_data__conversion_rates
    where block_time >= %(start)s and block_time < %(end)s
      and accounting_period is not null
)
select
    c.accounting_period,
    '0x' || encode(c.solver, 'hex') as solver,
    c.consistency_reward_share
from dbt.fct_consistency_rewards_per_solver_and_accounting_period as c
inner join selected_periods as p
    on c.accounting_period = p.accounting_period
order by c.accounting_period, c.solver
