-- Consistency-reward shares per (accounting_period, solver), for the periods
-- covered by the requested block range.
--
-- Bind params: %(block_lo)s, %(block_hi)s
with selected_periods as (
    select distinct accounting_period
    from dbt.int_accounting_period_data__conversion_rates
    where block_number between %(block_lo)s and %(block_hi)s
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
