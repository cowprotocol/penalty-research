-- Penalties dataset spine, assembled entirely from the dbt analytics layer
-- in the cow-analytics-db database for one network/environment.
--
-- Grain: one row per (auction_id, order_uid) where the order was in that
-- auction's WINNING solution (= one settlement attempt). ALL such orders are
-- kept; partially_fillable and is_out_of_market are carried as flags, not filters.
-- Two outcomes: settled (a real tx hash) or not settled. We do NOT separate
-- revert vs fail-to-submit, and do not special-case late settlements (kept simple).
--
-- Bind params:
--   %(start)s, %(end)s   auction-time window [start, end)
--   %(network)s          reward_config network key (e.g. 'polygon')
--   %(solver_env)s       solver-name environment ('prod' | 'barn')
--
-- IN-MARKET vs OUT-OF-MARKET: all orders are stored with class='limit'. The
-- marketable test is condition 1 of the quote-reward eligibility logic in
-- cow-dagster's int_backend_data__orders_with_winning_quotes: the *effective*
-- (gas + volume-fee adjusted) creation quote price meets the limit price. We
-- compute that test as the is_out_of_market flag (its negation), but apply NO
-- other condition (NOT verified / not-excluded / quoter-bid / CIP-72). The full
-- quote-reward flag is carried through as informational is_quote_reward_eligible.

-- PERF: prune winning_solutions by block_deadline up front, using a block-number range that
-- the caller (fetch_penalties_data.py) derives from the auction-time window and passes as
-- %(block_lo)s / %(block_hi)s. The bounds are literal values the planner can estimate range
-- selectivity from. Deriving them inline instead -- a min/max over the block-timestamp table
-- in a CTE -- makes their value opaque to the planner: it falls back to a generic range guess,
-- under-counts the windowed rows by ~10x, and freezes the whole query into per-row nested-loop
-- index probes (fine for a 1-day window, pathological for months -- e.g. ~150k random probes
-- into the 2 GB auction-prices table). With real bounds the planner instead flips to bulk hash
-- joins as the window grows. The exact bt.time filter below stays authoritative -- block_lo/hi
-- are only a (monotonic block<->time) bracket, so results are unchanged.
with windowed as (
    select
        ws.auction_id,
        ws.solution_uid,
        ws.solver,
        ws.block_deadline,
        ws.block_number          as settlement_block,
        ws.tx_hash,
        pte.order_uid,
        pte.executed_sell,
        pte.executed_buy,
        bt.time                  as auction_timestamp
    from dbt.int_backend_data__winning_solutions_with_onchain_status as ws
    join dbt.stg_rpc_data__block_timestamp as bt
        on bt.block_number = ws.block_deadline
       and bt.time >= %(start)s and bt.time < %(end)s
    join dbt.stg_backend_data__proposed_trade_executions as pte
        on pte.auction_id = ws.auction_id
       and pte.solution_uid = ws.solution_uid
    where ws.block_deadline between %(block_lo)s and %(block_hi)s
),

priced as (  -- attach the per-(auction, solver) reward / penalty
    select
        windowed.*,
        r.batch_reward_native    as reward_penalty_native,
        r.uncapped_reward        as reward_penalty_uncapped_native,
        r.is_excluded            as is_excluded_from_penalties,
        r.reference_score,
        r.observed_score,
        r.upper_reward_cap       as reward_cap_upper_native
    from windowed
    left join dbt.fct_solver_rewards_per_auction as r
        on r.auction_id = windowed.auction_id and r.solver = windowed.solver
),

-- one creation quote per order (stg_backend_data__order_quotes is unique on order_uid)
order_quote as (
    select order_uid, gas_amount, gas_price, sell_token_price, sell_amount, buy_amount
    from dbt.stg_backend_data__order_quotes
),

-- Corrected volume-fee multiplier per order. Unlike the dbt model (which takes
-- max(volume_factor) of a single fee), we compound ALL volume fees that apply in
-- the FIRST auction the order carries them in: multiplier = prod(1 - factor_i).
-- Most orders carry two volume fees (e.g. protocol + partner), so max() understates
-- the true take. prod() via exp(sum(ln(...))). Factors are protocol-configured well
-- under 1; a factor >= 1 is corrupt data, and we let ln() raise (aborting the run)
-- rather than silently mask it.
-- PERF: restrict to orders in this window. fee_policies has no order_uid-leading index, so the
-- table is still scanned, but the disk-spilling sort + window + group-by then runs over only the
-- windowed orders instead of every order in history (the partition still sees all of a kept
-- order's auctions, so first_auction is unchanged).
volume_fee as (
    select order_uid, exp(sum(ln(1 - volume_factor))) as volume_multiplier
    from (
        select order_uid, auction_id, volume_factor,
               min(auction_id) over (partition by order_uid) as first_auction
        from dbt.stg_backend_data__fee_policies
        where kind::text = 'volume'
          and order_uid in (select order_uid from windowed)
    ) v
    where auction_id = first_auction
    group by order_uid
),

-- Raw surplus-side native price (= auction price / 1e18; native atoms per surplus-token atom)
-- for NOT-settled attempts only. Settled attempts read it from int_backend_data__price_data
-- below, which is keyed and indexed on (auction_id, order_uid); they have a trade so they are
-- present there and never reach this CTE -- gating on tx_hash is null keeps the heavy
-- per-(auction, token) probe into the 2 GB auction_prices table off the settled majority.
-- Manual price corrections are NOT applied here: they are layered on top of BOTH price sources
-- in `enriched` (see surplus_token_native_price), so a corrected value always wins.
not_settled_surplus_price as (
    select
        w.auction_id,
        w.order_uid,
        ap.price / 1e18 as surplus_token_native_price
    from windowed as w
    join dbt.stg_backend_data__orders as o on o.uid = w.order_uid
    left join dbt.stg_backend_data__auction_prices as ap
        on ap.auction_id = w.auction_id
       and ap.token = case when o.kind::text = 'sell' then o.buy_token else o.sell_token end
    where w.tx_hash is null
),

enriched as (
    select
        p.auction_id,
        p.order_uid,
        p.solver,
        p.tx_hash,
        p.settlement_block,
        p.block_deadline,
        p.auction_timestamp,
        p.executed_sell,
        p.executed_buy,
        p.reward_penalty_native,
        p.reward_penalty_uncapped_native,
        -- excluded from penalties: an explicitly-excluded auction, OR the auction's
        -- block_deadline falls in a no-penalties block range (penalty floored to 0).
        (p.is_excluded_from_penalties
         or exists (
             select 1 from dbt.stg_no_penalties_auctions as npa
             where p.block_deadline between npa.block_deadline_start and npa.block_deadline_end
         ))                                  as is_excluded_from_penalties,
        p.reward_cap_upper_native,
        p.reference_score,
        p.observed_score,
        o.kind,
        o.partially_fillable,
        o.creation_timestamp,
        o.sell_token,
        o.buy_token,
        o.sell_amount                       as limit_sell_amount,
        o.buy_amount                        as limit_buy_amount,
        -- effective (corrected) quote amounts: gas-adjusted, then volume-fee adjusted
        -- by the compounded multiplier (see volume_fee). Buy orders pay more, so the
        -- sell side is divided by the multiplier; sell orders receive less, so the buy
        -- side is multiplied. The raw quote is dropped -- only the corrected one is kept.
        case
            when o.kind::text = 'buy'
                then (oq.sell_amount + oq.gas_amount * oq.gas_price / nullif(oq.sell_token_price, 0))
                     / coalesce(vf.volume_multiplier, 1)
        end as effective_sell_amount,
        case
            when o.kind::text = 'sell'
                then ((oq.sell_amount - oq.gas_amount * oq.gas_price / nullif(oq.sell_token_price, 0))
                      * oq.buy_amount / nullif(oq.sell_amount, 0))
                     * coalesce(vf.volume_multiplier, 1)
        end as effective_buy_amount,
        elig.is_eligible_for_quote_reward   as is_quote_reward_eligible,
        hm.smart_slippage,
        hm.order_duration                   as seconds_to_settle,
        sv.name                             as solver_name,
        rc.batch_reward_cap_lower           as penalty_cap_native,
        coalesce(apc.price / 1e18,
                 pd.surplus_token_native_price,
                 nsp.surplus_token_native_price) as surplus_token_native_price,
        sl.slippage_native,
        sl.slippage_usd
    from priced as p
    join dbt.stg_backend_data__orders as o on o.uid = p.order_uid
    left join order_quote as oq on oq.order_uid = p.order_uid
    left join volume_fee as vf on vf.order_uid = p.order_uid
    left join dbt.int_backend_data__orders_with_winning_quotes as elig on elig.order_uid = p.order_uid
    left join dbt.fct_time_to_happy_moo__sli as hm on hm.uid = '0x' || encode(p.order_uid, 'hex')
    left join dbt.dune_data__cow_protocol__solvers as sv
        on sv.address = p.solver and sv.environment = %(solver_env)s
    -- reward_config is the raw config seed (read directly; stg_reward_config only adds the
    -- network filter this join already applies). penalty_cap_native is its lower cap.
    left join dbt.reward_config as rc on rc.network = %(network)s
    -- value the order on its SURPLUS side (buy token for sell orders, sell token for buy).
    -- surplus_token_native_price priority, highest first (all are native atoms per surplus-token
    -- atom = auction price / 1e18; volume_native multiplies it directly, not /1e18):
    --   1. stg_auction_prices_corrections -- the explicit manual price-correction seed, joined
    --      LIVE on (auction_id, surplus token) so a corrected value always wins, rather than
    --      relying on price_data's build-time snapshot of the same seed.
    --   2. int_backend_data__price_data -- the per-trade corrected price for SETTLED attempts,
    --      keyed/indexed on (auction_id, order_uid) (avoids probing the 2 GB auction_prices).
    --   3. not_settled_surplus_price -- raw auction_prices, for NOT-settled attempts.
    left join dbt.stg_auction_prices_corrections as apc
        on apc.auction_id = p.auction_id
       and apc.token = case when o.kind::text = 'sell' then o.buy_token else o.sell_token end
    left join dbt.int_backend_data__price_data as pd
        on pd.auction_id = p.auction_id and pd.order_uid = p.order_uid
    left join not_settled_surplus_price as nsp
        on nsp.auction_id = p.auction_id and nsp.order_uid = p.order_uid
    -- slippage is keyed per (auction_id, tx_hash); join on both so a tx that batches
    -- multiple auctions does not fan out / mis-attach the per-batch slippage.
    left join dbt.fct_slippage_per_transaction as sl
        on sl.tx_hash = p.tx_hash and sl.auction_id = p.auction_id
)

select
    auction_id,
    '0x' || encode(order_uid, 'hex')                        as order_uid,
    (tx_hash is not null)                                   as settled,
    is_excluded_from_penalties,
    is_quote_reward_eligible,                -- informational: full quote-reward eligibility
    '0x' || encode(solver, 'hex')                           as solver,
    solver_name,
    case when tx_hash is not null then '0x' || encode(tx_hash, 'hex') end as tx_hash,
    '0x' || encode(sell_token, 'hex')                       as sell_token,
    '0x' || encode(buy_token, 'hex')                        as buy_token,
    kind,
    partially_fillable,
    -- out-of-market: effective quote price fails to meet the limit price (= the
    -- negation of in-market condition 1). null when no quote is available.
    case
        when kind::text = 'sell' then effective_buy_amount < limit_buy_amount
        when kind::text = 'buy'  then limit_sell_amount < effective_sell_amount
    end                                                     as is_out_of_market,
    executed_sell,
    executed_buy,
    limit_sell_amount,
    limit_buy_amount,
    -- order size in native-token wei, valued on the surplus side (buy amount for sell
    -- orders, sell amount for buy orders); present for settled and not-settled attempts.
    -- surplus_token_native_price already includes the /1e18 (see enriched).
    case
        when kind::text = 'sell' then executed_buy  * surplus_token_native_price
        when kind::text = 'buy'  then executed_sell * surplus_token_native_price
    end                                                     as volume_native,
    -- slippage tolerance: signed limit vs *effective* (corrected) quote price, in bps
    case
        when kind::text = 'sell' and effective_buy_amount > 0
            then (effective_buy_amount - limit_buy_amount) / effective_buy_amount * 10000
        when kind::text = 'buy' and effective_sell_amount > 0
            then (limit_sell_amount - effective_sell_amount) / effective_sell_amount * 10000
    end                                                     as slippage_tolerance_bps,
    smart_slippage,
    -- realized solver slippage on the settlement tx (null for not-settled)
    slippage_native,
    slippage_usd,
    -- reward / penalty (native token atoms). reward_penalty_native is signed
    -- (negative = penalty); reward_native / penalty_native are the split (>= 0).
    reward_penalty_native,
    reward_penalty_uncapped_native,
    greatest(0, reward_penalty_native)                      as reward_native,
    greatest(0, -reward_penalty_native)                     as penalty_native,
    greatest(0, -reward_penalty_uncapped_native)            as penalty_uncapped_native,
    penalty_cap_native,
    reward_cap_upper_native,
    reference_score,
    observed_score,
    auction_timestamp,
    creation_timestamp,
    extract(epoch from (auction_timestamp - creation_timestamp)) as seconds_since_created,
    seconds_to_settle,
    block_deadline,
    settlement_block
-- all winning-solution orders are kept; partially_fillable and is_out_of_market are
-- carried as flags rather than applied as filters.
from enriched
