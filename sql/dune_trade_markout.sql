-- Saved on Dune as query 7755542:
--   https://dune.com/queries/7755542
-- "penalties - trade USD valuation and markout (by order_uid)"
--
-- Minimal Dune contribution to the penalties dataset: only what the analytics DB
-- cannot provide -- USD order size, markout, and settlement gas cost. Token
-- addresses, amounts, order kind, settlement block etc. come from the DB and are
-- NOT duplicated here. Joined to the DB dataset on (order_uid, tx_hash).
--   markout_usd          = buy value - sell value, at trade-minute prices.usd
--   execution_cost_native = settlement tx gas cost in native wei (per tx; shared
--                           across orders in the same batch)
-- Tokens without a prices.usd feed yield NULL usd/markout. Not-settled attempts
-- are absent here (no on-chain trade) -- expected.
--
-- Params: {{blockchain}} (e.g. polygon, bnb), {{start_time}} inclusive, {{end_time}} exclusive
select
    t.order_uid,
    t.tx_hash,
    t.buy_value_usd                                                   as order_size_usd,
    t.buy_value_usd - t.sell_value_usd                               as markout_usd,
    (t.buy_value_usd - t.sell_value_usd) / nullif(t.sell_value_usd, 0) as markout_relative,
    cast(txn.gas_used as double) * cast(txn.gas_price as double)     as execution_cost_native
from cow_protocol_{{blockchain}}.trades as t
left join {{blockchain}}.transactions as txn
    on txn.hash = t.tx_hash
    and txn.block_time >= timestamp '{{start_time}}'
    and txn.block_time <  timestamp '{{end_time}}'
where t.block_time >= timestamp '{{start_time}}'
  and t.block_time <  timestamp '{{end_time}}'
