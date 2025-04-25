select
    block_number
    , block_date
    , block_time
    , tx_hash
    , order_uid
    , trader
    , sell_token_address
    , sell_token
    , buy_token_address
    , buy_token
    , token_pair
    , order_type
    , partial_fill
    , fill_proportion
    , surplus_usd
    , units_sold
    , atoms_sold
    , units_bought
    , atoms_bought
    , usd_value
    , buy_price
    , buy_value_usd
    , sell_price
    , sell_value_usd
    , fee
    , fee_atoms
    , fee_usd
from cow_protocol_ethereum.trades
where block_month >= date_trunc('month', timestamp '{{ts_start}}')
and block_month <= date_trunc('month', timestamp '{{ts_end}}')
and block_time >= timestamp '{{ts_start}}'
and block_time < timestamp '{{ts_end}}'
and token_pair in ('WETH-USDC', 'USDC-WETH')
