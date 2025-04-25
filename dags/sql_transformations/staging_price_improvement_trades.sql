INSERT INTO staging.price_improvements
with binance_trades as (
select
	time as binance_ts
	, sum("quoteQty") / sum(qty) as binance_weighted_price
from raw.binance_trades
where 1=1
    and "time" >= $binance_time_start
    and "time" <  $binance_time_end
group by 1
),
binance_range_ts as (
select
  binance_ts
  , lead(binance_ts) over (order by binance_ts) as lead_binance_ts
  , binance_weighted_price
from binance_trades
),
cow_trades as (
select
	1000 * extract(epoch from block_time) as cow_ts
	, *
from raw.cow_trades
where 1=1
    and block_time >= '$cow_block_time_start'
    and block_time <  '$cow_block_time_end'
),
joined as (
select
    cow_ts
    , buy_token
    , sell_token
    , units_bought
    , units_sold
    , binance_ts
    , lead_binance_ts
	, binance_weighted_price
	, abs(cow_ts - binance_ts) as diff_ms
from cow_trades
left join binance_range_ts on
	cow_ts > binance_ts - 10000
	and cow_ts < lead_binance_ts + 10000
),
pre_final as (
select
	*
	, case
		when buy_token = 'USDC' then units_bought / units_sold
		when buy_token = 'WETH' and sell_token = 'USDC' then units_sold / units_bought
	end as cow_price
	, case
		when buy_token = 'USDC' then units_bought / units_sold - binance_weighted_price
		when buy_token = 'WETH' and sell_token = 'USDC' then binance_weighted_price - units_sold / units_bought
	end as price_improvement
	, row_number() over (partition by cow_ts order by diff_ms desc) as rn_diff_ms
from joined
)
select
	cow_ts
    , buy_token
    , sell_token
    , cow_price
		, binance_weighted_price
		, diff_ms
		, price_improvement
    , units_bought
    , units_sold
    , binance_ts
from pre_final
where rn_diff_ms = 1
and buy_token = 'WETH'