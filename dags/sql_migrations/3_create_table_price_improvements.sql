CREATE TABLE IF NOT EXISTS staging.price_improvements (
	cow_ts int8,
	buy_token text,
	sell_token text,
	cow_price numeric,
	binance_weighted_price numeric,
	diff_ms float8,
	price_improvement numeric,
	units_bought numeric,
	units_sold numeric,
	binance_ts int8
);