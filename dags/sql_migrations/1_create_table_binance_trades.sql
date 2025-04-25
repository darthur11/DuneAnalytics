CREATE TABLE IF NOT EXISTS raw.binance_trades (
	id int8 PRIMARY KEY,
	price numeric,
	qty numeric,
	"quoteQty" numeric,
	"time" int8,
	"isBuyerMaker" bool,
	"isBestMatch" bool
);
