select
  sum(price_improvement * units_bought) / sum(units_bought) as weighted_price_improvement
from staging.price_improvements
