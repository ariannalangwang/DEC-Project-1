with distinct_dates as (
	select
		symbol,
		count(distinct date) as total_days
	from
		stock_prices_in_currencies
	group by
		symbol
),
calculations as (
	select
		spic.date,
		spic.symbol,
		avg(spic.close_usd) over (partition by spic.symbol) as avg_close_usd,
		avg(spic.close_eur) over (partition by spic.symbol) as avg_close_eur,
		avg(spic.close_cny) over (partition by spic.symbol) as avg_close_cny,
		avg(spic.close_inr) over (partition by spic.symbol) as avg_close_inr,
		avg(spic.close_aud) over (partition by spic.symbol) as avg_close_aud,
		sum(spic.volume) over (partition by spic.symbol) as total_volume,
		dd.total_days
	from
		stock_prices_in_currencies spic
	join
		distinct_dates dd
	on
		spic.symbol = dd.symbol
)
select
    date,
    symbol,
    round(cast(avg_close_usd as numeric), 2) as avg_close_usd,
    round(cast(avg_close_eur as numeric), 2) as avg_close_eur,
    round(cast(avg_close_cny as numeric), 2) as avg_close_cny,
    round(cast(avg_close_inr as numeric), 2) as avg_close_inr,
    round(cast(avg_close_aud as numeric), 2) as avg_close_aud,
    total_volume,
    total_days
from
    calculations
order by
    date, symbol
