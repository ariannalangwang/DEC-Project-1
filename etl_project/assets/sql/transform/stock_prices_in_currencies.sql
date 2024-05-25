with initial_calculations as (
	select
		sp.date,
		sp.symbol,
		sp.close as close_usd,
		(sp.close / cer.rate_usd) as close_eur,
		(sp.close / cer.rate_usd) * cer.rate_cny as close_cny,
		(sp.close / cer.rate_usd) * cer.rate_inr as close_inr,
		(sp.close / cer.rate_usd) * cer.rate_aud as close_aud,
		sp.volume
	from
		stock_price sp
	join
		currency_exchange_rate cer
	on
		sp.date = cer.date
	where
		sp.close > 0
) 
select
	date,
	symbol,
	round(cast(close_usd as numeric), 2) as close_usd,
	round(cast(close_eur as numeric), 2) as close_eur,
	round(cast(close_cny as numeric), 2) as close_cny,
	round(cast(close_inr as numeric), 2) as close_inr,
	round(cast(close_aud as numeric), 2) as close_aud,
	volume
from
	initial_calculations
order by
    date, symbol