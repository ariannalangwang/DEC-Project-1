select
    sp.date,
    sp.symbol,
    sp.close as close_usd,
    sp.close / cer.rate_usd as close_eur,
    (sp.close / cer.rate_usd) * cer.rate_cny as close_cny,
    (sp.close / cer.rate_usd) * cer.rate_inr as close_inr,
    (sp.close / cer.rate_usd) * cer.rate_aud as close_aud,
    volume
from
    stock_price sp
join
    currency_exchange_rate cer
on
    sp.date = cer.date
where
    sp.close > 0
order by
    sp.date, sp.symbol