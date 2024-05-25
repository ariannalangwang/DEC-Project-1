select
    date,
    symbol,
    avg(close_usd) over (partition by symbol) as avg_close_usd,
    avg(close_eur) over (partition by symbol) as avg_close_eur,
    avg(close_cny) over (partition by symbol) as avg_close_cny,
    avg(close_inr) over (partition by symbol) as avg_close_inr,
    avg(close_aud) over (partition by symbol) as avg_close_aud,
    sum(volume) over (partition by date, symbol) as total_volume
from
    stock_prices_in_currencies
order by
    date, symbol