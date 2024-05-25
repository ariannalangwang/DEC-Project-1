select
    payment_id,
    payment_date,
    concat(extract(year from payment_date), '-', extract(month from payment_date)) as payment_year_month,
    amount,
    sum(amount) over (
        partition by concat(extract(year from payment_date), '-', extract(month from payment_date))
        order by payment_date asc
        rows between unbounded preceding and current row -- (optional) this is optional because this is the default behavior
    ) as cumulative_sales
from payment
order by payment_date asc
