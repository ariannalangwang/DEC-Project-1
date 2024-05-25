with sorted_data as (
    select
        payment_year_month,
        cumulative_sales,
        row_number() over (
            partition by payment_year_month
            order by payment_id desc
        ) as rn
    from serving_sales_cumulative
)

select
    payment_year_month,
    cumulative_sales
from sorted_data where rn = 1
