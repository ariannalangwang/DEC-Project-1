{% set config = {
    "extract_type": "incremental",
    "incremental_column": "payment_date",
    "source_table_name": "payment"
} %}

select
    payment_id,
    customer_id,
    staff_id,
    rental_id,
    amount,
    payment_date
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
