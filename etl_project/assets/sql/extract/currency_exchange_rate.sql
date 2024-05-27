{% set config = {
    "extract_type": "incremental",
    "incremental_column": "date",
    "source_table_name": "currency_exchange_rate"
} %}

select
    date,
    base,
    rate_usd,
    rate_cny,
    rate_inr,
    rate_aud
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
