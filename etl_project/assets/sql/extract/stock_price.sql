{% set config = {
    "extract_type": "incremental",
    "incremental_column": "date",
    "source_table_name": "stock_price"
} %}

select
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}