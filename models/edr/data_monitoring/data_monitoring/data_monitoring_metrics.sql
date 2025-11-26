{{
  config(
    materialized = 'view'
  )
}}

select *
from {{ ref('data_monitoring_metrics_raw') }} {%- if target.type == 'clickhouse' -%} final {%- endif %}