{% macro edr_join_use_nulls_settings() %}
    {{ return(adapter.dispatch("edr_join_use_nulls_settings", "elementary")()) }}
{% endmacro %}

{% macro default__edr_join_use_nulls_settings() %} {% do return("") %} {% endmacro %}

{# ClickHouse fills unmatched LEFT JOIN columns with type defaults (e.g. 1970-01-01) instead of NULL unless this is enabled, which silently breaks `is null` anti-join checks. #}
{% macro clickhouse__edr_join_use_nulls_settings() %}
    {% do return("settings join_use_nulls = 1") %}
{% endmacro %}
