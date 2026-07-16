{% macro get_latest_full_refresh(model_node) %}
    {{ return(adapter.dispatch("get_latest_full_refresh", "elementary")(model_node)) }}
{% endmacro %}

{# Metrics only go stale when a successful refresh starts rebuilding the data, so anything computed after that moment (even by the same run) stays valid. #}
{% macro default__get_latest_full_refresh(model_node) %}
    {%- set dbt_run_results_relation = elementary.get_elementary_relation(
        "dbt_run_results"
    ) %}
    {% set query %}
        select coalesce(nullif(execute_started_at, ''), generated_at)
        from {{ dbt_run_results_relation }}
        where
          unique_id = '{{ model_node.unique_id }}' and
          full_refresh = true and
          status = 'success'
        order by generated_at desc
        limit 1
    {% endset %}
    {% do return(elementary.result_value(query)) %}
{% endmacro %}

{% macro fabric__get_latest_full_refresh(model_node) %}
    {%- set dbt_run_results_relation = elementary.get_elementary_relation(
        "dbt_run_results"
    ) %}
    {% set query %}
        select top 1 coalesce(nullif(execute_started_at, ''), generated_at)
        from {{ dbt_run_results_relation }}
        where
          unique_id = '{{ model_node.unique_id }}' and
          full_refresh = cast(1 as bit) and
          status = 'success'
        order by generated_at desc
    {% endset %}
    {% do return(elementary.result_value(query)) %}
{% endmacro %}

{% macro sqlserver__get_latest_full_refresh(model_node) %}
    {% do return(elementary.fabric__get_latest_full_refresh(model_node)) %}
{% endmacro %}
