{% macro optimize_table(relation, source_relation=none, partition_key=none) %}
    {% set queries = elementary.get_optimize_table_queries(relation, source_relation, partition_key) %}
    {% if queries %}
        {% do elementary.file_log("Optimizing table: {}".format(relation)) %}
        {% for query in queries %}
            {% do elementary.run_query(query) %}
        {% endfor %}
        {% do elementary.file_log("Finished optimizing table: {}".format(relation)) %}
    {% endif %}
{% endmacro %}

{% macro get_optimize_table_queries(relation, source_relation, partition_key) %}
    {% do return(adapter.dispatch("get_optimize_table_queries", "elementary")(relation, source_relation, partition_key)) %}
{% endmacro %}

{% macro default__get_optimize_table_queries(relation, source_relation, partition_key) %}
    {% do return(none) %}
{% endmacro %}

{% macro clickhouse__get_optimize_table_queries(relation, source_relation, partition_key) %}
    {% if source_relation and partition_key %}
        {% set get_partitions_query %}
            select distinct {{ partition_key }} from {{ source_relation }}
        {% endset %}
        {% set partitions = run_query(get_partitions_query).columns[0].values() %}

        {% if partitions %}
            {% set queries = [] %}
            {% for partition in partitions %}
                {% set query = "OPTIMIZE TABLE " ~ relation ~ " PARTITION " ~ partition ~ " FINAL" %}
                {% do queries.append(query) %}
            {% endfor %}
            {% do return(queries) %}
        {% endif %}
    {% endif %}

    {% do return(["OPTIMIZE TABLE " ~ relation ~ " FINAL"]) %}
{% endmacro %}
