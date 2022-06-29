{% macro from(table) %}
    {{ source('northwind', table ) }}
{% endmacro %}