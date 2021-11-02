{% macro map_list(fn, a_list) %}
  {# A simple map function that applies fn to every element of a_list #}
  {%- set results = [] %}
  {% for item in a_list %}
    {%- do results.append(fn(item)) -%}
  {% endfor %}
  {%- do return(results) -%}
{% endmacro %}
