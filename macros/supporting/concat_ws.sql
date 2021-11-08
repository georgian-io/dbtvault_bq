/* BQ: Introduced this file to add generic support for concatenating columns with a separator */

{% macro concat_ws(columns_list, separator='||') %}
  {# Usage (assuming default implementation):
    >>> concat_ws(['!just_a_string', 'col1', 'col2' ])
    CONCAT_WS("||", "just_a_string", col1 , col2)
   #}

  {{- adapter.dispatch('concat_ws', 'dbtvault_bq')(columns_list, separator) -}}
{% endmacro %}


{% macro default__concat_ws(columns_list, separator) %}
CONCAT_WS('||', {{ dbtvault_bq.map_list(dbtvault.as_constant, columns_list) | join(", ") }})
{% endmacro %}

{% macro bigquery__concat_ws(columns_list, separator) %}
{# Usage:
  >>> bigquery__concat_ws(["id", "!wow", "value"], "&&")
  CONCAT(id, "&&", "wow", "&&", value)
 #}
CONCAT({{ dbtvault_bq.map_list(dbtvault.as_constant, columns_list) | join(', "' ~ separator ~ '", ') }})
{% endmacro %}