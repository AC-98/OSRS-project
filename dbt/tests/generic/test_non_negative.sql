-- Custom test to check that a column contains only non-negative values
-- Usage: {{ test_non_negative('column_name') }}

{% test non_negative(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL 
  AND {{ column_name }} < 0

{% endtest %}
